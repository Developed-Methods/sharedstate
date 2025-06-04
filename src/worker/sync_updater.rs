use std::{hash::{Hash, Hasher, SipHasher}, time::{SystemTime, UNIX_EPOCH}};

use sequenced_broadcast::{SequencedBroadcast, SequencedBroadcastSettings, SequencedReceiver, SequencedSender};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::sync::CancellationToken;

use crate::{recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails, SourceId}, state::{DeterministicState, FlushedUpdater, SharedState}, utils::PanicHelper};

use super::{follow_worker::{FollowWorker, SequencedRxAndUpdater}, lead_worker::LeadWorker, task_and_cancel::TaskAndCancel};

pub struct SyncUpdater<I: SourceId, D: DeterministicState> {
    local_id: I,

    pub(super) local_action_tx: Sender<D::Action>,
    pub(super) action_tx: Sender<(I, D::Action)>,
    pub(super) state: SharedState<RecoverableState<I, D>>,

    mode: Mode<I, D>,

    broadcast_settings: SequencedBroadcastSettings,
    broadcast: SequencedBroadcast<RecoverableStateAction<I, D::AuthorityAction>>,
}

enum Mode<I: SourceId, D: DeterministicState> {
    Lead(LeadMode<I, D>),
    Offline(OfflineMode<I, D>),
    Follow(FollowMode<I, D>),
    Dead,
}

impl<I: SourceId, D: DeterministicState> SyncUpdater<I, D> where D::AuthorityAction: Clone {
    pub fn new(local_id: I, state: D, broadcast_settings: SequencedBroadcastSettings) -> Self {
        let (state, updater) = SharedState::new(RecoverableState::new(rnd_id(local_id), state));

        let (local_action_tx, mut local_action_rx) = channel(2048);
        let (action_tx, action_rx) = channel(2048);
        let (lead_tx, lead_rx) = channel(2048);

        let (worker, authority_rx) = LeadWorker::new(lead_rx, updater);
        let (broadcast, broadcast_tx) = SequencedBroadcast::new(authority_rx.next_seq(), broadcast_settings.clone());

        assert_eq!(authority_rx.next_seq(), broadcast_tx.seq());

        let mode = Mode::Lead(LeadMode {
            local_id,
            action_tx_task: ActionToLocalLead {
                action_rx,
                lead_tx,
            }.spawn(),
            updater_task: worker.spawn(),
            authority_to_broadcast_task: AuthorityToBroadcast { authority_rx, broadcast_tx }.spawn(),
        });

        {
            let action_tx = action_tx.clone();
            tokio::spawn(async move {
                while let Some(action) = local_action_rx.recv().await {
                    tokio::task::yield_now().await;
                    if action_tx.send((local_id, action)).await.is_err() {
                        break;
                    }
                }
            });
        }

        SyncUpdater {
            local_id,
            action_tx,
            local_action_tx,
            state,
            mode,
            broadcast_settings,
            broadcast,
        }
    }

    pub fn action_tx(&self) -> Sender<D::Action> {
        self.local_action_tx.clone()
    }

    pub fn addressed_action_tx(&self) -> Sender<(I, D::Action)> {
        self.action_tx.clone()
    }

    pub fn state(&self) -> SharedState<RecoverableState<I, D>> {
        self.state.clone()
    }

    pub fn provide_action_rx(&mut self, mut action_rx: Receiver<(I, D::Action)>) {
        let action_tx = self.action_tx.clone();
        let local_id = self.local_id;

        tokio::spawn(async move {
            while let Some((source, action)) = action_rx.recv().await {
                /* prevent loops */
                if source == local_id {
                    continue;
                }

                if action_tx.send((source, action)).await.is_err() {
                    break;
                }

                tokio::task::yield_now().await;
            }
        });
    }

    pub async fn go_offline(&mut self) -> &OfflineMode<I, D> {
        self.mode = match std::mem::replace(&mut self.mode, Mode::Dead) {
            Mode::Dead => unreachable!("dead state"),
            Mode::Lead(lead) => Mode::Offline(lead.into_offline_mode().await),
            Mode::Offline(offline) => Mode::Offline(offline),
            Mode::Follow(follow) => Mode::Offline(follow.into_offline_mode().await),
        };

        match &self.mode {
            Mode::Offline(offline) => offline,
            _ => unreachable!(),
        }
    }

    pub async fn lead(&mut self) {
        self.mode = match std::mem::replace(&mut self.mode, Mode::Dead) {
            Mode::Dead => unreachable!("dead state"),
            Mode::Lead(lead) => Mode::Lead(lead),
            Mode::Offline(offline) => Mode::Lead(offline.into_lead()),
            Mode::Follow(follow) => Mode::Lead(follow.into_offline_mode().await.into_lead()),
        };
    }

    pub async fn follow(&mut self, action_tx: Sender<(I, D::Action)>, state: ValidNewFollowState<RecoverableState<I, D>>) {
        let offline = match std::mem::replace(&mut self.mode, Mode::Dead) {
            Mode::Dead => unreachable!("dead state"),
            Mode::Offline(offline) => offline,
            Mode::Lead(lead) => lead.into_offline_mode().await,
            Mode::Follow(follow) => follow.into_offline_mode().await,
        };

        let (state, broadcast) = state.with_new_broadcast(self.broadcast_settings.clone());
        std::mem::replace(&mut self.broadcast, broadcast).shutdown();

        self.mode = Mode::Follow(offline.into_follow(action_tx, state));
    }

    pub async fn try_follow(&mut self, action_tx: Sender<(I, D::Action)>, target: FollowTarget<I, D>) -> bool {
        let offline = match std::mem::replace(&mut self.mode, Mode::Dead) {
            Mode::Dead => unreachable!("dead state"),
            Mode::Offline(offline) => offline,
            Mode::Lead(lead) => lead.into_offline_mode().await,
            Mode::Follow(follow) => follow.into_offline_mode().await,
        };

        if let Some(leader_check) = target.leader_state_check {
            let can_follow = offline.with_state(|state| {
                leader_check.can_recover_follower(state.details())
            });

            if !can_follow {
                self.mode = Mode::Offline(offline);
                return false;
            }
        }

        self.mode = match offline.try_follow(action_tx, target.authority_rx) {
            Ok(follow) => Mode::Follow(follow),
            Err(offline) => Mode::Offline(offline),
        };

        self.is_following()
    }

    pub async fn add_subscriber(&mut self, recovery_details: RecoverableStateDetails) -> Option<FollowTarget<I, D>> {
        if self.is_offline() {
            return None;
        }

        let leader_recovery_details = {
            let read = self.state.read();
            let details = read.details();
            if !details.can_recover_follower(&recovery_details) {
                return None;
            }
            details.clone()
        };

        let recv = self.broadcast.add_client(recovery_details.sequence, true).await.ok()?;

        Some(FollowTarget {
            authority_rx: recv,
            leader_state_check: Some(leader_recovery_details),
        })
    }

    pub async fn add_fresh_subscriber(&mut self) -> Option<ValidNewFollowState<RecoverableState<I, D>>> {
        if self.is_offline() {
            return None;
        }

        let state = {
            let read = self.state.read();
            read.clone()
        };

        let sub = self.broadcast.add_client(state.sequence(), true).await.ok()?;
        Some(ValidNewFollowState(NewFollowState {
            state,
            authority_rx: sub
        }))
    }

    pub fn is_leading(&self) -> bool {
        matches!(self.mode, Mode::Lead(_))
    }

    pub fn is_offline(&self) -> bool {
        matches!(self.mode, Mode::Offline(_))
    }

    pub fn is_following(&self) -> bool {
        matches!(self.mode, Mode::Follow(_))
    }

    pub fn address(&self) -> &I {
        &self.local_id
    }

    pub async fn clone_state(&self) -> RecoverableState<I, D> {
        self.state.read().clone()
    }

    pub async fn state_recovery_details(&self) -> RecoverableStateDetails {
        self.state.read().details().clone()
    }
}

pub struct FollowTarget<I: SourceId, D: DeterministicState> {
    pub authority_rx: SequencedReceiver<RecoverableStateAction<I, D::AuthorityAction>>,
    pub leader_state_check: Option<RecoverableStateDetails>,
}

struct LeadMode<I: SourceId, D: DeterministicState> {
    local_id: I,
    action_tx_task: TaskAndCancel<ActionToLocalLead<I, D>>,
    updater_task: TaskAndCancel<LeadWorker<RecoverableState<I, D>>>,
    authority_to_broadcast_task: TaskAndCancel<AuthorityToBroadcast<RecoverableState<I, D>>>,
}

impl<I: SourceId, D: DeterministicState> LeadMode<I, D> {
    async fn into_offline_mode(self) -> OfflineMode<I, D> {
        let action_rx = self.action_tx_task.cancel().await.panic("action to leader failed").action_rx;
        let flushed_updater = self.updater_task.cancel().await.panic("updater task failed").into_flushed();
        let authority_tx = self.authority_to_broadcast_task.cancel().await.panic("authority broadcast failed").broadcast_tx;

        assert_eq!(flushed_updater.next_sequence(), authority_tx.seq());

        OfflineMode {
            local_id: self.local_id,
            action_rx,
            flushed_updater,
            broadcast_tx: authority_tx,
            was_leading: true,
        }
    }
}

pub struct OfflineMode<I: SourceId, D: DeterministicState> {
    local_id: I,
    action_rx: Receiver<(I, D::Action)>,
    flushed_updater: FlushedUpdater<RecoverableState<I, D>>,
    broadcast_tx: SequencedSender<RecoverableStateAction<I, D::AuthorityAction>>,
    was_leading: bool,
}

impl<I: SourceId, D: DeterministicState> OfflineMode<I, D> where D::AuthorityAction: Clone {
    pub fn recovery_details(&self) -> RecoverableStateDetails {
        self.with_state(|s| s.details().clone())
    }

    pub fn next_sequence(&self) -> u64 {
        self.flushed_updater.next_sequence()
    }

    pub fn with_state<R, F: FnOnce(&RecoverableState<I, D>) -> R>(&self, update: F) -> R {
        self.flushed_updater.view_state(update)
    }

    fn into_lead(self) -> LeadMode<I, D> {
        let (lead_tx, lead_rx) = channel(2048);
        let (worker, authority_rx) = LeadWorker::new(lead_rx, self.flushed_updater);

        if !self.was_leading {
            lead_tx.try_send(RecoverableStateAction::BumpGeneration {
                new_id: rnd_id(self.local_id),
            }).unwrap();
        }

        LeadMode {
            local_id: self.local_id,
            action_tx_task: ActionToLocalLead { action_rx: self.action_rx, lead_tx }.spawn(),
            updater_task: worker.spawn(),
            authority_to_broadcast_task: AuthorityToBroadcast { authority_rx, broadcast_tx: self.broadcast_tx }.spawn(),
        }
    }

    fn into_follow(mut self, action_tx: Sender<(I, D::Action)>, state: ValidNewFollowStateWithBroadcast<RecoverableState<I, D>>) -> FollowMode<I, D> {
        self.flushed_updater.reset_state(state.0.state);

        let pair = SequencedRxAndUpdater {
            rx: state.0.authority_rx,
            updater: self.flushed_updater,
        }.try_into_valid().panic("state sequences do not match after reset");

        let (worker, authority_rx) = FollowWorker::new(pair);

        FollowMode {
            local_id: self.local_id,
            action_task: ActionPipe { action_rx: self.action_rx, action_tx }.spawn(),
            updater_task: worker.spawn(),
            broadcast_task: AuthorityToBroadcast { authority_rx, broadcast_tx: state.1 }.spawn(),
        }
    }

    fn try_follow(mut self, action_tx: Sender<(I, D::Action)>, authority_rx: SequencedReceiver<RecoverableStateAction<I, D::AuthorityAction>>) -> Result<FollowMode<I, D>, Self> {
        let pair = match (SequencedRxAndUpdater {
            rx: authority_rx,
            updater: self.flushed_updater,
        }).try_into_valid() {
            Ok(v) => v,
            Err(pair) => {
                self.flushed_updater = pair.updater;
                return Err(self);
            }
        };

        let (worker, authority_rx) = FollowWorker::new(pair);

        Ok(FollowMode {
            local_id: self.local_id,
            action_task: ActionPipe { action_rx: self.action_rx, action_tx }.spawn(),
            updater_task: worker.spawn(),
            broadcast_task: AuthorityToBroadcast { authority_rx, broadcast_tx: self.broadcast_tx }.spawn(),
        })
    }
}

pub struct NewFollowState<D: DeterministicState> {
    pub state: D,
    pub authority_rx: SequencedReceiver<D::AuthorityAction>,
}

impl<D: DeterministicState> NewFollowState<D> {
    pub fn try_into_valid(self) -> Result<ValidNewFollowState<D>, NewFollowState<D>> {
        if self.state.sequence() != self.authority_rx.next_seq() {
            return Err(self);
        }


        Ok(ValidNewFollowState(self))
    }
}

pub struct ValidNewFollowState<D: DeterministicState>(NewFollowState<D>);
pub struct ValidNewFollowStateWithBroadcast<D: DeterministicState>(NewFollowState<D>, SequencedSender<D::AuthorityAction>);

impl<D: DeterministicState> ValidNewFollowState<D> where D::AuthorityAction: Clone {
    pub fn into_inner(self) -> NewFollowState<D> {
        self.0
    }

    fn with_new_broadcast(self, settings: SequencedBroadcastSettings) -> (ValidNewFollowStateWithBroadcast<D>, SequencedBroadcast<D::AuthorityAction>) {
        let (broadcast, tx) = SequencedBroadcast::new(self.0.state.sequence(), settings);

        (
            ValidNewFollowStateWithBroadcast(self.0, tx),
            broadcast
        )
    }
}

struct FollowMode<I: SourceId, D: DeterministicState> {
    local_id: I,
    action_task: TaskAndCancel<ActionPipe<I, D>>,
    updater_task: TaskAndCancel<FollowWorker<RecoverableState<I, D>>>,
    broadcast_task: TaskAndCancel<AuthorityToBroadcast<RecoverableState<I, D>>>,
}

impl<I: SourceId, D: DeterministicState> FollowMode<I, D> where D::AuthorityAction: Clone {
    pub async fn into_offline_mode(self) -> OfflineMode<I, D> {
        let action_rx = self.action_task.cancel().await.panic("action to leader failed").action_rx;
        let flushed_updater = self.updater_task.cancel().await.panic("updater task failed").into_flushed();
        let authority_tx = self.broadcast_task.cancel().await.panic("authority broadcast failed").broadcast_tx;

        assert_eq!(flushed_updater.next_sequence(), authority_tx.seq());

        OfflineMode {
            local_id: self.local_id,
            action_rx,
            flushed_updater,
            broadcast_tx: authority_tx,
            was_leading: false,
        }
    }
}

struct AuthorityToBroadcast<D: DeterministicState> {
    authority_rx: SequencedReceiver<D::AuthorityAction>,
    broadcast_tx: SequencedSender<D::AuthorityAction>,
}

impl<D: DeterministicState> AuthorityToBroadcast<D> where D::AuthorityAction: Clone {
    fn spawn(mut self) -> TaskAndCancel<Self> {
        TaskAndCancel::spawn(|cancel| async move {
            self.run(cancel).await;
            self
        })
    }

    async fn run(&mut self, cancel: CancellationToken) {
        while let Some(Some((seq, authority))) = cancel.run_until_cancelled(self.authority_rx.recv()).await {
            self.broadcast_tx.safe_send(seq, authority).await.panic("invalid sequence");
        }

        /* flush remaining messages to client */
        while let Ok((seq, authority)) = self.authority_rx.try_recv() {
            self.broadcast_tx.safe_send(seq, authority).await.panic("invalid sequence");
        }
    }
}

struct ActionPipe<I: SourceId, D: DeterministicState> {
    action_rx: Receiver<(I, D::Action)>,
    action_tx: Sender<(I, D::Action)>,
}

impl<I: SourceId, D: DeterministicState> ActionPipe<I, D> {
    fn spawn(mut self) -> TaskAndCancel<Self> {
        TaskAndCancel::spawn(|cancel| async move {
            self.run(cancel).await;
            self
        })
    }

    async fn run(&mut self, cancel: CancellationToken) {
        cancel.run_until_cancelled_owned(async {
            while let Some(action) = self.action_rx.recv().await {
                if self.action_tx.send(action).await.is_err() {
                    break;
                }
            }
        }).await;
    }
}

struct ActionToLocalLead<I: SourceId, D: DeterministicState> {
    action_rx: Receiver<(I, D::Action)>,
    lead_tx: Sender<RecoverableStateAction<I, D::Action>>,
}

impl<I: SourceId, D: DeterministicState> ActionToLocalLead<I, D> {
    fn spawn(mut self) -> TaskAndCancel<Self> {
        TaskAndCancel::spawn(|cancel| async move {
            self.run(cancel).await;
            self
        })
    }

    async fn run(&mut self, cancel: CancellationToken) {
        cancel.run_until_cancelled_owned(async {
            while let Some((source, action)) = self.action_rx.recv().await {
                self.lead_tx.send(RecoverableStateAction::StateAction { source, action }).await.panic("failed to send action to local lead");
            }
        }).await;
    }
}

fn rnd_id<S: SourceId>(local_id: S) -> u64 {
    let mut hash = SipHasher::new();
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().hash(&mut hash);
    local_id.hash(&mut hash);
    hash.finish()
}

#[cfg(test)]
mod test {
    use super::SyncUpdater;
    use std::time::Duration;
    use tokio::sync::mpsc::channel;
    use crate::{state::DeterministicState, testing::{setup_logging, state_tests::{TestState, TestStateAction}}, utils::PanicHelper, worker::sync_updater::FollowTarget};

    #[tokio::test]
    async fn sync_updater_simple_leader_test() {
        setup_logging();

        let a = SyncUpdater::<u64, TestState>::new(1, Default::default(), Default::default());
        a.local_action_tx.send(TestStateAction::Set { slot: 0, value: 33 }).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(a.state.read().state().numbers[0], 33);
    }

    #[tokio::test]
    async fn sync_updater_recover_state_fork_same_seq_reject_test() {
        let mut a = SyncUpdater::<u64, TestState>::new(1, Default::default(), Default::default());
        let mut b = SyncUpdater::<u64, TestState>::new(2, Default::default(), Default::default());
        let mut c = SyncUpdater::<u64, TestState>::new(3, Default::default(), Default::default());
        let mut d = SyncUpdater::<u64, TestState>::new(4, Default::default(), Default::default());

        /* all follow A */
        {
            let follow = a.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            b.follow(action_tx, follow).await;
            a.provide_action_rx(action_rx);
        }

        {
            let follow = a.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            c.follow(action_tx, follow).await;
            a.provide_action_rx(action_rx);
        }

        /* D follows C */
        {
            let follow = c.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            d.follow(action_tx, follow).await;
            c.provide_action_rx(action_rx);
        }

        a.local_action_tx.send(TestStateAction::Set { slot: 1, value: 88 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Set { slot: 2, value: 22 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Set { slot: 3, value: 11 }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(a.state().read().sequence(), 4);
        assert_eq!(b.state().read().sequence(), 4);
        assert_eq!(c.state().read().sequence(), 4);
        assert_eq!(d.state().read().sequence(), 4);

        /* FORK: C lead followed by D */
        c.lead().await;
        /* FORK: B lead followed by A */
        b.lead().await;
        {
            let a_recover = a.go_offline().await.recovery_details();
            let follow = b.add_subscriber(a_recover).await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            a.try_follow(action_tx, follow).await.panic("failed to follow");
            b.provide_action_rx(action_rx);
        }


        a.local_action_tx.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Add { slot: 2, value: 100 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Add { slot: 3, value: 100 }).await.unwrap();

        d.local_action_tx.send(TestStateAction::Add { slot: 1, value: 1000 }).await.unwrap();
        d.local_action_tx.send(TestStateAction::Add { slot: 2, value: 1000 }).await.unwrap();
        d.local_action_tx.send(TestStateAction::Add { slot: 3, value: 1000 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(a.state().read().sequence(), 8);
        assert_eq!(b.state().read().sequence(), 8);
        assert_eq!(c.state().read().sequence(), 8);
        assert_eq!(d.state().read().sequence(), 8);

        /* a cannot recover to fork'd state */
        {
            let a_recover = a.go_offline().await.recovery_details();
            let follow = d.add_subscriber(a_recover).await;
            assert!(follow.is_none());
        }

        /* b cannot recover to fork'd state */
        {
            let b_recover = b.go_offline().await.recovery_details();
            let follow = d.add_subscriber(b_recover).await;
            assert!(follow.is_none());
        }
    }

    #[tokio::test]
    async fn sync_updater_recover_state_fork_reject_test() {
        let mut a = SyncUpdater::<u64, TestState>::new(1, Default::default(), Default::default());
        let mut b = SyncUpdater::<u64, TestState>::new(2, Default::default(), Default::default());
        let mut c = SyncUpdater::<u64, TestState>::new(3, Default::default(), Default::default());
        let mut d = SyncUpdater::<u64, TestState>::new(4, Default::default(), Default::default());

        /* all follow A */
        {
            let follow = a.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            b.follow(action_tx, follow).await;
            a.provide_action_rx(action_rx);
        }

        {
            let follow = a.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            c.follow(action_tx, follow).await;
            a.provide_action_rx(action_rx);
        }

        /* D follows C */
        {
            let follow = c.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            d.follow(action_tx, follow).await;
            c.provide_action_rx(action_rx);
        }

        a.local_action_tx.send(TestStateAction::Set { slot: 1, value: 88 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Set { slot: 2, value: 22 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Set { slot: 3, value: 11 }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(a.state().read().sequence(), 4);
        assert_eq!(b.state().read().sequence(), 4);
        assert_eq!(c.state().read().sequence(), 4);
        assert_eq!(d.state().read().sequence(), 4);

        /* FORK: c lead and d follow c */
        c.lead().await;

        a.local_action_tx.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Add { slot: 2, value: 100 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Add { slot: 3, value: 100 }).await.unwrap();

        d.local_action_tx.send(TestStateAction::Add { slot: 1, value: 1000 }).await.unwrap();
        d.local_action_tx.send(TestStateAction::Add { slot: 2, value: 1000 }).await.unwrap();
        d.local_action_tx.send(TestStateAction::Add { slot: 3, value: 1000 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(a.state().read().sequence(), 7);
        assert_eq!(b.state().read().sequence(), 7);
        assert_eq!(c.state().read().sequence(), 8);
        assert_eq!(d.state().read().sequence(), 8);

        /* a cannot recover to fork'd state */
        {
            let a_recover = a.go_offline().await.recovery_details();
            let follow = d.add_subscriber(a_recover).await;
            assert!(follow.is_none());
        }

        /* b cannot recover to fork'd state */
        {
            let b_recover = b.go_offline().await.recovery_details();
            let follow = d.add_subscriber(b_recover).await;
            assert!(follow.is_none());
        }
    }

    #[tokio::test]
    async fn sync_updater_recover_state_test() {
        let mut a = SyncUpdater::<u64, TestState>::new(1, Default::default(), Default::default());
        let mut b = SyncUpdater::<u64, TestState>::new(2, Default::default(), Default::default());
        let mut c = SyncUpdater::<u64, TestState>::new(3, Default::default(), Default::default());
        let mut d = SyncUpdater::<u64, TestState>::new(4, Default::default(), Default::default());

        {
            let follow = a.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            b.follow(action_tx, follow).await;
            a.provide_action_rx(action_rx);
        }

        {
            let follow = a.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            c.follow(action_tx, follow).await;
            a.provide_action_rx(action_rx);
        }

        {
            let follow = a.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            d.follow(action_tx, follow).await;
            a.provide_action_rx(action_rx);
        }

        a.local_action_tx.send(TestStateAction::Set { slot: 1, value: 88 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Set { slot: 2, value: 22 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Set { slot: 3, value: 11 }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(a.state().read().sequence(), 4);
        assert_eq!(b.state().read().sequence(), 4);
        assert_eq!(c.state().read().sequence(), 4);
        assert_eq!(d.state().read().sequence(), 4);

        /* d goes offline so we can recover later */
        d.go_offline().await;


        /* promote B and have A follow */
        b.lead().await;
        {
            let a_recover = a.go_offline().await.recovery_details();
            let follow = b.add_subscriber(a_recover).await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            a.try_follow(action_tx, follow).await.panic("failed to follow");
            b.provide_action_rx(action_rx);
        }

        a.local_action_tx.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Add { slot: 2, value: 100 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Add { slot: 3, value: 100 }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        /* promote C and have B follow */
        c.lead().await;
        {
            let b_recover = b.go_offline().await.recovery_details();
            let follow = c.add_subscriber(b_recover).await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            b.try_follow(action_tx, follow).await.panic("failed to follow");
            c.provide_action_rx(action_rx);
        }

        a.local_action_tx.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Add { slot: 2, value: 100 }).await.unwrap();
        a.local_action_tx.send(TestStateAction::Add { slot: 3, value: 100 }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(a.state().read().sequence(), 12);
        assert_eq!(b.state().read().sequence(), 12);
        assert_eq!(c.state().read().sequence(), 12);
        assert_eq!(d.state().read().sequence(), 4);

        /* have D recover from A */
        {
            let d_recover = d.go_offline().await.recovery_details();
            let follow = a.add_subscriber(d_recover).await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            d.try_follow(action_tx, follow).await.panic("failed to follow after mutli generation changes");
            a.provide_action_rx(action_rx);
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(a.state().read().sequence(), 12);
        assert_eq!(b.state().read().sequence(), 12);
        assert_eq!(c.state().read().sequence(), 12);
        assert_eq!(d.state().read().sequence(), 12);

        let a_state = a.state.read().clone();
        let b_state = b.state.read().clone();
        let c_state = c.state.read().clone();
        let d_state = d.state.read().clone();

        assert_eq!(a_state, b_state);
        assert_eq!(a_state, c_state);
        assert_eq!(a_state, d_state);
        assert_eq!(a_state.state().numbers[1], 288);
        assert_eq!(a_state.state().numbers[2], 222);
        assert_eq!(a_state.state().numbers[3], 211);
    }

    #[tokio::test]
    async fn sync_updater_simple_follower_test() {
        setup_logging();

        let mut a = SyncUpdater::<u64, TestState>::new(1, Default::default(), Default::default());
        let mut b = SyncUpdater::<u64, TestState>::new(2, Default::default(), Default::default());
        let mut c = SyncUpdater::<u64, TestState>::new(3, Default::default(), Default::default());

        {
            let follow = a.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            b.follow(action_tx, follow).await;
            a.provide_action_rx(action_rx);
        }

        {
            let follow = a.add_fresh_subscriber().await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            c.follow(action_tx, follow).await;
            a.provide_action_rx(action_rx);
        }

        a.local_action_tx.send(TestStateAction::Set { slot: 1, value: 88 }).await.unwrap();
        b.local_action_tx.send(TestStateAction::Set { slot: 2, value: 22 }).await.unwrap();
        c.local_action_tx.send(TestStateAction::Set { slot: 3, value: 11 }).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let a_state = a.state.read().clone();
        let b_state = b.state.read().clone();
        let c_state = c.state.read().clone();

        assert_eq!(a_state, b_state);
        assert_eq!(a_state, c_state);
        assert_eq!(a_state.state().numbers[1], 88);
        assert_eq!(a_state.state().numbers[2], 22);
        assert_eq!(a_state.state().numbers[3], 11);

        /* circular */
        {
            let recover_details = a.go_offline().await.recovery_details();
            let follow = b.add_subscriber(recover_details).await.unwrap();

            let (action_tx, action_rx) = channel(1024);
            a.try_follow(action_tx, follow).await.panic("failed to follow");

            b.provide_action_rx(action_rx);
        }

        /* would cause loops */
        a.local_action_tx.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();
        b.local_action_tx.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let a_state = a.state.read().clone();
        let b_state = b.state.read().clone();
        assert_eq!(a_state, b_state);
        assert_eq!(a_state.state().numbers[1], 88);
        assert_eq!(a_state.state().numbers[2], 22);
        assert_eq!(a_state.state().numbers[3], 11);

        b.lead().await;
        a.local_action_tx.send(TestStateAction::Add { slot: 1, value: 100 }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let a_state = a.state.read().clone();
        let b_state = b.state.read().clone();
        let c_state = c.state.read().clone();

        assert_eq!(a_state, b_state);
        assert_eq!(a_state, c_state);
        assert_eq!(a_state.state().numbers[1], 188);
        assert_eq!(a_state.state().numbers[2], 22);
        assert_eq!(a_state.state().numbers[3], 11);

        println!("STATE: {:?}", a_state);
    }
}

