use std::time::Duration;

use sequenced_broadcast::{SequencedBroadcast, SequencedReceiver, SequencedRecvError, SequencedSender, SubscribeError};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::state::{
    determinstic_state::DeterministicState,
    recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    shared_state::{SharedState, SharedStateHandle, SharedStateReader},
};

#[derive(Debug)]
pub enum LeaderError {
    SubscribeTimeout,
    BroadcastOffline,
    StateWorkerFailed(WorkerError),
    StateWorkerJoinFailed,
    MissingStateWorker,
}

#[derive(Debug)]
pub enum WorkerError {
    StateMaintainLagged,
}

pub struct AuthorativeState<D: DeterministicState> {
    authority_broadcast: SequencedBroadcast<RecoverableStateAction<D::AuthorityAction>>,
    authority_tx: SequencedSender<RecoverableStateAction<D::AuthorityAction>>,
    state_reader: SharedStateReader<RecoverableState<D>>,
    state_handle: Mutex<SharedStateHandle<RecoverableState<D>>>,
    cancel: CancellationToken,
    state_join: Option<JoinHandle<Result<SharedState<RecoverableState<D>>, WorkerError>>>,
}

impl<D: DeterministicState> AuthorativeState<D> {
    pub async fn new(state: RecoverableState<D>) -> Self {
        let next_seq = state.accept_seq();

        let (authority_broadcast, authority_tx) = Self::new_authority_broadcast(next_seq).await;

        let shared_state = SharedState::new(state);
        let state_reader = shared_state.create_reader();
        let state_handle = state_reader.create_handle();

        let cancel = CancellationToken::new();

        let worker = StateMaintainWorker {
            actions_rx: Self::subscribe_worker(&authority_broadcast, next_seq).await,
            state: shared_state,
            cancel: cancel.clone(),
        };

        AuthorativeState {
            authority_broadcast,
            authority_tx,
            state_reader,
            state_handle: Mutex::new(state_handle),
            cancel,
            state_join: Some(tokio::spawn(worker.run())),
        }
    }

    async fn new_authority_broadcast(
        next_seq: u64,
    ) -> (
        SequencedBroadcast<RecoverableStateAction<D::AuthorityAction>>,
        SequencedSender<RecoverableStateAction<D::AuthorityAction>>,
    ) {
        loop {
            match SequencedBroadcast::new(next_seq, Default::default()) {
                Ok(value) => return value,
                Err(error) => {
                    tracing::error!(?error, "failed to create authority broadcast");
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    async fn subscribe_worker(
        authority_broadcast: &SequencedBroadcast<RecoverableStateAction<D::AuthorityAction>>,
        next_seq: u64,
    ) -> SequencedReceiver<RecoverableStateAction<D::AuthorityAction>> {
        loop {
            match authority_broadcast.subscribe_from(next_seq).await {
                Ok(receiver) => return receiver,
                Err(error) => {
                    tracing::error!(?error, "failed to start state worker subscriber");
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    pub async fn state_clone(&self) -> RecoverableState<D> {
        let mut handle = self.state_handle.lock().await;

        let cloned = handle.read().clone();
        handle.quiescent();

        cloned
    }

    pub async fn recoverable_state_details(&self) -> RecoverableStateDetails {
        let mut handle = self.state_handle.lock().await;

        let details = handle.read().details().clone();
        handle.quiescent();

        details
    }

    pub async fn subscribe(
        &self,
    ) -> Result<(RecoverableState<D>, SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>), LeaderError> {
        tokio::time::timeout(Duration::from_millis(50), async {
            loop {
                let mut handle = self.state_handle.lock().await;

                let state_borrow = handle.read();

                let Ok(sub) = self.authority_broadcast.subscribe_from(state_borrow.accept_seq()).await else {
                    handle.quiescent();
                    drop(handle);

                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                };

                let state = state_borrow.clone();
                handle.quiescent();

                return (state, sub);
            }
        })
        .await
        .map_err(|_| LeaderError::SubscribeTimeout)
    }

    pub async fn subscribe_at(
        &self,
        seq: u64,
    ) -> Result<SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>, SubscribeError> {
        self.authority_broadcast.subscribe_from(seq).await
    }

    pub fn state_reader(&self) -> SharedStateReader<RecoverableState<D>> {
        self.state_reader.clone()
    }

    pub fn create_state_handle(&self) -> SharedStateHandle<RecoverableState<D>> {
        self.state_reader.create_handle()
    }

    pub async fn reset(&mut self, state: RecoverableState<D>) -> Result<(), LeaderError> {
        self.authority_tx.close();

        let Some(join_handle) = self.state_join.take() else {
            return Err(LeaderError::MissingStateWorker);
        };

        let mut shared_state = join_handle
            .await
            .map_err(|_| LeaderError::StateWorkerJoinFailed)?
            .map_err(LeaderError::StateWorkerFailed)?;

        let next_seq = state.accept_seq();
        shared_state.reset(state);

        let (authority_broadcast, authority_tx) = Self::new_authority_broadcast(next_seq).await;

        let worker = StateMaintainWorker {
            actions_rx: Self::subscribe_worker(&authority_broadcast, next_seq).await,
            state: shared_state,
            cancel: self.cancel.clone(),
        };

        self.authority_broadcast = authority_broadcast;
        self.authority_tx = authority_tx;
        self.state_join = Some(tokio::spawn(worker.run()));
        Ok(())
    }

    pub async fn apply_authority(
        &mut self,
        authority: RecoverableStateAction<D::AuthorityAction>,
    ) -> Result<(), LeaderError> {
        self.authority_tx
            .send(authority)
            .await
            .map(|_| ())
            .map_err(|_| LeaderError::BroadcastOffline)
    }
}

struct StateMaintainWorker<D: DeterministicState> {
    actions_rx: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    state: SharedState<RecoverableState<D>>,
    cancel: CancellationToken,
}

impl<D: DeterministicState> StateMaintainWorker<D> {
    async fn run(self) -> Result<SharedState<RecoverableState<D>>, WorkerError> {
        let StateMaintainWorker {
            mut actions_rx,
            mut state,
            cancel,
        } = self;

        loop {
            state.maintain_state();

            let (seq, action) = tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    continue;
                }
                action_res = actions_rx.recv() => {
                    match action_res {
                        Ok(v) => v,
                        Err(error) => {
                            match error {
                                SequencedRecvError::Closed => {
                                    tracing::info!("Authority action broadcast closed");
                                    break;
                                }
                                SequencedRecvError::Lagged { .. } => {
                                    tracing::error!("state maintain worker lagged, giving up");
                                    return Err(WorkerError::StateMaintainLagged);
                                }
                            }
                        },
                    }
                }
            };

            state.queue_updates(std::iter::once((seq, action)));

            let mut remaining = 512u32;
            state.queue_updates(std::iter::from_fn(|| {
                remaining = remaining.saturating_sub(1);
                if remaining == 0 {
                    return None;
                }
                actions_rx.try_recv().ok()
            }));
        }

        Ok(state)
    }
}
