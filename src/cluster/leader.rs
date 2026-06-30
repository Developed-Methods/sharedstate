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
pub enum LeaderStateError {
    SubscribeFailed,
    BroadcastOffline,
}

impl std::fmt::Display for LeaderStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SubscribeFailed => write!(f, "subscribe failed after 64 retries"),
            Self::BroadcastOffline => write!(f, "authority broadcast is offline"),
        }
    }
}

pub struct LeaderState<D: DeterministicState> {
    authority_broadcast: SequencedBroadcast<RecoverableStateAction<D::AuthorityAction>>,
    authority_tx: SequencedSender<RecoverableStateAction<D::AuthorityAction>>,
    state_reader: SharedStateReader<RecoverableState<D>>,
    state_handle: Mutex<SharedStateHandle<RecoverableState<D>>>,
    cancel: CancellationToken,
    state_join: Option<JoinHandle<SharedState<RecoverableState<D>>>>,
}

impl<D: DeterministicState> LeaderState<D> {
    pub async fn new(state: RecoverableState<D>) -> Self {
        let next_seq = state.accept_seq();

        let (authority_broadcast, authority_tx) = SequencedBroadcast::new(next_seq, Default::default()).unwrap();

        let shared_state = SharedState::new(state);
        let state_reader = shared_state.create_reader();
        let state_handle = state_reader.create_handle();

        let cancel = CancellationToken::new();

        let worker = StateMaintainWorker {
            actions_rx: authority_broadcast
                .subscribe_from(next_seq)
                .await
                .expect("failed to start first subscriber"),
            state: shared_state,
            cancel: cancel.clone(),
        };

        LeaderState {
            authority_broadcast,
            authority_tx,
            state_reader,
            state_handle: Mutex::new(state_handle),
            cancel,
            state_join: Some(tokio::spawn(worker.run())),
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
    ) -> Result<(RecoverableState<D>, SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>), LeaderStateError>
    {
        for _ in 0..64 {
            let mut handle = self.state_handle.lock().await;
            let seq = handle.read().accept_seq();

            match self.authority_broadcast.subscribe_from(seq).await {
                Ok(sub) => {
                    let state = handle.read().clone();
                    handle.quiescent();
                    return Ok((state, sub));
                }
                Err(_) => {
                    handle.quiescent();
                    drop(handle);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }

        Err(LeaderStateError::SubscribeFailed)
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

    pub async fn apply_authority(
        &mut self,
        authority: RecoverableStateAction<D::AuthorityAction>,
    ) -> Result<(), LeaderStateError> {
        self.authority_tx
            .send(authority)
            .await
            .map(|_| ())
            .map_err(|_| LeaderStateError::BroadcastOffline)
    }

    pub async fn reset(&mut self, state: RecoverableState<D>) {
        self.authority_tx.close();

        let join_handle = self.state_join.take().expect("state worker handle is missing");
        let mut shared_state = join_handle.await.expect("state worker got error, cannot receive state");

        let next_seq = state.accept_seq();
        shared_state.reset(state);

        let (authority_broadcast, authority_tx) = SequencedBroadcast::new(next_seq, Default::default()).unwrap();

        let worker = StateMaintainWorker {
            actions_rx: authority_broadcast
                .subscribe_from(next_seq)
                .await
                .expect("failed to start first subscriber"),
            state: shared_state,
            cancel: self.cancel.clone(),
        };

        self.authority_broadcast = authority_broadcast;
        self.authority_tx = authority_tx;
        self.state_join = Some(tokio::spawn(worker.run()));
    }
}

struct StateMaintainWorker<D: DeterministicState> {
    actions_rx: SequencedReceiver<RecoverableStateAction<D::AuthorityAction>>,
    state: SharedState<RecoverableState<D>>,
    cancel: CancellationToken,
}

impl<D: DeterministicState> StateMaintainWorker<D> {
    async fn run(self) -> SharedState<RecoverableState<D>> {
        let StateMaintainWorker {
            mut actions_rx,
            mut state,
            cancel,
        } = self;

        loop {
            state.maintain_state();

            let (seq, action) = tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(50)) => continue,
                action_res = actions_rx.recv() => match action_res {
                    Ok(v) => v,
                    Err(SequencedRecvError::Closed) => {
                        tracing::info!("authority action broadcast closed, state maintainer exiting");
                        break;
                    }
                    Err(error) => {
                        tracing::error!(
                            "state maintainer is consuming the authority feed too slowly — \
                             actions may be lost; this node should be demoted: {:?}",
                            error
                        );
                        break;
                    }
                },
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

        state
    }
}
