use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::{
    protocol::messages::{LeaderInfoMessage, SyncRequest},
    state::{determinstic_state::DeterministicState, recoverable_state::RecoverableStateAction},
    transport::traits::SyncIOAddress,
};

use super::node::Inner;

#[derive(Debug)]
pub enum SendActionError {
    Closed,
    NoLeader,
}

#[derive(Clone)]
pub struct NodeActionSender<Action, A: SyncIOAddress> {
    pub(crate) tx: mpsc::Sender<Action>,
    pub(crate) leader: watch::Receiver<LeaderInfoMessage<A>>,
}

impl<Action, A: SyncIOAddress> NodeActionSender<Action, A> {
    pub fn sender(&self) -> mpsc::Sender<Action> {
        self.tx.clone()
    }

    pub async fn send(&self, action: Action) -> Result<(), SendActionError> {
        if self.leader.borrow().leader.is_none() {
            return Err(SendActionError::NoLeader);
        }

        self.tx.send(action).await.map_err(|_| SendActionError::Closed)
    }

    pub async fn send_when_leader(&mut self, action: Action) -> Result<(), SendActionError> {
        while self.leader.borrow().leader.is_none() {
            self.leader.changed().await.map_err(|_| SendActionError::Closed)?;
        }

        self.tx.send(action).await.map_err(|_| SendActionError::Closed)
    }
}

impl<A: SyncIOAddress, D: DeterministicState> Inner<A, D> {
    pub(crate) async fn start_local_action_pump(self: &Arc<Self>)
    where
        D::Action: Send,
    {
        let Some(mut rx) = self.local_actions_rx.lock().await.take() else {
            return;
        };

        let inner = self.clone();

        tokio::spawn(async move {
            let mut state_handle = inner.state.lock().await.create_state_handle();

            while let Some(mut action) = rx.recv().await {
                loop {
                    let is_leader = inner.control.lock().await.leader.leader == Some(inner.address);
                    if is_leader {
                        let authority = {
                            let state = state_handle.read();
                            let authority = state.authority(RecoverableStateAction::StateAction { action });
                            state_handle.quiescent();
                            authority
                        };

                        if let Err(error) = inner.state.lock().await.apply_authority(authority).await {
                            tracing::error!(?error, "failed to apply local authority action");
                            inner.fail(format!("failed to apply local authority action: {error:?}"));
                            break;
                        }
                        break;
                    }

                    let follow_tx = {
                        let control = inner.control.lock().await;
                        control.follow.as_ref().map(|follow| follow.to_peer.clone())
                    };

                    if let Some(follow_tx) = follow_tx {
                        let to_send = SyncRequest::Action {
                            source: inner.address,
                            action,
                        };

                        match follow_tx.send(to_send).await {
                            Ok(()) => break,
                            Err(error) => {
                                if let SyncRequest::Action { action: returned, .. } = error.0 {
                                    action = returned;
                                } else {
                                    tracing::error!("action pump received unexpected failed request type");
                                    break;
                                };
                            }
                        }
                    }

                    tokio::time::sleep(inner.timing.follow_retry_interval).await;
                }
            }
        });
    }
}
