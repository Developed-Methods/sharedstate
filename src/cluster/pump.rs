use tokio::sync::{mpsc, watch};

use crate::{cluster::messages::LeaderInfoMessage, net::sync_io::SyncIOAddress};

#[derive(Debug)]
pub enum SendActionError {
    Closed,
    NoLeader,
}

impl std::fmt::Display for SendActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "action channel closed"),
            Self::NoLeader => write!(f, "no leader elected"),
        }
    }
}

pub struct NodeActionSender<Action, A: SyncIOAddress> {
    pub(crate) tx: mpsc::Sender<Action>,
    pub(crate) leader: watch::Receiver<LeaderInfoMessage<A>>,
}

impl<Action, A: SyncIOAddress> Clone for NodeActionSender<Action, A> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            leader: self.leader.clone(),
        }
    }
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
        self.leader
            .wait_for(|info| info.leader.is_some())
            .await
            .map_err(|_| SendActionError::Closed)?;
        self.tx.send(action).await.map_err(|_| SendActionError::Closed)
    }
}
