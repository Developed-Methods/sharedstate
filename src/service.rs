//! High-level entry point for a fixed-leader shared state service.

use std::sync::Arc;

use message_encoding::MessageEncoding;
use sequenced_broadcast::{SequencedBroadcastSettings, SettingsError};
use tokio::{
    sync::{mpsc, mpsc::error::SendError, watch},
    task::JoinHandle,
};

use crate::{
    cluster::{
        node_state::NodeState,
        rpc_server::RpcServer,
        state_sync::{StateSyncTask, StateSyncTiming},
    },
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::RecoverableState,
        subscribable_state::{StateHandle, SubscribableState},
    },
    transport::{channels::NetIoSettings, traits::SyncIOListener},
};

pub struct SharedStateConfig<I: SyncIOListener, D: DeterministicState> {
    pub io: Arc<I>,
    pub my_address: I::Address,
    pub leader_address: watch::Receiver<I::Address>,
    pub available_peers: watch::Receiver<Vec<I::Address>>,
    pub initial_state: RecoverableState<D>,
    pub settings: SharedStateSettings,
}

#[derive(Clone, Debug, Default)]
pub struct SharedStateSettings {
    pub net: NetIoSettings,
    pub broadcast: SequencedBroadcastSettings,
    pub sync_timing: StateSyncTiming,
}

const ACTION_QUEUE_CAPACITY: usize = 512;

/// A running shared-state node. Dropping it stops the background tasks.
pub struct SharedState<I: SyncIOListener, D: DeterministicState> {
    node: Arc<NodeState<I::Address, D>>,
    actions_tx: mpsc::Sender<D::Action>,
    tasks: Vec<JoinHandle<()>>,
}

impl<I, D> SharedState<I, D>
where
    I: SyncIOListener,
    D: DeterministicState + MessageEncoding,
    D::Action: MessageEncoding,
    D::AuthorityAction: MessageEncoding,
{
    pub fn start(config: SharedStateConfig<I, D>) -> Result<Self, SettingsError> {
        let SharedStateConfig {
            io,
            my_address,
            leader_address,
            available_peers,
            initial_state,
            settings,
        } = config;

        let state = SubscribableState::new(initial_state, settings.broadcast.clone())?;
        let node = Arc::new(NodeState::new(my_address, leader_address, available_peers, state));

        let (actions_tx, actions_rx) = mpsc::channel(ACTION_QUEUE_CAPACITY);
        let rpc_server = Arc::new(RpcServer::new(node.clone(), actions_tx.clone()));

        let tasks = vec![
            rpc_server.start_listener(io.clone(), settings.net.clone()),
            tokio::spawn(StateSyncTask::new(node.clone(), io, settings.net, actions_rx, settings.sync_timing).run()),
        ];

        Ok(Self {
            node,
            actions_tx,
            tasks,
        })
    }

    pub fn my_address(&self) -> I::Address {
        self.node.my_address
    }

    pub fn leader_address(&self) -> I::Address {
        self.node.leader_address()
    }

    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    pub fn is_connected_to_leader(&self) -> bool {
        self.node.is_connected_to_leader()
    }

    pub fn node(&self) -> &Arc<NodeState<I::Address, D>> {
        &self.node
    }

    pub fn state_handle(&self) -> StateHandle<D> {
        self.node.state.create_handle()
    }

    pub async fn submit_action(&self, action: D::Action) -> Result<(), SendError<D::Action>> {
        self.actions_tx.send(action).await
    }

    pub fn actions_sender(&self) -> mpsc::Sender<D::Action> {
        self.actions_tx.clone()
    }
}

impl<I: SyncIOListener, D: DeterministicState> Drop for SharedState<I, D> {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
