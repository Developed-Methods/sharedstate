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
        node_state::{DebugInfo, NodeState},
        rpc_server::RpcServer,
        state_sync::{StateSyncTask, StateSyncTiming},
    },
    metrics::SharedStateMetrics,
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
    pub leader_address: I::Address,
    pub available_peers: Vec<I::Address>,
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
    leader_address_tx: watch::Sender<I::Address>,
    available_peers_tx: watch::Sender<Vec<I::Address>>,
    actions_tx: mpsc::Sender<D::Action>,
    metrics: Arc<SharedStateMetrics>,
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
        let (leader_address_tx, leader_address) = watch::channel(leader_address);
        let (available_peers_tx, available_peers) = watch::channel(available_peers);
        let node = Arc::new(NodeState::new(my_address, leader_address, available_peers, state));
        let metrics = Arc::new(SharedStateMetrics::default());

        let (actions_tx, actions_rx) = mpsc::channel(ACTION_QUEUE_CAPACITY);
        let rpc_server = Arc::new(RpcServer::new(
            node.clone(),
            actions_tx.clone(),
            leader_address_tx.clone(),
            available_peers_tx.clone(),
            metrics.clone(),
        ));

        let tasks = vec![
            rpc_server.start_listener(io.clone(), settings.net.clone()),
            tokio::spawn(
                StateSyncTask::new(
                    node.clone(),
                    io,
                    settings.net,
                    actions_rx,
                    leader_address_tx.clone(),
                    available_peers_tx.clone(),
                    settings.sync_timing,
                    metrics.clone(),
                )
                .run(),
            ),
        ];

        Ok(Self {
            node,
            leader_address_tx,
            available_peers_tx,
            actions_tx,
            metrics,
            tasks,
        })
    }

    pub fn my_address(&self) -> I::Address {
        self.node.my_address
    }

    pub fn leader_address(&self) -> I::Address {
        self.node.leader_address()
    }

    pub fn set_leader_address(&self, leader_address: I::Address) {
        self.leader_address_tx.send_replace(leader_address);
    }

    pub fn set_available_peers(&self, available_peers: Vec<I::Address>) {
        self.available_peers_tx.send_replace(available_peers);
    }

    pub fn leader_address_sender(&self) -> watch::Sender<I::Address> {
        self.leader_address_tx.clone()
    }

    pub fn available_peers_sender(&self) -> watch::Sender<Vec<I::Address>> {
        self.available_peers_tx.clone()
    }

    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    pub fn is_connected_to_leader(&self) -> bool {
        self.node.is_connected_to_leader()
    }

    pub fn debug_info(&self) -> DebugInfo<I::Address> {
        self.node.debug_info()
    }

    pub fn node(&self) -> &Arc<NodeState<I::Address, D>> {
        &self.node
    }

    pub fn metrics(&self) -> &Arc<SharedStateMetrics> {
        &self.metrics
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
