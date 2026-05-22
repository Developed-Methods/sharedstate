use std::{collections::HashMap, time::Duration};

use serde::Serialize;
use sharedstate::{
    recoverable_state::{RecoverableState, RecoverableStateDetails},
    state::SharedState,
    worker::sync_manager::{SentinelSettings, SyncManager, SyncMangerSettings},
};
use tokio::{
    sync::{mpsc::Sender, oneshot},
    task::JoinHandle,
};

use crate::{
    network::{LinkSnapshot, NodeLinkSnapshot, SimNet, SimSyncIo},
    state::{WebTestAction, WebTestLogEntry, WebTestState},
};

pub const SENTINELS: std::ops::RangeInclusive<u64> = 1..=5;
pub const REGULAR_PEERS: std::ops::RangeInclusive<u64> = 6..=13;
pub const ALL_NODES: std::ops::RangeInclusive<u64> = 1..=13;
pub const INITIAL_MASTER: u64 = 1;

pub struct Simulation {
    net: SimNet,
    nodes: HashMap<u64, NodeRuntime>,
    current_master_guess: u64,
}

struct NodeRuntime {
    id: u64,
    is_sentinel: bool,
    task: Option<JoinHandle<()>>,
    stop_tx: Option<oneshot::Sender<()>>,
    shared: Option<SharedState<RecoverableState<u64, WebTestState>>>,
    action_tx: Option<Sender<WebTestAction>>,
    running: bool,
    networking_enabled: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct SimulationSnapshot {
    pub nodes: Vec<NodeSnapshot>,
    pub links: Vec<LinkSnapshot>,
    pub current_master_guess: u64,
    pub sentinels: Vec<u64>,
    pub regular_peers: Vec<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct NodeSnapshot {
    pub id: u64,
    pub is_sentinel: bool,
    pub running: bool,
    pub networking_enabled: bool,
    pub action_enabled: bool,
    pub state_id: Option<u64>,
    pub generation: Option<u64>,
    pub recover_seq: Option<u64>,
    pub state_seq: Option<u64>,
    pub app_seq: Option<u64>,
    pub counters: Option<[i64; 8]>,
    pub log_tail: Vec<WebTestLogEntry>,
    pub recoverable: Option<RecoverableSnapshot>,
    pub state: Option<WebTestState>,
    pub links: Vec<NodeLinkSnapshot>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecoverableSnapshot {
    pub id: u64,
    pub generation: u64,
    pub recover_accept_seq: u64,
    pub state_accept_seq: u64,
    pub history: Vec<GenerationEndSnapshot>,
}

#[derive(Clone, Debug, Serialize)]
pub struct GenerationEndSnapshot {
    pub old_id: u64,
    pub generation: u64,
    pub next_sequence: u64,
}

#[derive(Debug)]
pub struct SimError {
    pub status: axum::http::StatusCode,
    pub message: String,
}

impl Simulation {
    pub async fn new() -> Self {
        let net = SimNet::new();
        let mut nodes = HashMap::new();

        for id in ALL_NODES {
            let is_sentinel = SENTINELS.contains(&id);
            nodes.insert(
                id,
                NodeRuntime {
                    id,
                    is_sentinel,
                    task: None,
                    stop_tx: None,
                    shared: None,
                    action_tx: None,
                    running: false,
                    networking_enabled: true,
                },
            );
            net.set_node_networking(id, true);
        }

        Self {
            net,
            nodes,
            current_master_guess: INITIAL_MASTER,
        }
    }

    pub async fn start_all(&mut self) {
        for id in ALL_NODES {
            let _ = self.start_node(id).await;
        }
    }

    pub async fn stop_all(&mut self) {
        let ids = self.node_ids();
        for id in ids {
            let _ = self.stop_node(id).await;
        }
    }

    pub async fn reset(&mut self) {
        self.stop_all().await;
        self.net = SimNet::new();
        self.current_master_guess = INITIAL_MASTER;
        for node in self.nodes.values_mut() {
            node.networking_enabled = true;
            self.net.set_node_networking(node.id, true);
        }
        self.start_all().await;
    }

    pub async fn start_node(&mut self, id: u64) -> Result<(), SimError> {
        let node_ids = self.node_ids();
        let current_master = self.current_master_guess;
        let is_sentinel = {
            let node = self.node(id)?;
            if node.running {
                return Ok(());
            }
            node.is_sentinel
        };

        let io = self
            .net
            .create_io(id)
            .await
            .map_err(|err| SimError::conflict(format!("failed to start node {id}: {err}")))?;
        let settings = node_settings(id, is_sentinel, &node_ids);
        let manager =
            SyncManager::<SimSyncIo, WebTestState>::new(io, id, WebTestState::default(), settings);

        if id != current_master {
            manager.set_leader(current_master).await;
        }

        let shared = manager.shared();
        let action_tx = manager.action_tx();
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _manager = manager;
            let _ = stop_rx.await;
        });

        let node = self.node_mut(id)?;
        node.shared = Some(shared);
        node.action_tx = Some(action_tx);
        node.stop_tx = Some(stop_tx);
        node.task = Some(task);
        node.running = true;
        node.networking_enabled = true;
        self.net.set_node_networking(id, true);

        tracing::info!(node = id, "started webtester node");
        Ok(())
    }

    pub async fn stop_node(&mut self, id: u64) -> Result<(), SimError> {
        let node = self.node_mut(id)?;
        if !node.running {
            return Ok(());
        }

        if let Some(stop) = node.stop_tx.take() {
            let _ = stop.send(());
        }
        if let Some(task) = node.task.take() {
            task.abort();
        }

        node.shared = None;
        node.action_tx = None;
        node.running = false;
        self.net.unregister_node(id);
        tracing::info!(node = id, "stopped webtester node");
        Ok(())
    }

    pub fn set_node_networking(&mut self, id: u64, enabled: bool) -> Result<(), SimError> {
        let node = self.node_mut(id)?;
        node.networking_enabled = enabled;
        self.net.set_node_networking(id, enabled);
        tracing::info!(node = id, enabled, "updated node networking");
        Ok(())
    }

    pub fn set_all_networking(&mut self, enabled: bool) {
        for id in self.node_ids() {
            if let Some(node) = self.nodes.get_mut(&id) {
                node.networking_enabled = enabled;
            }
            self.net.set_node_networking(id, enabled);
        }
    }

    pub fn set_link(
        &mut self,
        a: u64,
        b: u64,
        enabled: bool,
        latency_ms: u64,
    ) -> Result<(), SimError> {
        self.node(a)?;
        self.node(b)?;
        if a == b {
            return Err(SimError::bad_request("link endpoints must be different"));
        }
        if 60_000 < latency_ms {
            return Err(SimError::bad_request("latency_ms must be <= 60000"));
        }
        self.net.set_link(a, b, enabled, latency_ms);
        tracing::info!(a, b, enabled, latency_ms, "updated link");
        Ok(())
    }

    pub async fn send_action(&self, node: u64, action: WebTestAction) -> Result<(), SimError> {
        validate_action(&action)?;
        let runtime = self.node(node)?;
        if !runtime.running {
            return Err(SimError::conflict("node is stopped"));
        }
        if !runtime.networking_enabled {
            return Err(SimError::conflict("node networking is disabled"));
        }
        let Some(action_tx) = &runtime.action_tx else {
            return Err(SimError::conflict("node action channel is unavailable"));
        };

        action_tx
            .send(action)
            .await
            .map_err(|_| SimError::conflict("node action channel is closed"))?;
        Ok(())
    }

    pub fn snapshot(&self) -> SimulationSnapshot {
        let ids = self.node_ids();
        let nodes = ids.iter().map(|id| self.node_snapshot(*id, &ids)).collect();

        SimulationSnapshot {
            nodes,
            links: self.net.link_snapshots(&ids),
            current_master_guess: self.current_master_guess,
            sentinels: SENTINELS.collect(),
            regular_peers: REGULAR_PEERS.collect(),
        }
    }

    fn node_snapshot(&self, id: u64, ids: &[u64]) -> NodeSnapshot {
        let node = self.nodes.get(&id).expect("node exists");
        let live = node.shared.as_ref().map(|shared| shared.read().clone());
        let recoverable = live
            .as_ref()
            .map(|state| recoverable_snapshot(state.details()));
        let app_state = live.as_ref().map(|state| state.state().clone());
        let log_tail = app_state
            .as_ref()
            .map(|state| {
                state
                    .log
                    .iter()
                    .rev()
                    .take(20)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect()
            })
            .unwrap_or_default();

        NodeSnapshot {
            id,
            is_sentinel: node.is_sentinel,
            running: node.running,
            networking_enabled: node.networking_enabled && self.net.node_networking_enabled(id),
            action_enabled: node.running && node.networking_enabled && node.action_tx.is_some(),
            state_id: recoverable.as_ref().map(|details| details.id),
            generation: recoverable.as_ref().map(|details| details.generation),
            recover_seq: recoverable
                .as_ref()
                .map(|details| details.recover_accept_seq),
            state_seq: recoverable.as_ref().map(|details| details.state_accept_seq),
            app_seq: app_state.as_ref().map(|state| state.sequence),
            counters: app_state.as_ref().map(|state| state.counters),
            log_tail,
            recoverable,
            state: app_state,
            links: self.net.node_link_snapshots(id, ids),
        }
    }

    fn node_ids(&self) -> Vec<u64> {
        let mut ids = self.nodes.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    fn node(&self, id: u64) -> Result<&NodeRuntime, SimError> {
        self.nodes
            .get(&id)
            .ok_or_else(|| SimError::not_found(format!("unknown node {id}")))
    }

    fn node_mut(&mut self, id: u64) -> Result<&mut NodeRuntime, SimError> {
        self.nodes
            .get_mut(&id)
            .ok_or_else(|| SimError::not_found(format!("unknown node {id}")))
    }
}

impl SimError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::CONFLICT,
            message: message.into(),
        }
    }
}

fn node_settings(id: u64, is_sentinel: bool, ids: &[u64]) -> SyncMangerSettings<u64> {
    SyncMangerSettings {
        is_sentinel,
        initial_peers: ids.iter().copied().filter(|peer| *peer != id).collect(),
        connect_timeout: Duration::from_millis(500),
        receive_state_timeout: Duration::from_secs(3),
        sentinel: SentinelSettings {
            report_interval: Duration::from_millis(500),
            election_interval: Duration::from_millis(500),
            unhealthy_disconnect_threshold: 2,
            stale_report_after: Duration::from_secs(3),
        },
        ..Default::default()
    }
}

fn recoverable_snapshot(details: &RecoverableStateDetails) -> RecoverableSnapshot {
    RecoverableSnapshot {
        id: details.id,
        generation: details.generation,
        recover_accept_seq: details.recover_accept_seq,
        state_accept_seq: details.state_accept_seq,
        history: details
            .history
            .iter()
            .map(|item| GenerationEndSnapshot {
                old_id: item.old_id,
                generation: item.generation,
                next_sequence: item.next_sequence,
            })
            .collect(),
    }
}

fn validate_action(action: &WebTestAction) -> Result<(), SimError> {
    match action {
        WebTestAction::Add { slot, .. } | WebTestAction::Set { slot, .. } if 8 <= *slot => {
            Err(SimError::bad_request("slot must be in 0..8"))
        }
        WebTestAction::AppendLog { label } if label.len() > 128 => {
            Err(SimError::bad_request("log label must be <= 128 bytes"))
        }
        _ => Ok(()),
    }
}
