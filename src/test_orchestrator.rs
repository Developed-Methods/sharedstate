use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    sync::Arc,
    time::Duration,
};

use message_encoding::MessageEncoding;
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{
    net::{
        message_channel::NetIoSettings,
        simulated::{SimulatedIo, SimulatedNet, SimulatedTopologySnapshot},
    },
    shared::node::{NodeActionSender, NodeDebugInfo, NodeState, PeerDebugInfo},
    state::{determinstic_state::DeterministicState, recoverable_state::RecoverableState},
};

const RECORDING_VERSION: u64 = 1;

pub struct SharedStateTestOrchestratorConfig<D: DeterministicState> {
    pub io_settings: NetIoSettings,
    pub initial_state: Arc<dyn Fn(u64) -> RecoverableState<D> + Send + Sync>,
}

impl<D: DeterministicState> Clone for SharedStateTestOrchestratorConfig<D> {
    fn clone(&self) -> Self {
        Self {
            io_settings: self.io_settings.clone(),
            initial_state: self.initial_state.clone(),
        }
    }
}

pub struct SharedStateTestOrchestrator<D: DeterministicState>
where
    D::Action: Clone + fmt::Debug,
{
    net: SimulatedNet,
    nodes: Mutex<HashMap<u64, ManagedNode<D>>>,
    recording: Mutex<Vec<RecordedOrchestratorEvent<D>>>,
    mutation_lock: Mutex<()>,
    config: SharedStateTestOrchestratorConfig<D>,
}

struct ManagedNode<D: DeterministicState> {
    address: u64,
    can_lead: bool,
    known_peers: BTreeSet<u64>,
    node: Option<NodeRuntime<D>>,
    networking_disabled: bool,
    blocked_peers: BTreeSet<u64>,
}

struct NodeRuntime<D: DeterministicState> {
    node: NodeState<u64, D>,
    _io: Arc<SimulatedIo>,
    listener: JoinHandle<()>,
    client: JoinHandle<()>,
    actions: NodeActionSender<D::Action>,
}

impl<D: DeterministicState> NodeRuntime<D> {
    async fn stop(self, net: &SimulatedNet, address: u64) {
        self.listener.abort();
        self.client.abort();
        net.stop_node(address).await;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "D: Serialize", deserialize = "D: DeterministicState + Deserialize<'de>"))]
pub struct OrchestratorSnapshot<D: DeterministicState> {
    pub nodes: Vec<NodeView<D>>,
    pub connections: Vec<ConnectionView>,
    pub topology: SimulatedTopologySnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(serialize = "D: Serialize", deserialize = "D: DeterministicState + Deserialize<'de>"))]
pub struct NodeView<D: DeterministicState> {
    pub address: u64,
    pub can_lead: bool,
    pub online: bool,
    pub networking_disabled: bool,
    pub blocked_peers: Vec<u64>,
    pub known_peers: Vec<u64>,
    pub leader: Option<u64>,
    pub leader_path: Option<Vec<u64>>,
    pub term: Option<u64>,
    pub follow_remote: Option<u64>,
    pub follow_leader_path: Option<Vec<u64>>,
    pub known_can_lead: Vec<u64>,
    pub last_promoted_leader: Option<u64>,
    pub status: NodeStatus,
    pub state: Option<D>,
    pub debug: Option<NodeDebugInfoDto>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum NodeStatus {
    Offline,
    Isolated,
    NoLeader,
    Leading,
    Connected,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionView {
    pub source: u64,
    pub target: u64,
    pub blocked: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeDebugInfoDto {
    pub address: u64,
    pub can_lead: bool,
    pub leader: Option<u64>,
    pub leader_path: Option<Vec<u64>>,
    pub term: u64,
    pub follow_remote: Option<u64>,
    pub follow_leader_path: Option<Vec<u64>>,
    pub known_can_lead: Vec<u64>,
    pub last_promoted_leader: Option<u64>,
    pub observations: Vec<ElectionObservationDto>,
    pub peers: Vec<PeerDebugInfoDto>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerDebugInfoDto {
    pub address: u64,
    pub known: bool,
    pub can_lead: Option<bool>,
    pub connected: Option<bool>,
    pub latency_ms: Option<u64>,
    pub repeat_connect_fails: Option<u64>,
    pub last_activity_ms_ago: Option<u64>,
    pub last_global_activity_ms_ago: Option<u64>,
    pub last_connect_attempt_ms_ago: Option<u64>,
    pub last_connect_fail_ms_ago: Option<u64>,
    pub observed_leader: Option<u64>,
    pub observed_term: Option<u64>,
    pub observed_leader_path: Option<Vec<u64>>,
    pub observed_reachable_can_lead: Option<Vec<u64>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ElectionObservationDto {
    pub observer: u64,
    pub term: u64,
    pub leader: Option<u64>,
    pub leader_path: Option<Vec<u64>>,
    pub can_lead: bool,
    pub reachable_can_lead: Vec<u64>,
    #[serde(default)]
    pub state_accept_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddNodeRequest {
    pub address: u64,
    pub can_lead: bool,
    pub peers: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetNetworkingRequest {
    pub disabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetBlockedPeersRequest {
    pub blocked_peers: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "D: Serialize, D::Action: Serialize",
    deserialize = "D: DeterministicState + Deserialize<'de>, D::Action: Deserialize<'de>"
))]
pub struct OrchestratorRecording<D: DeterministicState>
where
    D::Action: Clone + fmt::Debug,
{
    pub version: u64,
    pub events: Vec<RecordedOrchestratorEvent<D>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "D: Serialize, D::Action: Serialize",
    deserialize = "D: DeterministicState + Deserialize<'de>, D::Action: Deserialize<'de>"
))]
pub struct RecordedOrchestratorEvent<D: DeterministicState>
where
    D::Action: Clone + fmt::Debug,
{
    pub before: OrchestratorSnapshot<D>,
    pub before_checkpoint: ReplayCheckpoint<D>,
    pub action: OrchestratorAction<D::Action>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum OrchestratorAction<Action> {
    AddNode {
        address: u64,
        can_lead: bool,
        peers: Vec<u64>,
    },
    StartNode {
        address: u64,
    },
    StopNode {
        address: u64,
    },
    SetNetworking {
        address: u64,
        disabled: bool,
    },
    SetBlockedPeers {
        address: u64,
        blocked_peers: Vec<u64>,
    },
    SendAction {
        address: u64,
        action: Action,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(serialize = "D: Serialize", deserialize = "D: DeterministicState + Deserialize<'de>"))]
pub struct ReplayCheckpoint<D: DeterministicState> {
    pub nodes: Vec<ReplayNodeCheckpoint<D>>,
    pub topology: ReplayTopologyCheckpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(serialize = "D: Serialize", deserialize = "D: DeterministicState + Deserialize<'de>"))]
pub struct ReplayNodeCheckpoint<D: DeterministicState> {
    pub address: u64,
    pub can_lead: bool,
    pub online: bool,
    pub networking_disabled: bool,
    pub blocked_peers: Vec<u64>,
    pub known_peers: Vec<u64>,
    pub leader: Option<u64>,
    pub leader_path: Option<Vec<u64>>,
    pub follow_remote: Option<u64>,
    pub follow_leader_path: Option<Vec<u64>>,
    pub known_can_lead: Vec<u64>,
    pub status: NodeStatus,
    pub state: Option<D>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayTopologyCheckpoint {
    pub online: Vec<u64>,
    pub blocked_nodes: Vec<u64>,
    pub blocked_edges: Vec<(u64, u64)>,
}

#[derive(Clone, Debug)]
pub struct ReplayOptions {
    pub settle_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for ReplayOptions {
    fn default() -> Self {
        Self {
            settle_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(50),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplayReport<D: DeterministicState> {
    pub applied_events: usize,
    pub final_snapshot: OrchestratorSnapshot<D>,
}

#[derive(Debug)]
pub enum OrchestratorError {
    InvalidRequest(String),
    NotFound(String),
    Conflict(String),
    ActionQueueClosed { address: u64 },
}

impl fmt::Display for OrchestratorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrchestratorError::InvalidRequest(message)
            | OrchestratorError::NotFound(message)
            | OrchestratorError::Conflict(message) => f.write_str(message),
            OrchestratorError::ActionQueueClosed { address } => {
                write!(f, "node {address} action queue is closed")
            }
        }
    }
}

impl std::error::Error for OrchestratorError {}

#[derive(Debug)]
pub enum ReplayError<D: DeterministicState> {
    Orchestrator(OrchestratorError),
    CheckpointTimeout {
        event_index: usize,
        expected: Box<ReplayCheckpoint<D>>,
        actual: Box<ReplayCheckpoint<D>>,
    },
}

impl<D: DeterministicState> From<OrchestratorError> for ReplayError<D> {
    fn from(value: OrchestratorError) -> Self {
        Self::Orchestrator(value)
    }
}

impl<D> SharedStateTestOrchestrator<D>
where
    D: DeterministicState + MessageEncoding + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::Action: MessageEncoding + Clone + fmt::Debug + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::AuthorityAction: MessageEncoding,
{
    pub fn new(config: SharedStateTestOrchestratorConfig<D>) -> Self {
        Self {
            net: SimulatedNet::new(),
            nodes: Mutex::new(HashMap::new()),
            recording: Mutex::new(Vec::new()),
            mutation_lock: Mutex::new(()),
            config,
        }
    }

    pub async fn add_node(&self, request: AddNodeRequest) -> Result<NodeView<D>, OrchestratorError> {
        let action = OrchestratorAction::AddNode {
            address: request.address,
            can_lead: request.can_lead,
            peers: request.peers,
        };
        let address = request.address;
        self.apply_action(action, true).await?;
        self.node_details(address).await
    }

    pub async fn start_node(&self, address: u64) -> Result<(), OrchestratorError> {
        self.apply_action(OrchestratorAction::StartNode { address }, true).await
    }

    pub async fn stop_node(&self, address: u64) -> Result<(), OrchestratorError> {
        self.apply_action(OrchestratorAction::StopNode { address }, true).await
    }

    pub async fn set_networking(&self, address: u64, request: SetNetworkingRequest) -> Result<(), OrchestratorError> {
        self.apply_action(
            OrchestratorAction::SetNetworking {
                address,
                disabled: request.disabled,
            },
            true,
        )
        .await
    }

    pub async fn set_blocked_peers(
        &self,
        address: u64,
        request: SetBlockedPeersRequest,
    ) -> Result<(), OrchestratorError> {
        self.apply_action(
            OrchestratorAction::SetBlockedPeers {
                address,
                blocked_peers: request.blocked_peers,
            },
            true,
        )
        .await
    }

    pub async fn send_action(&self, address: u64, action: D::Action) -> Result<(), OrchestratorError> {
        self.apply_action(OrchestratorAction::SendAction { address, action }, true)
            .await
    }

    pub async fn snapshot(&self) -> OrchestratorSnapshot<D> {
        self.build_snapshot().await
    }

    pub async fn node_details(&self, address: u64) -> Result<NodeView<D>, OrchestratorError> {
        self.build_snapshot()
            .await
            .nodes
            .into_iter()
            .find(|node| node.address == address)
            .ok_or_else(|| OrchestratorError::NotFound(format!("node {address} not found")))
    }

    pub async fn recording(&self) -> OrchestratorRecording<D> {
        OrchestratorRecording {
            version: RECORDING_VERSION,
            events: self.recording.lock().await.clone(),
        }
    }

    pub async fn clear_recording(&self) {
        self.recording.lock().await.clear();
    }

    async fn apply_action(&self, action: OrchestratorAction<D::Action>, record: bool) -> Result<(), OrchestratorError> {
        let _guard = self.mutation_lock.lock().await;
        self.validate_action(&action).await?;
        let before = if record {
            Some(self.build_snapshot().await)
        } else {
            None
        };
        self.apply_action_unlocked(action.clone()).await?;

        if let Some(before) = before {
            self.recording.lock().await.push(RecordedOrchestratorEvent {
                before_checkpoint: checkpoint_from_snapshot(&before),
                before,
                action,
            });
        }

        Ok(())
    }

    async fn validate_action(&self, action: &OrchestratorAction<D::Action>) -> Result<(), OrchestratorError> {
        let nodes = self.nodes.lock().await;
        match action {
            OrchestratorAction::AddNode { address, .. } => {
                if *address == 0 {
                    return Err(OrchestratorError::InvalidRequest("address must be greater than zero".to_owned()));
                }
                if nodes.contains_key(address) {
                    return Err(OrchestratorError::Conflict(format!("node {address} already exists")));
                }
            }
            OrchestratorAction::StartNode { address }
            | OrchestratorAction::StopNode { address }
            | OrchestratorAction::SetNetworking { address, .. }
            | OrchestratorAction::SetBlockedPeers { address, .. } => {
                if !nodes.contains_key(address) {
                    return Err(OrchestratorError::NotFound(format!("node {address} not found")));
                }
            }
            OrchestratorAction::SendAction { address, .. } => {
                let Some(managed) = nodes.get(address) else {
                    return Err(OrchestratorError::NotFound(format!("node {address} not found")));
                };
                if managed.node.is_none() {
                    return Err(OrchestratorError::Conflict(format!("node {address} is offline")));
                }
            }
        }
        Ok(())
    }

    async fn apply_action_unlocked(&self, action: OrchestratorAction<D::Action>) -> Result<(), OrchestratorError> {
        match action {
            OrchestratorAction::AddNode {
                address,
                can_lead,
                peers,
            } => {
                let known_peers = peers.into_iter().collect::<BTreeSet<_>>();
                let runtime = self.create_runtime(address, can_lead, &known_peers).await;
                self.nodes.lock().await.insert(
                    address,
                    ManagedNode {
                        address,
                        can_lead,
                        known_peers,
                        node: Some(runtime),
                        networking_disabled: false,
                        blocked_peers: BTreeSet::new(),
                    },
                );
            }
            OrchestratorAction::StartNode { address } => {
                let (can_lead, known_peers, networking_disabled, blocked_peers, needs_start) = {
                    let nodes = self.nodes.lock().await;
                    let managed = nodes
                        .get(&address)
                        .ok_or_else(|| OrchestratorError::NotFound(format!("node {address} not found")))?;
                    (
                        managed.can_lead,
                        managed.known_peers.clone(),
                        managed.networking_disabled,
                        managed.blocked_peers.clone(),
                        managed.node.is_none(),
                    )
                };

                if needs_start {
                    let runtime = self.create_runtime(address, can_lead, &known_peers).await;
                    let mut nodes = self.nodes.lock().await;
                    if let Some(managed) = nodes.get_mut(&address) {
                        managed.node = Some(runtime);
                    }
                }

                self.net.set_node_blocked(address, networking_disabled).await;
                for peer in blocked_peers {
                    self.net.set_edge_blocked(address, peer, true).await;
                }
            }
            OrchestratorAction::StopNode { address } => {
                let runtime = {
                    let mut nodes = self.nodes.lock().await;
                    let managed = nodes
                        .get_mut(&address)
                        .ok_or_else(|| OrchestratorError::NotFound(format!("node {address} not found")))?;
                    managed.node.take()
                };
                if let Some(runtime) = runtime {
                    runtime.stop(&self.net, address).await;
                } else {
                    self.net.stop_node(address).await;
                }
            }
            OrchestratorAction::SetNetworking { address, disabled } => {
                {
                    let mut nodes = self.nodes.lock().await;
                    let managed = nodes
                        .get_mut(&address)
                        .ok_or_else(|| OrchestratorError::NotFound(format!("node {address} not found")))?;
                    managed.networking_disabled = disabled;
                }
                self.net.set_node_blocked(address, disabled).await;
            }
            OrchestratorAction::SetBlockedPeers { address, blocked_peers } => {
                let next = blocked_peers
                    .into_iter()
                    .filter(|peer| *peer != address)
                    .collect::<BTreeSet<_>>();
                let previous = {
                    let nodes = self.nodes.lock().await;
                    nodes
                        .get(&address)
                        .ok_or_else(|| OrchestratorError::NotFound(format!("node {address} not found")))?
                        .blocked_peers
                        .clone()
                };

                for peer in previous.difference(&next) {
                    self.net.set_edge_blocked(address, *peer, false).await;
                }
                for peer in next.difference(&previous) {
                    self.net.set_edge_blocked(address, *peer, true).await;
                }

                let mut nodes = self.nodes.lock().await;
                if let Some(managed) = nodes.get_mut(&address) {
                    managed.blocked_peers = next;
                }
            }
            OrchestratorAction::SendAction { address, action } => {
                let action_sender = {
                    let nodes = self.nodes.lock().await;
                    nodes
                        .get(&address)
                        .and_then(|managed| managed.node.as_ref())
                        .map(|runtime| runtime.actions.clone())
                        .ok_or_else(|| OrchestratorError::Conflict(format!("node {address} is offline")))?
                };
                action_sender
                    .send(action)
                    .await
                    .map_err(|_| OrchestratorError::ActionQueueClosed { address })?;
            }
        }
        Ok(())
    }

    async fn create_runtime(&self, address: u64, can_lead: bool, known_peers: &BTreeSet<u64>) -> NodeRuntime<D> {
        let io = self.net.start_io(address).await;
        let node =
            NodeState::new(address, (self.config.initial_state)(address), can_lead, self.config.io_settings.clone())
                .await;
        node.discover_peers(known_peers.iter().copied()).await;
        let listener = node.start_listener(io.clone()).await;
        let client = node.start_client(io.clone()).await;
        let actions = node.action_sender();
        NodeRuntime {
            node,
            _io: io,
            listener,
            client,
            actions,
        }
    }

    async fn build_snapshot(&self) -> OrchestratorSnapshot<D> {
        let topology = self.net.topology_snapshot().await;
        let nodes = self.nodes.lock().await;
        let mut views = Vec::with_capacity(nodes.len());
        for managed in nodes.values() {
            views.push(build_node_view(managed, &topology).await);
        }
        views.sort_by_key(|node| node.address);
        let connections = build_connections(&views, &topology);

        OrchestratorSnapshot {
            nodes: views,
            connections,
            topology,
        }
    }
}

pub async fn replay_recording<D>(
    config: SharedStateTestOrchestratorConfig<D>,
    recording: OrchestratorRecording<D>,
    options: ReplayOptions,
) -> Result<ReplayReport<D>, ReplayError<D>>
where
    D: DeterministicState + MessageEncoding + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::Action: MessageEncoding + Clone + fmt::Debug + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::AuthorityAction: MessageEncoding,
{
    let orchestrator = SharedStateTestOrchestrator::new(config);
    let event_count = recording.events.len();

    for (event_index, event) in recording.events.into_iter().enumerate() {
        wait_for_checkpoint(&orchestrator, &event.before_checkpoint, event_index, &options).await?;
        orchestrator.apply_action(event.action, false).await?;
    }

    let final_snapshot = orchestrator.snapshot().await;
    Ok(ReplayReport {
        applied_events: event_count,
        final_snapshot,
    })
}

async fn wait_for_checkpoint<D>(
    orchestrator: &SharedStateTestOrchestrator<D>,
    expected: &ReplayCheckpoint<D>,
    event_index: usize,
    options: &ReplayOptions,
) -> Result<(), ReplayError<D>>
where
    D: DeterministicState + MessageEncoding + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::Action: MessageEncoding + Clone + fmt::Debug + Serialize + for<'de> Deserialize<'de> + PartialEq + Eq,
    D::AuthorityAction: MessageEncoding,
{
    let deadline = tokio::time::Instant::now() + options.settle_timeout;
    loop {
        let snapshot = orchestrator.snapshot().await;
        let actual = checkpoint_from_snapshot(&snapshot);
        if &actual == expected {
            return Ok(());
        }
        if deadline <= tokio::time::Instant::now() {
            return Err(ReplayError::CheckpointTimeout {
                event_index,
                expected: Box::new(expected.clone()),
                actual: Box::new(actual),
            });
        }
        tokio::time::sleep(options.poll_interval).await;
    }
}

pub fn checkpoint_from_snapshot<D>(snapshot: &OrchestratorSnapshot<D>) -> ReplayCheckpoint<D>
where
    D: DeterministicState + Clone,
{
    ReplayCheckpoint {
        nodes: snapshot
            .nodes
            .iter()
            .map(|node| ReplayNodeCheckpoint {
                address: node.address,
                can_lead: node.can_lead,
                online: node.online,
                networking_disabled: node.networking_disabled,
                blocked_peers: node.blocked_peers.clone(),
                known_peers: node.known_peers.clone(),
                leader: node.leader,
                leader_path: node.leader_path.clone(),
                follow_remote: node.follow_remote,
                follow_leader_path: node.follow_leader_path.clone(),
                known_can_lead: node.known_can_lead.clone(),
                status: node.status,
                state: node.state.clone(),
            })
            .collect(),
        topology: ReplayTopologyCheckpoint {
            online: snapshot.topology.online.iter().copied().collect(),
            blocked_nodes: snapshot.topology.blocked_nodes.iter().copied().collect(),
            blocked_edges: snapshot.topology.blocked_edges.iter().copied().collect(),
        },
    }
}

async fn build_node_view<D>(managed: &ManagedNode<D>, topology: &SimulatedTopologySnapshot) -> NodeView<D>
where
    D: DeterministicState,
    D::Action: Clone,
{
    let online = managed.node.is_some();
    let mut state = None;
    let mut debug = None;

    if let Some(runtime) = &managed.node {
        let mut handle = runtime.node.create_state_handle();
        state = Some(handle.read().state().clone());
        handle.quiescent();
        debug = Some(runtime.node.debug_info().await);
    }

    let status = match (&debug, managed.networking_disabled || topology.blocked_nodes.contains(&managed.address)) {
        (_, _) if !online => NodeStatus::Offline,
        (_, true) => NodeStatus::Isolated,
        (Some(debug), _) if debug.leader == Some(managed.address) => NodeStatus::Leading,
        (Some(debug), _) if debug.leader.is_some() => NodeStatus::Connected,
        _ => NodeStatus::NoLeader,
    };

    let leader = debug.as_ref().and_then(|debug| debug.leader);
    let leader_path = debug.as_ref().and_then(|debug| debug.leader_path.clone());
    let term = debug.as_ref().map(|debug| debug.term);
    let follow_remote = debug.as_ref().and_then(|debug| debug.follow_remote);
    let follow_leader_path = debug.as_ref().and_then(|debug| debug.follow_leader_path.clone());
    let known_can_lead = debug
        .as_ref()
        .map(|debug| debug.known_can_lead.clone())
        .unwrap_or_default();
    let last_promoted_leader = debug.as_ref().and_then(|debug| debug.last_promoted_leader);

    NodeView {
        address: managed.address,
        can_lead: managed.can_lead,
        online,
        networking_disabled: managed.networking_disabled,
        blocked_peers: managed.blocked_peers.iter().copied().collect(),
        known_peers: managed.known_peers.iter().copied().collect(),
        leader,
        leader_path,
        term,
        follow_remote,
        follow_leader_path,
        known_can_lead,
        last_promoted_leader,
        status,
        state,
        debug: debug.map(node_debug_dto),
    }
}

fn build_connections<D>(views: &[NodeView<D>], topology: &SimulatedTopologySnapshot) -> Vec<ConnectionView>
where
    D: DeterministicState,
{
    let mut edges = BTreeMap::<(u64, u64), bool>::new();
    let known_nodes = views.iter().map(|view| view.address).collect::<BTreeSet<_>>();

    for edge in &topology.blocked_edges {
        if known_nodes.contains(&edge.0) && known_nodes.contains(&edge.1) {
            edges.insert(*edge, true);
        }
    }

    for view in views {
        if let Some(debug) = &view.debug {
            for peer in &debug.peers {
                if peer.address == view.address {
                    continue;
                }
                if peer.connected == Some(true) {
                    let edge = SimulatedNet::edge_key(view.address, peer.address);
                    edges.entry(edge).or_insert(topology.blocked_edges.contains(&edge));
                }
            }
        }
    }

    edges
        .into_iter()
        .map(|((source, target), blocked)| ConnectionView {
            source,
            target,
            blocked,
        })
        .collect()
}

fn node_debug_dto(debug: NodeDebugInfo<u64>) -> NodeDebugInfoDto {
    NodeDebugInfoDto {
        address: debug.address,
        can_lead: debug.can_lead,
        leader: debug.leader,
        leader_path: debug.leader_path,
        term: debug.term,
        follow_remote: debug.follow_remote,
        follow_leader_path: debug.follow_leader_path,
        known_can_lead: debug.known_can_lead,
        last_promoted_leader: debug.last_promoted_leader,
        observations: debug
            .observations
            .into_iter()
            .map(|observation| ElectionObservationDto {
                observer: observation.observer,
                term: observation.term,
                leader: observation.leader,
                leader_path: observation.leader_path,
                can_lead: observation.can_lead,
                reachable_can_lead: observation.reachable_can_lead,
                state_accept_seq: observation.state_accept_seq,
            })
            .collect(),
        peers: debug.peers.into_iter().map(peer_debug_dto).collect(),
    }
}

fn peer_debug_dto(peer: PeerDebugInfo<u64>) -> PeerDebugInfoDto {
    PeerDebugInfoDto {
        address: peer.address,
        known: peer.known,
        can_lead: peer.can_lead,
        connected: peer.connected,
        latency_ms: peer.latency_ms,
        repeat_connect_fails: peer.repeat_connect_fails,
        last_activity_ms_ago: peer.last_activity_ms_ago,
        last_global_activity_ms_ago: peer.last_global_activity_ms_ago,
        last_connect_attempt_ms_ago: peer.last_connect_attempt_ms_ago,
        last_connect_fail_ms_ago: peer.last_connect_fail_ms_ago,
        observed_leader: peer.observed_leader,
        observed_term: peer.observed_term,
        observed_leader_path: peer.observed_leader_path,
        observed_reachable_can_lead: peer.observed_reachable_can_lead,
    }
}

#[cfg(test)]
mod test;
