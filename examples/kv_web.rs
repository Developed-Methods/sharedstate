use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::{Error, ErrorKind, Result},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use message_encoding::MessageEncoding;
use serde::{Deserialize, Serialize};
use sharedstate::{
    net::{
        message_channel::NetIoSettings,
        simulated::{SimulatedNet, SimulatedTopologySnapshot},
    },
    shared::node::{NodeActionSender, NodeDebugInfo, NodeState, PeerDebugInfo},
    state::{determinstic_state::DeterministicState, recoverable_state::RecoverableState},
};
use tokio::{sync::Mutex, task::JoinHandle};
use tower_http::{cors::CorsLayer, services::ServeDir};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,

    #[arg(long, default_value = "examples/kv-web/dist")]
    static_dir: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
struct KvStore {
    seq: u64,
    values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
enum KvAction {
    Set { key: String, value: String },
    Delete { key: String },
}

impl DeterministicState for KvStore {
    type Action = KvAction;
    type AuthorityAction = KvAction;

    fn accept_seq(&self) -> u64 {
        self.seq
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        action
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        match action {
            KvAction::Set { key, value } => {
                self.values.insert(key.clone(), value.clone());
            }
            KvAction::Delete { key } => {
                self.values.remove(key);
            }
        }
        self.seq += 1;
    }
}

impl MessageEncoding for KvAction {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
        let mut sum = 0;
        sum += match self {
            Self::Set { key, value } => {
                sum += 1u16.write_to(out)?;
                sum += key.write_to(out)?;
                value.write_to(out)?
            }
            Self::Delete { key } => {
                sum += 2u16.write_to(out)?;
                key.write_to(out)?
            }
        };
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(Self::Set {
                key: MessageEncoding::read_from(read)?,
                value: MessageEncoding::read_from(read)?,
            }),
            2 => Ok(Self::Delete {
                key: MessageEncoding::read_from(read)?,
            }),
            id => Err(Error::new(ErrorKind::InvalidData, format!("unknown KvAction id {id}"))),
        }
    }
}

impl MessageEncoding for KvStore {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> Result<usize> {
        let mut sum = 0;
        sum += self.seq.write_to(out)?;
        sum += (self.values.len() as u64).write_to(out)?;
        for (key, value) in &self.values {
            sum += key.write_to(out)?;
            sum += value.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> Result<Self> {
        let seq = MessageEncoding::read_from(read)?;
        let len = u64::read_from(read)? as usize;
        let mut values = BTreeMap::new();
        for _ in 0..len {
            values.insert(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?);
        }
        Ok(Self { seq, values })
    }
}

struct AppState {
    net: SimulatedNet,
    nodes: Mutex<HashMap<u64, ManagedNode>>,
}

struct ManagedNode {
    address: u64,
    can_lead: bool,
    known_peers: BTreeSet<u64>,
    node: Option<NodeRuntime>,
    networking_disabled: bool,
    blocked_peers: BTreeSet<u64>,
}

struct NodeRuntime {
    node: NodeState<u64, KvStore>,
    _io: Arc<sharedstate::net::simulated::SimulatedIo>,
    listener: JoinHandle<()>,
    client: JoinHandle<()>,
    actions: NodeActionSender<KvAction>,
}

impl NodeRuntime {
    async fn stop(self, net: &SimulatedNet, address: u64) {
        self.listener.abort();
        self.client.abort();
        net.stop_node(address).await;
    }
}

#[derive(Serialize)]
struct SnapshotResponse {
    nodes: Vec<NodeView>,
    connections: Vec<ConnectionView>,
    topology: SimulatedTopologySnapshot,
}

#[derive(Serialize)]
struct NodeView {
    address: u64,
    can_lead: bool,
    online: bool,
    networking_disabled: bool,
    blocked_peers: Vec<u64>,
    known_peers: Vec<u64>,
    leader: Option<u64>,
    leader_path: Option<Vec<u64>>,
    term: Option<u64>,
    follow_remote: Option<u64>,
    follow_leader_path: Option<Vec<u64>>,
    known_can_lead: Vec<u64>,
    last_promoted_leader: Option<u64>,
    status: NodeStatus,
    state: Option<KvStore>,
    debug: Option<NodeDebugInfoDto>,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
enum NodeStatus {
    Offline,
    Isolated,
    NoLeader,
    Leading,
    Connected,
}

#[derive(Serialize)]
struct ConnectionView {
    source: u64,
    target: u64,
    blocked: bool,
}

#[derive(Serialize)]
struct NodeDebugInfoDto {
    address: u64,
    can_lead: bool,
    leader: Option<u64>,
    leader_path: Option<Vec<u64>>,
    term: u64,
    follow_remote: Option<u64>,
    follow_leader_path: Option<Vec<u64>>,
    known_can_lead: Vec<u64>,
    last_promoted_leader: Option<u64>,
    observations: Vec<ElectionObservationDto>,
    peers: Vec<PeerDebugInfoDto>,
}

#[derive(Serialize)]
struct PeerDebugInfoDto {
    address: u64,
    known: bool,
    can_lead: Option<bool>,
    connected: Option<bool>,
    latency_ms: Option<u64>,
    repeat_connect_fails: Option<u64>,
    last_activity_ms_ago: Option<u64>,
    last_global_activity_ms_ago: Option<u64>,
    last_connect_attempt_ms_ago: Option<u64>,
    last_connect_fail_ms_ago: Option<u64>,
    observed_leader: Option<u64>,
    observed_term: Option<u64>,
    observed_leader_path: Option<Vec<u64>>,
    observed_reachable_can_lead: Option<Vec<u64>>,
}

#[derive(Serialize)]
struct ElectionObservationDto {
    observer: u64,
    term: u64,
    leader: Option<u64>,
    leader_path: Option<Vec<u64>>,
    can_lead: bool,
    reachable_can_lead: Vec<u64>,
}

#[derive(Deserialize)]
struct AddNodeRequest {
    address: u64,
    can_lead: bool,
    peers: Vec<u64>,
}

#[derive(Deserialize)]
struct SetNetworkingRequest {
    disabled: bool,
}

#[derive(Deserialize)]
struct SetBlockedPeersRequest {
    blocked_peers: Vec<u64>,
}

#[derive(Deserialize)]
struct SetKeyRequest {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct DeleteKeyRequest {
    key: String,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(ErrorBody { error: self.message })).into_response()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_writer(std::io::stderr).try_init();
    let args = Args::parse();
    let state = Arc::new(AppState {
        net: SimulatedNet::new(),
        nodes: Mutex::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/api/snapshot", get(snapshot))
        .route("/api/nodes", post(add_node))
        .route("/api/nodes/:address", get(node_details))
        .route("/api/nodes/:address/start", post(start_node))
        .route("/api/nodes/:address/stop", post(stop_node))
        .route("/api/nodes/:address/networking", post(set_networking))
        .route("/api/nodes/:address/blocked-peers", post(set_blocked_peers))
        .route("/api/nodes/:address/actions/set", post(set_key))
        .route("/api/nodes/:address/actions/delete", post(delete_key))
        .fallback_service(ServeDir::new(args.static_dir).append_index_html_on_directories(true))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    println!("serving kv web example at http://{}", listener.local_addr()?);
    axum::serve(listener, app).await
}

async fn snapshot(State(state): State<Arc<AppState>>) -> Json<SnapshotResponse> {
    Json(build_snapshot(&state).await)
}

async fn node_details(
    State(state): State<Arc<AppState>>,
    Path(address): Path<u64>,
) -> std::result::Result<Json<NodeView>, ApiError> {
    let snapshot = build_snapshot(&state).await;
    snapshot
        .nodes
        .into_iter()
        .find(|node| node.address == address)
        .map(Json)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, format!("node {address} not found")))
}

async fn add_node(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AddNodeRequest>,
) -> std::result::Result<Json<NodeView>, ApiError> {
    if request.address == 0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "address must be greater than zero"));
    }

    let mut nodes = state.nodes.lock().await;
    if nodes.contains_key(&request.address) {
        return Err(ApiError::new(StatusCode::CONFLICT, format!("node {} already exists", request.address)));
    }

    let known_peers = request.peers.into_iter().collect::<BTreeSet<_>>();
    let runtime = create_runtime(&state.net, request.address, request.can_lead, &known_peers).await;
    nodes.insert(
        request.address,
        ManagedNode {
            address: request.address,
            can_lead: request.can_lead,
            known_peers,
            node: Some(runtime),
            networking_disabled: false,
            blocked_peers: BTreeSet::new(),
        },
    );
    drop(nodes);

    node_details(State(state), Path(request.address)).await
}

async fn stop_node(
    State(state): State<Arc<AppState>>,
    Path(address): Path<u64>,
) -> std::result::Result<StatusCode, ApiError> {
    let mut nodes = state.nodes.lock().await;
    let managed = nodes
        .get_mut(&address)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, format!("node {address} not found")))?;
    if let Some(runtime) = managed.node.take() {
        runtime.stop(&state.net, address).await;
    } else {
        state.net.stop_node(address).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn start_node(
    State(state): State<Arc<AppState>>,
    Path(address): Path<u64>,
) -> std::result::Result<StatusCode, ApiError> {
    let mut nodes = state.nodes.lock().await;
    let managed = nodes
        .get_mut(&address)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, format!("node {address} not found")))?;
    if managed.node.is_none() {
        managed.node = Some(create_runtime(&state.net, address, managed.can_lead, &managed.known_peers).await);
    }

    state.net.set_node_blocked(address, managed.networking_disabled).await;
    for peer in &managed.blocked_peers {
        state.net.set_edge_blocked(address, *peer, true).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn set_networking(
    State(state): State<Arc<AppState>>,
    Path(address): Path<u64>,
    Json(request): Json<SetNetworkingRequest>,
) -> std::result::Result<StatusCode, ApiError> {
    let mut nodes = state.nodes.lock().await;
    let managed = nodes
        .get_mut(&address)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, format!("node {address} not found")))?;
    managed.networking_disabled = request.disabled;
    state.net.set_node_blocked(address, request.disabled).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_blocked_peers(
    State(state): State<Arc<AppState>>,
    Path(address): Path<u64>,
    Json(request): Json<SetBlockedPeersRequest>,
) -> std::result::Result<StatusCode, ApiError> {
    let mut nodes = state.nodes.lock().await;
    let managed = nodes
        .get_mut(&address)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, format!("node {address} not found")))?;
    let next = request
        .blocked_peers
        .into_iter()
        .filter(|peer| *peer != address)
        .collect::<BTreeSet<_>>();

    for peer in managed.blocked_peers.difference(&next) {
        state.net.set_edge_blocked(address, *peer, false).await;
    }
    for peer in next.difference(&managed.blocked_peers) {
        state.net.set_edge_blocked(address, *peer, true).await;
    }

    managed.blocked_peers = next;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_key(
    State(state): State<Arc<AppState>>,
    Path(address): Path<u64>,
    Json(request): Json<SetKeyRequest>,
) -> std::result::Result<StatusCode, ApiError> {
    send_action(
        &state,
        address,
        KvAction::Set {
            key: request.key,
            value: request.value,
        },
    )
    .await
}

async fn delete_key(
    State(state): State<Arc<AppState>>,
    Path(address): Path<u64>,
    Json(request): Json<DeleteKeyRequest>,
) -> std::result::Result<StatusCode, ApiError> {
    send_action(&state, address, KvAction::Delete { key: request.key }).await
}

async fn send_action(
    state: &Arc<AppState>,
    address: u64,
    action: KvAction,
) -> std::result::Result<StatusCode, ApiError> {
    let action_sender = {
        let nodes = state.nodes.lock().await;
        let managed = nodes
            .get(&address)
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, format!("node {address} not found")))?;
        managed
            .node
            .as_ref()
            .map(|runtime| runtime.actions.clone())
            .ok_or_else(|| ApiError::new(StatusCode::CONFLICT, format!("node {address} is offline")))?
    };

    action_sender
        .send(action)
        .await
        .map_err(|_| ApiError::new(StatusCode::CONFLICT, format!("node {address} action queue is closed")))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_runtime(net: &SimulatedNet, address: u64, can_lead: bool, known_peers: &BTreeSet<u64>) -> NodeRuntime {
    let io = net.start_io(address).await;
    let node = NodeState::new(
        address,
        RecoverableState::new(
            address,
            KvStore {
                seq: 1,
                values: BTreeMap::new(),
            },
        ),
        can_lead,
        NetIoSettings::default(),
    )
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

async fn build_snapshot(state: &Arc<AppState>) -> SnapshotResponse {
    let topology = state.net.topology_snapshot().await;
    let nodes = state.nodes.lock().await;
    let mut views = Vec::with_capacity(nodes.len());
    for managed in nodes.values() {
        views.push(build_node_view(managed, &topology).await);
    }
    views.sort_by_key(|node| node.address);
    let connections = build_connections(&views, &topology);

    SnapshotResponse {
        nodes: views,
        connections,
        topology,
    }
}

async fn build_node_view(managed: &ManagedNode, topology: &SimulatedTopologySnapshot) -> NodeView {
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

fn build_connections(views: &[NodeView], topology: &SimulatedTopologySnapshot) -> Vec<ConnectionView> {
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
