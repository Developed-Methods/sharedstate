use std::{
    collections::BTreeMap,
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
    net::message_channel::NetIoSettings,
    state::{determinstic_state::DeterministicState, recoverable_state::RecoverableState},
    test_orchestrator::{
        AddNodeRequest, NodeView, OrchestratorError, OrchestratorRecording, OrchestratorSnapshot,
        SetBlockedPeersRequest, SetNetworkingRequest, SharedStateTestOrchestrator, SharedStateTestOrchestratorConfig,
    },
};
use tower_http::{cors::CorsLayer, services::ServeDir};

type AppState = Arc<SharedStateTestOrchestrator<KvStore>>;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,

    #[arg(long, default_value = "examples/kv-web/dist")]
    static_dir: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct KvStore {
    seq: u64,
    values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "type")]
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

struct ApiError(OrchestratorError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            OrchestratorError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            OrchestratorError::NotFound(_) => StatusCode::NOT_FOUND,
            OrchestratorError::Conflict(_) | OrchestratorError::ActionQueueClosed { .. } => StatusCode::CONFLICT,
        };
        (
            status,
            Json(ErrorBody {
                error: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

impl From<OrchestratorError> for ApiError {
    fn from(value: OrchestratorError) -> Self {
        Self(value)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_writer(std::io::stderr).try_init();
    let args = Args::parse();
    let state = Arc::new(SharedStateTestOrchestrator::new(SharedStateTestOrchestratorConfig {
        io_settings: NetIoSettings::default(),
        initial_state: Arc::new(|address| {
            RecoverableState::new(
                address,
                KvStore {
                    seq: 1,
                    values: BTreeMap::new(),
                },
            )
        }),
    }));

    let app = Router::new()
        .route("/api/snapshot", get(snapshot))
        .route("/api/recording", get(recording))
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

async fn snapshot(State(state): State<AppState>) -> Json<OrchestratorSnapshot<KvStore>> {
    Json(state.snapshot().await)
}

async fn recording(State(state): State<AppState>) -> Json<OrchestratorRecording<KvStore>> {
    Json(state.recording().await)
}

async fn node_details(
    State(state): State<AppState>,
    Path(address): Path<u64>,
) -> std::result::Result<Json<NodeView<KvStore>>, ApiError> {
    Ok(Json(state.node_details(address).await?))
}

async fn add_node(
    State(state): State<AppState>,
    Json(request): Json<AddNodeRequest>,
) -> std::result::Result<Json<NodeView<KvStore>>, ApiError> {
    Ok(Json(state.add_node(request).await?))
}

async fn stop_node(
    State(state): State<AppState>,
    Path(address): Path<u64>,
) -> std::result::Result<StatusCode, ApiError> {
    state.stop_node(address).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn start_node(
    State(state): State<AppState>,
    Path(address): Path<u64>,
) -> std::result::Result<StatusCode, ApiError> {
    state.start_node(address).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_networking(
    State(state): State<AppState>,
    Path(address): Path<u64>,
    Json(request): Json<SetNetworkingRequest>,
) -> std::result::Result<StatusCode, ApiError> {
    state.set_networking(address, request).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_blocked_peers(
    State(state): State<AppState>,
    Path(address): Path<u64>,
    Json(request): Json<SetBlockedPeersRequest>,
) -> std::result::Result<StatusCode, ApiError> {
    state.set_blocked_peers(address, request).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_key(
    State(state): State<AppState>,
    Path(address): Path<u64>,
    Json(request): Json<SetKeyRequest>,
) -> std::result::Result<StatusCode, ApiError> {
    state
        .send_action(
            address,
            KvAction::Set {
                key: request.key,
                value: request.value,
            },
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_key(
    State(state): State<AppState>,
    Path(address): Path<u64>,
    Json(request): Json<DeleteKeyRequest>,
) -> std::result::Result<StatusCode, ApiError> {
    state
        .send_action(address, KvAction::Delete { key: request.key })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
