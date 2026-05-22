use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    node::{SimError, Simulation},
    state::WebTestAction,
};

type AppState = Arc<Mutex<Simulation>>;

pub fn router(simulation: AppState) -> Router {
    Router::new()
        .route("/api/status", get(status))
        .route("/api/reset", post(reset))
        .route("/api/nodes/start-all", post(start_all))
        .route("/api/nodes/stop-all", post(stop_all))
        .route("/api/nodes/network", post(set_all_networking))
        .route("/api/nodes/{id}/start", post(start_node))
        .route("/api/nodes/{id}/stop", post(stop_node))
        .route("/api/nodes/{id}/network", post(set_node_networking))
        .route("/api/links", post(set_link))
        .route("/api/actions", post(send_action))
        .with_state(simulation)
}

async fn status(State(sim): State<AppState>) -> impl IntoResponse {
    Json(sim.lock().await.snapshot())
}

async fn reset(State(sim): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    sim.lock().await.reset().await;
    Ok(Json(OkBody::ok()))
}

async fn start_all(State(sim): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    sim.lock().await.start_all().await;
    Ok(Json(OkBody::ok()))
}

async fn stop_all(State(sim): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    sim.lock().await.stop_all().await;
    Ok(Json(OkBody::ok()))
}

async fn start_node(
    State(sim): State<AppState>,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    sim.lock().await.start_node(id).await.map_err(ApiError)?;
    Ok(Json(OkBody::ok()))
}

async fn stop_node(
    State(sim): State<AppState>,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, ApiError> {
    sim.lock().await.stop_node(id).await.map_err(ApiError)?;
    Ok(Json(OkBody::ok()))
}

async fn set_node_networking(
    State(sim): State<AppState>,
    Path(id): Path<u64>,
    Json(req): Json<NetworkRequest>,
) -> Result<impl IntoResponse, ApiError> {
    sim.lock()
        .await
        .set_node_networking(id, req.enabled)
        .map_err(ApiError)?;
    Ok(Json(OkBody::ok()))
}

async fn set_all_networking(
    State(sim): State<AppState>,
    Json(req): Json<NetworkRequest>,
) -> Result<impl IntoResponse, ApiError> {
    sim.lock().await.set_all_networking(req.enabled);
    Ok(Json(OkBody::ok()))
}

async fn set_link(
    State(sim): State<AppState>,
    Json(req): Json<LinkRequest>,
) -> Result<impl IntoResponse, ApiError> {
    sim.lock()
        .await
        .set_link(req.a, req.b, req.enabled, req.latency_ms)
        .map_err(ApiError)?;
    Ok(Json(OkBody::ok()))
}

async fn send_action(
    State(sim): State<AppState>,
    Json(req): Json<ActionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    sim.lock()
        .await
        .send_action(req.node, req.action)
        .await
        .map_err(ApiError)?;
    Ok(Json(OkBody::ok()))
}

#[derive(Deserialize)]
struct NetworkRequest {
    enabled: bool,
}

#[derive(Deserialize)]
struct LinkRequest {
    a: u64,
    b: u64,
    enabled: bool,
    latency_ms: u64,
}

#[derive(Deserialize)]
struct ActionRequest {
    node: u64,
    action: WebTestAction,
}

#[derive(Serialize)]
struct OkBody {
    ok: bool,
}

impl OkBody {
    fn ok() -> Self {
        Self { ok: true }
    }
}

pub struct ApiError(SimError);

#[derive(Serialize)]
struct ApiErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.0.status;
        let body = Json(ApiErrorBody {
            error: self.0.message,
        });
        (status, body).into_response()
    }
}

impl From<StatusCode> for ApiError {
    fn from(status: StatusCode) -> Self {
        Self(SimError {
            status,
            message: status.to_string(),
        })
    }
}
