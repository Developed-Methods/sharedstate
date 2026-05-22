mod api;
mod network;
mod node;
mod state;
mod ui;

use std::{net::SocketAddr, sync::Arc};

use axum::Router;
use node::Simulation;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let simulation = Arc::new(Mutex::new(Simulation::new().await));
    simulation.lock().await.start_all().await;

    let app = Router::new()
        .merge(api::router(simulation))
        .merge(ui::router());

    let addr = SocketAddr::from(([127, 0, 0, 1], 3030));
    tracing::info!(%addr, "starting sharedstate webtester");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind webtester");
    axum::serve(listener, app).await.expect("webtester failed");
}
