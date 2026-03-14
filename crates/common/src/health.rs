use axum::{extract::State, http::StatusCode, routing::get, Router};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct HealthState {
    pub ready: Arc<AtomicBool>,
    pub alive: Arc<AtomicBool>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(false)),
            alive: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl HealthState {
    pub fn set_ready(&self, v: bool) {
        self.ready.store(v, Ordering::SeqCst);
    }

    pub fn set_alive(&self, v: bool) {
        self.alive.store(v, Ordering::SeqCst);
    }
}

async fn healthz(State(state): State<HealthState>) -> StatusCode {
    if state.alive.load(Ordering::SeqCst) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn readyz(State(state): State<HealthState>) -> StatusCode {
    if state.ready.load(Ordering::SeqCst) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

pub async fn serve_health(port: u16, state: HealthState) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    tracing::info!(port, "Health server listening");
    axum::serve(listener, app).await?;
    Ok(())
}
