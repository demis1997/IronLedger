//! # ironledger-api-http
//!
//! Operational HTTP surface: liveness, readiness, Prometheus metrics and
//! administrative queries. Administrative routes require an [`Authorizer`];
//! the bundled [`DevTokenAuthorizer`] is for local development only.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod auth;

use auth::{AuthError, Authorizer};
use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use ironledger_ledger::{ConsumerRepo, LedgerService, Page};
// ConsumerRepo is used for `PostgresStore::status`.
use ironledger_reconciler::{ReconcileReport, Reconciler};
use ironledger_storage_postgres::{PostgresBalanceViews, PostgresHistory, PostgresStore};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use std::sync::Arc;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

/// Shared HTTP state.
pub struct HttpState {
    /// Command handlers.
    pub ledger: Arc<LedgerService>,
    /// Storage (outbox + consumers).
    pub store: Arc<PostgresStore>,
    /// Reconciliation engine.
    pub reconciler: Arc<Reconciler<PostgresHistory, PostgresBalanceViews>>,
    /// Prometheus recorder handle.
    pub metrics: PrometheusHandle,
    /// Admin auth.
    pub auth: Arc<dyn Authorizer>,
}

/// Build the Axum router.
pub fn router(state: Arc<HttpState>) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/admin/outbox", get(admin_outbox))
        .route("/admin/reconciliation", get(admin_reconciliation))
        .route("/admin/consumers", get(admin_consumers))
        .layer(RequestBodyLimitLayer::new(64 * 1024))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            std::time::Duration::from_secs(10),
        ))
        .with_state(state)
}

async fn live() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "live" }))
}

async fn ready(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    use ironledger_ledger::HealthCheck;
    if let Err(err) = state.store.ping().await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "not_ready",
                "dependency": state.store.name(),
                "error": err.to_string(),
            })),
        );
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "ready" })),
    )
}

async fn metrics(State(state): State<Arc<HttpState>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

async fn authorize_admin(state: &HttpState, headers: &HeaderMap) -> Result<(), Response> {
    state
        .auth
        .authorize(bearer_token(headers))
        .await
        .map_err(|_: AuthError| {
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            )
                .into_response()
        })
}

#[derive(Debug, Serialize)]
struct OutboxPendingRow {
    id: i64,
    event_id: String,
    topic: String,
    attempts: i32,
    created_at: String,
}

#[derive(Debug, Serialize)]
struct OutboxResponse {
    stats: ironledger_ledger::OutboxStats,
    pending: Vec<OutboxPendingRow>,
}

async fn admin_outbox(
    State(state): State<Arc<HttpState>>,
    headers: HeaderMap,
) -> Result<Json<OutboxResponse>, Response> {
    authorize_admin(&state, &headers).await?;
    let stats = state.ledger.outbox_stats().await.map_err(internal)?;
    let pending = state
        .ledger
        .pending_outbox(Page::new(25, 0))
        .await
        .map_err(internal)?
        .into_iter()
        .map(|row| OutboxPendingRow {
            id: row.id,
            event_id: row.event.event_id.to_string(),
            topic: row.topic,
            attempts: row.attempts,
            created_at: row.created_at.to_rfc3339(),
        })
        .collect();
    Ok(Json(OutboxResponse { stats, pending }))
}

async fn admin_reconciliation(
    State(state): State<Arc<HttpState>>,
    headers: HeaderMap,
) -> Result<Json<ReconcileReport>, Response> {
    authorize_admin(&state, &headers).await?;
    let report = state
        .reconciler
        .reconcile_authoritative()
        .await
        .map_err(internal)?;
    Ok(Json(report))
}

async fn admin_consumers(
    State(state): State<Arc<HttpState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ironledger_ledger::ConsumerStatus>>, Response> {
    authorize_admin(&state, &headers).await?;
    let status = state.store.status().await.map_err(internal)?;
    Ok(Json(status))
}

fn internal(err: impl std::fmt::Display) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": err.to_string() })),
    )
        .into_response()
}

/// Install the global Prometheus recorder and return its handle.
pub fn install_metrics() -> PrometheusHandle {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("prometheus recorder")
}
