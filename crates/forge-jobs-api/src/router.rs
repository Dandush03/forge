//! Axum router. Mount via `Router::merge(jobs_api::router::build(storage))`
//! or use it standalone in the `jobs-server` binary.
//!
//! Endpoints live alongside the [`crate::handlers`] functions —
//! each route is a one-line adapter from the handler return to
//! `axum::Json`. The handler does all the work.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use forge_jobs::Storage;

use crate::Error;
use crate::dto::{QueueOverviewDto, SetBackoffRequest, StorageInfoDto};
use crate::handlers;

/// Build the queue API router. Pass the shared `Storage` bundle in;
/// it's cloned into request handlers via Axum's `State` extractor.
///
/// # Security
///
/// **The returned router is unauthenticated.** Some routes mutate
/// state (`POST /queue/{name}/backoff`). Mount behind your own auth
/// middleware (`Router::nest(...).layer(auth_layer)`) **or** bind
/// the resulting `axum::serve` to `127.0.0.1`. Do not bind to
/// `0.0.0.0` in production without a layer of authentication first.
///
/// The router also has no `DefaultBodyLimit`, no `CorsLayer`, and
/// no rate limiting — apply those as layers at your mount point if
/// you need them.
pub fn build(storage: Arc<Storage>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_route))
        .route("/storage/info", get(storage_info_route))
        .route("/queue/overview", get(queue_overview_route))
        .route("/queue/{name}/backoff", post(queue_set_backoff_route))
        .with_state(storage)
}

/// `GET /metrics` — Prometheus exposition. Plain text, one gauge block
/// per metric. Scrape target for Prometheus/HPA; KEDA can alternatively
/// query the DB directly (see docs/deploy.md).
async fn metrics_route(State(storage): State<Arc<Storage>>) -> Result<Response, Error> {
    let body = crate::metrics::render(&storage).await?;
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response())
}

/// `GET /health` — liveness check for k8s readiness probes. Static
/// `"ok"` text; doesn't touch storage. If you want to also probe the
/// DB connection, hit `/storage/info` instead — that exercises a
/// real query.
async fn health() -> &'static str {
    "ok"
}

async fn storage_info_route(
    State(storage): State<Arc<Storage>>,
) -> Result<Json<StorageInfoDto>, Error> {
    handlers::storage_info(&storage).await.map(Json)
}

async fn queue_overview_route(
    State(storage): State<Arc<Storage>>,
) -> Result<Json<Vec<QueueOverviewDto>>, Error> {
    handlers::queue_overview(&storage).await.map(Json)
}

async fn queue_set_backoff_route(
    State(storage): State<Arc<Storage>>,
    Path(name): Path<String>,
    Json(body): Json<SetBackoffRequest>,
) -> Result<(), Error> {
    handlers::queue_set_backoff(
        &storage,
        &name,
        body.enabled,
        body.base_seconds,
        body.max_seconds,
    )
    .await
}
