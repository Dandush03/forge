//! Axum router. Mount via `Router::merge(jobs_api::router::build(storage))`
//! or use it standalone in the `jobs-server` binary.
//!
//! Each route is a one-line adapter from an extractor to a
//! [`crate::handlers`] function — the handler does all the work, so the
//! Tauri plugin and the HTTP transport share one implementation.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use forge_jobs::Storage;

use crate::Error;
use crate::dto;
use crate::handlers;

/// Build the queue API router. Pass the shared `Storage` bundle in;
/// it's cloned into request handlers via Axum's `State` extractor.
///
/// # Security
///
/// **The returned router is unauthenticated.** Many routes mutate
/// state. Mount behind your own auth middleware
/// (`Router::nest(...).layer(auth_layer)`) **or** bind the resulting
/// `axum::serve` to `127.0.0.1`. Do not bind to `0.0.0.0` in production
/// without authentication first.
///
/// The router also has no `DefaultBodyLimit`, no `CorsLayer`, and no
/// rate limiting — apply those as layers at your mount point.
pub fn build(storage: Arc<Storage>) -> Router {
    Router::new()
        // ops
        .route("/health", get(health))
        .route("/metrics", get(metrics_route))
        .route("/storage/info", get(storage_info_route))
        // queue reads
        .route("/queue/overview", get(queue_overview_route))
        .route("/queue/processes", get(queue_processes_route))
        .route("/queue/workers", get(queue_workers_route))
        .route("/queue/timeline", get(queue_timeline_route))
        .route("/queue/metric-series", get(queue_metric_series_route))
        .route("/queue/resource-series", get(queue_resource_series_route))
        .route("/queue/db-series", get(queue_db_series_route))
        // queue mutations
        .route("/queue/cleanup", post(queue_cleanup_route))
        .route(
            "/queue/{name}/max-workers",
            post(queue_set_max_workers_route),
        )
        .route("/queue/{name}/paused", post(queue_set_paused_route))
        .route("/queue/{name}/retention", post(queue_set_retention_route))
        .route("/queue/{name}/backoff", post(queue_set_backoff_route))
        // jobs reads
        .route("/jobs/list", post(jobs_list_route))
        .route("/jobs/failed", get(jobs_failed_route))
        .route("/jobs/kinds", get(jobs_kinds_route))
        .route("/jobs/scheduled", get(jobs_scheduled_route))
        .route("/jobs/inspect/{id}", get(job_inspect_route))
        // jobs mutations
        .route("/jobs/enqueue", post(jobs_enqueue_route))
        .route("/jobs/enqueue-demo", post(jobs_enqueue_demo_route))
        .route("/jobs/run-now/{id}", post(jobs_run_now_route))
        .route("/jobs/retry", post(jobs_retry_route))
        .route("/jobs/retry-all-failed", post(jobs_retry_all_failed_route))
        .route(
            "/jobs/retry-all-by-status",
            post(jobs_retry_all_by_status_route),
        )
        .route("/jobs/delete", post(jobs_delete_route))
        .route("/jobs/requeue", post(jobs_requeue_route))
        .route(
            "/jobs/delete-done-older-than",
            post(jobs_delete_done_older_than_route),
        )
        .route("/jobs/delete-by-status", post(jobs_delete_by_status_route))
        // cron
        .route("/cron", get(cron_list_route))
        .route("/cron/{name}/enabled", post(cron_set_enabled_route))
        .route("/cron/{name}/expr", post(cron_set_expr_route))
        .route("/cron/{name}/dedupe", post(cron_set_dedupe_route))
        .route("/cron/{name}/trigger", post(cron_trigger_now_route))
        .with_state(storage)
}

// ── ops ──────────────────────────────────────────────────────────────

/// `GET /metrics` — Prometheus exposition.
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

/// `GET /health` — static liveness check; doesn't touch storage.
async fn health() -> &'static str {
    "ok"
}

async fn storage_info_route(
    State(storage): State<Arc<Storage>>,
) -> Result<Json<dto::StorageInfoDto>, Error> {
    handlers::storage_info(&storage).await.map(Json)
}

// ── queue reads ──────────────────────────────────────────────────────

async fn queue_overview_route(
    State(storage): State<Arc<Storage>>,
) -> Result<Json<Vec<dto::QueueOverviewDto>>, Error> {
    handlers::queue_overview(&storage).await.map(Json)
}

async fn queue_processes_route(
    State(storage): State<Arc<Storage>>,
    Query(q): Query<dto::ProcessesQuery>,
) -> Result<Json<Vec<dto::QueueProcessDto>>, Error> {
    handlers::queue_processes(&storage, q.queue_name.as_deref())
        .await
        .map(Json)
}

async fn queue_workers_route(
    State(storage): State<Arc<Storage>>,
) -> Result<Json<dto::WorkersOverviewDto>, Error> {
    handlers::queue_workers(&storage).await.map(Json)
}

async fn queue_timeline_route(
    State(storage): State<Arc<Storage>>,
    Query(q): Query<dto::TimelineQuery>,
) -> Result<Json<Vec<dto::TimelineBucket>>, Error> {
    handlers::queue_timeline_range(&storage, q.from, q.to, q.bucket_secs)
        .await
        .map(Json)
}

async fn queue_metric_series_route(
    State(storage): State<Arc<Storage>>,
    Query(q): Query<dto::MetricSeriesQuery>,
) -> Result<Json<Vec<dto::MetricSeriesBucket>>, Error> {
    handlers::queue_metric_series(&storage, &q.queue, q.from, q.to, q.bucket_secs)
        .await
        .map(Json)
}

async fn queue_resource_series_route(
    State(storage): State<Arc<Storage>>,
    Query(q): Query<dto::SeriesQuery>,
) -> Result<Json<Vec<dto::ResourceHostSeries>>, Error> {
    handlers::queue_resource_series(&storage, q.from, q.to, q.bucket_secs)
        .await
        .map(Json)
}

async fn queue_db_series_route(
    State(storage): State<Arc<Storage>>,
    Query(q): Query<dto::SeriesQuery>,
) -> Result<Json<Vec<dto::DbHealthHostSeries>>, Error> {
    handlers::queue_db_series(&storage, q.from, q.to, q.bucket_secs)
        .await
        .map(Json)
}

// ── queue mutations ──────────────────────────────────────────────────

async fn queue_cleanup_route(
    State(storage): State<Arc<Storage>>,
) -> Result<Json<dto::CleanupReportDto>, Error> {
    handlers::queue_cleanup_now(&storage).await.map(Json)
}

async fn queue_set_max_workers_route(
    State(storage): State<Arc<Storage>>,
    Path(name): Path<String>,
    Json(body): Json<dto::SetMaxWorkersRequest>,
) -> Result<(), Error> {
    handlers::queue_set_max_workers(&storage, &name, body.n).await
}

async fn queue_set_paused_route(
    State(storage): State<Arc<Storage>>,
    Path(name): Path<String>,
    Json(body): Json<dto::SetPausedRequest>,
) -> Result<(), Error> {
    handlers::queue_set_paused(&storage, &name, body.paused).await
}

async fn queue_set_retention_route(
    State(storage): State<Arc<Storage>>,
    Path(name): Path<String>,
    Json(body): Json<dto::SetRetentionRequest>,
) -> Result<(), Error> {
    handlers::queue_set_retention(&storage, &name, body.done_days, body.dead_days).await
}

async fn queue_set_backoff_route(
    State(storage): State<Arc<Storage>>,
    Path(name): Path<String>,
    Json(body): Json<dto::SetBackoffRequest>,
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

// ── jobs reads ───────────────────────────────────────────────────────

async fn jobs_list_route(
    State(storage): State<Arc<Storage>>,
    Json(args): Json<dto::JobsListArgs>,
) -> Result<Json<dto::JobsPageDto>, Error> {
    handlers::jobs_list(&storage, args).await.map(Json)
}

async fn jobs_failed_route(
    State(storage): State<Arc<Storage>>,
    Query(q): Query<dto::FailedQuery>,
) -> Result<Json<Vec<dto::JobRowDto>>, Error> {
    handlers::jobs_failed(&storage, q.limit).await.map(Json)
}

async fn jobs_kinds_route(
    State(storage): State<Arc<Storage>>,
    Query(q): Query<dto::KindsQuery>,
) -> Result<Json<Vec<String>>, Error> {
    handlers::jobs_kinds(&storage, q.queue_name.as_deref())
        .await
        .map(Json)
}

async fn jobs_scheduled_route(
    State(storage): State<Arc<Storage>>,
    Query(q): Query<dto::ScheduledQuery>,
) -> Result<Json<Vec<dto::JobRowDto>>, Error> {
    handlers::jobs_scheduled(&storage, q.queue_name.as_deref())
        .await
        .map(Json)
}

async fn job_inspect_route(
    State(storage): State<Arc<Storage>>,
    Path(id): Path<String>,
) -> Result<Json<dto::JobInspectDto>, Error> {
    handlers::job_inspect(&storage, &id).await.map(Json)
}

// ── jobs mutations ───────────────────────────────────────────────────

async fn jobs_enqueue_route(
    State(storage): State<Arc<Storage>>,
    Json(body): Json<dto::JobsEnqueueRequest>,
) -> Result<Json<String>, Error> {
    handlers::jobs_enqueue(&storage, body).await.map(Json)
}

async fn jobs_enqueue_demo_route(
    State(storage): State<Arc<Storage>>,
    Json(body): Json<dto::EnqueueDemoRequest>,
) -> Result<Json<String>, Error> {
    handlers::queue_enqueue_demo(&storage, body.payload)
        .await
        .map(Json)
}

async fn jobs_run_now_route(
    State(storage): State<Arc<Storage>>,
    Path(id): Path<String>,
) -> Result<Json<bool>, Error> {
    handlers::jobs_run_now(&storage, &id).await.map(Json)
}

async fn jobs_retry_route(
    State(storage): State<Arc<Storage>>,
    Json(body): Json<dto::IdsRequest>,
) -> Result<Json<u64>, Error> {
    handlers::jobs_retry(&storage, &body.ids).await.map(Json)
}

async fn jobs_retry_all_failed_route(
    State(storage): State<Arc<Storage>>,
) -> Result<Json<u64>, Error> {
    handlers::jobs_retry_all_failed(&storage).await.map(Json)
}

async fn jobs_retry_all_by_status_route(
    State(storage): State<Arc<Storage>>,
    Json(body): Json<dto::StatusRequest>,
) -> Result<Json<u64>, Error> {
    handlers::jobs_retry_all_by_status(&storage, &body.status)
        .await
        .map(Json)
}

async fn jobs_delete_route(
    State(storage): State<Arc<Storage>>,
    Json(body): Json<dto::IdsRequest>,
) -> Result<Json<u64>, Error> {
    handlers::jobs_delete(&storage, &body.ids).await.map(Json)
}

async fn jobs_requeue_route(
    State(storage): State<Arc<Storage>>,
    Json(body): Json<dto::IdsRequest>,
) -> Result<Json<u64>, Error> {
    handlers::jobs_requeue(&storage, &body.ids).await.map(Json)
}

async fn jobs_delete_done_older_than_route(
    State(storage): State<Arc<Storage>>,
    Json(body): Json<dto::DeleteDoneOlderThanRequest>,
) -> Result<Json<u64>, Error> {
    handlers::jobs_delete_done_older_than(&storage, body.days, body.queue_name.as_deref())
        .await
        .map(Json)
}

async fn jobs_delete_by_status_route(
    State(storage): State<Arc<Storage>>,
    Json(body): Json<dto::DeleteByStatusRequest>,
) -> Result<Json<u64>, Error> {
    handlers::jobs_delete_by_status(&storage, &body.status, body.queue_name.as_deref())
        .await
        .map(Json)
}

// ── cron ─────────────────────────────────────────────────────────────

async fn cron_list_route(
    State(storage): State<Arc<Storage>>,
) -> Result<Json<Vec<dto::CronScheduleDto>>, Error> {
    handlers::cron_list(&storage).await.map(Json)
}

async fn cron_set_enabled_route(
    State(storage): State<Arc<Storage>>,
    Path(name): Path<String>,
    Json(body): Json<dto::CronSetEnabledRequest>,
) -> Result<(), Error> {
    handlers::cron_set_enabled(&storage, &name, body.enabled).await
}

async fn cron_set_expr_route(
    State(storage): State<Arc<Storage>>,
    Path(name): Path<String>,
    Json(body): Json<dto::CronSetExprRequest>,
) -> Result<(), Error> {
    handlers::cron_set_expr(&storage, &name, &body.expr).await
}

async fn cron_set_dedupe_route(
    State(storage): State<Arc<Storage>>,
    Path(name): Path<String>,
    Json(body): Json<dto::CronSetDedupeRequest>,
) -> Result<(), Error> {
    handlers::cron_set_dedupe(&storage, &name, body.dedupe).await
}

async fn cron_trigger_now_route(
    State(storage): State<Arc<Storage>>,
    Path(name): Path<String>,
) -> Result<Json<String>, Error> {
    handlers::cron_trigger_now(&storage, &name).await.map(Json)
}
