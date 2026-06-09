//! Request/response shapes shared by the Tauri plugin and the
//! HTTP transport.
//!
//! The DTOs here are the single source of truth for the queue wire
//! format. The Tauri plugin (`tauri-plugin-queue`) re-exports them and
//! the HTTP routes serialize them, so the Leptos panel sees identical
//! JSON whichever transport it talks to.

use chrono::{DateTime, Utc};
use forge_jobs::{JobRecord, ProcessRecord, QueueConfigRow, QueueCounts};
use serde::{Deserialize, Serialize};

/// One queue's snapshot for the Mission Control overview card.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueueOverviewDto {
    pub name: String,
    pub paused: bool,
    pub max_workers: i32,
    pub counts: StatusCountsDto,
    pub processes: Vec<QueueProcessDto>,
    pub retain_done_days: u32,
    pub retain_dead_days: u32,
    pub backoff_enabled: bool,
    pub backoff_base_seconds: u32,
    pub backoff_max_seconds: u32,
    /// Cool-down deadline while the queue is throttled; `None` otherwise.
    pub throttled_until: Option<DateTime<Utc>>,
    /// Age (seconds) of the oldest ready job — the queue lag.
    pub oldest_pending_age_seconds: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StatusCountsDto {
    pub pending: u64,
    pub scheduled: u64,
    pub in_progress: u64,
    pub done: u64,
    pub failed: u64,
    pub dead: u64,
}

impl From<QueueCounts> for StatusCountsDto {
    fn from(c: QueueCounts) -> Self {
        Self {
            pending: c.pending,
            scheduled: c.scheduled,
            in_progress: c.in_progress,
            done: c.done,
            failed: c.failed,
            dead: c.dead,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueueProcessDto {
    pub process_id: String,
    pub queue_name: String,
    pub host_id: String,
    pub started_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    pub current_job_id: Option<String>,
}

impl From<ProcessRecord> for QueueProcessDto {
    fn from(p: ProcessRecord) -> Self {
        Self {
            process_id: p.process_id,
            queue_name: p.queue_name,
            host_id: p.host_id,
            started_at: p.started_at,
            heartbeat_at: p.heartbeat_at,
            current_job_id: p.current_job.map(|id| id.as_str().to_owned()),
        }
    }
}

/// Helper: build a `QueueOverviewDto` from the storage-layer pieces.
/// Lives here (rather than `From<>`) because building it needs three
/// independent storage calls — the handler does the orchestration.
#[must_use]
pub fn overview_dto(
    cfg: QueueConfigRow,
    counts: QueueCounts,
    processes: Vec<ProcessRecord>,
    oldest_pending_age_seconds: u64,
) -> QueueOverviewDto {
    QueueOverviewDto {
        name: cfg.name,
        paused: cfg.paused,
        max_workers: cfg.max_workers,
        counts: counts.into(),
        processes: processes.into_iter().map(Into::into).collect(),
        retain_done_days: u32::try_from(cfg.retain_done_for_days).unwrap_or(7),
        retain_dead_days: u32::try_from(cfg.retain_dead_for_days).unwrap_or(30),
        backoff_enabled: cfg.backoff_enabled,
        backoff_base_seconds: u32::try_from(cfg.backoff_base_seconds).unwrap_or(60),
        backoff_max_seconds: u32::try_from(cfg.backoff_max_seconds).unwrap_or(1800),
        throttled_until: cfg.throttled_until,
        oldest_pending_age_seconds,
    }
}

/// `GET /storage/info` response — surfaces the backend's
/// `describe()` output. Useful for ops checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StorageInfoDto {
    pub backend: String,
    pub fields: Vec<(String, String)>,
}

impl From<forge_jobs::StorageInfo> for StorageInfoDto {
    fn from(info: forge_jobs::StorageInfo) -> Self {
        Self {
            backend: info.backend,
            fields: info.fields,
        }
    }
}

// ── job rows ─────────────────────────────────────────────────────────

/// One job row for the Jobs / Scheduled / Retries / Dead tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobRowDto {
    pub id: String,
    pub queue_name: String,
    pub kind: String,
    pub status: String,
    pub priority: i32,
    pub attempts: i32,
    pub max_attempts: i32,
    pub enqueued_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub process_id: Option<String>,
    pub dedupe_key: Option<String>,
    pub heartbeat_at: Option<DateTime<Utc>>,
}

impl From<&JobRecord> for JobRowDto {
    fn from(row: &JobRecord) -> Self {
        Self {
            id: row.id.as_str().to_owned(),
            queue_name: row.queue_name.clone(),
            kind: row.kind.clone(),
            status: row.status.as_str().to_owned(),
            priority: row.priority,
            attempts: row.attempts,
            max_attempts: row.max_attempts,
            enqueued_at: row.enqueued_at,
            scheduled_at: row.scheduled_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
            last_error: row.last_error.clone(),
            process_id: row.process_id.clone(),
            dedupe_key: row.dedupe_key.clone(),
            heartbeat_at: row.heartbeat_at,
        }
    }
}

/// A single job plus its decoded payload and error history.
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobInspectDto {
    pub row: JobRowDto,
    pub payload: serde_json::Value,
    pub error_history: Vec<serde_json::Value>,
}

/// Filter for [`crate::handlers::jobs_list`]. All fields optional;
/// queue / kind / payload-search are applied in Rust over the rows
/// the status query returns (keeps the storage trait simple).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobsFilterDto {
    #[serde(default)]
    pub queues: Vec<String>,
    #[serde(default)]
    pub kinds: Vec<String>,
    #[serde(default)]
    pub statuses: Vec<String>,
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
    #[serde(default)]
    pub payload_search: Option<String>,
}

/// `POST /jobs/list` body — filter plus pagination window.
#[derive(Debug, Default, Deserialize)]
pub struct JobsListArgs {
    #[serde(default)]
    pub filter: JobsFilterDto,
    pub limit: u32,
    pub offset: u32,
}

/// One page of [`JobRowDto`] plus the total matched count.
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobsPageDto {
    pub rows: Vec<JobRowDto>,
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
}

// ── timeline + series buckets ────────────────────────────────────────

/// One bucket of the workload timeline (counts + latency percentiles).
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TimelineBucket {
    pub at: DateTime<Utc>,
    pub enqueued: u64,
    pub started: u64,
    pub retried: u64,
    pub completed: u64,
    pub failed: u64,
    pub processing_p50_ms: u64,
    pub processing_p95_ms: u64,
    pub processing_p99_ms: u64,
    pub total_p50_ms: u64,
    pub total_p95_ms: u64,
    pub total_p99_ms: u64,
}

/// One bucket of a per-queue throughput + latency series.
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MetricSeriesBucket {
    pub at: DateTime<Utc>,
    pub enqueued: u64,
    pub completed: u64,
    pub failed: u64,
    pub proc_p50_ms: u64,
    pub proc_p95_ms: u64,
    pub proc_p99_ms: u64,
    pub total_p50_ms: u64,
    pub total_p95_ms: u64,
    pub total_p99_ms: u64,
}

/// One bucket of a single pod's resource series.
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ResourceBucket {
    pub at: DateTime<Utc>,
    /// CPU normalized to % of all cores (100 = whole box maxed).
    pub cpu_pct: f64,
    pub rss_bytes: u64,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
    pub disk_used_pct: f64,
}

/// Resource series for one pod (`host_id`).
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ResourceHostSeries {
    pub host: String,
    pub buckets: Vec<ResourceBucket>,
}

/// One bucket of a single pod's DB-health series.
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DbHealthBucket {
    pub at: DateTime<Utc>,
    pub read_p50_ms: u64,
    pub read_p95_ms: u64,
    pub read_p99_ms: u64,
    pub reads_per_min: u64,
    pub write_p50_ms: u64,
    pub write_p95_ms: u64,
    pub write_p99_ms: u64,
    pub writes_per_min: u64,
    pub pool_active: u64,
    pub pool_idle: u64,
    pub pool_max: u64,
    pub pool_used_pct: f64,
    pub db_size_bytes: u64,
    pub wal_bytes: u64,
}

/// DB-health series for one pod (`host_id`).
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DbHealthHostSeries {
    pub host: String,
    pub buckets: Vec<DbHealthBucket>,
}

// ── cron + cleanup ───────────────────────────────────────────────────

/// One cron schedule row for the Cron tab.
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CronScheduleDto {
    pub name: String,
    pub kind: String,
    pub payload: serde_json::Value,
    pub queue_name: Option<String>,
    pub cron_expr: String,
    pub enabled: bool,
    pub max_attempts: Option<i32>,
    pub last_fired_at: Option<DateTime<Utc>>,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<forge_jobs::CronScheduleRecord> for CronScheduleDto {
    fn from(r: forge_jobs::CronScheduleRecord) -> Self {
        Self {
            name: r.name,
            kind: r.kind,
            payload: r.payload,
            queue_name: r.queue_name,
            cron_expr: r.cron_expr,
            enabled: r.enabled,
            max_attempts: r.max_attempts,
            last_fired_at: r.last_fired_at,
            next_fire_at: r.next_fire_at,
            last_error: r.last_error,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

/// Result of a manual `queue_cleanup_now`.
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CleanupReportDto {
    pub done_deleted: u64,
    pub dead_deleted: u64,
}

/// Every status the schema understands, in display order. Lets the
/// panel populate the status filter without an extra round-trip.
pub const JOB_STATUSES: &[&str] = &["pending", "in_progress", "done", "failed", "dead"];

// ── request bodies (HTTP transport) ──────────────────────────────────

/// Request body for `POST /queue/{name}/backoff`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetBackoffRequest {
    pub enabled: bool,
    pub base_seconds: i32,
    pub max_seconds: i32,
}

/// Request body for `POST /queue/{name}/max-workers`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetMaxWorkersRequest {
    pub n: i32,
}

/// Request body for `POST /queue/{name}/paused`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetPausedRequest {
    pub paused: bool,
}

/// Request body for `POST /queue/{name}/retention`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetRetentionRequest {
    pub done_days: i32,
    pub dead_days: i32,
}

/// Request body for the bulk id ops (`retry` / `delete` / `requeue`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdsRequest {
    pub ids: Vec<String>,
}

/// Request body for `retry-all-by-status` (queue-wide; no scope).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusRequest {
    pub status: String,
}

/// Request body for `POST /jobs/delete-by-status` — optional queue scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteByStatusRequest {
    pub status: String,
    #[serde(default)]
    pub queue_name: Option<String>,
}

/// Request body for `POST /jobs/delete-done-older-than` — optional
/// queue scope (`None` = every queue).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteDoneOlderThanRequest {
    pub days: u32,
    #[serde(default)]
    pub queue_name: Option<String>,
}

/// Request body for `POST /cron/{name}/enabled`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronSetEnabledRequest {
    pub enabled: bool,
}

/// Request body for `POST /cron/{name}/expr`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronSetExprRequest {
    pub expr: String,
}

/// Request body for `POST /jobs/enqueue` — the generic typed enqueue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsEnqueueRequest {
    pub kind: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub queue_name: Option<String>,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub max_attempts: Option<i32>,
}

/// Request body for `POST /jobs/enqueue-demo`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnqueueDemoRequest {
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

// ── query strings (HTTP GET transport) ───────────────────────────────

/// Query for `GET /queue/processes`.
#[derive(Debug, Clone, Deserialize)]
pub struct ProcessesQuery {
    #[serde(default)]
    pub queue_name: Option<String>,
}

/// Query for `GET /jobs/kinds` — optional queue scope.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct KindsQuery {
    #[serde(default)]
    pub queue_name: Option<String>,
}

/// Query for `GET /jobs/failed`.
#[derive(Debug, Clone, Deserialize)]
pub struct FailedQuery {
    pub limit: u32,
}

/// Query for `GET /jobs/scheduled`.
#[derive(Debug, Clone, Deserialize)]
pub struct ScheduledQuery {
    #[serde(default)]
    pub queue_name: Option<String>,
}

/// Query for `GET /queue/timeline`.
#[derive(Debug, Clone, Deserialize)]
pub struct TimelineQuery {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub bucket_secs: u32,
}

/// Query for `GET /queue/metric-series`.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricSeriesQuery {
    pub queue: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub bucket_secs: u32,
}

/// Query for the per-pod series (`GET /queue/resource-series`,
/// `GET /queue/db-series`).
#[derive(Debug, Clone, Deserialize)]
pub struct SeriesQuery {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub bucket_secs: u32,
}
