//! The crate's IPC contract.
//!
//! Components don't call `invoke()` directly — they reach for a
//! `QueueIpc` trait object from Leptos context. The consumer
//! implements the trait around their host's own IPC mechanism (Tauri's
//! `invoke`, a REST client, an in-process mock for tests, etc.), so
//! the panel is reusable across hosts.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── DTOs ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, Deserialize)]
pub struct StatusCounts {
    #[serde(default)]
    pub pending: u64,
    #[serde(default)]
    pub scheduled: u64,
    #[serde(default)]
    pub in_progress: u64,
    #[serde(default)]
    pub done: u64,
    #[serde(default)]
    pub failed: u64,
    #[serde(default)]
    pub dead: u64,
}

impl StatusCounts {
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.pending + self.scheduled + self.in_progress + self.done + self.failed + self.dead
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct QueueProcess {
    pub process_id: String,
    pub queue_name: String,
    pub host_id: String,
    pub started_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    #[serde(default)]
    pub current_job_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct QueueOverview {
    pub name: String,
    pub max_workers: i32,
    pub paused: bool,
    pub retain_done_days: i32,
    pub retain_dead_days: i32,
    #[serde(default)]
    pub backoff_enabled: bool,
    #[serde(default = "default_backoff_base")]
    pub backoff_base_seconds: i32,
    #[serde(default = "default_backoff_max")]
    pub backoff_max_seconds: i32,
    /// Cool-down deadline while the queue is throttled; drives the
    /// "resuming in Ns" countdown on the card. `None` when not throttled.
    #[serde(default)]
    pub throttled_until: Option<DateTime<Utc>>,
    /// Age (seconds) of the oldest ready job — the queue lag. Sampled
    /// by the live-metrics chart.
    #[serde(default)]
    pub oldest_pending_age_seconds: u64,
    pub counts: StatusCounts,
    #[serde(default)]
    pub processes: Vec<QueueProcess>,
}

const fn default_backoff_base() -> i32 {
    60
}

const fn default_backoff_max() -> i32 {
    1800
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobRow {
    pub id: String,
    pub queue_name: String,
    pub kind: String,
    pub status: String,
    pub priority: i32,
    pub attempts: i32,
    pub max_attempts: i32,
    pub enqueued_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub process_id: Option<String>,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    /// Last heartbeat the worker wrote on this row. Fresh while a
    /// handler is actively running; stale → the reaper will revive.
    #[serde(default)]
    pub heartbeat_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobInspect {
    pub row: JobRow,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub error_history: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct JobsFilter {
    #[serde(default)]
    pub queues: Vec<String>,
    #[serde(default)]
    pub kinds: Vec<String>,
    #[serde(default)]
    pub statuses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_search: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobsPage {
    pub rows: Vec<JobRow>,
    pub total: u64,
    pub limit: u32,
    pub offset: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TimelineBucket {
    pub at: DateTime<Utc>,
    pub enqueued: u64,
    /// Count of jobs claimed by a worker in this bucket.
    #[serde(default)]
    pub started: u64,
    /// Count of worker-claimed rows that returned to the schedulable
    /// pool without finishing (throttle / non-terminal failure
    /// re-queue) in this bucket. Plotted as the "Retried" series.
    #[serde(default)]
    pub retried: u64,
    pub completed: u64,
    pub failed: u64,
    /// Latency percentiles (ms) over jobs that finalized in this bucket.
    /// `processing_*` = claim→finalize (handler speed); `total_*` =
    /// enqueue→finalize (end-to-end). Zero when nothing completed in the
    /// bucket. Plotted as the two latency charts.
    #[serde(default)]
    pub processing_p50_ms: u64,
    #[serde(default)]
    pub processing_p95_ms: u64,
    #[serde(default)]
    pub processing_p99_ms: u64,
    #[serde(default)]
    pub total_p50_ms: u64,
    #[serde(default)]
    pub total_p95_ms: u64,
    #[serde(default)]
    pub total_p99_ms: u64,
}

/// One bucket of a per-queue metric series (mirror of the plugin's
/// `MetricSeriesBucket`) — throughput + latency. Resources are
/// per-process; see [`ResourceHostSeries`].
#[derive(Clone, Debug, Default, Deserialize)]
pub struct MetricSeriesBucket {
    pub at: DateTime<Utc>,
    #[serde(default)]
    pub enqueued: u64,
    #[serde(default)]
    pub completed: u64,
    #[serde(default)]
    pub failed: u64,
    #[serde(default)]
    pub proc_p50_ms: u64,
    #[serde(default)]
    pub proc_p95_ms: u64,
    #[serde(default)]
    pub proc_p99_ms: u64,
    #[serde(default)]
    pub total_p50_ms: u64,
    #[serde(default)]
    pub total_p95_ms: u64,
    #[serde(default)]
    pub total_p99_ms: u64,
}

/// One bucket of a single pod's resource series (mirror of the plugin's
/// `ResourceBucket`).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ResourceBucket {
    pub at: DateTime<Utc>,
    #[serde(default)]
    pub cpu_pct: f64,
    #[serde(default)]
    pub rss_bytes: u64,
    #[serde(default)]
    pub disk_read_bytes: u64,
    #[serde(default)]
    pub disk_write_bytes: u64,
    #[serde(default)]
    pub disk_used_pct: f64,
}

/// Resource series for one pod (`host_id`). One per pod; locally, one.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ResourceHostSeries {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub buckets: Vec<ResourceBucket>,
}

/// One bucket of a single pod's DB-health series (mirror of the
/// plugin's `DbHealthBucket`). All gauges past `ops_per_min` come
/// from the running database — `SQLite` fills `db_size_bytes` +
/// `wal_bytes` and leaves `pool_*` at zero; Postgres fills `pool_*`
/// + `db_size_bytes` and leaves `wal_bytes` at zero.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct DbHealthBucket {
    pub at: DateTime<Utc>,
    #[serde(default)]
    pub read_p50_ms: u64,
    #[serde(default)]
    pub read_p95_ms: u64,
    #[serde(default)]
    pub read_p99_ms: u64,
    #[serde(default)]
    pub reads_per_min: u64,
    #[serde(default)]
    pub write_p50_ms: u64,
    #[serde(default)]
    pub write_p95_ms: u64,
    #[serde(default)]
    pub write_p99_ms: u64,
    #[serde(default)]
    pub writes_per_min: u64,
    #[serde(default)]
    pub pool_active: u64,
    #[serde(default)]
    pub pool_idle: u64,
    #[serde(default)]
    pub pool_max: u64,
    #[serde(default)]
    pub pool_used_pct: f64,
    #[serde(default)]
    pub db_size_bytes: u64,
    #[serde(default)]
    pub wal_bytes: u64,
}

/// DB-health series for one pod (`host_id`). One per pod; locally, one.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct DbHealthHostSeries {
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub buckets: Vec<DbHealthBucket>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct CleanupReport {
    #[serde(default)]
    pub done_deleted: u64,
    #[serde(default)]
    pub dead_deleted: u64,
}

/// Every status the schema understands, in display order. Used by the
/// panel to populate the status filter dropdown without an extra
/// round-trip.
pub const JOB_STATUSES: &[&str] = &["pending", "in_progress", "done", "failed", "dead"];

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct CronSchedule {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub queue_name: Option<String>,
    pub cron_expr: String,
    pub enabled: bool,
    #[serde(default)]
    pub max_attempts: Option<i32>,
    #[serde(default)]
    pub last_fired_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub next_fire_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Args for [`QueueIpc::jobs_enqueue`] — the Rails `perform_later`
/// analog. Every field except `kind` + `payload` is optional;
/// defaults match the host's enqueue defaults (route by kind prefix,
/// no dedupe, priority 0, runs immediately).
#[derive(Clone, Debug, Serialize)]
pub struct JobsEnqueueReq {
    pub kind: String,
    pub payload: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<String>,
    /// Future timestamp; the worker won't claim until `>=` this.
    /// `None` = enqueue runs as soon as a worker is free.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<i32>,
}

// ── trait ────────────────────────────────────────────────────────────

/// Structured IPC error.
///
/// Mirrors the host plugin's `Error` enum so the frontend can branch on
/// `kind` (e.g. show a retry pill on `RateLimited`, jump to the
/// offending input on `Validation`) instead of regex-matching a string.
/// Variants are kept identical to the host's tagged-enum shape so
/// `serde_wasm_bindgen::from_value` round-trips cleanly.
///
/// When the host wasn't reachable or returned an unstructured error,
/// the frontend bridge constructs `Internal { msg }` from whatever
/// JS-side message it could extract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IpcError {
    Validation { field: String, msg: String },
    NotFound { msg: String },
    Storage { msg: String },
    RateLimited { retry_after_secs: u32 },
    Internal { msg: String },
}

impl IpcError {
    /// Single source of truth for the user-visible message — every
    /// variant carries one. The panel's default error pill displays
    /// this directly; `kind`-specific UI affordances (a retry button
    /// on `RateLimited`, a focus on `Validation.field`) are layered
    /// on top in the views that care.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::Validation { msg, .. }
            | Self::NotFound { msg }
            | Self::Storage { msg }
            | Self::Internal { msg } => msg,
            Self::RateLimited { .. } => "rate limited",
        }
    }

    /// Wrap an unstructured string (legacy JS-side message, network
    /// error, …) as an `Internal`. The bridge calls this as its
    /// fallback when the JSON the host emitted didn't deserialize.
    #[must_use]
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal { msg: msg.into() }
    }
}

impl std::fmt::Display for IpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

/// Bridge between the panel and the host's queue API. The host
/// implements this once around its preferred IPC mechanism (Tauri's
/// `invoke`, REST, in-process mock) and provides the `Arc<dyn QueueIpc>`
/// via Leptos context.
///
/// `?Send` because Leptos CSR is single-threaded — the futures don't
/// need to cross threads. The trait itself still requires `Send + Sync`
/// because Leptos' `provide_context` wraps it in a globally-shareable
/// handle even in CSR mode.
#[async_trait(?Send)]
pub trait QueueIpc: Send + Sync + 'static {
    // ── reads
    async fn queue_overview(&self) -> Result<Vec<QueueOverview>, IpcError>;
    async fn queue_processes(
        &self,
        queue_name: Option<&str>,
    ) -> Result<Vec<QueueProcess>, IpcError>;
    /// Bucketed enqueue/completion/failure counts across the half-open
    /// `[from, to)` range at the given granularity. Buckets are aligned
    /// to `from` (not wall-clock) and walk forward in `bucket_secs`
    /// steps. The host caps the bucket count to keep payloads bounded.
    async fn queue_timeline_range(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket_secs: u32,
    ) -> Result<Vec<TimelineBucket>, IpcError>;
    /// Per-queue metric series from the pre-aggregated rollup, bucketed
    /// to `bucket_secs` (clamped up to the 60s base). Pass `queue = ""`
    /// for the process-wide CPU/RAM gauges; a queue name for that
    /// queue's throughput + latency.
    async fn queue_metric_series(
        &self,
        queue: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket_secs: u32,
    ) -> Result<Vec<MetricSeriesBucket>, IpcError>;
    /// Per-pod resource series (CPU/RAM/disk) from the rollup, one entry
    /// per `host_id`. Locally there's exactly one pod; a cluster returns
    /// one series per pod.
    async fn queue_resource_series(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket_secs: u32,
    ) -> Result<Vec<ResourceHostSeries>, IpcError>;
    /// Per-pod DB-health series (op-latency percentiles + pool
    /// saturation) from the rollup, one entry per `host_id`.
    async fn queue_db_series(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket_secs: u32,
    ) -> Result<Vec<DbHealthHostSeries>, IpcError>;
    async fn jobs_list(
        &self,
        filter: JobsFilter,
        limit: u32,
        offset: u32,
    ) -> Result<JobsPage, IpcError>;
    async fn jobs_failed(&self, limit: u32) -> Result<Vec<JobRow>, IpcError>;
    async fn jobs_kinds(&self) -> Result<Vec<String>, IpcError>;
    async fn job_inspect(&self, id: &str) -> Result<JobInspect, IpcError>;

    // ── mutations
    async fn queue_set_max_workers(&self, queue_name: &str, n: i32) -> Result<(), IpcError>;
    async fn queue_set_paused(&self, queue_name: &str, paused: bool) -> Result<(), IpcError>;
    async fn queue_set_retention(
        &self,
        queue_name: &str,
        done_days: i32,
        dead_days: i32,
    ) -> Result<(), IpcError>;
    async fn queue_set_backoff(
        &self,
        queue_name: &str,
        enabled: bool,
        base_seconds: i32,
        max_seconds: i32,
    ) -> Result<(), IpcError>;
    async fn queue_cleanup_now(&self) -> Result<CleanupReport, IpcError>;
    async fn queue_enqueue_demo(&self, payload: serde_json::Value) -> Result<String, IpcError>;

    /// Generic typed-job enqueue — the Rails `perform_later` analog.
    /// Returns the new job's id. `run_at` schedules a future run;
    /// pass `None` for "immediately." All other knobs follow the
    /// queue's defaults when `None`.
    async fn jobs_enqueue(&self, req: JobsEnqueueReq) -> Result<String, IpcError>;

    /// Pending rows scheduled to run strictly after now. Powers the
    /// "Scheduled" tab. Ordered ascending by `scheduled_at`.
    async fn jobs_scheduled(&self, queue_name: Option<&str>) -> Result<Vec<JobRow>, IpcError>;

    /// Advance one pending row's `scheduled_at` to now so the next
    /// worker claim picks it up. Returns `true` when the row was
    /// touched; `false` when the row isn't `pending` (already
    /// running, terminal, or missing).
    async fn jobs_run_now(&self, id: &str) -> Result<bool, IpcError>;
    async fn jobs_retry(&self, ids: &[String]) -> Result<u64, IpcError>;
    async fn jobs_retry_all_failed(&self) -> Result<u64, IpcError>;
    /// Requeue every job currently in `status` (`failed` or `dead`).
    /// Backs the per-tab "Retry all" buttons on the Retries / Dead panels.
    async fn jobs_retry_all_by_status(&self, status: &str) -> Result<u64, IpcError>;
    async fn jobs_delete(&self, ids: &[String]) -> Result<u64, IpcError>;
    async fn jobs_requeue(&self, ids: &[String]) -> Result<u64, IpcError>;
    async fn jobs_delete_done_older_than(&self, days: u32) -> Result<u64, IpcError>;
    /// Delete every job currently in `status` (`done` / `failed` / `dead`).
    /// Backs the per-tab Purge buttons. Returns the row count deleted.
    async fn jobs_delete_by_status(&self, status: &str) -> Result<u64, IpcError>;

    // ── cron schedules
    async fn cron_list(&self) -> Result<Vec<CronSchedule>, IpcError>;
    async fn cron_set_enabled(&self, name: &str, enabled: bool) -> Result<(), IpcError>;
    async fn cron_set_expr(&self, name: &str, expr: &str) -> Result<(), IpcError>;
    /// Force a schedule to fire immediately. Returns the enqueued job id.
    async fn cron_trigger_now(&self, name: &str) -> Result<String, IpcError>;
}

/// Type alias for the context value the consumer provides via
/// `provide_context::<IpcCtx>(Arc::new(my_impl))`. `Arc` (not `Rc`)
/// because Leptos requires context values to be `Send + Sync` even in
/// CSR mode.
pub type IpcCtx = std::sync::Arc<dyn QueueIpc>;
