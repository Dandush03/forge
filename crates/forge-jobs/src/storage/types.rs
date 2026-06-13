//! Backend-agnostic types for the storage traits.
//!
//! These are deliberately plain Rust structs with no SQL or KV
//! library types. The traits in `super::mod.rs` produce + consume
//! these; backend impls map them to/from whatever the underlying
//! store uses (rows, hashes, JSON blobs, etc).

use std::borrow::Cow;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ────────────────────────────────────────────────────────────────────
// Job identity + status
// ────────────────────────────────────────────────────────────────────

/// Opaque identifier for a job. Backed by a string for backend
/// portability — `SQLite` uses ULIDs, Redis would use the same, Postgres
/// could use UUIDs. The runtime never inspects the inner string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub String);

impl JobId {
    #[must_use]
    pub fn new(inner: impl Into<String>) -> Self {
        Self(inner.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for JobId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Job lifecycle state. Backend impls map this to whatever string /
/// enum encoding they prefer; this is the canonical Rust-side value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum JobStatus {
    Pending,
    InProgress,
    Done,
    Failed,
    Dead,
}

impl JobStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Dead => "dead",
        }
    }

    /// Parse the string form back into the enum. Returns `None` for
    /// unknown values (defensive — a corrupted row shouldn't panic
    /// the supervisor).
    #[must_use]
    #[allow(
        clippy::should_implement_trait,
        reason = "returning Option<Self> on unknown values is intentional; impl FromStr would force an Err type"
    )]
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => Self::Pending,
            "in_progress" => Self::InProgress,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "dead" => Self::Dead,
            _ => return None,
        })
    }
}

// ────────────────────────────────────────────────────────────────────
// JobRecord — what the queue stores per job
// ────────────────────────────────────────────────────────────────────

/// One row in the queue. Returned by `JobQueue::get` /
/// `list_by_status` / `claim_next`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: JobId,
    pub queue_name: String,
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    pub status: JobStatus,
    #[serde(default)]
    pub priority: i32,
    pub enqueued_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    /// Incremented every time a worker claims this row — i.e. once on
    /// the initial run plus once per retry. `FinalizeOutcome::Throttled`
    /// is the documented exception: a 429/rate-limit re-queue does
    /// NOT increment this so we don't burn the retry budget on a
    /// transient upstream throttle.
    #[serde(default)]
    pub attempts: i32,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i32,
    /// Consecutive `FinalizeOutcome::Throttled` finalizes on this row.
    /// Resets to 0 on `Done`. Diagnostic only — the backoff curve is
    /// driven by the queue-wide [`QueueConfigRow::throttle_attempts`],
    /// since rate limits are per-token, not per-job.
    #[serde(default)]
    pub throttle_attempts: i32,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub error_history: Vec<ErrorHistoryEntry>,
    #[serde(default)]
    pub process_id: Option<String>,
    #[serde(default)]
    pub heartbeat_at: Option<DateTime<Utc>>,
    /// Caller-supplied dedupe key. **Global across queues** — two
    /// `EnqueueRequest`s with the same `dedupe_key` collapse onto one
    /// row even if they would have routed to different queues. The
    /// `SQLite` backend enforces this with a unique partial index on
    /// `(dedupe_key) WHERE status IN ('pending','in_progress')`; see
    /// `crates/jobs/src/storage/sqlite/jobs.rs`.
    #[serde(default)]
    pub dedupe_key: Option<String>,
}

/// One entry in a job's append-only error log. Capped at 10 entries
/// by the runtime; older entries fall off when the cap is hit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorHistoryEntry {
    pub at: DateTime<Utc>,
    pub attempt: i32,
    pub message: String,
}

#[must_use]
pub const fn default_max_attempts() -> i32 {
    5
}

// ────────────────────────────────────────────────────────────────────
// NewJob — payload for direct (low-level) inserts
// ────────────────────────────────────────────────────────────────────

/// Direct insert builder used by tests + the migration path. Most
/// callers go through `EnqueueRequest` + `JobQueue::enqueue` instead.
#[derive(Debug, Clone)]
pub struct NewJob {
    pub queue_name: String,
    pub kind: String,
    pub payload: serde_json::Value,
    pub priority: i32,
    pub scheduled_at: DateTime<Utc>,
    pub max_attempts: i32,
    pub dedupe_key: Option<String>,
}

impl NewJob {
    #[must_use]
    pub fn new(
        queue_name: impl Into<String>,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            queue_name: queue_name.into(),
            kind: kind.into(),
            payload,
            priority: 0,
            scheduled_at: Utc::now(),
            max_attempts: default_max_attempts(),
            dedupe_key: None,
        }
    }

    #[must_use]
    pub const fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    #[must_use]
    pub const fn with_scheduled_at(mut self, at: DateTime<Utc>) -> Self {
        self.scheduled_at = at;
        self
    }

    #[must_use]
    pub const fn with_max_attempts(mut self, n: i32) -> Self {
        self.max_attempts = n;
        self
    }

    #[must_use]
    pub fn with_dedupe_key(mut self, key: impl Into<String>) -> Self {
        self.dedupe_key = Some(key.into());
        self
    }
}

// ────────────────────────────────────────────────────────────────────
// EnqueueRequest / EnqueueOutcome — the higher-level enqueue API
// ────────────────────────────────────────────────────────────────────

/// One job to drop on the queue via `JobQueue::enqueue`. Builder
/// pattern with `with_*` chaining.
///
/// `Cow<'static, str>` for `kind` / `queue_name` keeps static call
/// sites zero-copy while allowing dynamic strings from the cron
/// service.
#[derive(Debug, Clone)]
pub struct EnqueueRequest {
    pub kind: Cow<'static, str>,
    pub payload: serde_json::Value,
    /// `None` → router decides.
    pub queue_name: Option<Cow<'static, str>>,
    /// Lower wins. `SolidQueue` convention.
    pub priority: i32,
    /// Override the default `max_attempts` (5).
    pub max_attempts: Option<i32>,
    /// If set, the backend collapses against any active (pending /
    /// in-progress) row with this same key. **Global across queues** —
    /// see [`JobRecord::dedupe_key`] for the scope contract.
    pub dedupe_key: Option<String>,
    /// Defer to a specific time. Default is "now".
    pub run_at: Option<DateTime<Utc>>,
}

impl EnqueueRequest {
    #[must_use]
    pub fn new(kind: impl Into<Cow<'static, str>>, payload: serde_json::Value) -> Self {
        Self {
            kind: kind.into(),
            payload,
            queue_name: None,
            priority: 0,
            max_attempts: None,
            dedupe_key: None,
            run_at: None,
        }
    }

    #[must_use]
    pub fn on_queue(mut self, queue_name: impl Into<Cow<'static, str>>) -> Self {
        self.queue_name = Some(queue_name.into());
        self
    }

    #[must_use]
    pub const fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    #[must_use]
    pub const fn with_max_attempts(mut self, max: i32) -> Self {
        self.max_attempts = Some(max);
        self
    }

    #[must_use]
    pub fn with_dedupe_key(mut self, key: impl Into<String>) -> Self {
        self.dedupe_key = Some(key.into());
        self
    }

    #[must_use]
    pub const fn with_run_at(mut self, at: DateTime<Utc>) -> Self {
        self.run_at = Some(at);
        self
    }
}

/// Result of an enqueue: either a new row was inserted or an
/// existing active row was reused (dedupe).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EnqueueOutcome {
    Enqueued(JobId),
    Deduped(JobId),
}

impl EnqueueOutcome {
    /// The row id either way — useful for callers tracking identity
    /// without caring about the dedupe distinction.
    #[must_use]
    pub const fn id(&self) -> &JobId {
        match self {
            Self::Enqueued(id) | Self::Deduped(id) => id,
        }
    }

    #[must_use]
    pub const fn is_deduped(&self) -> bool {
        matches!(self, Self::Deduped(_))
    }
}

// ────────────────────────────────────────────────────────────────────
// FinalizeOutcome — what `JobQueue::finalize` receives
// ────────────────────────────────────────────────────────────────────

/// The terminal lifecycle transition for a job.
///
/// The runtime maps `JobOutcome` (which the handler returned) + the
/// retry policy into the right variant here, so the backend doesn't
/// have to know about retry budgets or backoff curves.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FinalizeOutcome {
    /// Status → done, `completed_at` = now.
    Done,
    /// Status → pending, `scheduled_at` = now + delay.
    /// `attempts` is *not* incremented (used for throttle backoff —
    /// we don't want a 429 to burn a retry).
    ///
    /// `cool_down_queue` tells the backend to also extend the queue-wide
    /// cool-down (`throttled_until` + `throttle_attempts`) so every
    /// worker on the queue backs off, not just this row. The runtime
    /// sets it `true` for all throttles (rate limits are per-token, so
    /// the whole queue must pause); `false` defers only the single row
    /// (used by tests for the legacy per-job path).
    Throttled {
        retry_after: Duration,
        cool_down_queue: bool,
    },
    /// Status → failed, `scheduled_at` = now + backoff. Used for
    /// retryable failures (attempts < max).
    Failed {
        retry_after: Duration,
        message: String,
    },
    /// Status → dead, `completed_at` = now. Terminal — no more retries.
    Dead { message: String },
}

// ────────────────────────────────────────────────────────────────────
// ProcessRecord — live worker row
// ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessRecord {
    pub process_id: String,
    pub queue_name: String,
    pub host_id: String,
    pub started_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    #[serde(default)]
    pub current_job: Option<JobId>,
}

// ────────────────────────────────────────────────────────────────────
// PodRecord — one live worker process (pod) in the cluster
// ────────────────────────────────────────────────────────────────────

/// A worker process's cluster-level identity, published on every pod
/// heartbeat. Distinct from [`ProcessRecord`] (one row per worker *slot*):
/// a pod has one `PodRecord` and N `ProcessRecord`s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodRecord {
    pub host_id: String,
    /// Optional human-friendly label (`FORGE_WORKER_NAME`). `None` →
    /// callers fall back to `host_id` for display.
    #[serde(default)]
    pub worker_name: Option<String>,
    /// Queues this worker is responsible for. New workers always declare
    /// at least one (`start()` rejects an empty set), so an empty `queues`
    /// here can only be a NULL pre-upgrade row — a worker still running the
    /// old binary mid-rollout. Such a pod is treated as eligible for
    /// *every* queue ([`PodRecord::handles`]) so the fleet keeps draining
    /// during a rolling upgrade until it re-heartbeats with the new code.
    #[serde(default)]
    pub queues: Vec<String>,
    pub heartbeat_at: DateTime<Utc>,
}

impl PodRecord {
    /// Is this pod eligible to run `queue`?
    ///
    /// True when it declared `queue`, or when its declared set is empty —
    /// the legacy/pre-upgrade case, treated as eligible-for-all so a
    /// mid-rollout pod keeps draining every queue until its first
    /// new-code heartbeat narrows it (a new worker can't reach this state:
    /// `start()` rejects an empty declaration). This is the single
    /// eligibility predicate shared by the rebalancer and the worker view.
    #[must_use]
    pub fn handles(&self, queue: &str) -> bool {
        self.queues.is_empty() || self.queues.iter().any(|q| q == queue)
    }
}

// ────────────────────────────────────────────────────────────────────
// SlotAssignment — one (queue, pod) worker-count allocation
// ────────────────────────────────────────────────────────────────────

/// The rebalancer's per-(queue, pod) worker-count allocation. Read by the
/// monitoring view to show how many slots each worker runs per queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotAssignment {
    pub queue_name: String,
    pub host_id: String,
    pub slots: i32,
}

// ────────────────────────────────────────────────────────────────────
// QueueConfigRow — one row in the per-queue config table
// ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfigRow {
    pub name: String,
    pub max_workers: i32,
    pub paused: bool,
    pub retain_done_for_days: i32,
    pub retain_dead_for_days: i32,
    /// Opt-in toggle for the configurable exponential throttle curve.
    /// When false, throttle finalizes use a flat fallback (60s) — the
    /// pre-toggle behavior of every queue.
    pub backoff_enabled: bool,
    pub backoff_base_seconds: i32,
    pub backoff_max_seconds: i32,
    /// Consecutive queue-wide throttles. Drives the backoff exponent
    /// (`base * 2^throttle_attempts`); reset to 0 when any job on the
    /// queue completes. This is the curve's real driver — the per-job
    /// [`JobRecord::throttle_attempts`] is a diagnostic counter only.
    pub throttle_attempts: i32,
    /// Cool-down deadline. While in the future, `claim_next` refuses to
    /// hand out rows for this queue so the whole fleet backs off
    /// together. `None` unless the queue is actively throttled.
    pub throttled_until: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

// ────────────────────────────────────────────────────────────────────
// JobLatency — one completed job's processing + total durations
// ────────────────────────────────────────────────────────────────────

/// Latency sample for one completed job, used to chart processing-time
/// and end-to-end percentiles over the timeline window.
#[derive(Debug, Clone, Copy)]
pub struct JobLatency {
    /// When the job reached `done` — the bucket key.
    pub completed_at: DateTime<Utc>,
    /// `completed_at - started_at`: how long the worker took once it
    /// claimed the job. The handler/processing latency.
    pub processing_ms: i64,
    /// `completed_at - enqueued_at`: end-to-end time in the system,
    /// including queue wait. Dominated by backlog when one exists.
    pub total_ms: i64,
}

// ────────────────────────────────────────────────────────────────────
// MetricBucket — one pre-aggregated rollup row
// ────────────────────────────────────────────────────────────────────

/// Canonical `metric` column values for [`MetricBucket`].
///
/// Kept as constants (not an enum) so the storage layer stays
/// string-keyed and new metrics don't force a schema/enum change — see
/// `docs/adr/0009-metrics-rollup.md`.
pub mod metric {
    /// Jobs enqueued in the bucket (count).
    pub const ENQUEUED: &str = "enqueued";
    /// Jobs that reached `done` in the bucket (count).
    pub const COMPLETED: &str = "completed";
    /// Jobs that failed/died in the bucket (count).
    pub const FAILED: &str = "failed";
    /// Processing latency, claim→finalize (ms; percentiles set).
    pub const PROC_MS: &str = "proc_ms";
    /// Total latency, enqueue→finalize (ms; percentiles set).
    pub const TOTAL_MS: &str = "total_ms";
    /// Process CPU usage, normalized to % of all cores (gauge; 100% =
    /// whole box maxed).
    pub const CPU_PCT: &str = "cpu_pct";
    /// Process resident memory (bytes; gauge).
    pub const RSS_BYTES: &str = "rss_bytes";
    /// Bytes the process read from disk in the bucket (gauge).
    pub const DISK_READ_BYTES: &str = "disk_read_bytes";
    /// Bytes the process wrote to disk in the bucket (gauge).
    pub const DISK_WRITE_BYTES: &str = "disk_write_bytes";
    /// Data-volume fullness, percent used (gauge).
    pub const DISK_USED_PCT: &str = "disk_used_pct";
    /// Storage-backend operation latency (ms; percentiles set). One
    /// sample per `JobQueue` method call; `count` carries
    /// operations-per-bucket so the UI doesn't need a parallel
    /// throughput metric.
    ///
    /// Superseded by [`DB_READ_MS`] + [`DB_WRITE_MS`] which split the
    /// stream by op kind (the throughput chart shows two lines).
    /// Kept as a constant so old rollup rows still parse; new writes
    /// only use the split metrics.
    pub const DB_OP_MS: &str = "db_op_ms";
    /// Read-only storage-backend op latency (`SELECT`-only paths).
    /// One sample per `JobQueue` call; `count` carries reads/min.
    pub const DB_READ_MS: &str = "db_read_ms";
    /// Mutating storage-backend op latency (everything that touches
    /// `write_pool` on `SQLite` or runs a non-`SELECT` statement on
    /// Postgres). `count` carries writes/min.
    pub const DB_WRITE_MS: &str = "db_write_ms";
    /// Server-side active backends from `pg_stat_activity` (Postgres
    /// only — `SQLite` has no server-side connection model). Gauge.
    pub const DB_POOL_ACTIVE: &str = "db_pool_active";
    /// Server-side idle backends from `pg_stat_activity` (Postgres
    /// only). Gauge.
    pub const DB_POOL_IDLE: &str = "db_pool_idle";
    /// Server-side `max_connections` from `pg_settings` (Postgres
    /// only). Constant on a steady config, but kept as a metric so
    /// saturation % can be computed without a config lookup.
    pub const DB_POOL_MAX: &str = "db_pool_max";
    /// Database file size in bytes (gauge). On `SQLite`,
    /// `PRAGMA page_count * page_size`. On Postgres,
    /// `pg_database_size(current_database())`. Truthful + DB-sourced
    /// on both backends.
    pub const DB_SIZE_BYTES: &str = "db_size_bytes";
    /// `SQLite` WAL file size in bytes (gauge).
    ///
    /// A persistent non-trivial number means writers are running ahead
    /// of auto-checkpoint — a real "DB is hot" signal on the embedded
    /// backend that doesn't have a server-side connection model.
    pub const DB_WAL_BYTES: &str = "db_wal_bytes";
}

/// Sentinel `queue` value for a process-wide (not per-queue) metric —
/// e.g. CPU/RAM of the whole process, which can't be split per queue
/// for in-process work. See `docs/adr/0009-metrics-rollup.md`.
pub const PROCESS_WIDE_QUEUE: &str = "";

/// One pre-aggregated rollup row.
///
/// A single (queue, metric) aggregated over one base time bucket.
/// Written by the metrics roller, read by the per-queue + resource
/// charts. Percentile fields are `Some` only for latency metrics
/// (`proc_ms`/`total_ms`).
#[derive(Debug, Clone)]
pub struct MetricBucket {
    /// Queue name, or [`PROCESS_WIDE_QUEUE`] for a process-wide gauge.
    pub queue: String,
    /// One of the [`metric`] constants.
    pub metric: String,
    /// Bucket start, aligned to the base granularity.
    pub bucket_start: DateTime<Utc>,
    /// Number of samples aggregated into this bucket.
    pub count: i64,
    /// Sum of the sampled values (`avg = sum / count`; additive metrics
    /// aggregate by summing this across coarser windows).
    pub sum: f64,
    /// Per-bucket percentiles — `Some` for latency metrics only. These
    /// must NOT be averaged across buckets (see the ADR).
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    /// Peak sampled value in the bucket (gauges).
    pub max: f64,
}

// ────────────────────────────────────────────────────────────────────
// QueueCounts — per-status totals for one queue
// ────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct QueueCounts {
    /// `status = 'pending' AND scheduled_at <= now` — eligible for
    /// the next worker claim. Excludes rows deferred to the future;
    /// those are counted separately in [`Self::scheduled`].
    pub pending: u64,
    /// `status = 'pending' AND scheduled_at > now` — deferred work
    /// (a throttle re-queue, a `with_run_at`, etc.). Matches the
    /// predicate the Scheduled tab lists.
    pub scheduled: u64,
    pub in_progress: u64,
    pub done: u64,
    pub failed: u64,
    pub dead: u64,
}

/// One row in the append-only timeline event log.
///
/// Written by the backend on every enqueue + terminal transition.
/// Survives `cleanup_aged` of the underlying job rows so the chart
/// keeps history independent of the queue's retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub at: DateTime<Utc>,
    pub kind: String,
    pub queue_name: String,
    pub event_type: TimelineEventType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TimelineEventType {
    /// A new row was inserted into the queue.
    Enqueued,
    /// A worker claimed the row (status → `in_progress`). The chart's
    /// running in-flight gauge is
    /// `cumulative(Started) - cumulative(Completed) - cumulative(Failed) - cumulative(Retried)`.
    Started,
    /// A worker-claimed row went back to the schedulable pool without
    /// finishing — Throttled, retryable Failed, or non-terminal reap.
    /// Counterweights the prior `Started` so the running gauge stays
    /// accurate across retry cycles. (Before this event existed each
    /// retry left a ghost in-flight and the gauge drifted upward.)
    Retried,
    /// Terminal success.
    Completed,
    /// Terminal failure (attempts exhausted, status → dead).
    Failed,
}

impl TimelineEventType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enqueued => "enqueued",
            Self::Started => "started",
            Self::Retried => "retried",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    #[allow(
        clippy::should_implement_trait,
        reason = "returning Option<Self> on unknown values is intentional; impl FromStr would force an Err type"
    )]
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "enqueued" => Self::Enqueued,
            "started" => Self::Started,
            "retried" => Self::Retried,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            _ => return None,
        })
    }
}

// ────────────────────────────────────────────────────────────────────
// Cron — schedule rows
// ────────────────────────────────────────────────────────────────────

/// Builder for `CronStorage::ensure`.
#[derive(Debug, Clone)]
pub struct NewCronSchedule {
    pub name: String,
    pub kind: String,
    pub payload: serde_json::Value,
    pub queue_name: Option<String>,
    pub cron_expr: String,
    pub enabled: bool,
    pub max_attempts: Option<i32>,
    /// When set, each cron firing enqueues with this dedupe key, so a
    /// tick that lands while the previous run is still pending or
    /// in-progress collapses to a no-op instead of stacking the queue.
    /// `None` keeps the old fire-every-tick behavior. Convention: set it
    /// to the schedule `name` so the key is unique per schedule.
    pub dedupe_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronScheduleRecord {
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
    /// Dedupe key applied to each firing — see [`NewCronSchedule::dedupe_key`].
    #[serde(default)]
    pub dedupe_key: Option<String>,
    #[serde(default)]
    pub last_fired_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub next_fire_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
