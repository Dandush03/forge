//! Backend-agnostic storage layer for the queue subsystem.
//!
//! The queue runtime (workers, supervisors, cron, reaper, cleanup) talks
//! to four small traits defined here. Each trait is a logical concern —
//! "the queue of jobs", "the registry of running workers", "per-queue
//! configuration", "cron schedules" — expressed as domain verbs, never
//! raw SQL or KV primitives. A new backend (Redis, Postgres, anything)
//! is added by implementing the four traits under `storage/<backend>/`
//! and swapping one line in the host's `setup()`.
//!
//! ## Why four traits, not one
//!
//! - The runtime composes them independently: the supervisor only needs
//!   `QueueConfig`, the workers only need `JobQueue` + `ProcessRegistry`,
//!   the cron service only needs `CronStorage` + `JobQueue::enqueue`.
//!   Splitting lets a backend mix-and-match (e.g. cron in `SQLite`,
//!   jobs in Redis) if a deployment ever wants that.
//! - It keeps each trait small enough to mock for tests cheaply.
//! - Implementations sit in separate files per trait, which keeps PRs
//!   focused.
//!
//! ## Why the traits expose verbs, not SQL
//!
//! Every method is *one logical operation* the runtime cares about:
//! "claim the next job", "mark this job done", "list pending". None of
//! them takes a SQL string or query builder. That means Redis (LPUSH /
//! BRPOP / Lua scripts) and Postgres (`SKIP LOCKED`) can implement the
//! same contract with completely different primitives.

// Storage submodules are internal — the SemVer surface is the curated
// re-exports below. `#[allow(unreachable_pub)]` keeps inner `pub` items
// as module-local API documentation instead of forcing every item to
// be `pub(crate)`. Same pattern as `forge_charts` / `forge_jobs_ui`.
#[allow(unreachable_pub)]
pub(crate) mod database_config;
#[allow(unreachable_pub)]
pub(crate) mod db_timing;
#[allow(unreachable_pub)]
pub(crate) mod error;
#[allow(unreachable_pub)]
pub(crate) mod paths;
#[cfg(feature = "postgres")]
#[allow(unreachable_pub)]
pub(crate) mod postgres;
mod retry;
#[allow(unreachable_pub)]
pub(crate) mod sqlite;
#[allow(unreachable_pub)]
pub(crate) mod types;

pub use paths::{PathsError, QueuePaths};
pub(crate) use retry::with_transient_retry;

pub use database_config::{DatabaseConfig, PostgresConfig, SqliteConfig};
pub use db_timing::DrainedSamples;
pub use error::{Result, StorageError};
#[cfg(feature = "postgres")]
pub use postgres::PostgresStorage;
pub use sqlite::SqliteStorage;
pub use types::{
    CronScheduleRecord, EnqueueOutcome, EnqueueRequest, FinalizeOutcome, JobId, JobLatency,
    JobRecord, JobStatus, MetricBucket, NewCronSchedule, NewJob, PROCESS_WIDE_QUEUE, ProcessRecord,
    QueueConfigRow, QueueCounts, TimelineEvent, TimelineEventType, metric,
};

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Cap on `JobRecord::error_history.len()`. Older entries fall off
/// when a finalize or reaper-revive appends past the cap. The same
/// value is used by every adapter so a job's history reads the same
/// regardless of backend — and so an operator switching from `SQLite`
/// to Postgres doesn't see the tail of debug context silently
/// expand or shrink.
pub(crate) const ERROR_HISTORY_CAP: usize = 32;

/// Adapter-agnostic snapshot of static storage facts.
///
/// Surfaced in the boot banner so production logs always show which
/// adapter + which knobs the process started with. Each adapter
/// publishes its own flavor: `SqliteStorage` reports `sqlite_version`
/// / pool sizes; a future `PostgresStorage` would report
/// `server_version` / `max_connections` / listen-channel prefix; etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StorageInfo {
    /// Stable adapter identifier — `"sqlite"`, `"postgres"`, `"redis"`, …
    pub backend: String,
    /// Free-form key/value pairs the adapter wants surfaced at boot.
    pub fields: Vec<(String, String)>,
}

/// Outcome of [`JobQueue::delete`].
///
/// Distinguishes an actual removal from the in-progress "cancel
/// requested" path, so callers (and the panel) can report the difference
/// instead of conflating both as "deleted".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeleteOutcome {
    /// A non-running row (`pending`/`failed`/`dead`/`done`) was removed.
    Deleted,
    /// The row was `in_progress`: a cancel was requested instead of an
    /// immediate delete. The row is removed once its worker finalizes (or
    /// by a later `delete` / the retention sweep).
    CancelRequested,
    /// No row matched the id.
    NotFound,
}

/// What a [`JobQueue::heartbeat_job`] tick tells the worker to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HeartbeatStatus {
    /// Row is still `in_progress` and owned by this process; carry on.
    Active,
    /// Still ours, but a cancel was requested (via [`JobQueue::delete`]
    /// on the in-progress row). The worker signals its cancel token.
    CancelRequested,
    /// The row is no longer ours — it vanished, or it was reaped past the
    /// stale threshold and re-claimed by another worker. The worker stops
    /// running the job: continuing would duplicate work the new owner now
    /// holds.
    Lost,
}

// ────────────────────────────────────────────────────────────────────
// JobQueue — the queue of jobs proper.
// ────────────────────────────────────────────────────────────────────

/// Operations on the job queue itself: enqueue, claim, ack/nak,
/// heartbeat, reads.
#[async_trait]
pub trait JobQueue: Send + Sync + std::fmt::Debug {
    /// Insert a job, or no-op if a dedupe-eligible row already exists
    /// for `req.dedupe_key`. The returned outcome tells the caller
    /// which path fired and the id either way.
    async fn enqueue(&self, req: EnqueueRequest) -> Result<EnqueueOutcome>;

    /// Insert many jobs atomically. The list order is preserved in the
    /// returned outcomes. Implementations are free to batch (SQL tx,
    /// Redis pipeline, Lua script) but must apply dedupe per-row.
    async fn enqueue_bulk(&self, reqs: Vec<EnqueueRequest>) -> Result<Vec<EnqueueOutcome>>;

    /// Atomically claim the next eligible job for `queue` on behalf of
    /// `process_id`. Eligibility = `status ∈ {pending, failed}` AND
    /// `scheduled_at ≤ now`. Orders by `(priority asc, scheduled_at asc)`.
    /// Returns `Ok(None)` when no candidates exist or the worker
    /// otherwise lost the claim race.
    ///
    /// The returned row's `status` has already transitioned to
    /// `in_progress` and its `attempts` has been incremented — dropping
    /// the result without running the job means the row is now claimed
    /// by `process_id` until the reaper revives it via stale-heartbeat.
    /// Always consume the `Option`; never `.await` and discard.
    #[must_use = "claim_next has already transitioned the row to in_progress; dropping the result strands the claim until the reaper revives it"]
    async fn claim_next(&self, queue: &str, process_id: &str) -> Result<Option<JobRecord>>;

    /// Finalize a job's lifecycle. The runtime maps `JobOutcome` from
    /// the handler into the right `FinalizeOutcome` variant (with
    /// backoff already computed).
    ///
    /// `owner` is the `process_id` that claimed the row. When `Some`, the
    /// transition only fires if the row is *still* `in_progress` and still
    /// owned by that process — so a worker whose claim was reaped and
    /// re-claimed by another worker (it stalled past the stale threshold)
    /// can't clobber the new claimant's row. When `None` the guard is
    /// skipped (admin / test paths that legitimately finalize an
    /// arbitrary row). The worker loop always passes `Some`.
    async fn finalize(
        &self,
        job_id: &JobId,
        owner: Option<&str>,
        outcome: FinalizeOutcome,
    ) -> Result<()>;

    /// Touch the in-flight row's `heartbeat_at` and report what the
    /// worker should do next. The heartbeat UPDATE is scoped to `id =
    /// job_id AND process_id = process_id`, so it doubles as an ownership
    /// probe:
    ///
    /// - [`HeartbeatStatus::Active`] — still ours, run on.
    /// - [`HeartbeatStatus::CancelRequested`] — still ours, but a cancel
    ///   was requested via [`Self::delete`] on the in-progress row; the
    ///   worker signals its per-job cancel token.
    /// - [`HeartbeatStatus::Lost`] — the row is no longer ours (it
    ///   vanished, or it was reaped past the stale threshold and another
    ///   worker re-claimed it). The worker stops running the job; the new
    ///   owner holds it now.
    async fn heartbeat_job(&self, job_id: &JobId, process_id: &str) -> Result<HeartbeatStatus>;

    /// Find `in_progress` rows whose `heartbeat_at` is older than
    /// `stale_before` and revive them as a `Failed` outcome (so
    /// backoff still applies). Returns the count of revived rows.
    async fn revive_stale(&self, stale_before: DateTime<Utc>) -> Result<u64>;

    /// Delete aged `done` / `dead` rows on `queue` whose
    /// `completed_at < threshold`. Returns the count of removed rows.
    async fn cleanup_aged(
        &self,
        queue: &str,
        status: JobStatus,
        threshold: DateTime<Utc>,
    ) -> Result<u64>;

    // -------- reads --------

    /// Fetch a single job by id. Read-only; `None` means the row was
    /// never inserted, or was deleted by `delete` / `cleanup_aged`.
    #[must_use]
    async fn get_job(&self, job_id: &JobId) -> Result<Option<JobRecord>>;

    /// Most recent rows with `status = status` on `queue` (or any
    /// queue if `queue is None`). Capped at `limit`.
    async fn list_by_status(
        &self,
        queue: Option<&str>,
        status: JobStatus,
        limit: usize,
    ) -> Result<Vec<JobRecord>>;

    /// Per-status totals on `queue` (the Overview cards).
    async fn count_by_status(&self, queue: &str) -> Result<QueueCounts>;

    /// `scheduled_at` of the oldest *ready* pending job on `queue`
    /// (`status = 'pending' AND scheduled_at <= now`), or `None` if
    /// nothing is waiting. `now - this` is the queue's lag — the metric
    /// autoscalers (KEDA/HPA) scale on. Future-scheduled rows are
    /// excluded: they're deferred, not lagging.
    async fn oldest_ready_at(&self, queue: &str) -> Result<Option<DateTime<Utc>>>;

    /// Distinct kinds currently in the queue, optionally scoped to a
    /// single `queue` (`None` = across all queues). Used by Mission
    /// Control's filter dropdown — scoping it lets selecting a queue
    /// narrow the kind options to that queue's jobs.
    async fn distinct_kinds(&self, queue: Option<&str>) -> Result<Vec<String>>;

    /// Append-only event log events in `[from, to)`. Used by
    /// Mission Control's timeline chart, which buckets in Rust.
    /// Survives `cleanup_aged` of the underlying job rows so the
    /// historical chart doesn't lose data after retention.
    ///
    /// **Best-effort writes.** A worker that crashes between the
    /// `claim_next` UPDATE and the matching event INSERT loses the
    /// `Started` event for that attempt; the reaper revives the row
    /// and the re-claim writes a fresh `Started`, so the chart is
    /// only inconsistent for the crash window. See the inline note
    /// next to `claim_next` in the `SQLite` backend
    /// (`src/storage/sqlite/jobs.rs`).
    async fn list_for_timeline(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<TimelineEvent>>;

    /// Per-job processing + total latency for jobs that reached `done`
    /// with `completed_at` in `[from, to]`, newest first, capped at
    /// `limit`. Feeds the timeline's latency-percentile charts. Only
    /// rows with both `started_at` and `enqueued_at` set are returned
    /// (a job can't have a meaningful latency otherwise). The cap keeps
    /// a wide window from scanning unboundedly — percentiles from a
    /// recent sample are fine for a monitoring view.
    async fn completed_latencies(
        &self,
        queue: Option<&str>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<JobLatency>>;

    /// Idempotently upsert pre-aggregated rollup rows (keyed on
    /// `(queue, metric, bucket_start)`). Written by the metrics roller;
    /// a re-run overwrites rather than double-counts. No-op on an empty
    /// slice. See `docs/adr/0009-metrics-rollup.md`.
    async fn upsert_metric_buckets(&self, rows: &[MetricBucket]) -> Result<()>;

    /// Read rollup rows for the given `metrics` (and optional `queue`)
    /// with `bucket_start` in `[from, to]`, ascending by `bucket_start`.
    /// The base granularity is fixed (60s); callers that want a coarser
    /// resolution aggregate the returned rows themselves. An empty
    /// `metrics` slice returns no rows.
    async fn metric_buckets(
        &self,
        queue: Option<&str>,
        metrics: &[&str],
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<MetricBucket>>;

    /// Delete rollup rows with `bucket_start < before` (retention sweep,
    /// run by the cleanup loop). Returns the number of rows removed.
    async fn delete_metric_buckets_before(&self, before: DateTime<Utc>) -> Result<u64>;

    /// Delete a job by id (used by Mission Control's "Delete selected"
    /// action). For non-running statuses (`pending`, `failed`, `dead`,
    /// `done`) the row is removed → [`DeleteOutcome::Deleted`]. For
    /// `in_progress` rows it instead sets `cancel_requested_at = now()` so
    /// the worker's heartbeat observes it and stops the handler — the row
    /// stays until the worker finalizes it (then a later `delete` / the
    /// retention sweep removes it) → [`DeleteOutcome::CancelRequested`].
    /// [`DeleteOutcome::NotFound`] when no row matched. (L4: the tri-state
    /// lets callers report "cancellation requested" instead of conflating
    /// it with an actual delete.)
    async fn delete(&self, job_id: &JobId) -> Result<DeleteOutcome>;

    /// Move a `failed` / `dead` row back to `pending` so a worker
    /// re-claims it. Used by Mission Control's "Retry" action.
    async fn requeue(&self, job_id: &JobId) -> Result<bool>;

    /// Bulk-delete up to `batch_size` rows matching `status` (FIFO by
    /// id), optionally scoped to `queue`. Returns the count deleted.
    /// Callers should loop until the returned count is less than
    /// `batch_size` and `yield_now()` between calls so a 50k-row
    /// purge doesn't hold the writer pool for its full duration.
    async fn delete_batch_by_status(
        &self,
        queue: Option<&str>,
        status: JobStatus,
        batch_size: usize,
    ) -> Result<u64>;

    /// Bulk-requeue up to `batch_size` rows matching `status` (FIFO
    /// by id) back to `pending` with `attempts` reset. Returns the
    /// count requeued. Same loop-and-yield contract as
    /// [`Self::delete_batch_by_status`].
    async fn requeue_batch_by_status(
        &self,
        queue: Option<&str>,
        status: JobStatus,
        batch_size: usize,
    ) -> Result<u64>;

    /// Mark every `failed` row whose `dedupe_key` already has an active
    /// (`pending`/`in_progress`) sibling as `dead` with `last_error =
    /// "superseded by active sibling"`. Returns the count marked.
    ///
    /// Those rows are redundant — the active sibling covers the work —
    /// and would otherwise trip the `jq_dedupe` UNIQUE index whenever
    /// `claim_next` tries to flip them to `in_progress`, sending the
    /// worker into a 1-second-backoff loop on the same row. Host code
    /// calls this once at boot to unstick any queue that landed in
    /// that state before the claim-time pre-filter was deployed.
    async fn cleanup_superseded_retries(&self) -> Result<u64>;

    /// Pending rows scheduled to run strictly after `now` (i.e. the
    /// Rails `perform_later(wait_until: …)` future-cohort). Ordered
    /// ascending by `scheduled_at`. Capped at `limit`.
    async fn list_scheduled_after(
        &self,
        queue: Option<&str>,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<JobRecord>>;

    /// Advance one pending row's `scheduled_at` to `now` so the next
    /// claim picks it up immediately. Returns `true` when a row was
    /// touched. No-op when the row is missing, already running, or
    /// terminal.
    async fn run_now(&self, job_id: &JobId) -> Result<bool>;

    // -------- notification --------

    /// Block until either a new job arrives on `queue` or `timeout`
    /// elapses. Returns `true` when notified, `false` on timeout.
    ///
    /// `SQLite` uses an in-process `tokio::sync::Notify`. Redis would
    /// use `BLPOP` or `SUBSCRIBE`. Postgres would use `LISTEN/NOTIFY`.
    async fn wait_for_work(&self, queue: &str, timeout: Duration) -> Result<bool>;

    /// Wake any waiters on this queue. Called by `enqueue` so workers
    /// pick up new work without polling. Default impl: no-op (a
    /// backend with BLPOP-style blocking doesn't need explicit notify).
    async fn notify(&self, _queue: &str) -> Result<()> {
        Ok(())
    }

    /// Snapshot of static facts about this storage backend, surfaced
    /// in the host's boot banner. See [`StorageInfo`] for the shape;
    /// each adapter chooses what to expose. Called once at startup.
    async fn describe(&self) -> Result<StorageInfo>;

    /// Take all buffered db-operation latency samples (one per
    /// `JobQueue` call) since the last drain, split by read/write
    /// kind. Called once per metrics tick. Default: empty buckets
    /// (test mocks don't need to instrument).
    fn drain_op_samples(&self) -> db_timing::DrainedSamples {
        db_timing::DrainedSamples::default()
    }

    /// DB-sourced health gauges to write this tick — `(metric_name,
    /// value)` pairs. Each backend returns only what it can truthfully
    /// query from its own database state, never derived bookkeeping:
    ///
    /// - `SQLite`: file/WAL bytes from `PRAGMA page_count * page_size`
    ///   + a stat of the `-wal` sidecar. (`SQLite` has no server-side
    ///   connection model, so no `db_pool_*` rows.)
    /// - Postgres: `db_pool_active` / `db_pool_idle` from
    ///   `pg_stat_activity`, `db_pool_max` from `pg_settings`,
    ///   `db_size_bytes` from `pg_database_size()`.
    ///
    /// Default: empty (test mocks have no DB-side state to surface).
    async fn db_health_snapshot(&self) -> Vec<(&'static str, f64)> {
        Vec::new()
    }
}

// ────────────────────────────────────────────────────────────────────
// ProcessRegistry — live worker roster.
// ────────────────────────────────────────────────────────────────────

/// Tracks running workers so the supervisor + reaper can see who's
/// alive. Rows are operational state, not history.
#[async_trait]
pub trait ProcessRegistry: Send + Sync + std::fmt::Debug {
    async fn register(&self, process_id: &str, queue: &str, host: &str) -> Result<()>;

    /// Set `heartbeat_at = now` and, optionally, the job this worker
    /// is currently processing (pass `None` to clear).
    async fn heartbeat(&self, process_id: &str, current_job: Option<JobId>) -> Result<()>;

    async fn deregister(&self, process_id: &str) -> Result<()>;

    /// Delete process rows with `heartbeat_at < stale_before`.
    /// Returns the count.
    async fn reap_stale(&self, stale_before: DateTime<Utc>) -> Result<u64>;

    /// Snapshot of live workers, optionally filtered by queue.
    async fn list(&self, queue: Option<&str>) -> Result<Vec<ProcessRecord>>;

    /// Drop every process row belonging to `host`. Used on graceful
    /// shutdown to clean up the current process's slot.
    async fn delete_for_host(&self, host: &str) -> Result<u64>;

    // ── cluster rebalancing ──────────────────────────────────────────

    /// Record this pod's liveness (upsert `heartbeat_at = now`). A pod
    /// heartbeats its own existence independent of workers so the
    /// rebalancer can see it even when it's been assigned 0 slots.
    async fn pod_heartbeat(&self, host: &str) -> Result<()>;

    /// Sorted `host_id`s of pods whose `pod.heartbeat_at >=
    /// stale_before`. The rebalancer splits each queue's `max_workers`
    /// across exactly this set.
    async fn list_live_pods(&self, stale_before: DateTime<Utc>) -> Result<Vec<String>>;

    /// Write a pod's worker allocation for a queue (upsert). The
    /// rebalancer leader calls this; each supervisor reads its own row.
    async fn set_slots(&self, queue: &str, host: &str, slots: i32) -> Result<()>;

    /// This pod's assigned slot count for a queue, or `None` if the
    /// rebalancer hasn't written one yet (caller falls back to the
    /// cluster total so a fresh pod still does work pre-rebalance).
    async fn get_slots(&self, queue: &str, host: &str) -> Result<Option<i32>>;
}

// ────────────────────────────────────────────────────────────────────
// QueueConfig — per-queue config knobs.
// ────────────────────────────────────────────────────────────────────

/// Stores `max_workers`, `paused`, and retention windows per named
/// queue. The supervisor reads `max_workers` + `paused` every tick;
/// Mission Control writes via the IPCs.
#[async_trait]
pub trait QueueConfig: Send + Sync + std::fmt::Debug {
    /// Insert a queue row with the given default if absent. Does NOT
    /// overwrite an existing row's `max_workers` (the user may have
    /// tuned it).
    async fn ensure_queue(&self, name: &str, default_max_workers: i32) -> Result<()>;

    #[must_use]
    async fn get_queue(&self, name: &str) -> Result<Option<QueueConfigRow>>;

    async fn list_queues(&self) -> Result<Vec<QueueConfigRow>>;

    async fn set_max_workers(&self, name: &str, n: i32) -> Result<()>;

    async fn set_paused(&self, name: &str, paused: bool) -> Result<()>;

    async fn set_retention(&self, name: &str, done_days: i32, dead_days: i32) -> Result<()>;

    /// Set the per-queue throttle backoff. `enabled = false` keeps the
    /// runtime on the legacy flat 60s throttle delay for this queue.
    /// `base_seconds` and `max_seconds` are clamped to `[1, 86400]`.
    async fn set_backoff(
        &self,
        name: &str,
        enabled: bool,
        base_seconds: i32,
        max_seconds: i32,
    ) -> Result<()>;
}

// ────────────────────────────────────────────────────────────────────
// CronStorage — schedules table for the cron service.
// ────────────────────────────────────────────────────────────────────

/// Persists cron schedule rows and tracks their last-fired /
/// next-fire timestamps. The cron service polls `list_all()` every
/// tick and decides which to enqueue.
#[async_trait]
pub trait CronStorage: Send + Sync + std::fmt::Debug {
    async fn ensure_schedule(&self, schedule: NewCronSchedule) -> Result<()>;

    async fn list_schedules(&self) -> Result<Vec<CronScheduleRecord>>;

    /// Record that the named schedule fired at `fired_at`, and
    /// schedule its next firing at `next_at`. Unconditional — used for
    /// seeding `next_fire_at` on a freshly-enabled schedule.
    async fn record_fire(
        &self,
        name: &str,
        fired_at: DateTime<Utc>,
        next_at: DateTime<Utc>,
    ) -> Result<()>;

    /// Atomically claim a due fire: advance `next_fire_at` to `next_at`
    /// (and stamp `last_fired_at = fired_at`) only if it still equals
    /// `expected` — the value the caller read when it decided the
    /// schedule was due. Returns `true` if this caller won the claim.
    ///
    /// The fence against cross-replica double-fire: if a slow leader's
    /// lease lapsed mid-tick and a second replica picked up the same due
    /// schedule, only one CAS succeeds, so only one enqueues. The caller
    /// must enqueue **only** when this returns `true`.
    async fn try_advance_fire(
        &self,
        name: &str,
        expected: DateTime<Utc>,
        fired_at: DateTime<Utc>,
        next_at: DateTime<Utc>,
    ) -> Result<bool>;

    /// Persist a parse error against a schedule. Disables it.
    async fn record_parse_error(&self, name: &str, message: &str) -> Result<()>;

    async fn set_enabled(&self, name: &str, enabled: bool) -> Result<()>;

    async fn set_expr(&self, name: &str, expr: &str) -> Result<()>;

    /// Delete the named schedule row. Idempotent — no-op when the
    /// row doesn't exist. Used by host crates to clean up defunct
    /// schedules after a handler rename or fold (otherwise the
    /// row sits in the table forever, surfacing as a useless entry
    /// in the Cron tab).
    async fn delete_schedule(&self, name: &str) -> Result<()>;

    #[must_use]
    async fn get_schedule(&self, name: &str) -> Result<Option<CronScheduleRecord>>;

    /// Try to hold the cluster-wide cron lease for `ttl`. Returns
    /// `true` if this process (`holder`) is the leader for the current
    /// tick — only the leader fires schedules, so N replicas don't each
    /// enqueue every schedule N times.
    ///
    /// Lease-based (not an advisory lock): the holder must renew within
    /// `ttl` or another replica takes over, so leadership recovers
    /// ~`ttl` after a leader crashes. `ttl` must exceed the cron tick
    /// interval. `SQLite` is single-process, so its impl always grants.
    async fn try_cron_lease(&self, holder: &str, ttl: std::time::Duration) -> Result<bool>;
}

// ────────────────────────────────────────────────────────────────────
// RateLimitStorage — cluster-wide token-bucket budget.
// ────────────────────────────────────────────────────────────────────

/// Outcome of a single [`RateLimitStorage::acquire`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RateLimitOutcome {
    /// The acquire consumed a token; handler should proceed with
    /// the upstream call.
    Granted,
    /// Bucket is empty. Handler should return `Throttled` without
    /// making the upstream call; the runtime's queue cool-down
    /// engages and other workers on the same scope also back off.
    Throttled,
}

/// Cluster-wide rate-limit budget.
///
/// One row per `scope` (today: per queue name — `"slack"`, `"gh"`).
/// Token bucket math is server-side so concurrent acquires across
/// replicas can't both spend the same last token.
///
/// `acquire` is the hot path on every handler that talks to an
/// external API; `drain` is called when the handler observes a real
/// 429 from upstream (so the next acquire in the window is forced
/// throttled, not just rate-shaped); `ensure_default` seeds a row
/// at host boot.
#[async_trait]
pub trait RateLimitStorage: Send + Sync + std::fmt::Debug {
    /// Try to consume one token from the bucket. Idempotent under
    /// failure: if the SQL succeeds the token is gone; if it errors
    /// the bucket is unchanged.
    async fn acquire(&self, scope: &str) -> Result<RateLimitOutcome>;

    /// Zero out the bucket for this scope. Used when the handler
    /// observes a real upstream 429: even though we thought we had
    /// budget, the server's accounting disagreed — drain so the next
    /// acquire sees empty and the cool-down engages.
    async fn drain(&self, scope: &str) -> Result<()>;

    /// Insert a row for this scope at `capacity` tokens if it
    /// doesn't exist yet. No-ops if a row is already present so an
    /// operator-tuned bucket (future feature) survives a reboot.
    async fn ensure_default(&self, scope: &str, capacity: i64, refill_per_sec: f64) -> Result<()>;
}

// ────────────────────────────────────────────────────────────────────
// Storage — convenience bundle.
// ────────────────────────────────────────────────────────────────────

/// Aggregates the five trait Arcs so the runtime can be constructed
/// from a single value. A backend that implements all five traits on
/// one type can build this via `Storage::from_one(Arc::new(impl))`.
///
/// `#[non_exhaustive]` so a sixth trait concern (e.g. a future
/// `MetricsStorage`) can land without bumping the major version.
/// External consumers construct via `from_one`; internal code (this
/// crate) still accesses the fields directly.
#[derive(Clone)]
#[non_exhaustive]
pub struct Storage {
    pub jobs: Arc<dyn JobQueue>,
    pub procs: Arc<dyn ProcessRegistry>,
    pub config: Arc<dyn QueueConfig>,
    pub cron: Arc<dyn CronStorage>,
    pub rate_limit: Arc<dyn RateLimitStorage>,
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage").finish_non_exhaustive()
    }
}

impl Storage {
    /// Build a `Storage` from a single type that implements all four
    /// traits — the common case. The same `Arc` is shared four ways.
    #[must_use]
    pub fn from_one<T>(inner: Arc<T>) -> Self
    where
        T: JobQueue + ProcessRegistry + QueueConfig + CronStorage + RateLimitStorage + 'static,
    {
        Self {
            jobs: inner.clone(),
            procs: inner.clone(),
            config: inner.clone(),
            cron: inner.clone(),
            rate_limit: inner,
        }
    }
}
