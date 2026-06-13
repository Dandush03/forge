//! `QueueRuntime` — supervisors + workers + reaper + cleanup + cron.
//!
//! Everything in this module talks to the storage layer through the
//! four trait Arcs bundled in [`crate::storage::Storage`]. Swapping
//! from `SQLite` to Redis (or anything else) only requires a new impl
//! of the four traits — nothing in this file changes.
//!
//! On `start()` the runtime spawns:
//!  - one supervisor per registered queue (scales workers to match
//!    `max_workers` / `paused`)
//!  - one reaper task (revives stuck jobs, sweeps stale workers)
//!  - one cleanup task (purges aged `done` / `dead` rows per
//!    retention)
//!  - one cron task (fires recurring schedules)
//!
//! Worker loop: claim → run → finalize → `wait_for_work` → repeat.
//! The `wait_for_work` primitive is the storage layer's job — `SQLite`
//! uses an in-process `Notify`; Redis would use BLPOP; Postgres
//! would use LISTEN/NOTIFY.

mod cmd_exec;
mod cron;
mod demo;
mod handler;
mod metrics;
mod rate_limit;
mod rebalance;
mod retry;
mod routing;
mod worker_pool;

pub use cmd_exec::{CMD_EXEC_KIND, CmdExecHandler, CmdExecPayload};
pub use cron::{CRON_TICK, CronTickReport, cron_tick_once, ensure_schedules};
pub use demo::{NOOP_ECHO_KIND, NoopEcho};
pub use handler::{HandlerRegistry, JobCtx, JobHandler, JobOutcome};
pub use metrics::{METRICS_BUCKET_SECS, METRICS_TICK, metrics_roll_once};
pub use rate_limit::{
    AcquireOutcome, DEFAULT_RATE_LIMIT_SCOPES, RateLimiter, ensure_default_rate_limits,
};
pub use rebalance::{REBALANCE_TICK, rebalance_once};
pub(crate) use retry::{THROTTLE_DECAY_GRACE_SECS, failed_delay};
pub use routing::{DefaultRouter, KindPrefixRouter, Router};
pub use worker_pool::{WorkerPoolConfig, WorkerPoolHandler};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Duration as ChronoDuration;
use chrono::Utc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::storage::HeartbeatStatus;
use crate::storage::Storage;
use crate::storage::error::Result;
use crate::storage::types::{
    EnqueueOutcome, EnqueueRequest, FinalizeOutcome, JobId, JobRecord, JobStatus,
};

/// Default worker counts per named queue.
///
/// Hosts iterate this at boot after `QueueRuntime::new` and call
/// `ensure_queue(name, n)` for each. `ensure_queue` won't overwrite an
/// existing row's `max_workers`, so these are only used for a freshly-
/// created queue config row; user-tuned values from the Mission Control
/// panel persist.
///
/// The `gh` and `slack` queues get > 1 worker so an initial backfill
/// drains in parallel out of the box; `default` stays at 1 because
/// most catch-all kinds are I/O-bound rather than CPU-bound.
pub const DEFAULT_QUEUE_WORKERS: &[(&str, i32)] = &[("default", 1), ("gh", 3), ("slack", 2)];

/// Env var naming the comma-separated queues a worker consumes.
pub const QUEUES_ENV: &str = "FORGE_QUEUES";
/// Env var giving a worker its human-friendly monitoring label.
pub const WORKER_NAME_ENV: &str = "FORGE_WORKER_NAME";

/// Parse [`QUEUES_ENV`] (`FORGE_QUEUES=gh,slack`) into a queue list.
///
/// For [`QueueRuntime::with_queues`]. Whitespace is trimmed and blanks
/// dropped. Returns an empty vec when unset — `start()` then rejects it,
/// surfacing the misconfiguration rather than silently running nothing.
#[must_use]
pub fn queues_from_env() -> Vec<String> {
    std::env::var(QUEUES_ENV)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Read [`WORKER_NAME_ENV`] for [`QueueRuntime::with_worker_name`].
/// `None` when unset/blank.
#[must_use]
pub fn worker_name_from_env() -> Option<String> {
    std::env::var(WORKER_NAME_ENV)
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

const SUPERVISOR_TICK: Duration = Duration::from_secs(1);
/// Worker idle-poll fallback when the storage's `wait_for_work`
/// returns without a notify (timeout). 500 ms keeps a missed wake
/// from stalling the queue more than half a second.
const IDLE_POLL: Duration = Duration::from_millis(500);
const WORKER_CAP: usize = 64;
/// In-flight job heartbeat cadence.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// Reaper tick cadence.
pub const REAPER_TICK: Duration = Duration::from_secs(15);
/// Jobs / processes with heartbeat older than this are reaped.
///
/// L5 — clock domain: the staleness horizon is computed as
/// `Utc::now() - STALE_THRESHOLD` from the **runtime (app) clock** and
/// passed as an absolute instant to `revive_stale` / `reap_stale` /
/// `list_live_pods`. The cron/coordinator lease, by contrast, compares
/// and writes with the **DB clock** (`now()`) on both sides, so it's
/// internally consistent. On a single-process `SQLite` deploy these are
/// the same clock. On a multi-replica Postgres deploy they differ by the
/// app↔DB skew: a replica whose clock runs behind could briefly look
/// stale to the leader (and be trimmed from the live-pod set) and
/// self-heal on the next heartbeat. The 60s threshold is >> realistic
/// NTP-synced skew, so this is a documented assumption, not a live bug;
/// moving the horizons into SQL (`now() - interval`) would unify the
/// domain if a deploy ever runs with looser clocks.
const STALE_THRESHOLD: ChronoDuration = ChronoDuration::seconds(60);
/// Retention cleanup cadence.
pub const CLEANUP_TICK: Duration = Duration::from_mins(5);
/// Timeline-event flush cadence.
///
/// Workers buffer `queue_event` rows in-process (off the hot enqueue /
/// claim / finalize transactions); this loop drains and batch-inserts
/// them. Well under `METRICS_TICK` (60s) so a minute's events are
/// persisted before the metrics roller aggregates them, and short enough
/// that a crash loses only a small tail of chart data. Runs on every
/// replica — each flushes its own buffer.
pub const EVENT_FLUSH_TICK: Duration = Duration::from_secs(2);
/// Default `shutdown_graceful` timeout.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

// ────────────────────────────────────────────────────────────────────
// QueueRuntime
// ────────────────────────────────────────────────────────────────────

/// In-process map from currently-running `JobId` → per-job cancel
/// token. Lets `QueueHandle::request_cancel` short-circuit the
/// heartbeat-tick round-trip when the job runs on the same pod;
/// cross-pod cancels still flow through the DB flag.
type RunningJobs = Arc<Mutex<HashMap<JobId, CancellationToken>>>;

/// Top-level handle for the queue subsystem.
#[derive(Clone)]
pub struct QueueRuntime {
    storage: Storage,
    handlers: Arc<HandlerRegistry>,
    router: Arc<dyn Router>,
    host_id: String,
    running_jobs: RunningJobs,
    /// Constructed once at `new()` so the per-scope refill-rate
    /// cache lives across every worker's `JobCtx` borrow.
    rate_limit: Arc<RateLimiter>,
    /// Queues this worker is responsible for. **Required** — `start()`
    /// errors if empty. Set via [`QueueRuntime::with_queues`] /
    /// [`queues_from_env`]. The supervisor spawns one loop per queue
    /// here; the rebalancer only hands this pod slots for these queues.
    queues: Vec<String>,
    /// Optional human-friendly label for this worker (`FORGE_WORKER_NAME`),
    /// surfaced in the monitoring view. `None` → display falls back to
    /// `host_id`.
    worker_name: Option<String>,
}

impl std::fmt::Debug for QueueRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueRuntime")
            .field("host_id", &self.host_id)
            .field("handlers", &self.handlers)
            .finish_non_exhaustive()
    }
}

impl QueueRuntime {
    /// Build a new runtime. `host_id` is freshly minted from a ULID
    /// so each process boot has its own identity in the process
    /// registry.
    #[must_use]
    pub fn new(storage: Storage, handlers: HandlerRegistry, router: Arc<dyn Router>) -> Self {
        let rate_limit = Arc::new(RateLimiter::new(
            storage.clone(),
            rate_limit::DEFAULT_RATE_LIMIT_SCOPES,
        ));
        Self {
            storage,
            handlers: Arc::new(handlers),
            router,
            host_id: Ulid::new().to_string(),
            running_jobs: Arc::new(Mutex::new(HashMap::new())),
            rate_limit,
            queues: Vec::new(),
            worker_name: None,
        }
    }

    /// Declare the queues this worker is responsible for. **Required** —
    /// a worker that declares none fails at [`start`](Self::start).
    /// Names are de-duplicated, preserving first-seen order. Only
    /// supervisors for these queues are spawned, and the rebalancer only
    /// hands this pod slots for them.
    #[must_use]
    pub fn with_queues(mut self, queues: impl IntoIterator<Item = String>) -> Self {
        let mut seen = std::collections::HashSet::new();
        self.queues = queues
            .into_iter()
            .filter(|q| !q.is_empty() && seen.insert(q.clone()))
            .collect();
        self
    }

    /// Set a human-friendly label for this worker, shown in the
    /// monitoring view. Unset → the view falls back to `host_id`.
    #[must_use]
    pub fn with_worker_name(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        self.worker_name = (!name.is_empty()).then_some(name);
        self
    }

    /// Ensure a queue config row exists. Safe to call before or after
    /// `start`; the supervisor for a newly-added queue is *not*
    /// spawned until the next `start` invocation.
    pub async fn ensure_queue(&self, name: &str, default_max_workers: i32) -> Result<()> {
        self.storage
            .config
            .ensure_queue(name, default_max_workers)
            .await
    }

    /// Insert a job and wake the destination queue.
    pub async fn enqueue(&self, req: EnqueueRequest) -> Result<EnqueueOutcome> {
        let mut req = req;
        if req.queue_name.is_none() {
            req.queue_name = Some(std::borrow::Cow::Borrowed(
                self.router.route(req.kind.as_ref()),
            ));
        }
        self.storage.jobs.enqueue(req).await
    }

    /// Spawn one supervisor per registered queue + reaper + cleanup
    /// + cron. Returns a [`QueueHandle`] for orchestration.
    pub async fn start(self) -> Result<QueueHandle> {
        // Declaring queues is mandatory — running every queue implicitly
        // is no longer supported (a worker now owns an explicit subset so
        // the cluster can split responsibilities). Fail fast and loud.
        if self.queues.is_empty() {
            return Err(crate::storage::error::StorageError::Config(
                "no queues declared: set FORGE_QUEUES or call QueueRuntime::with_queues — \
                 running all queues implicitly is no longer supported"
                    .to_owned(),
            ));
        }

        let shutdown = CancellationToken::new();
        let mut join_set = JoinSet::new();

        for name in &self.queues {
            // A queue name is round-tripped through the `pod.queues` CSV
            // column, so it must not contain the ',' delimiter (else it
            // would decode into phantom queues). Reject at the declaration
            // gate rather than corrupting silently downstream.
            crate::storage::types::validate_queue_name(name)?;
            // Make sure the config row exists so the supervisor has a
            // max_workers/paused row to read even on a fresh DB. Seed
            // from the well-known defaults when we recognize the queue,
            // else 1.
            let default_workers = DEFAULT_QUEUE_WORKERS
                .iter()
                .find_map(|(q, n)| (*q == name).then_some(*n))
                .unwrap_or(1);
            self.storage.config.ensure_queue(name, default_workers).await?;
            join_set.spawn(supervisor_loop(
                self.storage.clone(),
                self.handlers.clone(),
                self.router.clone(),
                name.clone(),
                self.host_id.clone(),
                self.running_jobs.clone(),
                self.rate_limit.clone(),
                shutdown.clone(),
            ));
        }

        join_set.spawn(reaper_loop(self.storage.clone(), shutdown.clone()));
        join_set.spawn(cleanup_loop(
            self.storage.clone(),
            self.host_id.clone(),
            shutdown.clone(),
        ));
        join_set.spawn(cron::cron_loop(
            self.storage.clone(),
            self.router.clone(),
            self.host_id.clone(),
            shutdown.clone(),
        ));
        // Cluster rebalancing: every pod stamps its liveness; the
        // coordinator (cron-lease holder) splits each queue's
        // max_workers across live pods into pod_slot_assignment.
        join_set.spawn(rebalance::pod_heartbeat_loop(
            self.storage.clone(),
            self.host_id.clone(),
            self.worker_name.clone(),
            self.queues.clone(),
            shutdown.clone(),
        ));
        join_set.spawn(rebalance::rebalance_loop(
            self.storage.clone(),
            self.host_id.clone(),
            shutdown.clone(),
        ));
        // Metrics rollup: the cron-lease holder pre-aggregates per-queue
        // counts + latency percentiles into metric_bucket every minute.
        join_set.spawn(metrics::metrics_loop(
            self.storage.clone(),
            self.host_id.clone(),
            shutdown.clone(),
        ));
        // Timeline-event flush: drain this replica's in-process event
        // buffer and batch-insert into queue_event. Not lease-gated —
        // every replica flushes the events its own workers produced.
        join_set.spawn(event_flush_loop(self.storage.clone(), shutdown.clone()));

        Ok(QueueHandle {
            shutdown,
            join_set,
            host_id: self.host_id,
            storage: self.storage,
            handlers: self.handlers,
            router: self.router,
            running_jobs: self.running_jobs,
            rate_limit: self.rate_limit,
        })
    }
}

// ────────────────────────────────────────────────────────────────────
// QueueHandle
// ────────────────────────────────────────────────────────────────────

pub struct QueueHandle {
    shutdown: CancellationToken,
    join_set: JoinSet<()>,
    host_id: String,
    storage: Storage,
    handlers: Arc<HandlerRegistry>,
    router: Arc<dyn Router>,
    running_jobs: RunningJobs,
    rate_limit: Arc<RateLimiter>,
}

impl std::fmt::Debug for QueueHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueHandle")
            .field("host_id", &self.host_id)
            .field("handlers", &self.handlers)
            .finish_non_exhaustive()
    }
}

impl QueueHandle {
    /// Per-boot identifier. Surfaced for tests + diagnostics.
    #[must_use]
    pub fn host_id(&self) -> &str {
        &self.host_id
    }

    /// Read-only access to the storage for IPC commands.
    #[must_use]
    pub const fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Cluster-wide rate limiter — same instance every worker's
    /// `JobCtx::rate_limit` points at. Surfaced so out-of-band
    /// callers (e.g. cron tasks spawned outside the worker loop)
    /// can `acquire` against the same budget as in-flight jobs.
    #[must_use]
    pub fn rate_limit(&self) -> &RateLimiter {
        &self.rate_limit
    }

    /// Signal a cancel for an in-flight job by id.
    ///
    /// If the job is running on this pod, cancels its in-process
    /// token immediately and returns `true`. If the job isn't on
    /// this pod (or isn't running at all) returns `false`; cross-pod
    /// cancels go through `JobQueue::delete` on an `in_progress`
    /// row, which sets a DB flag the owning pod's heartbeat tick
    /// observes within `HEARTBEAT_INTERVAL`.
    #[must_use]
    pub fn request_cancel(&self, job_id: &JobId) -> bool {
        // poison-safe: a panic-poisoned lock means *some* worker hit
        // a panic mid-insert/remove; better to take the lock and try
        // than to silently fail every cancel.
        let map = match self.running_jobs.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        map.get(job_id).is_some_and(|token| {
            token.cancel();
            true
        })
    }

    /// Enqueue from a different task after start. Same semantics as
    /// `QueueRuntime::enqueue`.
    pub async fn enqueue(&self, req: EnqueueRequest) -> Result<EnqueueOutcome> {
        let mut req = req;
        if req.queue_name.is_none() {
            req.queue_name = Some(std::borrow::Cow::Borrowed(
                self.router.route(req.kind.as_ref()),
            ));
        }
        self.storage.jobs.enqueue(req).await
    }

    /// Signal shutdown, wait up to `timeout` for in-flight jobs to
    /// drain, then delete process registry rows for this host.
    pub async fn shutdown_graceful(mut self, timeout: Duration) {
        self.shutdown.cancel();
        let drain = async { while self.join_set.join_next().await.is_some() {} };
        if tokio::time::timeout(timeout, drain).await.is_err() {
            tracing::warn!(
                timeout_secs = timeout.as_secs(),
                host_id = %self.host_id,
                "shutdown_graceful: timeout exceeded, aborting remaining tasks",
            );
            self.join_set.abort_all();
            while self.join_set.join_next().await.is_some() {}
        }
        // Workers have now drained (or been aborted); flush once more so
        // any timeline events buffered during the drain window — after
        // event_flush_loop did its own final flush and exited — still
        // land. Best-effort: chart data, never job state.
        if let Err(e) = self.storage.jobs.flush_event_buffer().await {
            tracing::warn!(?e, host_id = %self.host_id, "event flush on shutdown failed");
        }
        if let Err(e) = self.storage.procs.delete_for_host(&self.host_id).await {
            tracing::warn!(?e, host_id = %self.host_id, "delete_for_host on shutdown failed");
        }
    }
}

// ────────────────────────────────────────────────────────────────────
// Supervisor — one per queue.
// ────────────────────────────────────────────────────────────────────

struct WorkerSlot {
    handle: JoinHandle<()>,
    cancel: CancellationToken,
}

#[allow(
    clippy::too_many_arguments,
    reason = "per-supervisor scratch state; `running_jobs` is the in-process cancel registry shared with workers, `rate_limit` is the boot-constructed limiter passed by ref to JobCtx"
)]
async fn supervisor_loop(
    storage: Storage,
    handlers: Arc<HandlerRegistry>,
    router: Arc<dyn Router>,
    queue_name: String,
    host_id: String,
    running_jobs: RunningJobs,
    rate_limit: Arc<RateLimiter>,
    shutdown: CancellationToken,
) {
    tracing::info!(queue = %queue_name, host_id = %host_id, "supervisor: start");
    let mut workers: HashMap<usize, WorkerSlot> = HashMap::new();
    let mut tick = tokio::time::interval(SUPERVISOR_TICK);
    tick.tick().await;

    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::info!(queue = %queue_name, "supervisor: shutdown signal");
                drain_all(&mut workers).await;
                tracing::info!(queue = %queue_name, "supervisor: stopped");
                return;
            }
            _ = tick.tick() => {
                reap_finished(&mut workers).await;
                let Some(target) = resolve_target(&storage, &queue_name, &host_id).await else {
                    continue;
                };
                scale_down(&mut workers, target);
                scale_up(
                    &mut workers,
                    target,
                    &storage,
                    &handlers,
                    &router,
                    &queue_name,
                    &host_id,
                    &running_jobs,
                    &rate_limit,
                );
            }
        }
    }
}

/// Drop every worker slot whose join handle has resolved.
///
/// We collect the finished slot ids first, then await each handle to
/// surface its `JoinError`. A `panic!()` inside a handler shows up
/// here as `Err(JoinError::panic)` — without the await, the panic
/// payload disappears silently. Cancellation (the shutdown path) is
/// also a `JoinError` but `drain_all` does its own await + log.
async fn reap_finished(workers: &mut HashMap<usize, WorkerSlot>) {
    let finished: Vec<usize> = workers
        .iter()
        .filter_map(|(&id, slot)| slot.handle.is_finished().then_some(id))
        .collect();
    for id in finished {
        if let Some(slot) = workers.remove(&id)
            && let Err(e) = slot.handle.await
        {
            tracing::error!(?e, slot = id, "supervisor: worker task panicked");
        }
    }
}

async fn resolve_target(storage: &Storage, queue_name: &str, host_id: &str) -> Option<usize> {
    let q = match storage.config.get_queue(queue_name).await {
        Ok(Some(q)) => q,
        Ok(None) => {
            tracing::warn!(queue = %queue_name, "supervisor: queue row vanished");
            return None;
        }
        Err(e) => {
            tracing::warn!(queue = %queue_name, ?e, "supervisor: queue lookup failed");
            return None;
        }
    };
    if q.paused {
        return Some(0);
    }
    // This pod's share of the cluster-wide max_workers, as set by the
    // rebalancer. When no assignment exists yet (fresh pod,
    // pre-first-rebalance, or rebalancer down) fall back to a fair-share
    // *estimate* — NOT the full total. M6: running the whole total here
    // means a rolling deploy's N replacement pods each spin up the full
    // count, an N× over-parallelism storm aimed at the very upstreams the
    // cluster rate budget exists to protect. The next rebalance refines
    // the estimate. On SQLite the lone live pod estimates the whole total.
    let raw = match storage.procs.get_slots(queue_name, host_id).await {
        Ok(Some(slots)) => usize::try_from(slots).unwrap_or(0),
        Ok(None) => fair_fallback(storage, queue_name, q.max_workers).await,
        Err(e) => {
            tracing::warn!(queue = %queue_name, ?e, "supervisor: slot lookup failed; estimating fair share");
            fair_fallback(storage, queue_name, q.max_workers).await
        }
    };
    Some(raw.min(WORKER_CAP))
}

/// Fair-share worker estimate for a pod that has no slot assignment yet.
/// `max_workers` is the cluster total; divide by the count of live pods
/// *eligible for this queue* (those that declared it) — rounded up so the
/// fleet never *under*-serves the total, clamped to ≥1 pod. Counting only
/// eligible pods mirrors the rebalancer: if just one pod runs `gh`, it
/// estimates the whole `gh` total rather than a fraction. Used only on the
/// rare unassigned / rebalancer-down path, so the extra `list_live_pods`
/// read isn't on the steady per-tick path.
async fn fair_fallback(storage: &Storage, queue_name: &str, max_workers: i32) -> usize {
    let total = usize::try_from(max_workers).unwrap_or(0);
    if total == 0 {
        return 0;
    }
    let stale_before = Utc::now() - STALE_THRESHOLD;
    let eligible = storage.procs.list_live_pods(stale_before).await.map_or(1, |pods| {
        pods.iter().filter(|p| p.handles(queue_name)).count()
    });
    total.div_ceil(eligible.max(1))
}

fn scale_down(workers: &mut HashMap<usize, WorkerSlot>, target: usize) {
    while workers.len() > target {
        let Some(&max_slot) = workers.keys().max() else {
            break;
        };
        if let Some(slot) = workers.remove(&max_slot) {
            slot.cancel.cancel();
            drop(slot.handle);
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "supervisor scratch state passed through to each spawned worker. Threshold to revisit: if scale_up ever needs one more parameter, OR if any of the loops here (supervisor_loop, worker_loop) grows another ~30-line block, extract a `WorkerCtx` struct."
)]
fn scale_up(
    workers: &mut HashMap<usize, WorkerSlot>,
    target: usize,
    storage: &Storage,
    handlers: &Arc<HandlerRegistry>,
    router: &Arc<dyn Router>,
    queue_name: &str,
    host_id: &str,
    running_jobs: &RunningJobs,
    rate_limit: &Arc<RateLimiter>,
) {
    while workers.len() < target {
        let mut slot_idx: usize = 0;
        while workers.contains_key(&slot_idx) {
            slot_idx = slot_idx.saturating_add(1);
        }
        let process_id = format!("{queue_name}-{slot_idx}-{host_id}");
        tracing::info!(
            queue = %queue_name,
            slot = slot_idx,
            worker_id = %process_id,
            "worker spawned"
        );
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(worker_loop(
            storage.clone(),
            handlers.clone(),
            router.clone(),
            queue_name.to_owned(),
            process_id,
            host_id.to_owned(),
            running_jobs.clone(),
            rate_limit.clone(),
            cancel.clone(),
        ));
        workers.insert(slot_idx, WorkerSlot { handle, cancel });
    }
}

async fn drain_all(workers: &mut HashMap<usize, WorkerSlot>) {
    for (_, slot) in workers.drain() {
        slot.cancel.cancel();
        if let Err(e) = slot.handle.await {
            tracing::warn!(?e, "supervisor: worker join error on drain");
        }
    }
}

// ────────────────────────────────────────────────────────────────────
// Worker — claim/run/finalize loop.
// ────────────────────────────────────────────────────────────────────

/// Surface handler `Failed` / `Dead` outcomes via `tracing` so they don't
/// bury silently in `last_error`. The `error` field carries the full source
/// chain assembled by [`crate::format_error_chain`] upstream.
fn log_handler_outcome(job: &JobRecord, job_id: &JobId, outcome: &FinalizeOutcome) {
    match outcome {
        FinalizeOutcome::Failed {
            message,
            retry_after,
        } => tracing::warn!(
            kind = %job.kind,
            queue = %job.queue_name,
            job_id = %job_id.as_str(),
            attempts = job.attempts,
            max_attempts = job.max_attempts,
            retry_in_secs = retry_after.as_secs(),
            error = %message,
            "worker: handler failed; will retry",
        ),
        FinalizeOutcome::Dead { message } => tracing::error!(
            kind = %job.kind,
            queue = %job.queue_name,
            job_id = %job_id.as_str(),
            attempts = job.attempts,
            error = %message,
            "worker: handler failed terminally (dead)",
        ),
        FinalizeOutcome::Done | FinalizeOutcome::Throttled { .. } => {}
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "worker scratch state; B2 cancel registry + Dead-on-user-cancel branch pushed past the 100-line cap. The loop is one cohesive claim→run→finalize cycle; splitting it splits the lifetime of the per-job cancel token. Revisit when extracting a `WorkerCtx`."
)]
async fn worker_loop(
    storage: Storage,
    handlers: Arc<HandlerRegistry>,
    router: Arc<dyn Router>,
    queue_name: String,
    process_id: String,
    host_id: String,
    running_jobs: RunningJobs,
    rate_limit: Arc<RateLimiter>,
    cancel: CancellationToken,
) {
    tracing::debug!(%process_id, "worker: start");
    if let Err(e) = storage
        .procs
        .register(&process_id, &queue_name, &host_id)
        .await
    {
        tracing::error!(?e, %process_id, "worker: register failed; exiting");
        return;
    }

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let job = match storage.jobs.claim_next(&queue_name, &process_id).await {
            Ok(Some(j)) => j,
            Ok(None) => {
                if idle_wait(&storage, &queue_name, &process_id, &cancel).await {
                    break;
                }
                continue;
            }
            Err(e) => {
                tracing::warn!(?e, %process_id, "worker: claim failed, backing off 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let job_id = job.id.clone();

        if let Err(e) = storage
            .procs
            .heartbeat(&process_id, Some(job_id.clone()))
            .await
        {
            tracing::warn!(?e, %process_id, "worker: heartbeat-with-job failed");
        }

        // Per-job child token: parented to the worker cancel so
        // shutdown still propagates to the handler, but cancellable
        // independently for `QueueHandle::request_cancel` and the
        // heartbeat-tick observation of a DB cancel flag.
        let job_cancel = cancel.child_token();
        register_running_job(&running_jobs, &job_id, job_cancel.clone());

        // Side-task heartbeats both the job row and the process row
        // every HEARTBEAT_INTERVAL so a long handler doesn't trip
        // the reaper. It also reads the row's `cancel_requested_at`
        // each tick and triggers `job_cancel` when set.
        let heartbeat_cancel = CancellationToken::new();
        let heartbeat_task = tokio::spawn(heartbeat_loop(
            storage.clone(),
            process_id.clone(),
            job_id.clone(),
            heartbeat_cancel.clone(),
            job_cancel.clone(),
        ));

        let outcome = match handlers.get(&job.kind) {
            Some(handler) => {
                let ctx = JobCtx {
                    storage: &storage,
                    router: router.as_ref(),
                    rate_limit: rate_limit.as_ref(),
                    job_id: job_id.clone(),
                    process_id: &process_id,
                    host_id: &host_id,
                    cancel: job_cancel.clone(),
                };
                handler.run(ctx, job.payload.clone()).await
            }
            None => JobOutcome::Failed(format!("no handler registered for kind: {}", job.kind)),
        };

        heartbeat_cancel.cancel();
        if let Err(e) = heartbeat_task.await {
            tracing::warn!(?e, %process_id, "worker: heartbeat task join failed");
        }
        deregister_running_job(&running_jobs, &job_id);

        // User-initiated cancel (in-process via `request_cancel` or
        // cross-pod via the DB cancel flag) trips `job_cancel`
        // without tripping the worker-slot `cancel`. In that case
        // skip the retry curve and finalize as Dead — otherwise a
        // backoff-off queue would immediately re-claim and re-run
        // the same row, defeating the user's cancel intent. A
        // worker-slot cancel (shutdown / scale-down) cascades to
        // `job_cancel` too, so this branch ignores that path.
        let user_cancelled = !cancel.is_cancelled() && job_cancel.is_cancelled();
        // Fetch the queue's backoff config so map_outcome can size
        // the throttle delay. One extra read per finalize is fine —
        // this path is rare (only on failure / throttle) and matches
        // the supervisor's per-tick config read.
        let backoff_cfg = fetch_backoff_cfg(&storage, &queue_name).await;
        // M2: a cancel that lands while (or just after) the handler
        // already returned `Done` must NOT rewrite the row as Dead — the
        // work happened (e-mail sent, API call committed), and recording
        // it "cancelled by user" invites an operator retry that genuinely
        // double-executes it. The cancel simply arrived too late. The
        // Dead-on-cancel override only applies to non-`Done` outcomes,
        // where it prevents a backoff-off queue immediately re-claiming
        // and re-running the row the user asked to stop.
        let finalize_outcome = if user_cancelled && !matches!(outcome, JobOutcome::Done) {
            FinalizeOutcome::Dead {
                message: "cancelled by user".to_owned(),
            }
        } else {
            map_outcome(&job, outcome, backoff_cfg.as_ref())
        };
        log_handler_outcome(&job, &job_id, &finalize_outcome);
        // Capture the cool-down so this worker idles locally instead of
        // spinning empty claims while the queue gate (set by finalize)
        // holds. Other workers hit the gate in `claim_next` and idle via
        // their normal wait path.
        let throttle_pause = match &finalize_outcome {
            FinalizeOutcome::Throttled {
                retry_after,
                cool_down_queue: true,
            } => Some(*retry_after),
            _ => None,
        };
        // Pass our `process_id` as the ownership guard (H1): if this
        // worker stalled past the stale threshold and the reaper revived
        // + another worker re-claimed the row, this finalize no-ops
        // instead of clobbering the new claimant's in-flight job.
        if let Err(e) = storage
            .jobs
            .finalize(&job_id, Some(&process_id), finalize_outcome)
            .await
        {
            tracing::error!(?e, ?job_id, %process_id, "worker: finalize failed");
        }

        if let Err(e) = storage.procs.heartbeat(&process_id, None).await {
            tracing::warn!(?e, %process_id, "worker: heartbeat-clear failed");
        }

        if let Some(pause) = throttle_pause {
            tracing::info!(
                queue = %queue_name,
                secs = pause.as_secs(),
                "worker: queue throttled; pausing before next claim"
            );
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(pause) => {}
            }
        }
    }

    if let Err(e) = storage.procs.deregister(&process_id).await {
        tracing::warn!(?e, %process_id, "worker: deregister failed on exit");
    }
    tracing::debug!(%process_id, "worker: stop");
}

/// Idle handling when `claim_next` found nothing: heartbeat the process
/// row, then either sleep out an active queue cool-down (so idle workers
/// don't busy-poll the gate for the whole window) or block on
/// `wait_for_work` until the next enqueue / idle-poll timeout. Returns
/// `true` if the worker was cancelled (the caller should break).
async fn idle_wait(
    storage: &Storage,
    queue_name: &str,
    process_id: &str,
    cancel: &CancellationToken,
) -> bool {
    if let Err(e) = storage.procs.heartbeat(process_id, None).await {
        tracing::warn!(?e, %process_id, "worker: idle heartbeat failed");
    }
    let cool_down = fetch_backoff_cfg(storage, queue_name)
        .await
        .and_then(|c| c.throttled_until)
        .and_then(|until| (until - Utc::now()).to_std().ok());
    let wait = async {
        match cool_down {
            Some(dur) => tokio::time::sleep(dur).await,
            None => {
                let _ = storage.jobs.wait_for_work(queue_name, IDLE_POLL).await;
            }
        }
    };
    tokio::select! {
        biased;
        () = cancel.cancelled() => true,
        () = wait => false,
    }
}

/// Read the queue's config for `map_outcome`. A missing row or read
/// error degrades to `None` (legacy flat-60s throttle, no queue
/// cool-down) rather than failing the finalize.
async fn fetch_backoff_cfg(
    storage: &Storage,
    queue_name: &str,
) -> Option<crate::storage::types::QueueConfigRow> {
    match storage.config.get_queue(queue_name).await {
        Ok(Some(cfg)) => Some(cfg),
        Ok(None) => {
            tracing::warn!(
                queue = %queue_name,
                "worker: queue config vanished; using legacy throttle fallback"
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                ?e,
                queue = %queue_name,
                "worker: queue config read failed; using legacy throttle fallback"
            );
            None
        }
    }
}

/// Map a handler's `JobOutcome` into the storage layer's
/// `FinalizeOutcome`, applying retry budget + backoff.
///
/// `backoff_cfg` is the queue's current row (or `None` if the read
/// failed / the row vanished). The `backoff_enabled` toggle governs
/// both arms:
///  - `Throttled` — off → flat 60s; on → exponential on the
///    queue-wide throttle counter (`min(base * 2^throttle_attempts, max)`).
///  - `Failed` — off → no delay (immediately re-claimable, bounded
///    only by `max_attempts`); on → exponential on the job's own
///    attempt counter (`min(base * 2^attempts, max)`).
///
/// When `backoff_cfg` is `None`, both arms degrade to the legacy
/// shape (flat 60s for Throttled, no delay for Failed).
fn map_outcome(
    job: &JobRecord,
    outcome: JobOutcome,
    backoff_cfg: Option<&crate::storage::types::QueueConfigRow>,
) -> FinalizeOutcome {
    match outcome {
        JobOutcome::Done => FinalizeOutcome::Done,
        JobOutcome::Throttled { retry_after: _hint } => {
            // The handler's hint is intentionally ignored — the
            // runtime owns the throttle curve via per-queue config so
            // the cadence stays consistent across every handler that
            // returns `Throttled`.
            //
            // A throttle ALWAYS cools down the whole queue: rate limits
            // are per-token, so letting other workers keep claiming just
            // hammers the limiter. `backoff_enabled` only chooses the
            // delay *shape* — flat 60s when off, the exponential curve
            // (compounding on the queue-wide throttle count, since the
            // limit is shared) when on.
            let (enabled, base, max, attempts) = backoff_cfg.map_or((false, 60, 1800, 0), |c| {
                (
                    c.backoff_enabled,
                    c.backoff_base_seconds,
                    c.backoff_max_seconds,
                    c.throttle_attempts,
                )
            });
            let retry_after = retry::throttle_delay(attempts, enabled, base, max);
            FinalizeOutcome::Throttled {
                retry_after,
                cool_down_queue: true,
            }
        }
        JobOutcome::Dead(msg) => FinalizeOutcome::Dead { message: msg },
        JobOutcome::Failed(msg) => {
            if job.attempts >= job.max_attempts {
                FinalizeOutcome::Dead { message: msg }
            } else {
                // Mirror the Throttled arm: backoff_enabled toggles the
                // curve shape — off → no delay (`max_attempts` still
                // bounds the retry budget), on → the same exponential
                // curve the queue config configures.
                let (enabled, base, max) = backoff_cfg.map_or((false, 60, 1800), |c| {
                    (
                        c.backoff_enabled,
                        c.backoff_base_seconds,
                        c.backoff_max_seconds,
                    )
                });
                let retry_after = retry::failed_delay(job.attempts, enabled, base, max);
                FinalizeOutcome::Failed {
                    retry_after,
                    message: msg,
                }
            }
        }
    }
}

// ────────────────────────────────────────────────────────────────────
// Heartbeat side-task.
// ────────────────────────────────────────────────────────────────────

async fn heartbeat_loop(
    storage: Storage,
    process_id: String,
    job_id: JobId,
    stop: CancellationToken,
    job_cancel: CancellationToken,
) {
    let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
    tick.tick().await;
    // Latches `true` the first time we observe the DB cancel flag
    // so we don't re-fire `job_cancel.cancel()` and re-log on every
    // subsequent tick. Cancellation is idempotent, but a handler
    // that takes 30s to unwind would otherwise log "cancel
    // requested" three times.
    let mut cancel_signalled = false;
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = tick.tick() => {
                match storage.jobs.heartbeat_job(&job_id, &process_id).await {
                    Ok(HeartbeatStatus::CancelRequested) if !cancel_signalled => {
                        // First observation of the DB cancel flag.
                        // Signal the handler; keep ticking so the
                        // row's heartbeat_at stays fresh while the
                        // handler unwinds.
                        tracing::info!(
                            job_id = %job_id.as_str(),
                            %process_id,
                            "heartbeat: cancel requested; signalling handler"
                        );
                        job_cancel.cancel();
                        cancel_signalled = true;
                    }
                    Ok(HeartbeatStatus::Lost) if !cancel_signalled => {
                        // M1: we no longer own this row — it was reaped
                        // past the stale threshold and another worker
                        // re-claimed it. Stop running so the same job
                        // isn't executing on two workers at once; the
                        // new owner holds it now. Our eventual finalize
                        // is a no-op under the H1 ownership guard.
                        tracing::warn!(
                            job_id = %job_id.as_str(),
                            %process_id,
                            "heartbeat: lost row ownership (reaped + re-claimed); stopping handler"
                        );
                        job_cancel.cancel();
                        cancel_signalled = true;
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(?e, %process_id, "heartbeat: job update failed"),
                }
                if let Err(e) = storage.procs.heartbeat(&process_id, Some(job_id.clone())).await {
                    tracing::warn!(?e, %process_id, "heartbeat: process update failed");
                }
            }
        }
    }
}

fn register_running_job(map: &RunningJobs, job_id: &JobId, token: CancellationToken) {
    // poison-safe: prior poisoning means a worker panicked mid-op;
    // recover the guard and proceed so future cancels still work.
    let mut g = match map.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    g.insert(job_id.clone(), token);
}

fn deregister_running_job(map: &RunningJobs, job_id: &JobId) {
    let mut g = match map.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    g.remove(job_id);
}

// ────────────────────────────────────────────────────────────────────
// Reaper — one task per runtime.
// ────────────────────────────────────────────────────────────────────

async fn reaper_loop(storage: Storage, shutdown: CancellationToken) {
    tracing::debug!("reaper: start");
    let mut tick = tokio::time::interval(REAPER_TICK);
    tick.tick().await;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::debug!("reaper: shutdown");
                return;
            }
            _ = tick.tick() => {
                let stale_before = Utc::now() - STALE_THRESHOLD;
                match storage.jobs.revive_stale(stale_before).await {
                    Ok(n) if n > 0 => {
                        tracing::info!(revived = n, "reaper: revived stuck jobs");
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(?e, "reaper: revive_stale failed"),
                }
                if let Err(e) = storage.procs.reap_stale(stale_before).await {
                    tracing::warn!(?e, "reaper: process sweep failed");
                }
            }
        }
    }
}

/// Public one-off sweep — tests + ops can trigger it without waiting
/// for the 15 s background tick.
pub async fn reap_stale_jobs(storage: &Storage) -> Result<u64> {
    let stale_before = Utc::now() - STALE_THRESHOLD;
    storage.jobs.revive_stale(stale_before).await
}

// ────────────────────────────────────────────────────────────────────
// Timeline-event flush — one task per runtime, on every replica.
// ────────────────────────────────────────────────────────────────────

/// Drain this replica's in-process timeline-event buffer and batch-insert
/// the rows into `queue_event` on every tick, plus one final flush on
/// shutdown so a graceful exit doesn't drop the tail. The buffer is the
/// adapter's; mock backends default to a no-op `flush_event_buffer`.
async fn event_flush_loop(storage: Storage, shutdown: CancellationToken) {
    tracing::debug!("event flush: start");
    let mut tick = tokio::time::interval(EVENT_FLUSH_TICK);
    tick.tick().await;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                // Final drain — persist whatever workers buffered before
                // the cancel so a clean shutdown loses nothing.
                if let Err(e) = storage.jobs.flush_event_buffer().await {
                    tracing::warn!(?e, "event flush: final flush failed");
                }
                tracing::debug!("event flush: shutdown");
                return;
            }
            _ = tick.tick() => {
                if let Err(e) = storage.jobs.flush_event_buffer().await {
                    tracing::warn!(?e, "event flush: flush failed");
                }
            }
        }
    }
}

// ────────────────────────────────────────────────────────────────────
// Cleanup — per-queue retention.
// ────────────────────────────────────────────────────────────────────

/// Counts of rows the cleanup pass deleted, broken out by status.
#[derive(Debug, Default, Clone, Copy)]
pub struct CleanupReport {
    pub done_deleted: u64,
    pub dead_deleted: u64,
}

impl CleanupReport {
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.done_deleted + self.dead_deleted
    }
}

/// One pass over per-queue retention. Public so tests / ops can
/// trigger an immediate sweep.
pub async fn cleanup_once(storage: &Storage) -> Result<CleanupReport> {
    let queues = storage.config.list_queues().await?;
    let mut report = CleanupReport::default();
    let now = Utc::now();

    for q in queues {
        let done_threshold = now - ChronoDuration::days(i64::from(q.retain_done_for_days));
        let dead_threshold = now - ChronoDuration::days(i64::from(q.retain_dead_for_days));
        report.done_deleted += storage
            .jobs
            .cleanup_aged(&q.name, JobStatus::Done, done_threshold)
            .await?;
        report.dead_deleted += storage
            .jobs
            .cleanup_aged(&q.name, JobStatus::Dead, dead_threshold)
            .await?;
    }

    // Metrics rollup retention (ADR 0009) — global, not per-queue.
    let metric_threshold = now - ChronoDuration::days(metrics::METRIC_RETENTION_DAYS);
    storage
        .jobs
        .delete_metric_buckets_before(metric_threshold)
        .await?;

    Ok(report)
}

async fn cleanup_loop(storage: Storage, host_id: String, shutdown: CancellationToken) {
    tracing::debug!("cleanup: start");
    let mut tick = tokio::time::interval(CLEANUP_TICK);
    tick.tick().await;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::debug!("cleanup: shutdown");
                return;
            }
            _ = tick.tick() => {
                // Only the cron-lease holder runs the purge. The
                // DELETEs are idempotent across pods (a non-leader
                // re-run finds nothing the leader didn't already
                // sweep), but they take the writer lock — multiplying
                // contention by every pod every CLEANUP_TICK. On
                // SQLite the lease always grants (single-process).
                match storage.cron.try_cron_lease(&host_id, cron::CRON_LEASE_TTL).await {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        tracing::warn!(?e, %host_id, "cleanup: lease check failed");
                        continue;
                    }
                }
                match cleanup_once(&storage).await {
                    Ok(report) if report.total() > 0 => {
                        tracing::info!(
                            done = report.done_deleted,
                            dead = report.dead_deleted,
                            "cleanup: purged aged rows",
                        );
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(?e, "cleanup tick failed"),
                }
            }
        }
    }
}
