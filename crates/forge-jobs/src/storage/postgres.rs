//! Postgres-backed implementation of the storage traits.
//!
//! Selected at boot via `QUEUE_BACKEND=postgres` + `DATABASE_URL=...`.
//! The host builds a `Storage::from_one(Arc::new(PostgresStorage::open(...).await?))`
//! instead of the `SQLite` variant; the runtime + handlers are unchanged.
//!
//! ## Why Postgres (vs `SQLite`)
//!
//! `SQLite` tops out at ~200-1000 commits/sec because of its
//! single-writer constraint. Postgres MVCC commits in parallel,
//! scaling with `max_connections`. At your 10M-jobs/day target with
//! bursty backfills (peaks 1000+/sec), `SQLite` isn't enough; Postgres is.
//!
//! Additionally, Postgres lets multi-replica deploys share the same
//! queue:
//! - `SELECT … FOR UPDATE SKIP LOCKED` for atomic claim across replicas.
//! - `LISTEN`/`NOTIFY` to wake idle workers cluster-wide on enqueue.
//! - `pg_try_advisory_lock` for cron leader election (Phase 4.1).
//!
//! ## Connection pooling
//!
//! Single `PgPool` (Postgres handles concurrent writers natively — no
//! pool split like the `SQLite` adapter needs). Size for peak concurrent
//! demand; ~30 connections is plenty for a 50-worker replica. Each pg
//! connection costs ~10 MB server-side, so connect-pooler bouncers like
//! `PgBouncer` pay off above ~50 replicas (see Phase 4.3 docs).
//!
//! Note: a single process-wide listener task holds **one** dedicated
//! `PgListener` connection for the lifetime of the storage handle, and
//! fans `NOTIFY` wakes out to an in-process per-queue [`tokio::sync::Notify`]
//! hub. `wait_for_work` blocks on that in-process notify — it does NOT
//! open a connection per call. So a replica's listener footprint is one
//! connection total, independent of idle-worker count.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sqlx::Row;
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;

/// Single Postgres channel every replica's enqueue `NOTIFY`s and the
/// process listener `LISTEN`s on. The woken queue's name rides in the
/// notification payload (bound via `pg_notify($1, $2)`, so no identifier
/// escaping is needed), and the listener routes it to that queue's
/// in-process [`Notify`].
const WAKE_CHANNEL: &str = "forge_jobs_wake";

use super::db_timing::{DbRecorder, OpTimer};
use super::error::{Result, StorageError};
use super::event_buffer::{EventBuffer, EventRecord};
use super::types::{
    CronScheduleRecord, EnqueueOutcome, EnqueueRequest, ErrorHistoryEntry, FinalizeOutcome, JobId,
    JobLatency, JobRecord, JobStatus, MetricBucket, NewCronSchedule, ProcessRecord, QueueConfigRow,
    QueueCounts, TimelineEvent, TimelineEventType, metric,
};
use super::{
    CronStorage, DeleteOutcome, ERROR_HISTORY_CAP, HeartbeatStatus, JobQueue, ProcessRegistry,
    QueueConfig, RateLimitOutcome, RateLimitStorage, StorageInfo,
};

/// In-process per-queue wake registry. The single listener task signals
/// the right queue's `Notify` when a `NOTIFY` arrives; `wait_for_work`
/// blocks on it. `notify_one` (not `notify_waiters`) so a wake that races
/// all-workers-busy still stores a permit for the next waiter — same
/// no-lost-wakeup rationale as the `SQLite` `NotifyHub`.
#[derive(Default)]
struct WakeHub {
    queues: RwLock<HashMap<String, Arc<Notify>>>,
}

impl WakeHub {
    async fn for_queue(&self, name: &str) -> Arc<Notify> {
        // Drop the read guard before taking the write lock (clippy's
        // significant_drop_in_scrutinee) — mirrors the SQLite hub.
        let cached = self.queues.read().await.get(name).cloned();
        if let Some(n) = cached {
            return n;
        }
        let mut w = self.queues.write().await;
        w.entry(name.to_owned())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }
}

pub struct PostgresStorage {
    pool: PgPool,
    /// Bounded cap surfaced via `describe()` so the boot banner shows
    /// what the pool was configured with.
    max_connections: u32,
    /// Process-wide monotonic ULID generator — see the `SQLite`
    /// tree's matching field for the rationale. Strict FIFO at
    /// sub-ms resolution requires the ULID's random tail to be
    /// monotonic when the timestamp ties; `ulid::Ulid::new()` doesn't
    /// do that but `Generator` does.
    ulid_gen: Arc<tokio::sync::Mutex<ulid::Generator>>,
    /// Per-call latency buffer for the `db_op_ms` rollup. See
    /// [`super::db_timing`].
    db_recorder: Arc<DbRecorder>,
    /// In-process timeline-event buffer. Worker paths push committed
    /// `queue_event` rows here; the runtime's `event_flush_loop` drains
    /// and batch-inserts them, keeping the inserts off the hot enqueue /
    /// claim / finalize transactions. `Arc` so every clone of this
    /// storage shares the one buffer.
    events: Arc<EventBuffer>,
    /// Per-queue in-process wake registry, fed by the listener task and
    /// consumed by `wait_for_work` (H2).
    wake: Arc<WakeHub>,
    /// Cancels the background `LISTEN` task on `Drop` so a dropped
    /// storage handle (e.g. a torn-down test container) doesn't leak its
    /// listener connection.
    listener_cancel: CancellationToken,
}

impl Drop for PostgresStorage {
    fn drop(&mut self) {
        self.listener_cancel.cancel();
    }
}

impl std::fmt::Debug for PostgresStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStorage")
            .field("max_connections", &self.max_connections)
            .finish_non_exhaustive()
    }
}

/// Process-wide listener: hold one `PgListener`, `LISTEN` on the wake
/// channel, and route each notification's payload (the queue name) to the
/// in-process hub. Reconnects with a fixed backoff if the connection
/// drops; exits when `cancel` fires (storage dropped).
async fn listen_loop(pool: PgPool, wake: Arc<WakeHub>, cancel: CancellationToken) {
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match run_listener(&pool, &wake, &cancel).await {
            Ok(()) => return, // cancelled
            Err(e) => {
                tracing::warn!(?e, "pg notify listener dropped; reconnecting in 1s");
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    }
}

async fn run_listener(pool: &PgPool, wake: &WakeHub, cancel: &CancellationToken) -> Result<()> {
    // `connect_with(pool)` opens a dedicated connection outside the pool's
    // capacity, so the long-lived LISTEN never starves worker SQL.
    let mut listener = PgListener::connect_with(pool).await.map_err(map_sqlx_err)?;
    listener.listen(WAKE_CHANNEL).await.map_err(map_sqlx_err)?;
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            res = listener.recv() => {
                let notification = res.map_err(map_sqlx_err)?;
                let queue = notification.payload();
                if !queue.is_empty() {
                    wake.for_queue(queue).await.notify_one();
                }
            }
        }
    }
}

impl PostgresStorage {
    /// Open a Postgres-backed storage handle and run pending
    /// migrations. `max_connections` defaults to 30 — enough for the
    /// typical 6 worker tasks + UI panel + cron + reaper without
    /// queueing at the sqlx pool semaphore. Bump if you raise
    /// `max_workers` above ~20.
    pub async fn open(database_url: &str, max_connections: u32) -> Result<Self> {
        let cancel = CancellationToken::new();
        Self::open_with_cancel(database_url, max_connections, &cancel).await
    }

    /// Variant that aborts when `cancel` fires — useful for tests
    /// that spin a `testcontainers` pg instance and want to abort the
    /// connect on test timeout.
    pub async fn open_with_cancel(
        database_url: &str,
        max_connections: u32,
        cancel: &CancellationToken,
    ) -> Result<Self> {
        Self::open_inner(
            PgPoolOptions::new()
                .max_connections(max_connections)
                .min_connections(1)
                .acquire_timeout(Duration::from_secs(10))
                .connect(database_url),
            max_connections,
            cancel,
        )
        .await
    }

    /// Construct from a pre-built [`sqlx::postgres::PgConnectOptions`].
    /// Avoids URL encoding for credentials with special characters —
    /// used by the `queue_database.toml` loader, which has discrete
    /// `host` / `username` / `password` fields.
    pub async fn open_with_options(
        opts: sqlx::postgres::PgConnectOptions,
        max_connections: u32,
    ) -> Result<Self> {
        let cancel = CancellationToken::new();
        Self::open_inner(
            PgPoolOptions::new()
                .max_connections(max_connections)
                .min_connections(1)
                .acquire_timeout(Duration::from_secs(10))
                .connect_with(opts),
            max_connections,
            &cancel,
        )
        .await
    }

    async fn open_inner<F>(
        pool_fut: F,
        max_connections: u32,
        cancel: &CancellationToken,
    ) -> Result<Self>
    where
        F: std::future::Future<Output = std::result::Result<PgPool, sqlx::Error>>,
    {
        let pool = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(StorageError::Backend("cancelled".into())),
            res = pool_fut => res.map_err(map_sqlx_err)?,
        };

        sqlx::migrate!("src/storage/postgres/migrations")
            .run(&pool)
            .await
            .map_err(|e| StorageError::Migration {
                version: 0,
                message: e.to_string(),
            })?;

        // H2: one process-wide LISTEN task feeding the in-process wake
        // hub, instead of a dedicated listener connection per idle
        // `wait_for_work` call. Cancelled on `Drop`.
        let wake = Arc::new(WakeHub::default());
        let listener_cancel = CancellationToken::new();
        tokio::spawn(listen_loop(
            pool.clone(),
            wake.clone(),
            listener_cancel.clone(),
        ));

        Ok(Self {
            pool,
            max_connections,
            ulid_gen: Arc::new(tokio::sync::Mutex::new(ulid::Generator::new())),
            db_recorder: Arc::new(DbRecorder::default()),
            events: Arc::new(EventBuffer::default()),
            wake,
            listener_cancel,
        })
    }

    /// Generate the next monotonic ULID for an enqueue. Same shape as
    /// the `SQLite` tree's `next_ulid` — see that for the FIFO
    /// rationale.
    pub(crate) async fn next_ulid(&self) -> ulid::Ulid {
        let mut generator = self.ulid_gen.lock().await;
        generator.generate().unwrap_or_else(|_| ulid::Ulid::new())
    }

    /// Backend-specific accessor for tests + ops scripts.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl JobQueue for PostgresStorage {
    async fn enqueue(&self, req: EnqueueRequest) -> Result<EnqueueOutcome> {
        let _t = OpTimer::write(&self.db_recorder);
        let new_id = self.next_ulid().await.to_string();
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;
        let mut pending = Vec::new();
        let outcome = enqueue_in_tx(&mut tx, &req, &new_id, &mut pending).await?;
        tx.commit().await.map_err(map_sqlx_err)?;
        // Push events only after the commit so a rolled-back enqueue
        // leaves no phantom timeline row.
        self.events.push_all(pending);

        if matches!(outcome, EnqueueOutcome::Enqueued(_))
            && let Some(queue) = req.queue_name.as_deref()
        {
            self.notify(queue).await?;
        }
        Ok(outcome)
    }

    async fn enqueue_bulk(&self, reqs: Vec<EnqueueRequest>) -> Result<Vec<EnqueueOutcome>> {
        let _t = OpTimer::write(&self.db_recorder);
        // Pre-mint monotonic ULIDs in caller-order so the entire
        // batch sorts FIFO in the queue.
        let new_ids: Vec<String> = {
            let mut generator = self.ulid_gen.lock().await;
            reqs.iter()
                .map(|_| {
                    generator
                        .generate()
                        .unwrap_or_else(|_| ulid::Ulid::new())
                        .to_string()
                })
                .collect()
        };
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;
        let mut outcomes = Vec::with_capacity(reqs.len());
        let mut notify_queues: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut pending: Vec<EventRecord> = Vec::new();
        for (req, new_id) in reqs.iter().zip(new_ids.iter()) {
            let outcome = enqueue_in_tx(&mut tx, req, new_id, &mut pending).await?;
            if matches!(outcome, EnqueueOutcome::Enqueued(_))
                && let Some(q) = req.queue_name.as_deref()
            {
                notify_queues.insert(q.to_owned());
            }
            outcomes.push(outcome);
        }
        tx.commit().await.map_err(map_sqlx_err)?;
        // Push events only after the commit — see `enqueue`.
        self.events.push_all(pending);

        for q in notify_queues {
            self.notify(&q).await?;
        }
        Ok(outcomes)
    }

    async fn claim_next(&self, queue: &str, process_id: &str) -> Result<Option<JobRecord>> {
        let _t = OpTimer::write(&self.db_recorder);
        let now = Utc::now();
        // SELECT … FOR UPDATE SKIP LOCKED claims one eligible row
        // atomically across replicas — sibling workers racing the same
        // queue all walk past locked rows and pick the next free one.
        let row = sqlx::query(
            r"UPDATE sync_queue
                 SET status              = 'in_progress',
                     process_id          = $1,
                     started_at          = $2,
                     heartbeat_at        = $2,
                     attempts            = attempts + 1,
                     -- Clear any stale cancel flag from a previous
                     -- in-progress life of this row (set by `delete`
                     -- and never observed before requeue).
                     cancel_requested_at = NULL
               WHERE id = (
                   SELECT id FROM sync_queue
                    WHERE queue_name = $3
                      AND status IN ('pending', 'failed')
                      AND scheduled_at <= $2
                      -- Queue-wide throttle gate: while the queue is in
                      -- cool-down, hand out nothing so the whole fleet
                      -- (every replica) backs off together. NULL
                      -- `throttled_until` makes the comparison NULL →
                      -- not blocked.
                      AND NOT EXISTS (
                          SELECT 1 FROM queue q
                           WHERE q.name = $3 AND q.throttled_until > $2
                      )
                      -- Skip rows whose dedupe_key already has an
                      -- ACTIVE sibling. A claim of a `failed` row
                      -- flips it to `in_progress` (entering the
                      -- active dedupe index); if a sibling is already
                      -- pending/in_progress with the same key, the
                      -- UPDATE trips `jq_dedupe`. NULL key is always
                      -- claimable.
                      AND (
                          dedupe_key IS NULL OR NOT EXISTS (
                              SELECT 1 FROM sync_queue dup
                               WHERE dup.dedupe_key = sync_queue.dedupe_key
                                 AND dup.id != sync_queue.id
                                 AND dup.status IN ('pending', 'in_progress')
                          )
                      )
                    -- FIFO within priority + scheduled_at. ULIDs are
                    -- monotonically sortable so `id ASC` is true
                    -- insertion order. Index `jq_claim` covers all
                    -- five columns so the planner walks the index
                    -- without a sort step.
                    ORDER BY priority ASC, scheduled_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
               )
               RETURNING *",
        )
        .bind(process_id)
        .bind(now)
        .bind(queue)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let Some(job) = row.as_ref().map(row_to_job).transpose()? else {
            return Ok(None);
        };
        // Buffer the `started` event for the background flush. Same
        // crash semantics as before: a crash before it reaches storage
        // loses one chart event; the reaper revives the row and the
        // re-claim emits a fresh `started`.
        self.events.push(EventRecord::new(
            now,
            &job.kind,
            &job.queue_name,
            Some(job.id.as_str()),
            "started",
        ));
        Ok(Some(job))
    }

    async fn finalize(
        &self,
        job_id: &JobId,
        owner: Option<&str>,
        outcome: FinalizeOutcome,
    ) -> Result<()> {
        let _t = OpTimer::write(&self.db_recorder);
        // Postgres doesn't have SQLite's writer-lock contention class,
        // so the retry loop the SQLite adapter wraps `finalize` in is
        // unnecessary here. MVCC handles concurrent finalizes
        // natively; the only transient errors (serialization_failure,
        // deadlock_detected) come back as `StorageError::Conflict`
        // and the caller can surface them.
        self.do_finalize(job_id, owner, outcome).await
    }

    async fn heartbeat_job(&self, job_id: &JobId, process_id: &str) -> Result<HeartbeatStatus> {
        let _t = OpTimer::write(&self.db_recorder);
        // RETURNING tells us if a cancel was requested for this
        // (still-owned) row. 0 rows back = row vanished or process_id no
        // longer owns it (reaped + re-claimed) → `Lost` (M1).
        let row = sqlx::query(
            r"UPDATE sync_queue
                 SET heartbeat_at = $1
               WHERE id = $2 AND process_id = $3
               RETURNING cancel_requested_at IS NOT NULL AS cancel_requested",
        )
        .bind(Utc::now())
        .bind(job_id.as_str())
        .bind(process_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(row.map_or(HeartbeatStatus::Lost, |r| {
            if r.try_get::<bool, _>("cancel_requested").unwrap_or(false) {
                HeartbeatStatus::CancelRequested
            } else {
                HeartbeatStatus::Active
            }
        }))
    }

    async fn revive_stale(&self, stale_before: DateTime<Utc>) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        // Same shape as the SQLite adapter: pull each stale row's owning
        // queue backoff config in one SELECT so the per-queue toggle is
        // honoured.
        //
        // L1: this SELECT runs in autocommit, so any row lock would
        // release at statement end and fence nothing — the real
        // cross-reaper fence is the `status = 'in_progress' AND
        // heartbeat_at < cutoff` guard inside `append_error_and_update`'s
        // UPDATE. Two replicas' reapers may both scan the same rows; the
        // loser's guarded UPDATE no-ops (its `revived` count can then
        // over-report — a log number only). So no `FOR UPDATE` here:
        // it would read as a fence it isn't.
        let rows = sqlx::query(
            r"SELECT j.id, j.attempts, j.max_attempts,
                     COALESCE(q.backoff_enabled, FALSE)     AS backoff_enabled,
                     COALESCE(q.backoff_base_seconds, 60)   AS backoff_base_seconds,
                     COALESCE(q.backoff_max_seconds, 1800)  AS backoff_max_seconds
                FROM sync_queue j
                LEFT JOIN queue q ON q.name = j.queue_name
               WHERE j.status = 'in_progress' AND j.heartbeat_at < $1",
        )
        .bind(stale_before)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let mut revived = 0u64;
        for r in rows {
            let id: String = r.try_get("id").map_err(map_sqlx_err)?;
            let attempts: i32 = r.try_get("attempts").map_err(map_sqlx_err)?;
            let max_attempts: i32 = r.try_get("max_attempts").map_err(map_sqlx_err)?;
            let backoff_enabled: bool = r.try_get("backoff_enabled").map_err(map_sqlx_err)?;
            let backoff_base: i32 = r.try_get("backoff_base_seconds").map_err(map_sqlx_err)?;
            let backoff_max: i32 = r.try_get("backoff_max_seconds").map_err(map_sqlx_err)?;
            let job_id = JobId::new(id);
            let terminal = attempts >= max_attempts;
            if terminal {
                append_error_and_update(
                    &self.pool,
                    &self.events,
                    &job_id,
                    Utc::now(),
                    "reaped after stale heartbeat",
                    true,
                    None,
                    Some(stale_before),
                    /* guard_owner: */ None,
                )
                .await?;
            } else {
                let delay = crate::runtime::failed_delay(
                    attempts,
                    backoff_enabled,
                    backoff_base,
                    backoff_max,
                );
                let next = Utc::now() + ChronoDuration::from_std(delay).unwrap_or_default();
                append_error_and_update(
                    &self.pool,
                    &self.events,
                    &job_id,
                    Utc::now(),
                    "reaped after stale heartbeat",
                    false,
                    Some(next),
                    Some(stale_before),
                    /* guard_owner: */ None,
                )
                .await?;
            }
            revived += 1;
        }
        Ok(revived)
    }

    async fn cleanup_aged(
        &self,
        queue: &str,
        status: JobStatus,
        threshold: DateTime<Utc>,
    ) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;
        // Cascade-delete events first; the subquery needs the rows
        // to still exist.
        sqlx::query(
            r"DELETE FROM queue_event
               WHERE job_id IN (
                     SELECT id FROM sync_queue
                      WHERE queue_name = $1
                        AND status = $2
                        AND completed_at IS NOT NULL
                        AND completed_at < $3
                   )",
        )
        .bind(queue)
        .bind(status.as_str())
        .bind(threshold)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;
        let res = sqlx::query(
            r"DELETE FROM sync_queue
               WHERE queue_name = $1
                 AND status = $2
                 AND completed_at IS NOT NULL
                 AND completed_at < $3",
        )
        .bind(queue)
        .bind(status.as_str())
        .bind(threshold)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;
        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(res.rows_affected())
    }

    async fn get_job(&self, job_id: &JobId) -> Result<Option<JobRecord>> {
        let _t = OpTimer::read(&self.db_recorder);
        let row = sqlx::query("SELECT * FROM sync_queue WHERE id = $1")
            .bind(job_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_job).transpose()
    }

    async fn list_by_status(
        &self,
        queue: Option<&str>,
        status: JobStatus,
        limit: usize,
    ) -> Result<Vec<JobRecord>> {
        let _t = OpTimer::read(&self.db_recorder);
        let limit_i = i64::try_from(limit).unwrap_or(100);
        let rows = if let Some(q) = queue {
            sqlx::query(
                r"SELECT * FROM sync_queue
                   WHERE queue_name = $1 AND status = $2
                   ORDER BY enqueued_at DESC
                   LIMIT $3",
            )
            .bind(q)
            .bind(status.as_str())
            .bind(limit_i)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r"SELECT * FROM sync_queue
                   WHERE status = $1
                   ORDER BY enqueued_at DESC
                   LIMIT $2",
            )
            .bind(status.as_str())
            .bind(limit_i)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_job).collect()
    }

    async fn count_by_status(&self, queue: &str) -> Result<QueueCounts> {
        let _t = OpTimer::read(&self.db_recorder);
        // Conditional aggregation: pending splits into ready-now
        // (scheduled_at <= now) and deferred (scheduled_at > now)
        // so the UI can chip them separately. Single round-trip.
        let now = Utc::now();
        let row = sqlx::query(
            r"SELECT
                SUM(CASE WHEN status='pending'     AND scheduled_at <= $1 THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status='pending'     AND scheduled_at >  $1 THEN 1 ELSE 0 END) AS scheduled,
                SUM(CASE WHEN status='in_progress'                        THEN 1 ELSE 0 END) AS in_progress,
                SUM(CASE WHEN status='done'                               THEN 1 ELSE 0 END) AS done,
                SUM(CASE WHEN status='failed'                             THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status='dead'                               THEN 1 ELSE 0 END) AS dead
              FROM sync_queue
              WHERE queue_name = $2",
        )
        .bind(now)
        .bind(queue)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        let pick = |col: &str| -> u64 {
            row.try_get::<Option<i64>, _>(col)
                .ok()
                .flatten()
                .and_then(|n| u64::try_from(n).ok())
                .unwrap_or(0)
        };
        Ok(QueueCounts {
            pending: pick("pending"),
            scheduled: pick("scheduled"),
            in_progress: pick("in_progress"),
            done: pick("done"),
            failed: pick("failed"),
            dead: pick("dead"),
        })
    }

    async fn oldest_ready_at(&self, queue: &str) -> Result<Option<DateTime<Utc>>> {
        let _t = OpTimer::read(&self.db_recorder);
        let row = sqlx::query(
            r"SELECT MIN(scheduled_at) AS oldest FROM sync_queue
              WHERE queue_name = $1 AND status = 'pending' AND scheduled_at <= $2",
        )
        .bind(queue)
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        row.try_get("oldest").map_err(map_sqlx_err)
    }

    async fn completed_latencies(
        &self,
        queue: Option<&str>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<JobLatency>> {
        let _t = OpTimer::read(&self.db_recorder);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let base = "SELECT completed_at, started_at, enqueued_at FROM sync_queue
                     WHERE status = 'done' AND completed_at IS NOT NULL
                       AND started_at IS NOT NULL
                       AND completed_at >= $1 AND completed_at <= $2";
        let rows = if let Some(q) = queue {
            sqlx::query(&format!(
                "{base} AND queue_name = $3 ORDER BY completed_at DESC LIMIT $4"
            ))
            .bind(from)
            .bind(to)
            .bind(q)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(&format!("{base} ORDER BY completed_at DESC LIMIT $3"))
                .bind(from)
                .bind(to)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
        }
        .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_latency).collect()
    }

    async fn upsert_metric_buckets(&self, rows: &[MetricBucket]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let _t = OpTimer::write(&self.db_recorder);
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;
        for row in rows {
            sqlx::query(
                "INSERT INTO metric_bucket
                     (queue, metric, bucket_start, count, sum, p50, p95, p99, max)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (queue, metric, bucket_start) DO UPDATE SET
                     count = EXCLUDED.count,
                     sum   = EXCLUDED.sum,
                     p50   = EXCLUDED.p50,
                     p95   = EXCLUDED.p95,
                     p99   = EXCLUDED.p99,
                     max   = EXCLUDED.max",
            )
            .bind(&row.queue)
            .bind(&row.metric)
            .bind(row.bucket_start)
            .bind(row.count)
            .bind(row.sum)
            .bind(row.p50)
            .bind(row.p95)
            .bind(row.p99)
            .bind(row.max)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
        }
        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn metric_buckets(
        &self,
        queue: Option<&str>,
        metrics: &[&str],
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<MetricBucket>> {
        if metrics.is_empty() {
            return Ok(Vec::new());
        }
        let _t = OpTimer::read(&self.db_recorder);
        // $1/$2 are from/to; the metric IN-list starts at $3; the
        // optional queue filter binds last.
        let metric_ph = (0..metrics.len())
            .map(|i| format!("${}", i + 3))
            .collect::<Vec<_>>()
            .join(", ");
        let queue_clause = if queue.is_some() {
            format!(" AND queue = ${}", metrics.len() + 3)
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT queue, metric, bucket_start, count, sum, p50, p95, p99, max
               FROM metric_bucket
              WHERE bucket_start >= $1 AND bucket_start <= $2
                AND metric IN ({metric_ph}){queue_clause}
              ORDER BY bucket_start ASC"
        );

        let mut q = sqlx::query(&sql).bind(from).bind(to);
        for m in metrics {
            q = q.bind(*m);
        }
        if let Some(qn) = queue {
            q = q.bind(qn);
        }
        let rows = q.fetch_all(&self.pool).await.map_err(map_sqlx_err)?;
        rows.iter().map(row_to_metric).collect()
    }

    async fn delete_metric_buckets_before(&self, before: DateTime<Utc>) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        let res = sqlx::query("DELETE FROM metric_bucket WHERE bucket_start < $1")
            .bind(before)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(res.rows_affected())
    }

    async fn distinct_kinds(&self, queue: Option<&str>) -> Result<Vec<String>> {
        let _t = OpTimer::read(&self.db_recorder);
        let rows = sqlx::query(
            r"SELECT DISTINCT kind FROM sync_queue
               WHERE ($1::text IS NULL OR queue_name = $1)
               ORDER BY kind ASC",
        )
        .bind(queue)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        rows.iter()
            .map(|r| r.try_get::<String, _>("kind").map_err(map_sqlx_err))
            .collect()
    }

    async fn list_for_timeline(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<TimelineEvent>> {
        let _t = OpTimer::read(&self.db_recorder);
        let rows = sqlx::query(
            r"SELECT at, kind, queue_name, event_type FROM queue_event
               WHERE at >= $1 AND at < $2
               ORDER BY at ASC",
        )
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        rows.iter()
            .map(|r| {
                let at: DateTime<Utc> = r.try_get("at").map_err(map_sqlx_err)?;
                let kind: String = r.try_get("kind").map_err(map_sqlx_err)?;
                let queue_name: String = r.try_get("queue_name").map_err(map_sqlx_err)?;
                let event_s: String = r.try_get("event_type").map_err(map_sqlx_err)?;
                let event_type = TimelineEventType::from_str(&event_s).ok_or_else(|| {
                    StorageError::Backend(format!("unknown event_type {event_s:?}"))
                })?;
                Ok(TimelineEvent {
                    at,
                    kind,
                    queue_name,
                    event_type,
                })
            })
            .collect()
    }

    async fn delete(&self, job_id: &JobId) -> Result<DeleteOutcome> {
        let _t = OpTimer::write(&self.db_recorder);
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;
        // Cancel path: in-progress rows have their cancel flag set
        // instead of being removed. The worker's heartbeat picks it
        // up, signals the in-process cancel token, the handler
        // returns, finalize moves the row to its terminal state, and
        // a follow-up delete (or cleanup retention) removes it.
        let cancel_row = sqlx::query(
            r"UPDATE sync_queue
                 SET cancel_requested_at = $1
               WHERE id = $2 AND status = 'in_progress'
               RETURNING id",
        )
        .bind(Utc::now())
        .bind(job_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;
        if cancel_row.is_some() {
            tx.commit().await.map_err(map_sqlx_err)?;
            return Ok(DeleteOutcome::CancelRequested);
        }
        sqlx::query("DELETE FROM queue_event WHERE job_id = $1")
            .bind(job_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
        let res = sqlx::query("DELETE FROM sync_queue WHERE id = $1")
            .bind(job_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(if res.rows_affected() > 0 {
            DeleteOutcome::Deleted
        } else {
            DeleteOutcome::NotFound
        })
    }

    async fn requeue(&self, job_id: &JobId) -> Result<bool> {
        let _t = OpTimer::write(&self.db_recorder);
        // M4: skip the requeue if this row's dedupe_key already has an
        // active (pending/in_progress) sibling — flipping it to pending
        // would trip the `jq_dedupe` UNIQUE partial index and surface as
        // a 500. SQLite uses `UPDATE OR IGNORE`; Postgres has no such
        // clause, so we pre-filter with the index's own predicate (same
        // shape as `requeue_batch_by_status`). A NULL key is always
        // requeueable. 0 rows changed → caller sees "not requeued",
        // matching the SQLite adapter.
        let res = sqlx::query(
            r"UPDATE sync_queue
                 SET status       = 'pending',
                     scheduled_at = $1,
                     completed_at = NULL,
                     process_id   = NULL,
                     heartbeat_at = NULL
               WHERE id = $2 AND status IN ('failed', 'dead')
                 AND (dedupe_key IS NULL OR NOT EXISTS (
                       SELECT 1 FROM sync_queue dup
                        WHERE dup.dedupe_key = sync_queue.dedupe_key
                          AND dup.id != sync_queue.id
                          AND dup.status IN ('pending', 'in_progress')
                     ))",
        )
        .bind(Utc::now())
        .bind(job_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected() > 0)
    }

    async fn delete_batch_by_status(
        &self,
        queue: Option<&str>,
        status: JobStatus,
        batch_size: usize,
    ) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        let batch_i = i64::try_from(batch_size).unwrap_or(i64::MAX);
        crate::storage::with_transient_retry("delete_batch_by_status", || async {
            // L2: pick the victim ids ONCE in a CTE so the event-delete and
            // the row-delete operate on the same set. As two separate
            // statements (even in one READ COMMITTED tx) each re-snapshots,
            // so a row that became eligible between them could be deleted
            // with its `queue_event` rows orphaned (chart-gauge skew). All
            // CTEs in one statement share a snapshot; `FOR UPDATE SKIP
            // LOCKED` also keeps concurrent batch purges from contending.
            let res = sqlx::query(
                r"WITH victims AS (
                      SELECT id FROM sync_queue
                       WHERE status = $1
                         AND ($2::TEXT IS NULL OR queue_name = $2)
                       ORDER BY id ASC
                       LIMIT $3
                       FOR UPDATE SKIP LOCKED
                  ),
                  ev AS (
                      DELETE FROM queue_event WHERE job_id IN (SELECT id FROM victims)
                  )
                  DELETE FROM sync_queue WHERE id IN (SELECT id FROM victims)",
            )
            .bind(status.as_str())
            .bind(queue)
            .bind(batch_i)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
            Ok(res.rows_affected())
        })
        .await
    }

    async fn requeue_batch_by_status(
        &self,
        queue: Option<&str>,
        status: JobStatus,
        batch_size: usize,
    ) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        let batch_i = i64::try_from(batch_size).unwrap_or(i64::MAX);
        crate::storage::with_transient_retry("requeue_batch_by_status", || async {
            let res = sqlx::query(
                r"UPDATE sync_queue
                     SET status       = 'pending',
                         scheduled_at = $1,
                         completed_at = NULL,
                         process_id   = NULL,
                         heartbeat_at = NULL
                   WHERE id IN (
                       SELECT s.id FROM sync_queue s
                        WHERE s.status = $2
                          AND ($3::TEXT IS NULL OR s.queue_name = $3)
                          -- Skip rows whose dedupe_key already has an
                          -- active (pending/in_progress) sibling — they'd
                          -- hit the jq_dedupe UNIQUE index. No UPDATE OR
                          -- IGNORE on PG, so we pre-filter with the
                          -- index's own predicate.
                          AND (s.dedupe_key IS NULL OR NOT EXISTS (
                                SELECT 1 FROM sync_queue a
                                 WHERE a.dedupe_key = s.dedupe_key
                                   AND a.status IN ('pending', 'in_progress')
                              ))
                        ORDER BY s.id ASC
                        LIMIT $4
                   )",
            )
            .bind(Utc::now())
            .bind(status.as_str())
            .bind(queue)
            .bind(batch_i)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
            Ok(res.rows_affected())
        })
        .await
    }

    async fn cleanup_superseded_retries(&self) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        // See the sqlite twin for the rationale: failed retries with an
        // active dedupe sibling are redundant + would loop the claim path.
        let res = sqlx::query(
            r"UPDATE sync_queue
                 SET status       = 'dead',
                     completed_at = $1,
                     last_error   = 'superseded by active sibling'
               WHERE status     = 'failed'
                 AND dedupe_key IS NOT NULL
                 AND EXISTS (
                     SELECT 1 FROM sync_queue dup
                      WHERE dup.dedupe_key = sync_queue.dedupe_key
                        AND dup.id != sync_queue.id
                        AND dup.status IN ('pending', 'in_progress')
                 )",
        )
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected())
    }

    async fn list_scheduled_after(
        &self,
        queue: Option<&str>,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<JobRecord>> {
        let _t = OpTimer::read(&self.db_recorder);
        let limit_i = i64::try_from(limit).unwrap_or(100);
        let rows = if let Some(q) = queue {
            sqlx::query(
                r"SELECT * FROM sync_queue
                   WHERE status = 'pending'
                     AND scheduled_at > $1
                     AND queue_name = $2
                   ORDER BY scheduled_at ASC, id ASC
                   LIMIT $3",
            )
            .bind(now)
            .bind(q)
            .bind(limit_i)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r"SELECT * FROM sync_queue
                   WHERE status = 'pending'
                     AND scheduled_at > $1
                   ORDER BY scheduled_at ASC, id ASC
                   LIMIT $2",
            )
            .bind(now)
            .bind(limit_i)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_job).collect()
    }

    async fn run_now(&self, job_id: &JobId) -> Result<bool> {
        let _t = OpTimer::write(&self.db_recorder);
        let res = sqlx::query(
            r"UPDATE sync_queue
                 SET scheduled_at = $1
               WHERE id = $2 AND status = 'pending'",
        )
        .bind(Utc::now())
        .bind(job_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected() > 0)
    }

    async fn wait_for_work(&self, queue: &str, timeout: Duration) -> Result<bool> {
        // H2: block on the in-process per-queue notify the process
        // listener feeds — no per-call connection. A wake delivered while
        // no waiter exists yet still leaves a permit (`notify_one`), and a
        // missed wake is bounded by the caller's `timeout` (the runtime
        // re-polls `claim_next` every `IDLE_POLL`), so correctness holds
        // regardless of `NOTIFY` delivery timing.
        let notify = self.wake.for_queue(queue).await;
        tokio::select! {
            biased;
            () = notify.notified() => Ok(true),
            () = tokio::time::sleep(timeout) => Ok(false),
        }
    }

    async fn notify(&self, queue: &str) -> Result<()> {
        let _t = OpTimer::write(&self.db_recorder);
        // Wake every replica's listener via one shared channel, carrying
        // the queue name as the payload. `pg_notify($1, $2)` binds both
        // args, so a hostile queue name can't inject — no manual
        // identifier escaping needed. The listener routes the payload to
        // that queue's in-process notify.
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(WAKE_CHANNEL)
            .bind(queue)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn describe(&self) -> Result<StorageInfo> {
        let _t = OpTimer::read(&self.db_recorder);
        let server_version: String = sqlx::query_scalar("SHOW server_version")
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(StorageInfo {
            backend: "postgres".to_owned(),
            fields: vec![
                ("server_version".to_owned(), server_version),
                (
                    "max_connections".to_owned(),
                    self.max_connections.to_string(),
                ),
            ],
        })
    }

    fn drain_op_samples(&self) -> super::db_timing::DrainedSamples {
        self.db_recorder.drain()
    }

    async fn flush_event_buffer(&self) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        let (events, dropped) = self.events.drain();
        if dropped > 0 {
            tracing::warn!(
                dropped,
                "queue_event: buffered events dropped at cap before flush"
            );
        }
        if events.is_empty() {
            return Ok(0);
        }
        let total = events.len();
        // Postgres caps bound params at 65535; 5 cols/row → 1000 rows
        // per multi-row INSERT keeps each statement comfortably small.
        for chunk in events.chunks(1000) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
                "INSERT INTO queue_event (at, kind, queue_name, event_type, job_id) ",
            );
            qb.push_values(chunk, |mut b, ev| {
                b.push_bind(ev.at)
                    .push_bind(ev.kind.as_str())
                    .push_bind(ev.queue_name.as_str())
                    .push_bind(ev.event_type)
                    .push_bind(ev.job_id.as_deref());
            });
            qb.build().execute(&self.pool).await.map_err(map_sqlx_err)?;
        }
        Ok(u64::try_from(total).unwrap_or(u64::MAX))
    }

    async fn db_health_snapshot(&self) -> Vec<(&'static str, f64)> {
        // Source everything from the server, not sqlx's pool counters —
        // `pg_stat_activity` is the canonical view of who's connected
        // and what they're doing, regardless of which client/library
        // opened the connection.
        let mut out = Vec::with_capacity(4);
        // `warn!`: if any of these silently fail the DB-health charts
        // sit at zero forever — exactly the symptom that would take
        // an hour to diagnose without a log. Loud is the right level.
        match query_conn_counts(&self.pool).await {
            Ok((active, idle)) => {
                out.push((metric::DB_POOL_ACTIVE, f64::from(active)));
                out.push((metric::DB_POOL_IDLE, f64::from(idle)));
            }
            Err(e) => tracing::warn!(?e, "postgres db_health: pg_stat_activity query failed"),
        }
        match query_max_connections(&self.pool).await {
            Ok(max) => out.push((metric::DB_POOL_MAX, f64::from(max))),
            Err(e) => tracing::warn!(?e, "postgres db_health: pg_settings query failed"),
        }
        match query_database_size(&self.pool).await {
            Ok(bytes) => out.push((metric::DB_SIZE_BYTES, bytes)),
            Err(e) => tracing::warn!(?e, "postgres db_health: pg_database_size query failed"),
        }
        out
    }
}

/// Count active vs idle server-side backends **across the whole
/// Postgres cluster**, not just the current database.
///
/// `pg_settings.max_connections` is a *server-wide* cap — every
/// backend on every DB shares it. Filtering this count by
/// `current_database()` would make `pool_used_pct = our_db_active /
/// server_max` mix per-DB usage with the server-wide cap, hiding
/// pressure from sibling apps / ad-hoc `psql` sessions on the same
/// cluster. Counting all backends (regardless of DB) gives the
/// honest "is the cluster running out of connection slots?" answer
/// — the question that actually matters when scaling more workers
/// or spinning up another app process.
async fn query_conn_counts(pool: &PgPool) -> Result<(u32, u32)> {
    let row = sqlx::query(
        r"SELECT
            COUNT(*) FILTER (WHERE state = 'active' AND pid <> pg_backend_pid()) AS active,
            COUNT(*) FILTER (WHERE state IN ('idle', 'idle in transaction')) AS idle
          FROM pg_stat_activity",
    )
    .fetch_one(pool)
    .await
    .map_err(map_sqlx_err)?;
    let active: i64 = row.try_get("active").map_err(map_sqlx_err)?;
    let idle: i64 = row.try_get("idle").map_err(map_sqlx_err)?;
    Ok((
        u32::try_from(active.max(0)).unwrap_or(u32::MAX),
        u32::try_from(idle.max(0)).unwrap_or(u32::MAX),
    ))
}

async fn query_max_connections(pool: &PgPool) -> Result<u32> {
    let row: i32 =
        sqlx::query_scalar("SELECT setting::int FROM pg_settings WHERE name = 'max_connections'")
            .fetch_one(pool)
            .await
            .map_err(map_sqlx_err)?;
    Ok(u32::try_from(row.max(0)).unwrap_or(u32::MAX))
}

#[allow(
    clippy::cast_precision_loss,
    reason = "DB size in bytes fits f64 exactly well past any practical workload"
)]
async fn query_database_size(pool: &PgPool) -> Result<f64> {
    let bytes: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_err)?;
    Ok(bytes.max(0) as f64)
}

impl PostgresStorage {
    async fn do_finalize(
        &self,
        job_id: &JobId,
        owner: Option<&str>,
        outcome: FinalizeOutcome,
    ) -> Result<()> {
        let now = Utc::now();
        match outcome {
            FinalizeOutcome::Done => self.finalize_done(job_id, owner, now).await,
            FinalizeOutcome::Throttled {
                retry_after,
                cool_down_queue,
            } => {
                self.finalize_throttled(job_id, owner, retry_after, cool_down_queue, now)
                    .await
            }
            FinalizeOutcome::Failed {
                retry_after,
                message,
            } => {
                let next = now
                    + chrono::Duration::from_std(retry_after)
                        .unwrap_or_else(|_| chrono::Duration::seconds(60));
                append_error_and_update(
                    &self.pool,
                    &self.events,
                    job_id,
                    now,
                    &message,
                    false,
                    Some(next),
                    None,
                    owner,
                )
                .await
            }
            FinalizeOutcome::Dead { message } => {
                append_error_and_update(
                    &self.pool,
                    &self.events,
                    job_id,
                    now,
                    &message,
                    true,
                    None,
                    None,
                    owner,
                )
                .await
            }
        }
    }

    /// `Done` finalize: mark the row done, log a `completed` event, and
    /// clear any queue-wide throttle cool-down. One tx. Mirrors the
    /// `SQLite` adapter.
    async fn finalize_done(
        &self,
        job_id: &JobId,
        owner: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;
        // Ownership guard (H1): only transition a row still in_progress and
        // owned by this process; a reaped + re-claimed row fails the guard
        // → 0 rows → clean no-op. `None` skips the guard.
        let guard = owner.map_or("", |_| " AND process_id = $3 AND status = 'in_progress'");
        let sql = format!(
            "UPDATE sync_queue
                 SET status            = 'done',
                     completed_at      = $1,
                     throttle_attempts = 0,
                     process_id        = NULL,
                     heartbeat_at      = NULL
               WHERE id = $2{guard}
               RETURNING kind, queue_name"
        );
        let mut q = sqlx::query(&sql).bind(now).bind(job_id.as_str());
        if let Some(pid) = owner {
            q = q.bind(pid);
        }
        let row = q.fetch_optional(&mut *tx).await.map_err(map_sqlx_err)?;
        // Cool-down clear is queue *state* — stays in the tx. The
        // `completed` event is buffered, pushed after commit.
        let pending = if let Some(r) = row {
            let kind: String = r.try_get("kind").map_err(map_sqlx_err)?;
            let queue_name: String = r.try_get("queue_name").map_err(map_sqlx_err)?;
            clear_queue_cooldown(&mut *tx, &queue_name, now).await?;
            Some(EventRecord::new(
                now,
                kind,
                queue_name,
                Some(job_id.as_str()),
                "completed",
            ))
        } else {
            None
        };
        tx.commit().await.map_err(map_sqlx_err)?;
        if let Some(ev) = pending {
            self.events.push(ev);
        }
        Ok(())
    }

    /// `Throttled` finalize: re-queue the row (without burning a retry)
    /// and log a `retried` event. When `cool_down_queue` is set, also
    /// extend the queue-wide cool-down. One tx. Mirrors the `SQLite`
    /// adapter.
    async fn finalize_throttled(
        &self,
        job_id: &JobId,
        owner: Option<&str>,
        retry_after: std::time::Duration,
        cool_down_queue: bool,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let next = now
            + chrono::Duration::from_std(retry_after)
                .unwrap_or_else(|_| chrono::Duration::seconds(60));
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;
        // Ownership guard (H1) — see `finalize_done`.
        let guard = owner.map_or("", |_| " AND process_id = $3 AND status = 'in_progress'");
        let sql = format!(
            "UPDATE sync_queue
                 SET status            = 'pending',
                     scheduled_at      = $1,
                     attempts          = GREATEST(attempts - 1, 0),
                     throttle_attempts = throttle_attempts + 1,
                     process_id        = NULL,
                     heartbeat_at      = NULL
               WHERE id = $2{guard}
               RETURNING kind, queue_name"
        );
        let mut q = sqlx::query(&sql).bind(next).bind(job_id.as_str());
        if let Some(pid) = owner {
            q = q.bind(pid);
        }
        let row = q.fetch_optional(&mut *tx).await.map_err(map_sqlx_err)?;
        // Cool-down extension is queue state — stays in the tx. The
        // `retried` event is buffered, pushed after commit.
        let pending = if let Some(r) = row {
            let kind: String = r.try_get("kind").map_err(map_sqlx_err)?;
            let queue_name: String = r.try_get("queue_name").map_err(map_sqlx_err)?;
            if cool_down_queue {
                extend_queue_cooldown(&mut *tx, &queue_name, next, now).await?;
            }
            Some(EventRecord::new(
                now,
                kind,
                queue_name,
                Some(job_id.as_str()),
                "retried",
            ))
        } else {
            None
        };
        tx.commit().await.map_err(map_sqlx_err)?;
        if let Some(ev) = pending {
            self.events.push(ev);
        }
        Ok(())
    }
}

#[async_trait]
impl ProcessRegistry for PostgresStorage {
    async fn register(&self, process_id: &str, queue: &str, host: &str) -> Result<()> {
        let now = Utc::now();
        // INSERT ... ON CONFLICT DO UPDATE — process_id is the PK.
        // Replaces any existing partial row stamped by heartbeat()
        // during a restart, healing the row to the right shape.
        sqlx::query(
            r"INSERT INTO queue_process
                (process_id, queue_name, host_id, started_at, heartbeat_at, current_job)
              VALUES ($1, $2, $3, $4, $4, NULL)
              ON CONFLICT (process_id) DO UPDATE
                SET queue_name   = EXCLUDED.queue_name,
                    host_id      = EXCLUDED.host_id,
                    started_at   = EXCLUDED.started_at,
                    heartbeat_at = EXCLUDED.heartbeat_at,
                    current_job  = NULL",
        )
        .bind(process_id)
        .bind(queue)
        .bind(host)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn heartbeat(&self, process_id: &str, current_job: Option<JobId>) -> Result<()> {
        let now = Utc::now();
        let current_job_str = current_job.as_ref().map(JobId::as_str);
        // UPDATE first; if no row touched, INSERT a partial row that
        // the next `register` heals. Same self-healing semantics as
        // the SQLite version.
        let res = sqlx::query(
            r"UPDATE queue_process
                 SET heartbeat_at = $1, current_job = $2
               WHERE process_id = $3",
        )
        .bind(now)
        .bind(current_job_str)
        .bind(process_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        if res.rows_affected() > 0 {
            return Ok(());
        }
        sqlx::query(
            r"INSERT INTO queue_process
                (process_id, queue_name, host_id, started_at, heartbeat_at, current_job)
              VALUES ($1, '', '', $2, $2, $3)
              ON CONFLICT (process_id) DO UPDATE
                SET heartbeat_at = EXCLUDED.heartbeat_at,
                    current_job  = EXCLUDED.current_job",
        )
        .bind(process_id)
        .bind(now)
        .bind(current_job_str)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn deregister(&self, process_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM queue_process WHERE process_id = $1")
            .bind(process_id)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn reap_stale(&self, stale_before: DateTime<Utc>) -> Result<u64> {
        let res = sqlx::query("DELETE FROM queue_process WHERE heartbeat_at < $1")
            .bind(stale_before)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        // Evict crashed pods + orphaned slot assignments — see the
        // SQLite adapter. delete_for_host only runs on a clean shutdown,
        // so without this an ungraceful exit leaks rows per dead host.
        sqlx::query("DELETE FROM pod WHERE heartbeat_at < $1")
            .bind(stale_before)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        sqlx::query(
            "DELETE FROM pod_slot_assignment
              WHERE host_id NOT IN (SELECT host_id FROM pod)",
        )
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected())
    }

    async fn list(&self, queue: Option<&str>) -> Result<Vec<ProcessRecord>> {
        let rows = if let Some(q) = queue {
            sqlx::query(
                r"SELECT * FROM queue_process
                   WHERE queue_name = $1
                   ORDER BY process_id ASC",
            )
            .bind(q)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query("SELECT * FROM queue_process ORDER BY queue_name ASC, process_id ASC")
                .fetch_all(&self.pool)
                .await
        }
        .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_proc).collect()
    }

    async fn delete_for_host(&self, host: &str) -> Result<u64> {
        // queue_process + pod presence + slot assignments — a graceful
        // exit frees the pod from the cluster view immediately.
        let res = sqlx::query("DELETE FROM queue_process WHERE host_id = $1")
            .bind(host)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        for sql in [
            "DELETE FROM pod WHERE host_id = $1",
            "DELETE FROM pod_slot_assignment WHERE host_id = $1",
        ] {
            sqlx::query(sql)
                .bind(host)
                .execute(&self.pool)
                .await
                .map_err(map_sqlx_err)?;
        }
        Ok(res.rows_affected())
    }

    async fn pod_heartbeat(&self, host: &str) -> Result<()> {
        // Runtime clock (bound), not DB now(), to match queue_process
        // and the runtime-computed cutoff in list_live_pods / reap_stale.
        // Mixing a DB-written timestamp with a runtime-computed cutoff
        // would misjudge liveness under clock skew. (Leadership uses the
        // DB clock end-to-end — see try_cron_lease — which is the
        // skew-immune path that actually needs it.)
        sqlx::query(
            r"INSERT INTO pod (host_id, heartbeat_at) VALUES ($1, $2)
              ON CONFLICT (host_id) DO UPDATE SET heartbeat_at = EXCLUDED.heartbeat_at",
        )
        .bind(host)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn list_live_pods(&self, stale_before: DateTime<Utc>) -> Result<Vec<String>> {
        let rows =
            sqlx::query("SELECT host_id FROM pod WHERE heartbeat_at >= $1 ORDER BY host_id ASC")
                .bind(stale_before)
                .fetch_all(&self.pool)
                .await
                .map_err(map_sqlx_err)?;
        rows.iter()
            .map(|r| r.try_get::<String, _>("host_id").map_err(map_sqlx_err))
            .collect()
    }

    async fn set_slots(&self, queue: &str, host: &str, slots: i32) -> Result<()> {
        sqlx::query(
            r"INSERT INTO pod_slot_assignment (queue_name, host_id, slots, updated_at)
              VALUES ($1, $2, $3, now())
              ON CONFLICT (queue_name, host_id) DO UPDATE
                 SET slots = EXCLUDED.slots, updated_at = EXCLUDED.updated_at",
        )
        .bind(queue)
        .bind(host)
        .bind(slots.max(0))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn get_slots(&self, queue: &str, host: &str) -> Result<Option<i32>> {
        let row = sqlx::query(
            "SELECT slots FROM pod_slot_assignment WHERE queue_name = $1 AND host_id = $2",
        )
        .bind(queue)
        .bind(host)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        row.map(|r| r.try_get::<i32, _>("slots").map_err(map_sqlx_err))
            .transpose()
    }
}

#[async_trait]
impl QueueConfig for PostgresStorage {
    async fn ensure_queue(&self, name: &str, default_max_workers: i32) -> Result<()> {
        sqlx::query(
            r"INSERT INTO queue
                (name, max_workers, paused, retain_done_for_days, retain_dead_for_days,
                 backoff_enabled, backoff_base_seconds, backoff_max_seconds, updated_at)
              VALUES ($1, $2, FALSE, 7, 30, FALSE, 60, 1800, $3)
              ON CONFLICT (name) DO NOTHING",
        )
        .bind(name)
        .bind(default_max_workers)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn get_queue(&self, name: &str) -> Result<Option<QueueConfigRow>> {
        let row = sqlx::query("SELECT * FROM queue WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_queue_config).transpose()
    }

    async fn list_queues(&self) -> Result<Vec<QueueConfigRow>> {
        let rows = sqlx::query("SELECT * FROM queue ORDER BY name ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_queue_config).collect()
    }

    async fn set_max_workers(&self, name: &str, n: i32) -> Result<()> {
        let clamped = n.clamp(0, 64);
        sqlx::query("UPDATE queue SET max_workers = $1, updated_at = $2 WHERE name = $3")
            .bind(clamped)
            .bind(Utc::now())
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn set_paused(&self, name: &str, paused: bool) -> Result<()> {
        sqlx::query("UPDATE queue SET paused = $1, updated_at = $2 WHERE name = $3")
            .bind(paused)
            .bind(Utc::now())
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn set_retention(&self, name: &str, done_days: i32, dead_days: i32) -> Result<()> {
        sqlx::query(
            r"UPDATE queue
                 SET retain_done_for_days = $1,
                     retain_dead_for_days = $2,
                     updated_at = $3
               WHERE name = $4",
        )
        .bind(done_days.max(0))
        .bind(dead_days.max(0))
        .bind(Utc::now())
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn set_backoff(
        &self,
        name: &str,
        enabled: bool,
        base_seconds: i32,
        max_seconds: i32,
    ) -> Result<()> {
        let base = base_seconds.clamp(1, 86_400);
        let max = max_seconds.clamp(1, 86_400);
        sqlx::query(
            r"UPDATE queue
                 SET backoff_enabled      = $1,
                     backoff_base_seconds = $2,
                     backoff_max_seconds  = $3,
                     updated_at           = $4
               WHERE name = $5",
        )
        .bind(enabled)
        .bind(base)
        .bind(max)
        .bind(Utc::now())
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }
}

#[async_trait]
impl CronStorage for PostgresStorage {
    async fn ensure_schedule(&self, schedule: NewCronSchedule) -> Result<()> {
        let now = Utc::now();
        sqlx::query(
            r"INSERT INTO cron_schedule (
                name, kind, payload, queue_name, cron_expr, enabled,
                max_attempts, created_at, updated_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
              ON CONFLICT (name) DO NOTHING",
        )
        .bind(&schedule.name)
        .bind(&schedule.kind)
        .bind(&schedule.payload)
        .bind(schedule.queue_name.as_deref())
        .bind(&schedule.cron_expr)
        .bind(schedule.enabled)
        .bind(schedule.max_attempts)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn list_schedules(&self) -> Result<Vec<CronScheduleRecord>> {
        let rows = sqlx::query("SELECT * FROM cron_schedule ORDER BY name ASC")
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_cron).collect()
    }

    async fn record_fire(
        &self,
        name: &str,
        fired_at: DateTime<Utc>,
        next_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r"UPDATE cron_schedule
                 SET last_fired_at = $1,
                     next_fire_at  = $2,
                     last_error    = NULL,
                     updated_at    = $3
               WHERE name = $4",
        )
        .bind(fired_at)
        .bind(next_at)
        .bind(Utc::now())
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn try_advance_fire(
        &self,
        name: &str,
        expected: DateTime<Utc>,
        fired_at: DateTime<Utc>,
        next_at: DateTime<Utc>,
    ) -> Result<bool> {
        let res = sqlx::query(
            r"UPDATE cron_schedule
                 SET last_fired_at = $1,
                     next_fire_at  = $2,
                     last_error    = NULL,
                     updated_at    = $3
               WHERE name = $4 AND next_fire_at = $5",
        )
        .bind(fired_at)
        .bind(next_at)
        .bind(Utc::now())
        .bind(name)
        .bind(expected)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected() == 1)
    }

    async fn record_parse_error(&self, name: &str, message: &str) -> Result<()> {
        sqlx::query(
            r"UPDATE cron_schedule
                 SET last_error = $1,
                     enabled    = FALSE,
                     updated_at = $2
               WHERE name = $3",
        )
        .bind(message)
        .bind(Utc::now())
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn set_enabled(&self, name: &str, enabled: bool) -> Result<()> {
        let now = Utc::now();
        // Re-enabling clears any stale next_fire_at so the cron loop
        // recomputes from the schedule expression on the next tick.
        if enabled {
            sqlx::query(
                r"UPDATE cron_schedule
                     SET enabled      = TRUE,
                         next_fire_at = NULL,
                         last_error   = NULL,
                         updated_at   = $1
                   WHERE name = $2",
            )
            .bind(now)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        } else {
            sqlx::query(
                r"UPDATE cron_schedule
                     SET enabled      = FALSE,
                         next_fire_at = NULL,
                         updated_at   = $1
                   WHERE name = $2",
            )
            .bind(now)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        }
        Ok(())
    }

    async fn set_expr(&self, name: &str, expr: &str) -> Result<()> {
        sqlx::query(
            r"UPDATE cron_schedule
                 SET cron_expr    = $1,
                     next_fire_at = NULL,
                     last_error   = NULL,
                     updated_at   = $2
               WHERE name = $3",
        )
        .bind(expr)
        .bind(Utc::now())
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn delete_schedule(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM cron_schedule WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn get_schedule(&self, name: &str) -> Result<Option<CronScheduleRecord>> {
        let row = sqlx::query("SELECT * FROM cron_schedule WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_cron).transpose()
    }

    async fn try_cron_lease(&self, holder: &str, ttl: std::time::Duration) -> Result<bool> {
        let ttl_secs = i32::try_from(ttl.as_secs()).unwrap_or(15).max(1);
        // Atomic upsert gated on lease state; RETURNING yields a row
        // only when we won (or renewed) the lease.
        let row = sqlx::query(
            r"INSERT INTO cron_leader (id, holder, lease_until)
              VALUES (1, $1, now() + ($2 * interval '1 second'))
              ON CONFLICT (id) DO UPDATE
                 SET holder      = EXCLUDED.holder,
                     lease_until = EXCLUDED.lease_until
               WHERE cron_leader.lease_until < now()
                  OR cron_leader.holder = $1
              RETURNING 1",
        )
        .bind(holder)
        .bind(ttl_secs)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(row.is_some())
    }
}

// ────────────────────────────────────────────────────────────────────
// Free helpers — kept module-private. Same shape as the SQLite tree's
// `enqueue_in_tx` / `record_event` / `append_error_and_update`.
// ────────────────────────────────────────────────────────────────────

const DEFAULT_MAX_ATTEMPTS: i32 = 5;

/// Insert one row inside an open tx. Dedupe is the `SQLite` tree's
/// two-statement pattern: `SELECT FOR UPDATE` on an active row with
/// `dedupe_key`; if found, return `Deduped(id)`. Else `INSERT`.
async fn enqueue_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    req: &EnqueueRequest,
    new_id: &str,
    pending: &mut Vec<EventRecord>,
) -> Result<EnqueueOutcome> {
    let queue = req
        .queue_name
        .as_deref()
        .ok_or_else(|| StorageError::InvalidInput("queue_name required".into()))?;

    // Dedupe pre-check. Lock the candidate row so a concurrent inserter
    // with the same dedupe_key blocks until we commit.
    if let Some(key) = req.dedupe_key.as_deref() {
        let existing: Option<String> = sqlx::query_scalar(
            r"SELECT id FROM sync_queue
               WHERE dedupe_key = $1
                 AND status IN ('pending', 'in_progress')
               LIMIT 1
               FOR UPDATE",
        )
        .bind(key)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_err)?;
        if let Some(id) = existing {
            return Ok(EnqueueOutcome::Deduped(JobId::new(id)));
        }
    }

    let id = new_id.to_owned();
    let now = Utc::now();
    let scheduled = req.run_at.unwrap_or(now);
    let max_attempts = req.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);

    // ON CONFLICT backstops the pre-check: a concurrent enqueue that
    // inserted the same active dedupe_key after our SELECT makes this a
    // no-op (the jq_dedupe UNIQUE partial index). A NULL dedupe_key is
    // not in the index, so it never conflicts.
    let inserted: Option<String> = sqlx::query_scalar(
        r"INSERT INTO sync_queue (
              id, queue_name, kind, payload, status, priority,
              enqueued_at, scheduled_at, attempts, max_attempts,
              error_history, dedupe_key
          ) VALUES ($1, $2, $3, $4, 'pending', $5, $6, $7, 0, $8, '[]'::jsonb, $9)
          ON CONFLICT (dedupe_key)
              WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'in_progress')
          DO NOTHING
          RETURNING id",
    )
    .bind(&id)
    .bind(queue)
    .bind(req.kind.as_ref())
    .bind(&req.payload)
    .bind(req.priority)
    .bind(now)
    .bind(scheduled)
    .bind(max_attempts)
    .bind(req.dedupe_key.as_deref())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_err)?;

    if inserted.is_none() {
        // Lost the dedupe race — return the active winner as Deduped.
        if let Some(key) = req.dedupe_key.as_deref() {
            let existing: Option<String> = sqlx::query_scalar(
                r"SELECT id FROM sync_queue
                   WHERE dedupe_key = $1 AND status IN ('pending', 'in_progress')
                   LIMIT 1",
            )
            .bind(key)
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_sqlx_err)?;
            if let Some(existing) = existing {
                return Ok(EnqueueOutcome::Deduped(JobId::new(existing)));
            }
        }
        return Err(StorageError::Backend(
            "enqueue: insert affected no rows".into(),
        ));
    }

    // Buffer the `enqueued` event; the caller pushes it after the tx
    // commits, so a rolled-back insert leaves no orphan event.
    pending.push(EventRecord::new(
        now,
        req.kind.as_ref(),
        queue,
        Some(&id),
        "enqueued",
    ));
    Ok(EnqueueOutcome::Enqueued(JobId::new(id)))
}

/// Open (or extend) the queue-wide throttle cool-down for one limiter
/// event. The `throttled_until <= now` guard means only the *first*
/// throttle in a window bumps + sets the deadline; concurrent throttles
/// from sibling workers reacting to the same rate-limit hit land inside
/// the live window and no-op (under READ COMMITTED the row-locked
/// `UPDATE … WHERE` re-checks the latest row, so it's a CAS). So the
/// exponent counts limiter *events*, not workers-in-flight. Clamped so
/// a long outage can't overflow it. Mirrors the `SQLite` adapter.
async fn extend_queue_cooldown<'e, E>(
    executor: E,
    queue_name: &str,
    until: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        r"UPDATE queue
             SET throttle_attempts = LEAST(throttle_attempts + 1, 30),
                 throttled_until   = $1,
                 updated_at        = $2
           WHERE name = $3
             AND (throttled_until IS NULL OR throttled_until <= $4)",
    )
    .bind(until)
    .bind(now)
    .bind(queue_name)
    .bind(now)
    .execute(executor)
    .await
    .map_err(map_sqlx_err)?;
    Ok(())
}

/// Clear the queue-wide throttle cool-down after a job succeeds — but
/// only once the window elapsed **and stayed quiet** for the throttle
/// decay grace (`throttled_until <= now - grace`). A success from a job
/// that was already in-flight when the limit hit doesn't prove the
/// window passed, so clearing on it would reopen the gate into the
/// still-active limit. The grace is also the exponent's decay: clearing
/// at the bare deadline let a single success in the gap before the
/// limiter flapped back reset the curve to `base`, so a flapping limiter
/// never escalated. No-op on the un-throttled hot path. Mirrors the
/// `SQLite` adapter.
async fn clear_queue_cooldown<'e, E>(
    executor: E,
    queue_name: &str,
    now: DateTime<Utc>,
) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let decay_before = now - chrono::Duration::seconds(crate::runtime::THROTTLE_DECAY_GRACE_SECS);
    sqlx::query(
        r"UPDATE queue
             SET throttle_attempts = 0,
                 throttled_until   = NULL,
                 updated_at        = $1
           WHERE name = $2
             AND throttle_attempts > 0
             AND (throttled_until IS NULL OR throttled_until <= $3)",
    )
    .bind(now)
    .bind(queue_name)
    .bind(decay_before)
    .execute(executor)
    .await
    .map_err(map_sqlx_err)?;
    Ok(())
}

/// Append a row to `error_history`, cap at `ERROR_HISTORY_CAP`, set
/// `last_error`, and transition status. Same shape as the `SQLite` tree.
#[allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "one cohesive read-modify-write of error_history + the status transition; the terminal/non-terminal arms are inherent and splitting hurts readability. The two mutually-exclusive guard params (owner / stale) are the cost of sharing this between the finalize and reaper paths"
)]
async fn append_error_and_update(
    pool: &PgPool,
    events: &EventBuffer,
    job_id: &JobId,
    now: DateTime<Utc>,
    message: &str,
    terminal: bool,
    next_scheduled_at: Option<DateTime<Utc>>,
    // See the `SQLite` adapter: reaper path passes the stale cutoff so a
    // row its worker finalized between scan and write isn't clobbered.
    guard_stale_before: Option<DateTime<Utc>>,
    // Finalize path (H1): when set, the transition only fires if the row
    // is still `in_progress` and owned by this `process_id`. Mutually
    // exclusive with `guard_stale_before`; both `None` skips the guard.
    guard_owner: Option<&str>,
) -> Result<()> {
    let mut tx = pool.begin().await.map_err(map_sqlx_err)?;
    let row =
        sqlx::query("SELECT attempts, error_history FROM sync_queue WHERE id = $1 FOR UPDATE")
            .bind(job_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
    let Some(row) = row else {
        tx.commit().await.map_err(map_sqlx_err)?;
        return Ok(());
    };
    let attempts: i32 = row.try_get("attempts").map_err(map_sqlx_err)?;
    let history_v: serde_json::Value = row.try_get("error_history").map_err(map_sqlx_err)?;
    let mut entries: Vec<ErrorHistoryEntry> = serde_json::from_value(history_v).unwrap_or_default();
    entries.push(ErrorHistoryEntry {
        at: now,
        attempt: attempts,
        message: message.to_owned(),
    });
    if entries.len() > ERROR_HISTORY_CAP {
        let drop = entries.len() - ERROR_HISTORY_CAP;
        entries.drain(0..drop);
    }
    let history_jsonb = serde_json::to_value(&entries)?;

    // Status-transition guard. At most one of the two is set; both bind
    // $5 (owner = text process_id, stale = timestamp), so the bind sites
    // branch on which is present.
    let guard = match (guard_owner, guard_stale_before) {
        (Some(_), _) => " AND status = 'in_progress' AND process_id = $5",
        (None, Some(_)) => " AND status = 'in_progress' AND heartbeat_at < $5",
        (None, None) => "",
    };

    // Event buffered here, pushed only after the tx commits below.
    let mut pending: Option<EventRecord> = None;
    if terminal {
        let sql = format!(
            "UPDATE sync_queue
                 SET status         = 'dead',
                     completed_at   = $1,
                     process_id     = NULL,
                     heartbeat_at   = NULL,
                     last_error     = $2,
                     error_history  = $3
               WHERE id = $4{guard}
               RETURNING kind, queue_name"
        );
        let mut q = sqlx::query(&sql)
            .bind(now)
            .bind(message)
            .bind(&history_jsonb)
            .bind(job_id.as_str());
        if let Some(pid) = guard_owner {
            q = q.bind(pid);
        } else if let Some(g) = guard_stale_before {
            q = q.bind(g);
        }
        let dead_row = q.fetch_optional(&mut *tx).await.map_err(map_sqlx_err)?;
        if let Some(r) = dead_row {
            let kind: String = r.try_get("kind").map_err(map_sqlx_err)?;
            let queue_name: String = r.try_get("queue_name").map_err(map_sqlx_err)?;
            pending = Some(EventRecord::new(
                now,
                kind,
                queue_name,
                Some(job_id.as_str()),
                "failed",
            ));
        }
    } else {
        let sql = format!(
            "UPDATE sync_queue
                 SET status         = 'failed',
                     scheduled_at   = $1,
                     process_id     = NULL,
                     heartbeat_at   = NULL,
                     last_error     = $2,
                     error_history  = $3
               WHERE id = $4{guard}
               RETURNING kind, queue_name"
        );
        let mut q = sqlx::query(&sql)
            .bind(next_scheduled_at.unwrap_or(now))
            .bind(message)
            .bind(&history_jsonb)
            .bind(job_id.as_str());
        if let Some(pid) = guard_owner {
            q = q.bind(pid);
        } else if let Some(g) = guard_stale_before {
            q = q.bind(g);
        }
        let row = q.fetch_optional(&mut *tx).await.map_err(map_sqlx_err)?;
        if let Some(r) = row {
            let kind: String = r.try_get("kind").map_err(map_sqlx_err)?;
            let queue_name: String = r.try_get("queue_name").map_err(map_sqlx_err)?;
            pending = Some(EventRecord::new(
                now,
                kind,
                queue_name,
                Some(job_id.as_str()),
                "retried",
            ));
        }
    }
    tx.commit().await.map_err(map_sqlx_err)?;
    if let Some(ev) = pending {
        events.push(ev);
    }
    Ok(())
}

fn row_to_latency(r: &sqlx::postgres::PgRow) -> Result<JobLatency> {
    let completed_at: DateTime<Utc> = r.try_get("completed_at").map_err(map_sqlx_err)?;
    let started_at: DateTime<Utc> = r.try_get("started_at").map_err(map_sqlx_err)?;
    let enqueued_at: DateTime<Utc> = r.try_get("enqueued_at").map_err(map_sqlx_err)?;
    Ok(JobLatency {
        completed_at,
        processing_ms: (completed_at - started_at).num_milliseconds(),
        total_ms: (completed_at - enqueued_at).num_milliseconds(),
    })
}

fn row_to_metric(r: &sqlx::postgres::PgRow) -> Result<MetricBucket> {
    Ok(MetricBucket {
        queue: r.try_get("queue").map_err(map_sqlx_err)?,
        metric: r.try_get("metric").map_err(map_sqlx_err)?,
        bucket_start: r.try_get("bucket_start").map_err(map_sqlx_err)?,
        count: r.try_get("count").map_err(map_sqlx_err)?,
        sum: r.try_get("sum").map_err(map_sqlx_err)?,
        p50: r.try_get("p50").map_err(map_sqlx_err)?,
        p95: r.try_get("p95").map_err(map_sqlx_err)?,
        p99: r.try_get("p99").map_err(map_sqlx_err)?,
        max: r.try_get("max").map_err(map_sqlx_err)?,
    })
}

fn row_to_job(r: &sqlx::postgres::PgRow) -> Result<JobRecord> {
    let status_s: String = r.try_get("status").map_err(map_sqlx_err)?;
    let status = JobStatus::from_str(&status_s)
        .ok_or_else(|| StorageError::Backend(format!("unknown status {status_s:?}")))?;
    let history_v: serde_json::Value = r.try_get("error_history").map_err(map_sqlx_err)?;
    let error_history: Vec<ErrorHistoryEntry> =
        serde_json::from_value(history_v).unwrap_or_default();
    Ok(JobRecord {
        id: JobId::new(r.try_get::<String, _>("id").map_err(map_sqlx_err)?),
        queue_name: r.try_get("queue_name").map_err(map_sqlx_err)?,
        kind: r.try_get("kind").map_err(map_sqlx_err)?,
        payload: r.try_get("payload").map_err(map_sqlx_err)?,
        status,
        priority: r.try_get("priority").map_err(map_sqlx_err)?,
        enqueued_at: r.try_get("enqueued_at").map_err(map_sqlx_err)?,
        scheduled_at: r.try_get("scheduled_at").map_err(map_sqlx_err)?,
        started_at: r.try_get("started_at").map_err(map_sqlx_err)?,
        completed_at: r.try_get("completed_at").map_err(map_sqlx_err)?,
        attempts: r.try_get("attempts").map_err(map_sqlx_err)?,
        max_attempts: r.try_get("max_attempts").map_err(map_sqlx_err)?,
        throttle_attempts: r.try_get("throttle_attempts").map_err(map_sqlx_err)?,
        last_error: r.try_get("last_error").map_err(map_sqlx_err)?,
        error_history,
        process_id: r.try_get("process_id").map_err(map_sqlx_err)?,
        heartbeat_at: r.try_get("heartbeat_at").map_err(map_sqlx_err)?,
        dedupe_key: r.try_get("dedupe_key").map_err(map_sqlx_err)?,
    })
}

fn row_to_proc(r: &sqlx::postgres::PgRow) -> Result<ProcessRecord> {
    Ok(ProcessRecord {
        process_id: r.try_get("process_id").map_err(map_sqlx_err)?,
        queue_name: r.try_get("queue_name").map_err(map_sqlx_err)?,
        host_id: r.try_get("host_id").map_err(map_sqlx_err)?,
        started_at: r.try_get("started_at").map_err(map_sqlx_err)?,
        heartbeat_at: r.try_get("heartbeat_at").map_err(map_sqlx_err)?,
        current_job: r
            .try_get::<Option<String>, _>("current_job")
            .map_err(map_sqlx_err)?
            .map(JobId::new),
    })
}

fn row_to_queue_config(r: &sqlx::postgres::PgRow) -> Result<QueueConfigRow> {
    Ok(QueueConfigRow {
        name: r.try_get("name").map_err(map_sqlx_err)?,
        max_workers: r.try_get("max_workers").map_err(map_sqlx_err)?,
        paused: r.try_get("paused").map_err(map_sqlx_err)?,
        retain_done_for_days: r.try_get("retain_done_for_days").map_err(map_sqlx_err)?,
        retain_dead_for_days: r.try_get("retain_dead_for_days").map_err(map_sqlx_err)?,
        backoff_enabled: r.try_get("backoff_enabled").map_err(map_sqlx_err)?,
        backoff_base_seconds: r.try_get("backoff_base_seconds").map_err(map_sqlx_err)?,
        backoff_max_seconds: r.try_get("backoff_max_seconds").map_err(map_sqlx_err)?,
        throttle_attempts: r.try_get("throttle_attempts").map_err(map_sqlx_err)?,
        throttled_until: r.try_get("throttled_until").map_err(map_sqlx_err)?,
        updated_at: r.try_get("updated_at").map_err(map_sqlx_err)?,
    })
}

fn row_to_cron(r: &sqlx::postgres::PgRow) -> Result<CronScheduleRecord> {
    Ok(CronScheduleRecord {
        name: r.try_get("name").map_err(map_sqlx_err)?,
        kind: r.try_get("kind").map_err(map_sqlx_err)?,
        payload: r.try_get("payload").map_err(map_sqlx_err)?,
        queue_name: r.try_get("queue_name").map_err(map_sqlx_err)?,
        cron_expr: r.try_get("cron_expr").map_err(map_sqlx_err)?,
        enabled: r.try_get("enabled").map_err(map_sqlx_err)?,
        max_attempts: r.try_get("max_attempts").map_err(map_sqlx_err)?,
        last_fired_at: r.try_get("last_fired_at").map_err(map_sqlx_err)?,
        next_fire_at: r.try_get("next_fire_at").map_err(map_sqlx_err)?,
        last_error: r.try_get("last_error").map_err(map_sqlx_err)?,
        created_at: r.try_get("created_at").map_err(map_sqlx_err)?,
        updated_at: r.try_get("updated_at").map_err(map_sqlx_err)?,
    })
}

// ────────────────────────────────────────────────────────────────────
// RateLimitStorage — cluster-wide token-bucket budget.
// ────────────────────────────────────────────────────────────────────

#[async_trait]
impl RateLimitStorage for PostgresStorage {
    async fn acquire(&self, scope: &str) -> Result<RateLimitOutcome> {
        // Same token-bucket math as the SQLite twin, but the row
        // lock from `UPDATE … RETURNING` is what makes this work
        // across replicas — two pods racing the same last token
        // serialize on the row and only one walks away with it.
        let row = sqlx::query(
            r"UPDATE rate_limit_state
                 SET tokens = LEAST(
                       capacity::DOUBLE PRECISION,
                       tokens + GREATEST(0,
                         EXTRACT(EPOCH FROM (now() - last_refilled_at))
                       ) * refill_per_sec
                     ) - 1.0,
                     last_refilled_at = now()
               WHERE scope = $1
                 AND LEAST(
                       capacity::DOUBLE PRECISION,
                       tokens + GREATEST(0,
                         EXTRACT(EPOCH FROM (now() - last_refilled_at))
                       ) * refill_per_sec
                     ) >= 1.0
               RETURNING tokens",
        )
        .bind(scope)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(if row.is_some() {
            RateLimitOutcome::Granted
        } else {
            RateLimitOutcome::Throttled
        })
    }

    async fn drain(&self, scope: &str) -> Result<()> {
        sqlx::query(
            r"UPDATE rate_limit_state
                 SET tokens = 0.0,
                     last_refilled_at = now()
               WHERE scope = $1",
        )
        .bind(scope)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn ensure_default(&self, scope: &str, capacity: i64, refill_per_sec: f64) -> Result<()> {
        sqlx::query(
            r"INSERT INTO rate_limit_state
                (scope, tokens, capacity, refill_per_sec, last_refilled_at)
              VALUES ($1, $2::DOUBLE PRECISION, $2, $3, now())
              ON CONFLICT (scope) DO NOTHING",
        )
        .bind(scope)
        .bind(capacity)
        .bind(refill_per_sec)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }
}

/// Convert any `sqlx::Error` into our backend-agnostic `StorageError`.
fn map_sqlx_err(e: sqlx::Error) -> StorageError {
    use sqlx::Error as E;
    match e {
        E::RowNotFound => StorageError::NotFound("row not found".into()),
        E::Database(db) => {
            let code = db.code().unwrap_or_default();
            // Postgres SQLSTATE: 23505 unique_violation,
            // 40001 serialization_failure (MVCC abort — retry),
            // 40P01 deadlock_detected (retry),
            // 55P03 lock_not_available.
            if code == "23505" || code == "40001" || code == "40P01" || code == "55P03" {
                StorageError::Conflict(db.message().to_owned())
            } else {
                StorageError::Backend(format!("postgres [{code}]: {db}"))
            }
        }
        other => StorageError::Backend(other.to_string()),
    }
}
