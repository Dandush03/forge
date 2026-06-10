//! `JobQueue` impl on `SQLite`.
//!
//! Every method is one of: a single SQL statement, or one statement
//! wrapped in a short transaction. `SQLite`'s WAL journal makes the
//! single-writer constraint cheap (~ms hold) and `busy_timeout(5s)`
//! covers normal contention.
//!
//! `finalize` is the one exception: a 500-row `enqueue_bulk` from a
//! bootstrap can hold the writer lock past `busy_timeout`, and a
//! worker losing that race would otherwise abandon the row in
//! `in_progress` until the reaper sweeps. So `finalize` retries 3×
//! on transient conflicts (100ms / 300ms / 1s) before surfacing.
//!
//! Claim atomicity comes from `UPDATE … WHERE id = (SELECT … LIMIT 1)
//! AND status IN (…) RETURNING …` — a single statement, so the read
//! and write happen in one implicit transaction. No explicit
//! BEGIN/COMMIT block needed.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sqlx::{Row, Sqlite, Transaction};

use super::{READ_POOL_MAX, SqliteStorage, WRITE_POOL_MAX, map_sqlx_err};
use crate::storage::JobQueue;
use crate::storage::db_timing::OpTimer;
use crate::storage::error::{Result, StorageError};
use crate::storage::types::{
    EnqueueOutcome, EnqueueRequest, ErrorHistoryEntry, FinalizeOutcome, JobId, JobLatency,
    JobRecord, JobStatus, MetricBucket, QueueCounts, TimelineEvent, TimelineEventType, metric,
};

use crate::storage::ERROR_HISTORY_CAP;

const ACTIVE_STATUSES: &[&str] = &["pending", "in_progress"];
/// Default `max_attempts` used when `EnqueueRequest` doesn't set one.
const DEFAULT_MAX_ATTEMPTS: i32 = 5;

#[async_trait]
impl JobQueue for SqliteStorage {
    async fn enqueue(&self, req: EnqueueRequest) -> Result<EnqueueOutcome> {
        let _t = OpTimer::write(&self.db_recorder);
        let new_id = self.next_ulid().await.to_string();
        let mut tx = self.write_pool.begin().await.map_err(map_sqlx_err)?;
        let outcome = enqueue_in_tx(&mut tx, &req, &new_id).await?;
        tx.commit().await.map_err(map_sqlx_err)?;

        if matches!(outcome, EnqueueOutcome::Enqueued(_))
            && let Some(queue) = req.queue_name.as_deref()
        {
            self.notify.for_queue(queue).await.notify_one();
        }
        Ok(outcome)
    }

    async fn enqueue_bulk(&self, reqs: Vec<EnqueueRequest>) -> Result<Vec<EnqueueOutcome>> {
        let _t = OpTimer::write(&self.db_recorder);
        // Pre-mint one monotonic ULID per request so the entire bulk
        // batch is FIFO-ordered relative to the order the caller
        // assembled it. Minting inside the tx would still be
        // monotonic, but tying the ids to the request order outside
        // the tx is clearer.
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
        let mut tx = self.write_pool.begin().await.map_err(map_sqlx_err)?;
        let mut outcomes = Vec::with_capacity(reqs.len());
        let mut notify_queues: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (req, new_id) in reqs.iter().zip(new_ids.iter()) {
            let outcome = enqueue_in_tx(&mut tx, req, new_id).await?;
            if matches!(outcome, EnqueueOutcome::Enqueued(_))
                && let Some(q) = req.queue_name.as_deref()
            {
                notify_queues.insert(q.to_owned());
            }
            outcomes.push(outcome);
        }
        tx.commit().await.map_err(map_sqlx_err)?;

        for q in notify_queues {
            self.notify.for_queue(&q).await.notify_one();
        }
        Ok(outcomes)
    }

    async fn claim_next(&self, queue: &str, process_id: &str) -> Result<Option<JobRecord>> {
        let _t = OpTimer::write(&self.db_recorder);
        let now = Utc::now();
        let now_iso = iso(now);
        // Single atomic UPDATE — no enclosing tx unless we actually
        // claim something. Workers poll claim_next every 500 ms when
        // idle; wrapping every poll in BEGIN/COMMIT would burn one
        // empty transaction per worker per poll across the lifetime
        // of the process. The implicit per-statement tx on this
        // single-statement UPDATE is enough for atomicity.
        let row = sqlx::query(
            r"UPDATE sync_queue
                 SET status              = 'in_progress',
                     process_id          = ?1,
                     started_at          = ?2,
                     heartbeat_at        = ?2,
                     attempts            = attempts + 1,
                     -- Clear any stale cancel flag from a previous
                     -- in-progress life of this row (set by `delete`
                     -- and never observed before requeue). Future
                     -- cancels for this attempt set it from NULL.
                     cancel_requested_at = NULL
               WHERE id = (
                   SELECT id FROM sync_queue
                    WHERE queue_name   = ?3
                      AND status IN ('pending', 'failed')
                      AND scheduled_at <= ?2
                      -- Queue-wide throttle gate: while the queue is in
                      -- cool-down, hand out nothing so the whole fleet
                      -- backs off. NULL `throttled_until` (the common
                      -- case) makes the comparison NULL → not blocked.
                      AND NOT EXISTS (
                          SELECT 1 FROM queue q
                           WHERE q.name = ?3 AND q.throttled_until > ?2
                      )
                      -- Skip rows whose dedupe_key already has an
                      -- ACTIVE sibling. A claim of a `failed` row
                      -- flips it to `in_progress` (entering the
                      -- active dedupe index); if a sibling is already
                      -- pending/in_progress with the same key, the
                      -- UPDATE trips `jq_dedupe` and the worker
                      -- spins on the same row forever. NULL key is
                      -- always claimable.
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
               )
                 AND status IN ('pending', 'failed')
               RETURNING *",
        )
        .bind(process_id)
        .bind(&now_iso)
        .bind(queue)
        .fetch_optional(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;

        let Some(job) = row.as_ref().map(row_to_job).transpose()? else {
            return Ok(None);
        };
        // Record the `started` event as a separate statement. If we
        // crash between the UPDATE and this INSERT the chart loses
        // one start event (the reaper will revive the row and the
        // re-claim writes a new event then); acceptable since the
        // event log is for chart aggregates, not correctness.
        let _ = record_event(
            &self.write_pool,
            &now_iso,
            &job.kind,
            &job.queue_name,
            Some(job.id.as_str()),
            "started",
        )
        .await;
        Ok(Some(job))
    }

    async fn finalize(
        &self,
        job_id: &JobId,
        owner: Option<&str>,
        outcome: FinalizeOutcome,
    ) -> Result<()> {
        let _t = OpTimer::write(&self.db_recorder);
        // Retry on transient writer-lock conflicts. See the module
        // doc for the rationale.
        crate::storage::with_transient_retry("finalize", || {
            self.do_finalize(job_id, owner, outcome.clone())
        })
        .await
    }

    async fn heartbeat_job(&self, job_id: &JobId, process_id: &str) -> Result<bool> {
        let _t = OpTimer::write(&self.db_recorder);
        let now = iso(Utc::now());
        // UPDATE … RETURNING is supported in SQLite >= 3.35. Returns
        // 0 rows when the row vanished or the process_id no longer
        // owns it (re-claimed) — both treated as "no cancel."
        let row = sqlx::query(
            r"UPDATE sync_queue
                 SET heartbeat_at = ?1
               WHERE id = ?2 AND process_id = ?3
               RETURNING cancel_requested_at IS NOT NULL AS cancel_requested",
        )
        .bind(&now)
        .bind(job_id.as_str())
        .bind(process_id)
        .fetch_optional(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        let cancel_requested = row.as_ref().is_some_and(|r| {
            r.try_get::<i64, _>("cancel_requested")
                .is_ok_and(|n| n != 0)
        });
        Ok(cancel_requested)
    }

    async fn revive_stale(&self, stale_before: DateTime<Utc>) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        // Find stale in-flight rows + the owning queue's backoff
        // config in one shot, then revive each via the Failed outcome
        // so the per-queue backoff toggle applies. LEFT JOIN +
        // COALESCE: if the queue row vanished, fall back to off / 60
        // / 1800 (same legacy defaults `map_outcome` uses when the
        // queue config read fails).
        let rows = sqlx::query(
            r"SELECT j.id, j.attempts, j.max_attempts,
                     COALESCE(q.backoff_enabled, 0)         AS backoff_enabled,
                     COALESCE(q.backoff_base_seconds, 60)   AS backoff_base_seconds,
                     COALESCE(q.backoff_max_seconds, 1800)  AS backoff_max_seconds
                FROM sync_queue j
                LEFT JOIN queue q ON q.name = j.queue_name
               WHERE j.status = 'in_progress' AND j.heartbeat_at < ?1",
        )
        .bind(iso(stale_before))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx_err)?;

        let stale_iso = iso(stale_before);
        let mut revived = 0u64;
        for r in rows {
            let id: String = r.try_get("id").map_err(map_sqlx_err)?;
            let attempts: i64 = r.try_get("attempts").map_err(map_sqlx_err)?;
            let max_attempts: i64 = r.try_get("max_attempts").map_err(map_sqlx_err)?;
            let backoff_enabled: i64 = r.try_get("backoff_enabled").map_err(map_sqlx_err)?;
            let backoff_base: i64 = r.try_get("backoff_base_seconds").map_err(map_sqlx_err)?;
            let backoff_max: i64 = r.try_get("backoff_max_seconds").map_err(map_sqlx_err)?;
            let job_id = JobId::new(id);
            let terminal = attempts >= max_attempts;
            let now = iso(Utc::now());
            if terminal {
                append_error_and_update(
                    &self.write_pool,
                    &job_id,
                    &now,
                    "reaped after stale heartbeat",
                    /* terminal: */ true,
                    None,
                    Some(&stale_iso),
                    /* guard_owner: */ None,
                )
                .await?;
            } else {
                let delay = crate::runtime::failed_delay(
                    i32::try_from(attempts).unwrap_or(0),
                    backoff_enabled != 0,
                    i32::try_from(backoff_base).unwrap_or(60),
                    i32::try_from(backoff_max).unwrap_or(1800),
                );
                let next = iso(Utc::now() + ChronoDuration::from_std(delay).unwrap_or_default());
                append_error_and_update(
                    &self.write_pool,
                    &job_id,
                    &now,
                    "reaped after stale heartbeat",
                    /* terminal: */ false,
                    Some(&next),
                    Some(&stale_iso),
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
        // Cascade-delete events of the rows we're about to drop so
        // the chart's running gauge doesn't carry ghost `started`s.
        // Order matters: the events DELETE's subquery needs the rows
        // to still exist.
        let threshold_s = iso(threshold);
        let mut tx = self.write_pool.begin().await.map_err(map_sqlx_err)?;
        sqlx::query(
            r"DELETE FROM queue_event
               WHERE job_id IN (
                     SELECT id FROM sync_queue
                      WHERE queue_name   = ?1
                        AND status       = ?2
                        AND completed_at IS NOT NULL
                        AND completed_at < ?3
                   )",
        )
        .bind(queue)
        .bind(status.as_str())
        .bind(&threshold_s)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;
        let res = sqlx::query(
            r"DELETE FROM sync_queue
               WHERE queue_name   = ?1
                 AND status       = ?2
                 AND completed_at IS NOT NULL
                 AND completed_at < ?3",
        )
        .bind(queue)
        .bind(status.as_str())
        .bind(&threshold_s)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;
        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(res.rows_affected())
    }

    async fn get_job(&self, job_id: &JobId) -> Result<Option<JobRecord>> {
        let _t = OpTimer::read(&self.db_recorder);
        let row = sqlx::query("SELECT * FROM sync_queue WHERE id = ?1")
            .bind(job_id.as_str())
            .fetch_optional(&self.read_pool)
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
                  WHERE queue_name = ?1 AND status = ?2
                  ORDER BY enqueued_at DESC LIMIT ?3",
            )
            .bind(q)
            .bind(status.as_str())
            .bind(limit_i)
            .fetch_all(&self.read_pool)
            .await
        } else {
            sqlx::query(
                r"SELECT * FROM sync_queue
                  WHERE status = ?1
                  ORDER BY enqueued_at DESC LIMIT ?2",
            )
            .bind(status.as_str())
            .bind(limit_i)
            .fetch_all(&self.read_pool)
            .await
        }
        .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_job).collect()
    }

    async fn count_by_status(&self, queue: &str) -> Result<QueueCounts> {
        let _t = OpTimer::read(&self.db_recorder);
        // Conditional aggregation in one round-trip. Splits pending
        // into "ready-now" (pending) and "deferred" (scheduled),
        // matching the Scheduled tab's predicate.
        let now_iso = iso(Utc::now());
        let row = sqlx::query(
            r"SELECT
                SUM(CASE WHEN status='pending'     AND scheduled_at <= ?1 THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status='pending'     AND scheduled_at >  ?1 THEN 1 ELSE 0 END) AS scheduled,
                SUM(CASE WHEN status='in_progress'                        THEN 1 ELSE 0 END) AS in_progress,
                SUM(CASE WHEN status='done'                               THEN 1 ELSE 0 END) AS done,
                SUM(CASE WHEN status='failed'                             THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status='dead'                               THEN 1 ELSE 0 END) AS dead
              FROM sync_queue
              WHERE queue_name = ?2",
        )
        .bind(&now_iso)
        .bind(queue)
        .fetch_one(&self.read_pool)
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
        let now_iso = iso(Utc::now());
        let row = sqlx::query(
            r"SELECT MIN(scheduled_at) AS oldest FROM sync_queue
              WHERE queue_name = ?1 AND status = 'pending' AND scheduled_at <= ?2",
        )
        .bind(queue)
        .bind(&now_iso)
        .fetch_one(&self.read_pool)
        .await
        .map_err(map_sqlx_err)?;
        let raw: Option<String> = row.try_get("oldest").map_err(map_sqlx_err)?;
        raw.map(|s| {
            DateTime::parse_from_rfc3339(&s)
                .map(|d| d.with_timezone(&Utc))
                .map_err(|e| StorageError::Backend(format!("bad datetime {s:?}: {e}")))
        })
        .transpose()
    }

    async fn completed_latencies(
        &self,
        queue: Option<&str>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<JobLatency>> {
        let _t = OpTimer::read(&self.db_recorder);
        let from_iso = iso(from);
        let to_iso = iso(to);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let base = "SELECT completed_at, started_at, enqueued_at FROM sync_queue
                     WHERE status = 'done' AND completed_at IS NOT NULL
                       AND started_at IS NOT NULL
                       AND completed_at >= ?1 AND completed_at <= ?2";
        let rows = if let Some(q) = queue {
            sqlx::query(&format!(
                "{base} AND queue_name = ?3 ORDER BY completed_at DESC LIMIT ?4"
            ))
            .bind(&from_iso)
            .bind(&to_iso)
            .bind(q)
            .bind(limit)
            .fetch_all(&self.read_pool)
            .await
        } else {
            sqlx::query(&format!("{base} ORDER BY completed_at DESC LIMIT ?3"))
                .bind(&from_iso)
                .bind(&to_iso)
                .bind(limit)
                .fetch_all(&self.read_pool)
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
        let mut tx = self.write_pool.begin().await.map_err(map_sqlx_err)?;
        for row in rows {
            sqlx::query(
                "INSERT INTO metric_bucket
                     (queue, metric, bucket_start, count, sum, p50, p95, p99, max)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(queue, metric, bucket_start) DO UPDATE SET
                     count = excluded.count,
                     sum   = excluded.sum,
                     p50   = excluded.p50,
                     p95   = excluded.p95,
                     p99   = excluded.p99,
                     max   = excluded.max",
            )
            .bind(&row.queue)
            .bind(&row.metric)
            .bind(iso(row.bucket_start))
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
        // ?1/?2 are from/to; the metric IN-list starts at ?3; the
        // optional queue filter binds last.
        let metric_ph = (0..metrics.len())
            .map(|i| format!("?{}", i + 3))
            .collect::<Vec<_>>()
            .join(", ");
        let queue_clause = if queue.is_some() {
            format!(" AND queue = ?{}", metrics.len() + 3)
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT queue, metric, bucket_start, count, sum, p50, p95, p99, max
               FROM metric_bucket
              WHERE bucket_start >= ?1 AND bucket_start <= ?2
                AND metric IN ({metric_ph}){queue_clause}
              ORDER BY bucket_start ASC"
        );

        let mut q = sqlx::query(&sql).bind(iso(from)).bind(iso(to));
        for m in metrics {
            q = q.bind(*m);
        }
        if let Some(qn) = queue {
            q = q.bind(qn);
        }
        let rows = q.fetch_all(&self.read_pool).await.map_err(map_sqlx_err)?;
        rows.iter().map(row_to_metric).collect()
    }

    async fn delete_metric_buckets_before(&self, before: DateTime<Utc>) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        let res = sqlx::query("DELETE FROM metric_bucket WHERE bucket_start < ?1")
            .bind(iso(before))
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(res.rows_affected())
    }

    async fn distinct_kinds(&self, queue: Option<&str>) -> Result<Vec<String>> {
        let _t = OpTimer::read(&self.db_recorder);
        let rows = sqlx::query(
            r"SELECT DISTINCT kind FROM sync_queue
               WHERE (?1 IS NULL OR queue_name = ?1)
               ORDER BY kind ASC",
        )
        .bind(queue)
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx_err)?;
        rows.into_iter()
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
            r"SELECT at, kind, queue_name, event_type
                FROM queue_event
               WHERE at >= ?1 AND at < ?2
               ORDER BY at ASC",
        )
        .bind(iso(from))
        .bind(iso(to))
        .fetch_all(&self.read_pool)
        .await
        .map_err(map_sqlx_err)?;

        rows.into_iter()
            .map(|r| {
                let at_s: String = r.try_get("at").map_err(map_sqlx_err)?;
                let event_s: String = r.try_get("event_type").map_err(map_sqlx_err)?;
                let event_type = TimelineEventType::from_str(&event_s).ok_or_else(|| {
                    StorageError::Backend(format!("unknown event_type: {event_s}"))
                })?;
                Ok(TimelineEvent {
                    at: parse_dt(&at_s)?,
                    kind: r.try_get("kind").map_err(map_sqlx_err)?,
                    queue_name: r.try_get("queue_name").map_err(map_sqlx_err)?,
                    event_type,
                })
            })
            .collect()
    }

    async fn delete(&self, job_id: &JobId) -> Result<bool> {
        let _t = OpTimer::write(&self.db_recorder);
        let now_iso = iso(Utc::now());
        let mut tx = self.write_pool.begin().await.map_err(map_sqlx_err)?;
        // First try the cancel path: set the flag on an in-progress
        // row. RETURNING tells us if we hit one. The worker's
        // heartbeat will observe `cancel_requested_at` and signal the
        // in-process cancel token; the row stays until finalize.
        let cancel_row = sqlx::query(
            r"UPDATE sync_queue
                 SET cancel_requested_at = ?1
               WHERE id = ?2 AND status = 'in_progress'
               RETURNING id",
        )
        .bind(&now_iso)
        .bind(job_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;
        if cancel_row.is_some() {
            tx.commit().await.map_err(map_sqlx_err)?;
            return Ok(true);
        }
        // Otherwise it's a terminal/pending row — cascade-delete the
        // timeline events first so a crash can't leave orphan events
        // that would skew the chart's gauge.
        sqlx::query("DELETE FROM queue_event WHERE job_id = ?1")
            .bind(job_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
        let res = sqlx::query("DELETE FROM sync_queue WHERE id = ?1")
            .bind(job_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(res.rows_affected() > 0)
    }

    async fn requeue(&self, job_id: &JobId) -> Result<bool> {
        let _t = OpTimer::write(&self.db_recorder);
        let now = iso(Utc::now());
        let res = sqlx::query(
            // OR IGNORE: skip if this row's dedupe_key already has an active
            // sibling (would trip `jq_dedupe`); returns 0 rows changed rather
            // than erroring, so the caller sees "not requeued".
            r"UPDATE OR IGNORE sync_queue
                 SET status       = 'pending',
                     scheduled_at = ?1,
                     completed_at = NULL,
                     process_id   = NULL,
                     heartbeat_at = NULL
               WHERE id = ?2 AND status IN ('failed', 'dead')",
        )
        .bind(&now)
        .bind(job_id.as_str())
        .execute(&self.write_pool)
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
        // Pick the next batch of victim ids (FIFO by id). Materializing
        // into a temp table isn't worth it for these sizes; SQLite
        // optimizes IN (subquery) cleanly.
        crate::storage::with_transient_retry("delete_batch_by_status", || async {
            let mut tx = self.write_pool.begin().await.map_err(map_sqlx_err)?;
            sqlx::query(
                r"DELETE FROM queue_event
                   WHERE job_id IN (
                       SELECT id FROM sync_queue
                        WHERE status = ?1
                          AND (?2 IS NULL OR queue_name = ?2)
                        ORDER BY id ASC
                        LIMIT ?3
                   )",
            )
            .bind(status.as_str())
            .bind(queue)
            .bind(batch_i)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
            let res = sqlx::query(
                r"DELETE FROM sync_queue
                   WHERE id IN (
                       SELECT id FROM sync_queue
                        WHERE status = ?1
                          AND (?2 IS NULL OR queue_name = ?2)
                        ORDER BY id ASC
                        LIMIT ?3
                   )",
            )
            .bind(status.as_str())
            .bind(queue)
            .bind(batch_i)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
            tx.commit().await.map_err(map_sqlx_err)?;
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
            let now = iso(Utc::now());
            let res = sqlx::query(
                // OR IGNORE: a requeued row whose dedupe_key already has an
                // active (pending/in_progress) sibling would trip the
                // `jq_dedupe` UNIQUE index. Skip just those rows instead of
                // aborting the whole batch — the active sibling already
                // covers that work.
                r"UPDATE OR IGNORE sync_queue
                     SET status       = 'pending',
                         scheduled_at = ?1,
                         completed_at = NULL,
                         process_id   = NULL,
                         heartbeat_at = NULL
                   WHERE id IN (
                       SELECT id FROM sync_queue
                        WHERE status = ?2
                          AND (?3 IS NULL OR queue_name = ?3)
                        ORDER BY id ASC
                        LIMIT ?4
                   )",
            )
            .bind(&now)
            .bind(status.as_str())
            .bind(queue)
            .bind(batch_i)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
            Ok(res.rows_affected())
        })
        .await
    }

    async fn cleanup_superseded_retries(&self) -> Result<u64> {
        let _t = OpTimer::write(&self.db_recorder);
        // Failed retries whose dedupe_key already has an active sibling are
        // redundant — the sibling does the work — and would otherwise trip
        // the `jq_dedupe` UNIQUE index on every claim, looping the worker.
        // Mark them dead with a reason so they surface in the Dead tab.
        let now = iso(Utc::now());
        let res = sqlx::query(
            r"UPDATE sync_queue
                 SET status       = 'dead',
                     completed_at = ?1,
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
        .bind(&now)
        .execute(&self.write_pool)
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
        let now_iso = iso(now);
        let rows = if let Some(q) = queue {
            sqlx::query(
                r"SELECT * FROM sync_queue
                   WHERE status = 'pending'
                     AND scheduled_at > ?1
                     AND queue_name = ?2
                   ORDER BY scheduled_at ASC, id ASC
                   LIMIT ?3",
            )
            .bind(&now_iso)
            .bind(q)
            .bind(limit_i)
            .fetch_all(&self.read_pool)
            .await
        } else {
            sqlx::query(
                r"SELECT * FROM sync_queue
                   WHERE status = 'pending'
                     AND scheduled_at > ?1
                   ORDER BY scheduled_at ASC, id ASC
                   LIMIT ?2",
            )
            .bind(&now_iso)
            .bind(limit_i)
            .fetch_all(&self.read_pool)
            .await
        }
        .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_job).collect()
    }

    async fn run_now(&self, job_id: &JobId) -> Result<bool> {
        let _t = OpTimer::write(&self.db_recorder);
        let now = iso(Utc::now());
        // Only act on pending rows. failed / dead use `requeue`;
        // in_progress / done / dead are not eligible.
        let res = sqlx::query(
            r"UPDATE sync_queue
                 SET scheduled_at = ?1
               WHERE id = ?2 AND status = 'pending'",
        )
        .bind(&now)
        .bind(job_id.as_str())
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected() > 0)
    }

    async fn wait_for_work(&self, queue: &str, timeout: Duration) -> Result<bool> {
        let notify = self.notify.for_queue(queue).await;
        tokio::select! {
            () = notify.notified() => Ok(true),
            () = tokio::time::sleep(timeout) => Ok(false),
        }
    }

    async fn notify(&self, queue: &str) -> Result<()> {
        self.notify.for_queue(queue).await.notify_one();
        Ok(())
    }

    async fn describe(&self) -> Result<crate::storage::StorageInfo> {
        let _t = OpTimer::read(&self.db_recorder);
        let sqlite_version: String = sqlx::query_scalar("SELECT sqlite_version()")
            .fetch_one(&self.read_pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(crate::storage::StorageInfo {
            backend: "sqlite".to_owned(),
            fields: vec![
                ("sqlite_version".to_owned(), sqlite_version),
                ("journal_mode".to_owned(), "wal".to_owned()),
                ("busy_timeout_secs".to_owned(), "30".to_owned()),
                ("write_pool_max".to_owned(), WRITE_POOL_MAX.to_string()),
                ("read_pool_max".to_owned(), READ_POOL_MAX.to_string()),
            ],
        })
    }

    fn drain_op_samples(&self) -> crate::storage::db_timing::DrainedSamples {
        self.db_recorder.drain()
    }

    async fn db_health_snapshot(&self) -> Vec<(&'static str, f64)> {
        // SQLite has no server-side connection model — `pool_active`
        // here would just reflect sqlx's in-process accounting between
        // sub-ms ops and read zero. Instead, surface what the DB
        // *itself* can answer: data file + WAL sidecar sizes.
        let mut out = Vec::with_capacity(2);
        match query_db_size(&self.read_pool).await {
            Ok(bytes) => out.push((metric::DB_SIZE_BYTES, bytes)),
            Err(e) => tracing::debug!(?e, "sqlite db_health: page_count query failed"),
        }
        if let Some(path) = self.db_path.as_deref()
            && let Some(bytes) = wal_file_bytes(path)
        {
            out.push((metric::DB_WAL_BYTES, bytes));
        }
        out
    }
}

/// Stat the `-wal` sidecar next to the DB file. Returns `None` for
/// in-memory storages, when WAL hasn't been created yet, or when the
/// stat fails — every case the caller treats the same (skip the row).
fn wal_file_bytes(db_path: &std::path::Path) -> Option<f64> {
    let mut wal = db_path.as_os_str().to_owned();
    wal.push("-wal");
    let len = std::fs::metadata(std::path::Path::new(&wal)).ok()?.len();
    #[allow(
        clippy::cast_precision_loss,
        reason = "file sizes well within f64's exact integer range"
    )]
    Some(len as f64)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "page_count * page_size for a job DB fits f64 exactly past any practical size"
)]
async fn query_db_size(pool: &sqlx::SqlitePool) -> Result<f64> {
    // PRAGMA returns scalars; sqlx wants the column name back via Row.
    let page_count: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_err)?;
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_err)?;
    Ok((page_count.max(0) * page_size.max(0)) as f64)
}

impl SqliteStorage {
    /// One attempt at the finalize transitions. `finalize` wraps this
    /// in a retry loop so transient writer-lock conflicts (from a
    /// bulk enqueue holding the lock past `busy_timeout`) don't leave
    /// the row stuck in `in_progress`.
    async fn do_finalize(
        &self,
        job_id: &JobId,
        owner: Option<&str>,
        outcome: FinalizeOutcome,
    ) -> Result<()> {
        let now = iso(Utc::now());
        match outcome {
            FinalizeOutcome::Done => self.finalize_done(job_id, owner, &now).await,
            FinalizeOutcome::Throttled {
                retry_after,
                cool_down_queue,
            } => {
                self.finalize_throttled(job_id, owner, retry_after, cool_down_queue, &now)
                    .await
            }
            FinalizeOutcome::Failed {
                retry_after,
                message,
            } => {
                let next = iso(Utc::now()
                    + chrono::Duration::from_std(retry_after)
                        .unwrap_or_else(|_| chrono::Duration::seconds(60)));
                append_error_and_update(
                    &self.write_pool,
                    job_id,
                    &now,
                    &message,
                    /* terminal: */ false,
                    Some(&next),
                    /* guard_stale_before: */ None,
                    /* guard_owner: */ owner,
                )
                .await
            }
            FinalizeOutcome::Dead { message } => {
                append_error_and_update(
                    &self.write_pool,
                    job_id,
                    &now,
                    &message,
                    /* terminal: */ true,
                    None,
                    /* guard_stale_before: */ None,
                    /* guard_owner: */ owner,
                )
                .await
            }
        }
    }

    /// `Done` finalize: mark the row done, log a `completed` event, and
    /// clear any queue-wide throttle cool-down (a success means the
    /// rate-limit window passed). All in one tx so a crash can't leave
    /// the event without the row transition.
    async fn finalize_done(&self, job_id: &JobId, owner: Option<&str>, now: &str) -> Result<()> {
        let mut tx = self.write_pool.begin().await.map_err(map_sqlx_err)?;
        // Ownership guard (H1): when `owner` is set, only transition a row
        // still `in_progress` and still owned by this process. A reaped +
        // re-claimed row fails the guard → 0 rows → clean no-op (no event,
        // no cool-down clear). `None` skips the guard (admin/test paths).
        let guard = owner.map_or("", |_| " AND process_id = ?3 AND status = 'in_progress'");
        let sql = format!(
            "UPDATE sync_queue
                 SET status            = 'done',
                     completed_at      = ?1,
                     throttle_attempts = 0,
                     process_id        = NULL,
                     heartbeat_at      = NULL
               WHERE id = ?2{guard}
               RETURNING kind, queue_name"
        );
        let mut q = sqlx::query(&sql).bind(now).bind(job_id.as_str());
        if let Some(pid) = owner {
            q = q.bind(pid);
        }
        let row = q.fetch_optional(&mut *tx).await.map_err(map_sqlx_err)?;
        if let Some(r) = row {
            let kind: String = r.try_get("kind").map_err(map_sqlx_err)?;
            let queue_name: String = r.try_get("queue_name").map_err(map_sqlx_err)?;
            record_event(
                &mut *tx,
                now,
                &kind,
                &queue_name,
                Some(job_id.as_str()),
                "completed",
            )
            .await?;
            clear_queue_cooldown(&mut *tx, &queue_name, now).await?;
        }
        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(())
    }

    /// `Throttled` finalize: re-queue the row (without burning a retry)
    /// and log a `retried` event. When `cool_down_queue` is set, also
    /// extend the queue-wide cool-down so every worker backs off. One tx
    /// so the running gauge stays balanced even on a mid-finalize crash.
    async fn finalize_throttled(
        &self,
        job_id: &JobId,
        owner: Option<&str>,
        retry_after: Duration,
        cool_down_queue: bool,
        now: &str,
    ) -> Result<()> {
        let next = iso(Utc::now()
            + chrono::Duration::from_std(retry_after)
                .unwrap_or_else(|_| chrono::Duration::seconds(60)));
        let mut tx = self.write_pool.begin().await.map_err(map_sqlx_err)?;
        // Ownership guard (H1) — see `finalize_done`. A stale worker's
        // throttle finalize must not re-queue a row another worker now
        // owns (that would let the same job run on 2+ workers).
        let guard = owner.map_or("", |_| " AND process_id = ?3 AND status = 'in_progress'");
        let sql = format!(
            "UPDATE sync_queue
                 SET status            = 'pending',
                     scheduled_at      = ?1,
                     attempts          = MAX(attempts - 1, 0),
                     throttle_attempts = throttle_attempts + 1,
                     process_id        = NULL,
                     heartbeat_at      = NULL
               WHERE id = ?2{guard}
               RETURNING kind, queue_name"
        );
        let mut q = sqlx::query(&sql).bind(&next).bind(job_id.as_str());
        if let Some(pid) = owner {
            q = q.bind(pid);
        }
        let row = q.fetch_optional(&mut *tx).await.map_err(map_sqlx_err)?;
        if let Some(r) = row {
            let kind: String = r.try_get("kind").map_err(map_sqlx_err)?;
            let queue_name: String = r.try_get("queue_name").map_err(map_sqlx_err)?;
            record_event(
                &mut *tx,
                now,
                &kind,
                &queue_name,
                Some(job_id.as_str()),
                "retried",
            )
            .await?;
            if cool_down_queue {
                extend_queue_cooldown(&mut *tx, &queue_name, &next, now).await?;
            }
        }
        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(())
    }
}

// ────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────

/// Format a `DateTime` as RFC3339 — SQLite-friendly ISO string.
/// Map a `(completed_at, started_at, enqueued_at)` row to a latency
/// sample. All three are RFC3339 text; the query guarantees they're set.
fn row_to_latency(r: &sqlx::sqlite::SqliteRow) -> Result<JobLatency> {
    let parse = |col: &str| -> Result<DateTime<Utc>> {
        let s: String = r.try_get(col).map_err(map_sqlx_err)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|d| d.with_timezone(&Utc))
            .map_err(|e| StorageError::Backend(format!("bad datetime {s:?}: {e}")))
    };
    let completed_at = parse("completed_at")?;
    let started_at = parse("started_at")?;
    let enqueued_at = parse("enqueued_at")?;
    Ok(JobLatency {
        completed_at,
        processing_ms: (completed_at - started_at).num_milliseconds(),
        total_ms: (completed_at - enqueued_at).num_milliseconds(),
    })
}

fn iso(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn row_to_metric(r: &sqlx::sqlite::SqliteRow) -> Result<MetricBucket> {
    let bucket_s: String = r.try_get("bucket_start").map_err(map_sqlx_err)?;
    let bucket_start = DateTime::parse_from_rfc3339(&bucket_s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| StorageError::Backend(format!("bad datetime {bucket_s:?}: {e}")))?;
    Ok(MetricBucket {
        queue: r.try_get("queue").map_err(map_sqlx_err)?,
        metric: r.try_get("metric").map_err(map_sqlx_err)?,
        bucket_start,
        count: r.try_get("count").map_err(map_sqlx_err)?,
        sum: r.try_get("sum").map_err(map_sqlx_err)?,
        p50: r.try_get("p50").map_err(map_sqlx_err)?,
        p95: r.try_get("p95").map_err(map_sqlx_err)?,
        p99: r.try_get("p99").map_err(map_sqlx_err)?,
        max: r.try_get("max").map_err(map_sqlx_err)?,
    })
}

/// Map a row from `sync_queue` to a `JobRecord`.
fn row_to_job(r: &sqlx::sqlite::SqliteRow) -> Result<JobRecord> {
    let id: String = r.try_get("id").map_err(map_sqlx_err)?;
    let status_s: String = r.try_get("status").map_err(map_sqlx_err)?;
    let status = JobStatus::from_str(&status_s)
        .ok_or_else(|| StorageError::Backend(format!("unknown status: {status_s}")))?;
    let payload_s: String = r.try_get("payload").map_err(map_sqlx_err)?;
    let payload: serde_json::Value =
        serde_json::from_str(&payload_s).unwrap_or(serde_json::Value::Null);
    let error_history_s: String = r.try_get("error_history").map_err(map_sqlx_err)?;
    let error_history: Vec<ErrorHistoryEntry> =
        serde_json::from_str(&error_history_s).unwrap_or_default();

    Ok(JobRecord {
        id: JobId::new(id),
        queue_name: r.try_get("queue_name").map_err(map_sqlx_err)?,
        kind: r.try_get("kind").map_err(map_sqlx_err)?,
        payload,
        status,
        priority: r
            .try_get::<i64, _>("priority")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(0),
        enqueued_at: parse_dt(
            &r.try_get::<String, _>("enqueued_at")
                .map_err(map_sqlx_err)?,
        )?,
        scheduled_at: parse_dt(
            &r.try_get::<String, _>("scheduled_at")
                .map_err(map_sqlx_err)?,
        )?,
        started_at: r
            .try_get::<Option<String>, _>("started_at")
            .map_err(map_sqlx_err)?
            .as_deref()
            .map(parse_dt)
            .transpose()?,
        completed_at: r
            .try_get::<Option<String>, _>("completed_at")
            .map_err(map_sqlx_err)?
            .as_deref()
            .map(parse_dt)
            .transpose()?,
        attempts: r
            .try_get::<i64, _>("attempts")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(0),
        max_attempts: r
            .try_get::<i64, _>("max_attempts")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(5),
        throttle_attempts: r
            .try_get::<i64, _>("throttle_attempts")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(0),
        last_error: r.try_get("last_error").map_err(map_sqlx_err)?,
        error_history,
        process_id: r.try_get("process_id").map_err(map_sqlx_err)?,
        heartbeat_at: r
            .try_get::<Option<String>, _>("heartbeat_at")
            .map_err(map_sqlx_err)?
            .as_deref()
            .map(parse_dt)
            .transpose()?,
        dedupe_key: r.try_get("dedupe_key").map_err(map_sqlx_err)?,
    })
}

fn parse_dt(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| StorageError::Backend(format!("bad datetime {s:?}: {e}")))
}

/// Shared dedupe + insert path used by both `enqueue` and `enqueue_bulk`.
/// The id of the active (`pending` / `in_progress`) row holding `key`,
/// if any. Matches the `jq_dedupe` partial-index predicate.
async fn active_dedupe_id(tx: &mut Transaction<'_, Sqlite>, key: &str) -> Result<Option<JobId>> {
    let active_placeholders = active_status_placeholders();
    let query = format!(
        "SELECT id FROM sync_queue WHERE dedupe_key = ? AND status IN ({active_placeholders}) LIMIT 1"
    );
    let mut q = sqlx::query(&query).bind(key);
    for s in ACTIVE_STATUSES {
        q = q.bind(*s);
    }
    let row = q.fetch_optional(&mut **tx).await.map_err(map_sqlx_err)?;
    row.map(|r| r.try_get::<String, _>("id").map_err(map_sqlx_err))
        .transpose()
        .map(|opt| opt.map(JobId::new))
}

async fn enqueue_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    req: &EnqueueRequest,
    new_id: &str,
) -> Result<EnqueueOutcome> {
    let queue = req
        .queue_name
        .as_deref()
        .ok_or_else(|| StorageError::InvalidInput("enqueue: queue_name required".into()))?;

    // Dedupe fast-path: skip the insert if an active row already holds
    // the key. The UNIQUE index backstops the race; this avoids a doomed
    // insert in the common (non-racing) case.
    if let Some(key) = &req.dedupe_key
        && let Some(existing) = active_dedupe_id(tx, key).await?
    {
        return Ok(EnqueueOutcome::Deduped(existing));
    }

    let id = new_id.to_owned();
    let now = iso(Utc::now());
    let scheduled = iso(req.run_at.unwrap_or_else(Utc::now));
    let payload_s = serde_json::to_string(&req.payload)?;
    let max_attempts = req.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);

    // ON CONFLICT backstops the SELECT pre-check above: if a concurrent
    // enqueue inserted the same active dedupe_key after our check, the
    // UNIQUE partial index (jq_dedupe) makes this a no-op insert. A NULL
    // dedupe_key isn't in the index, so it never conflicts.
    let inserted: Option<String> = sqlx::query_scalar(
        r"INSERT INTO sync_queue (
              id, queue_name, kind, payload, status, priority,
              enqueued_at, scheduled_at, attempts, max_attempts,
              error_history, dedupe_key
          ) VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?6, ?7, 0, ?8, '[]', ?9)
          ON CONFLICT (dedupe_key)
              WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'in_progress')
          DO NOTHING
          RETURNING id",
    )
    .bind(&id)
    .bind(queue)
    .bind(req.kind.as_ref())
    .bind(&payload_s)
    .bind(i64::from(req.priority))
    .bind(&now)
    .bind(&scheduled)
    .bind(i64::from(max_attempts))
    .bind(req.dedupe_key.as_deref())
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_err)?;

    if inserted.is_none() {
        // Lost the dedupe race — the active row is whoever won. Return
        // it as Deduped rather than failing the enqueue.
        if let Some(key) = &req.dedupe_key {
            let active = active_dedupe_id(tx, key).await?;
            if let Some(existing) = active {
                return Ok(EnqueueOutcome::Deduped(existing));
            }
        }
        // No dedupe_key but still no insert shouldn't happen; surface it.
        return Err(StorageError::Backend(
            "enqueue: insert affected no rows".into(),
        ));
    }

    // Append-only timeline log. Same transaction so a crashed enqueue
    // doesn't leave the event without a job row.
    record_event(
        &mut **tx,
        &now,
        req.kind.as_ref(),
        queue,
        Some(&id),
        "enqueued",
    )
    .await?;

    Ok(EnqueueOutcome::Enqueued(JobId::new(id)))
}

/// Append one row to the `queue_event` log. Caller passes the
/// connection (transaction or pool) and the ISO timestamp so the
/// caller controls whether the write is in a tx with sibling
/// statements. `job_id` is `None` only for legacy callers — the
/// `Some` path lets purge / `cleanup_aged` cascade-delete events when
/// the matching `sync_queue` row is deleted.
async fn record_event<'e, E>(
    executor: E,
    at_iso: &str,
    kind: &str,
    queue_name: &str,
    job_id: Option<&str>,
    event_type: &str,
) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query(
        r"INSERT INTO queue_event (at, kind, queue_name, event_type, job_id)
          VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind(at_iso)
    .bind(kind)
    .bind(queue_name)
    .bind(event_type)
    .bind(job_id)
    .execute(executor)
    .await
    .map_err(map_sqlx_err)?;
    Ok(())
}

/// Open (or extend) the queue-wide throttle cool-down for one limiter
/// event. The `throttled_until <= now` guard means only the *first*
/// throttle in a window bumps the counter + sets the deadline; later
/// throttles by sibling workers reacting to the *same* rate-limit hit
/// land inside the live window and become no-ops — so the exponent
/// counts consecutive limiter *events*, not workers-in-flight, and
/// can't be over-counted by concurrent finalizes (the row-locked
/// `UPDATE … WHERE` is a CAS on both backends). The counter is clamped
/// so a long outage can't overflow it.
async fn extend_queue_cooldown<'e, E>(
    executor: E,
    queue_name: &str,
    until_iso: &str,
    now_iso: &str,
) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query(
        r"UPDATE queue
             SET throttle_attempts = MIN(throttle_attempts + 1, 30),
                 throttled_until   = ?1,
                 updated_at        = ?2
           WHERE name = ?3
             AND (throttled_until IS NULL OR throttled_until <= ?4)",
    )
    .bind(until_iso)
    .bind(now_iso)
    .bind(queue_name)
    .bind(now_iso)
    .execute(executor)
    .await
    .map_err(map_sqlx_err)?;
    Ok(())
}

/// Clear the queue-wide throttle cool-down after a job succeeds — but
/// only once the window elapsed **and stayed quiet** for the throttle
/// decay grace (`throttled_until <= now - grace`). A
/// success from a job that was already in-flight when the limit hit (it
/// fetched its data before the 429) does NOT prove the window passed,
/// so clearing on it would reopen the gate straight into the still-
/// active limit. The grace also fixes the exponent's decay: clearing at
/// the bare deadline let a single success in the gap before the limiter
/// flapped back reset the curve to `base`, so a flapping limiter never
/// escalated (it oscillated at `base`). Waiting `grace` past the last
/// window means the first success after the limiter has truly gone
/// quiet resets the exponent. No-op on the un-throttled hot path.
async fn clear_queue_cooldown<'e, E>(executor: E, queue_name: &str, now_iso: &str) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let decay_before =
        iso(Utc::now() - chrono::Duration::seconds(crate::runtime::THROTTLE_DECAY_GRACE_SECS));
    sqlx::query(
        r"UPDATE queue
             SET throttle_attempts = 0,
                 throttled_until   = NULL,
                 updated_at        = ?1
           WHERE name = ?2
             AND throttle_attempts > 0
             AND (throttled_until IS NULL OR throttled_until <= ?3)",
    )
    .bind(now_iso)
    .bind(queue_name)
    .bind(&decay_before)
    .execute(executor)
    .await
    .map_err(map_sqlx_err)?;
    Ok(())
}

const fn active_status_placeholders() -> &'static str {
    "?, ?"
}

/// Append a row to `error_history`, cap at `ERROR_HISTORY_CAP`, set
/// `last_error`, and transition status. `terminal = true` → status =
/// dead + `completed_at`. `terminal = false` → status = failed + the
/// supplied `next_scheduled_at`.
#[allow(
    clippy::too_many_lines,
    reason = "one cohesive read-modify-write of error_history + the status transition; the terminal/non-terminal arms are inherent and splitting hurts readability"
)]
async fn append_error_and_update(
    pool: &sqlx::SqlitePool,
    job_id: &JobId,
    now_iso: &str,
    message: &str,
    terminal: bool,
    next_scheduled_at: Option<&str>,
    // Reaper path: when set, the status transition only fires if the
    // row is *still* a stale in-flight row — so a job its worker
    // finalized between the reaper's scan and this write isn't clobbered
    // back to failed/dead (which would re-run it).
    guard_stale_before: Option<&str>,
    // Finalize path (H1): when set, the transition only fires if the row
    // is still `in_progress` and still owned by this `process_id` — so a
    // stalled worker whose claim was reaped + re-claimed can't clobber the
    // new claimant. Mutually exclusive with `guard_stale_before`; both
    // `None` skips the guard (legacy / admin paths).
    guard_owner: Option<&str>,
) -> Result<()> {
    // Read attempts + existing error_history, append, re-write. SQLite
    // doesn't have a JSON-array-append in standard syntax so we do the
    // append on the Rust side. The whole thing is one transaction so
    // concurrent reaper sweeps don't collide.
    let mut tx = pool.begin().await.map_err(map_sqlx_err)?;
    let row = sqlx::query("SELECT attempts, error_history FROM sync_queue WHERE id = ?1")
        .bind(job_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;
    let Some(row) = row else {
        // Job vanished — nothing to do. Caller's responsibility to
        // log if relevant.
        tx.commit().await.map_err(map_sqlx_err)?;
        return Ok(());
    };
    let attempts: i64 = row.try_get("attempts").map_err(map_sqlx_err)?;
    let existing_s: String = row.try_get("error_history").map_err(map_sqlx_err)?;
    let mut entries: Vec<ErrorHistoryEntry> = serde_json::from_str(&existing_s).unwrap_or_default();
    entries.push(ErrorHistoryEntry {
        at: Utc::now(),
        attempt: i32::try_from(attempts).unwrap_or(0),
        message: message.to_owned(),
    });
    if entries.len() > ERROR_HISTORY_CAP {
        let drop = entries.len() - ERROR_HISTORY_CAP;
        entries.drain(0..drop);
    }
    let history_s = serde_json::to_string(&entries)?;

    // Status-transition guard. At most one of the two is set; both bind
    // `?5`. Owner guard (finalize): still in_progress + owned by us.
    // Stale guard (reaper): still the stale in_progress row we scanned.
    let (guard, guard_bind): (&str, Option<&str>) = match (guard_owner, guard_stale_before) {
        (Some(pid), _) => (" AND status = 'in_progress' AND process_id = ?5", Some(pid)),
        (None, Some(stale)) => (
            " AND status = 'in_progress' AND heartbeat_at < ?5",
            Some(stale),
        ),
        (None, None) => ("", None),
    };

    if terminal {
        // UPDATE + event-log INSERT in the same tx so a crash can't
        // leave the event without the row transition.
        let sql = format!(
            "UPDATE sync_queue
                 SET status         = 'dead',
                     completed_at   = ?1,
                     process_id     = NULL,
                     heartbeat_at   = NULL,
                     last_error     = ?2,
                     error_history  = ?3
               WHERE id = ?4{guard}
               RETURNING kind, queue_name"
        );
        let mut q = sqlx::query(&sql)
            .bind(now_iso)
            .bind(message)
            .bind(&history_s)
            .bind(job_id.as_str());
        if let Some(g) = guard_bind {
            q = q.bind(g);
        }
        let dead_row = q.fetch_optional(&mut *tx).await.map_err(map_sqlx_err)?;
        if let Some(r) = dead_row {
            let kind: String = r.try_get("kind").map_err(map_sqlx_err)?;
            let queue_name: String = r.try_get("queue_name").map_err(map_sqlx_err)?;
            record_event(
                &mut *tx,
                now_iso,
                &kind,
                &queue_name,
                Some(job_id.as_str()),
                "failed",
            )
            .await?;
        }
    } else {
        // Non-terminal retry: status goes back to `failed` (the retry
        // pool, despite the name) with a scheduled_at in the future.
        // Emit a `retried` event so the chart's running gauge
        // counterweights the prior `started`.
        let sql = format!(
            "UPDATE sync_queue
                 SET status         = 'failed',
                     scheduled_at   = ?1,
                     process_id     = NULL,
                     heartbeat_at   = NULL,
                     last_error     = ?2,
                     error_history  = ?3
               WHERE id = ?4{guard}
               RETURNING kind, queue_name"
        );
        let mut q = sqlx::query(&sql)
            .bind(next_scheduled_at.unwrap_or(now_iso))
            .bind(message)
            .bind(&history_s)
            .bind(job_id.as_str());
        if let Some(g) = guard_bind {
            q = q.bind(g);
        }
        let row = q.fetch_optional(&mut *tx).await.map_err(map_sqlx_err)?;
        if let Some(r) = row {
            let kind: String = r.try_get("kind").map_err(map_sqlx_err)?;
            let queue_name: String = r.try_get("queue_name").map_err(map_sqlx_err)?;
            record_event(
                &mut *tx,
                now_iso,
                &kind,
                &queue_name,
                Some(job_id.as_str()),
                "retried",
            )
            .await?;
        }
    }
    tx.commit().await.map_err(map_sqlx_err)?;
    Ok(())
}
