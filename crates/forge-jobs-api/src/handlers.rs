//! Pure async fns over `&Storage`.
//!
//! Single source of truth for what each queue operation does at the
//! storage layer; the Tauri plugin commands and the Axum HTTP routes
//! both call into these so the two transports can never drift apart.
//!
//! These were lifted from `tauri-plugin-queue`'s command bodies — the
//! commands are now thin wrappers around the functions here.

#![allow(
    clippy::missing_errors_doc,
    reason = "every handler surfaces the same `crate::Error`; its variants are documented on the type and the per-fn Err set is not a stable contract worth restating on 30 functions"
)]

use chrono::{DateTime, Utc};
use forge_jobs::{
    DeleteOutcome, EnqueueRequest, JobId, JobRecord, JobStatus, NOOP_ECHO_KIND, Storage,
    TimelineEventType, cleanup_once,
};

use crate::Error;
use crate::dto::{
    CleanupReportDto, CronScheduleDto, DbHealthHostSeries, JobInspectDto, JobRowDto,
    JobsEnqueueRequest, JobsListArgs, JobsPageDto, MetricSeriesBucket, QueueOverviewDto,
    QueueProcessDto, ResourceHostSeries, StorageInfoDto, TimelineBucket, WorkersOverviewDto,
    overview_dto, workers_overview_dto,
};
use crate::series;

/// Batch size for bulk ops. Each batch holds the writer pool for one
/// tx (~10-50ms for 500 rows); yielding between batches lets concurrent
/// workers interleave so a 50k-row purge doesn't stall the queue.
const BULK_BATCH: usize = 500;

fn parse_status(s: &str) -> Result<JobStatus, Error> {
    JobStatus::from_str(s)
        .ok_or_else(|| Error::validation("status", format!("unknown status `{s}`")))
}

// ── overview / processes ─────────────────────────────────────────────

/// `GET /queue/overview` — one DTO per registered queue with status
/// counts + live workers + retention settings.
pub async fn queue_overview(storage: &Storage) -> Result<Vec<QueueOverviewDto>, Error> {
    let queues = storage.config.list_queues().await?;
    let now = Utc::now();
    let mut out = Vec::with_capacity(queues.len());
    for cfg in queues {
        let counts = storage.jobs.count_by_status(&cfg.name).await?;
        let processes = storage.procs.list(Some(&cfg.name)).await?;
        let lag = storage
            .jobs
            .oldest_ready_at(&cfg.name)
            .await?
            .map_or(0, |t| u64::try_from((now - t).num_seconds()).unwrap_or(0));
        out.push(overview_dto(cfg, counts, processes, lag));
    }
    Ok(out)
}

/// `GET /queue/processes` — live workers, optionally scoped to a queue.
pub async fn queue_processes(
    storage: &Storage,
    queue_name: Option<&str>,
) -> Result<Vec<QueueProcessDto>, Error> {
    let rows = storage.procs.list(queue_name).await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

/// Liveness window for the worker view. Matches the runtime's 60s reap
/// horizon — a pod stale past this is reaped from the `pod` table, so a
/// wider window wouldn't surface more rows.
const WORKER_LIVENESS_SECS: i64 = 60;

/// `GET /queue/workers` — the worker-centric health view.
///
/// One entry per live worker process (pod) with its declared queues,
/// assigned slots, live/in-flight counts and heartbeat health, plus any
/// configured queue no live worker is covering.
pub async fn queue_workers(storage: &Storage) -> Result<WorkersOverviewDto, Error> {
    let now = Utc::now();
    let stale_before = now - chrono::Duration::seconds(WORKER_LIVENESS_SECS);
    let pods = storage.procs.list_live_pods(stale_before).await?;
    let processes = storage.procs.list(None).await?;
    let slots = storage.procs.list_slot_assignments().await?;
    let queue_names: Vec<String> = storage
        .config
        .list_queues()
        .await?
        .into_iter()
        .map(|q| q.name)
        .collect();
    Ok(workers_overview_dto(
        pods,
        &processes,
        &slots,
        &queue_names,
        now,
        stale_before,
    ))
}

/// `GET /storage/info` — adapter identifier + key/value facts.
pub async fn storage_info(storage: &Storage) -> Result<StorageInfoDto, Error> {
    let info = storage.jobs.describe().await?;
    Ok(info.into())
}

// ── queue config mutations ───────────────────────────────────────────

/// `POST /queue/{name}/max-workers`.
pub async fn queue_set_max_workers(storage: &Storage, name: &str, n: i32) -> Result<(), Error> {
    storage.config.set_max_workers(name, n).await?;
    Ok(())
}

/// `POST /queue/{name}/paused`.
pub async fn queue_set_paused(storage: &Storage, name: &str, paused: bool) -> Result<(), Error> {
    storage.config.set_paused(name, paused).await?;
    Ok(())
}

/// `POST /queue/{name}/retention`.
pub async fn queue_set_retention(
    storage: &Storage,
    name: &str,
    done_days: i32,
    dead_days: i32,
) -> Result<(), Error> {
    storage
        .config
        .set_retention(name, done_days, dead_days)
        .await?;
    Ok(())
}

/// `POST /queue/{name}/backoff` — set the per-queue throttle backoff
/// curve. `base_seconds` / `max_seconds` are clamped to `[1, 86400]`.
pub async fn queue_set_backoff(
    storage: &Storage,
    name: &str,
    enabled: bool,
    base_seconds: i32,
    max_seconds: i32,
) -> Result<(), Error> {
    storage
        .config
        .set_backoff(name, enabled, base_seconds, max_seconds)
        .await?;
    Ok(())
}

// ── jobs reads ───────────────────────────────────────────────────────

/// `POST /jobs/list` — filtered + paginated job listing. Status is
/// queried at the storage layer; queue / kind / payload-search are
/// applied in Rust (keeps the trait surface simple across backends).
pub async fn jobs_list(storage: &Storage, args: JobsListArgs) -> Result<JobsPageDto, Error> {
    let limit_total = args.limit.saturating_add(args.offset);

    let target_statuses: Vec<JobStatus> = if args.filter.statuses.is_empty() {
        vec![
            JobStatus::Pending,
            JobStatus::InProgress,
            JobStatus::Failed,
            JobStatus::Done,
            JobStatus::Dead,
        ]
    } else {
        args.filter
            .statuses
            .iter()
            .filter_map(|s| JobStatus::from_str(s))
            .collect()
    };

    let queue_filter: Option<&str> = if args.filter.queues.len() == 1 {
        args.filter.queues.first().map(String::as_str)
    } else {
        None
    };

    let mut all: Vec<JobRecord> = Vec::new();
    for status in target_statuses {
        let rows = storage
            .jobs
            .list_by_status(queue_filter, status, limit_total as usize)
            .await?;
        all.extend(rows);
    }

    let needle = args
        .filter
        .payload_search
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_lowercase);
    let kinds: Option<std::collections::HashSet<String>> = if args.filter.kinds.is_empty() {
        None
    } else {
        Some(args.filter.kinds.iter().cloned().collect())
    };
    let queues_set: Option<std::collections::HashSet<String>> = if args.filter.queues.len() > 1 {
        Some(args.filter.queues.iter().cloned().collect())
    } else {
        None
    };
    all.retain(|r| {
        if let Some(ref k) = kinds
            && !k.contains(&r.kind)
        {
            return false;
        }
        if let Some(ref qs) = queues_set
            && !qs.contains(&r.queue_name)
        {
            return false;
        }
        if let Some(from) = args.filter.from
            && r.enqueued_at < from
        {
            return false;
        }
        if let Some(to) = args.filter.to
            && r.enqueued_at > to
        {
            return false;
        }
        if let Some(ref n) = needle {
            // Treat serialization failure as "no match" rather than
            // silently rendering an empty string.
            let s = match serde_json::to_string(&r.payload) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(?e, job_id = %r.id, "payload serialization failed in filter scan; skipping needle match");
                    return false;
                }
            };
            if !s.to_lowercase().contains(n) {
                return false;
            }
        }
        true
    });
    all.sort_by_key(|r| std::cmp::Reverse(r.enqueued_at));

    let total = all.len() as u64;
    let offset = args.offset as usize;
    let limit = args.limit as usize;
    let rows = all
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|r| JobRowDto::from(&r))
        .collect();
    Ok(JobsPageDto {
        rows,
        total,
        limit: args.limit,
        offset: args.offset,
    })
}

/// `GET /jobs/failed` — most recent failed rows, newest first.
pub async fn jobs_failed(storage: &Storage, limit: u32) -> Result<Vec<JobRowDto>, Error> {
    let mut rows = storage
        .jobs
        .list_by_status(None, JobStatus::Failed, limit as usize)
        .await?;
    rows.sort_by_key(|r| std::cmp::Reverse(r.enqueued_at));
    Ok(rows.iter().map(JobRowDto::from).collect())
}

/// `GET /jobs/kinds` — distinct job kinds across the queue (drives the
/// filter dropdown), optionally scoped to one `queue_name`.
pub async fn jobs_kinds(storage: &Storage, queue_name: Option<&str>) -> Result<Vec<String>, Error> {
    Ok(storage.jobs.distinct_kinds(queue_name).await?)
}

/// `GET /jobs/{id}` — full row + decoded payload + error history.
pub async fn job_inspect(storage: &Storage, id: &str) -> Result<JobInspectDto, Error> {
    let job_id = JobId::new(id.to_owned());
    let row = storage
        .jobs
        .get_job(&job_id)
        .await?
        .ok_or_else(|| Error::not_found("job not found"))?;
    let error_history = row
        .error_history
        .iter()
        .map(|e| {
            serde_json::json!({
                "at": e.at,
                "attempt": e.attempt,
                "message": e.message,
            })
        })
        .collect();
    let dto_row = JobRowDto::from(&row);
    Ok(JobInspectDto {
        row: dto_row,
        payload: row.payload,
        error_history,
    })
}

// ── jobs mutations ───────────────────────────────────────────────────

/// `POST /jobs/retry` — requeue each id; returns the count touched.
pub async fn jobs_retry(storage: &Storage, ids: &[String]) -> Result<u64, Error> {
    let mut n = 0u64;
    for id in ids {
        if storage.jobs.requeue(&JobId::new(id.clone())).await? {
            n += 1;
        }
    }
    Ok(n)
}

/// `POST /jobs/requeue` — alias of [`jobs_retry`].
pub async fn jobs_requeue(storage: &Storage, ids: &[String]) -> Result<u64, Error> {
    jobs_retry(storage, ids).await
}

/// `POST /jobs/retry-all-failed`.
pub async fn jobs_retry_all_failed(storage: &Storage) -> Result<u64, Error> {
    retry_all_in_status(storage, JobStatus::Failed).await
}

/// `POST /jobs/retry-all-by-status`.
pub async fn jobs_retry_all_by_status(storage: &Storage, status: &str) -> Result<u64, Error> {
    retry_all_in_status(storage, parse_status(status)?).await
}

async fn retry_all_in_status(storage: &Storage, status: JobStatus) -> Result<u64, Error> {
    let mut total = 0u64;
    loop {
        let n = storage
            .jobs
            .requeue_batch_by_status(None, status, BULK_BATCH)
            .await?;
        total += n;
        if n < BULK_BATCH as u64 {
            break;
        }
        tokio::task::yield_now().await;
    }
    Ok(total)
}

/// `POST /jobs/delete` — delete each id; returns the count touched.
pub async fn jobs_delete(storage: &Storage, ids: &[String]) -> Result<u64, Error> {
    let mut n = 0u64;
    for id in ids {
        // Count both an actual removal and an in-progress cancel-request
        // as "touched" — the caller asked to delete, and something
        // happened. `NotFound` (already gone) doesn't count.
        match storage.jobs.delete(&JobId::new(id.clone())).await? {
            DeleteOutcome::Deleted | DeleteOutcome::CancelRequested => n += 1,
            // `NotFound` (already gone) and any future variant: not counted.
            _ => {}
        }
    }
    Ok(n)
}

/// `POST /jobs/delete-done-older-than` — purge `done` rows older than
/// `days`, across every queue.
pub async fn jobs_delete_done_older_than(
    storage: &Storage,
    days: u32,
    queue_name: Option<&str>,
) -> Result<u64, Error> {
    let threshold = Utc::now() - chrono::Duration::days(i64::from(days));
    // Scope to one queue when asked; otherwise sweep every queue.
    let queues: Vec<String> = match queue_name {
        Some(q) => vec![q.to_owned()],
        None => storage
            .config
            .list_queues()
            .await?
            .into_iter()
            .map(|q| q.name)
            .collect(),
    };
    let mut total = 0u64;
    for q in queues {
        total += storage
            .jobs
            .cleanup_aged(&q, JobStatus::Done, threshold)
            .await?;
    }
    Ok(total)
}

/// `POST /jobs/delete-by-status` — bulk-purge every row in `status`.
pub async fn jobs_delete_by_status(
    storage: &Storage,
    status: &str,
    queue_name: Option<&str>,
) -> Result<u64, Error> {
    let status = parse_status(status)?;
    let mut total = 0u64;
    loop {
        let n = storage
            .jobs
            .delete_batch_by_status(queue_name, status, BULK_BATCH)
            .await?;
        total += n;
        if n < BULK_BATCH as u64 {
            break;
        }
        tokio::task::yield_now().await;
    }
    Ok(total)
}

/// `POST /queue/cleanup` — run the retention sweep now.
pub async fn queue_cleanup_now(storage: &Storage) -> Result<CleanupReportDto, Error> {
    let report = cleanup_once(storage).await?;
    Ok(CleanupReportDto {
        done_deleted: report.done_deleted,
        dead_deleted: report.dead_deleted,
    })
}

// ── scheduled / run-now / enqueue ────────────────────────────────────

/// `GET /jobs/scheduled` — pending rows scheduled strictly after now,
/// ascending by `scheduled_at`. Capped at 500.
pub async fn jobs_scheduled(
    storage: &Storage,
    queue_name: Option<&str>,
) -> Result<Vec<JobRowDto>, Error> {
    let now = Utc::now();
    let rows = storage
        .jobs
        .list_scheduled_after(queue_name, now, 500)
        .await?;
    Ok(rows.iter().map(JobRowDto::from).collect())
}

/// `POST /jobs/{id}/run-now` — advance a pending row's `scheduled_at`
/// to now. No-op (returns `false`) when the row isn't pending.
pub async fn jobs_run_now(storage: &Storage, id: &str) -> Result<bool, Error> {
    Ok(storage.jobs.run_now(&JobId::new(id.to_owned())).await?)
}

/// `POST /jobs/enqueue` — generic typed enqueue (the `perform_later`
/// analog). Returns the new job id.
pub async fn jobs_enqueue(storage: &Storage, args: JobsEnqueueRequest) -> Result<String, Error> {
    if args.kind.trim().is_empty() {
        return Err(Error::validation("kind", "kind must be non-empty"));
    }
    let req = EnqueueRequest {
        kind: std::borrow::Cow::Owned(args.kind),
        payload: args.payload,
        queue_name: args.queue_name.map(std::borrow::Cow::Owned),
        dedupe_key: args.dedupe_key,
        max_attempts: args.max_attempts,
        run_at: args.run_at,
        priority: args.priority.unwrap_or(0),
    };
    let outcome = storage.jobs.enqueue(req).await?;
    Ok(outcome.id().as_str().to_owned())
}

/// `POST /jobs/enqueue-demo` — enqueue a `noop_echo` on `default`.
pub async fn queue_enqueue_demo(
    storage: &Storage,
    payload: Option<serde_json::Value>,
) -> Result<String, Error> {
    debug_assert_eq!(NOOP_ECHO_KIND, "noop_echo");
    let req = EnqueueRequest::new(
        "noop_echo",
        payload.unwrap_or_else(|| serde_json::json!({})),
    )
    .on_queue("default");
    let outcome = storage.jobs.enqueue(req).await?;
    Ok(outcome.id().as_str().to_owned())
}

// ── timeline ─────────────────────────────────────────────────────────

/// `GET /queue/timeline` — bucketed enqueue/start/retry/complete/fail
/// counts + per-bucket latency percentiles over `[from, to)`.
pub async fn queue_timeline_range(
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket_secs: u32,
) -> Result<Vec<TimelineBucket>, Error> {
    if to <= from || bucket_secs == 0 {
        return Ok(Vec::new());
    }
    let bucket_seconds = i64::from(bucket_secs);
    let total_secs = (to - from).num_seconds().max(0);
    let raw = ((total_secs + bucket_seconds - 1) / bucket_seconds).max(1);
    let n_buckets = usize::try_from(raw)
        .unwrap_or(series::TIMELINE_MAX_BUCKETS)
        .min(series::TIMELINE_MAX_BUCKETS);

    let mut buckets: Vec<TimelineBucket> = (0..n_buckets)
        .map(|i| TimelineBucket {
            at: from + chrono::Duration::seconds(bucket_seconds * i64::try_from(i).unwrap_or(0)),
            enqueued: 0,
            started: 0,
            retried: 0,
            completed: 0,
            failed: 0,
            processing_p50_ms: 0,
            processing_p95_ms: 0,
            processing_p99_ms: 0,
            total_p50_ms: 0,
            total_p95_ms: 0,
            total_p99_ms: 0,
        })
        .collect();

    let usable_to = buckets
        .last()
        .map_or(to, |b| b.at + chrono::Duration::seconds(bucket_seconds));

    let events = storage.jobs.list_for_timeline(from, usable_to).await?;
    for event in events {
        if let Some(i) = series::bucket_index(event.at, from, bucket_seconds, n_buckets) {
            match event.event_type {
                TimelineEventType::Enqueued => buckets[i].enqueued += 1,
                TimelineEventType::Started => buckets[i].started += 1,
                TimelineEventType::Retried => buckets[i].retried += 1,
                TimelineEventType::Completed => buckets[i].completed += 1,
                TimelineEventType::Failed => buckets[i].failed += 1,
                _ => {}
            }
        }
    }

    let latencies = storage
        .jobs
        .completed_latencies(None, from, usable_to, series::TIMELINE_LATENCY_CAP)
        .await?;
    let mut processing: Vec<Vec<u64>> = vec![Vec::new(); n_buckets];
    let mut total: Vec<Vec<u64>> = vec![Vec::new(); n_buckets];
    for lat in latencies {
        if let Some(i) = series::bucket_index(lat.completed_at, from, bucket_seconds, n_buckets) {
            processing[i].push(u64::try_from(lat.processing_ms.max(0)).unwrap_or(0));
            total[i].push(u64::try_from(lat.total_ms.max(0)).unwrap_or(0));
        }
    }
    for (i, bucket) in buckets.iter_mut().enumerate() {
        processing[i].sort_unstable();
        total[i].sort_unstable();
        bucket.processing_p50_ms = series::percentile(&processing[i], 50);
        bucket.processing_p95_ms = series::percentile(&processing[i], 95);
        bucket.processing_p99_ms = series::percentile(&processing[i], 99);
        bucket.total_p50_ms = series::percentile(&total[i], 50);
        bucket.total_p95_ms = series::percentile(&total[i], 95);
        bucket.total_p99_ms = series::percentile(&total[i], 99);
    }

    Ok(buckets)
}

// ── metric series (per-queue + per-pod, from the rollup) ─────────────

/// `GET /queue/metric-series` — per-queue throughput + latency from the
/// `metric_bucket` rollup, re-bucketed to `bucket_secs`.
pub async fn queue_metric_series(
    storage: &Storage,
    queue: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket_secs: u32,
) -> Result<Vec<MetricSeriesBucket>, Error> {
    if to <= from {
        return Ok(Vec::new());
    }
    let (bucket_seconds, n_buckets, usable_to) = series::series_window(from, to, bucket_secs);
    let rows = storage
        .jobs
        .metric_buckets(Some(queue), series::QUEUE_METRICS, from, usable_to)
        .await?;
    Ok(series::aggregate_series(
        &rows,
        from,
        bucket_seconds,
        n_buckets,
    ))
}

/// `GET /queue/resource-series` — per-pod CPU/RAM/disk from the rollup,
/// one series per `host_id`.
pub async fn queue_resource_series(
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket_secs: u32,
) -> Result<Vec<ResourceHostSeries>, Error> {
    if to <= from {
        return Ok(Vec::new());
    }
    let (bucket_seconds, n_buckets, usable_to) = series::series_window(from, to, bucket_secs);
    let rows = storage
        .jobs
        .metric_buckets(None, series::RESOURCE_METRICS, from, usable_to)
        .await?;
    Ok(series::aggregate_resources(
        &rows,
        from,
        bucket_seconds,
        n_buckets,
    ))
}

/// `GET /queue/db-series` — per-pod DB-health (op-latency percentiles +
/// pool saturation) from the rollup, one series per `host_id`.
pub async fn queue_db_series(
    storage: &Storage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket_secs: u32,
) -> Result<Vec<DbHealthHostSeries>, Error> {
    if to <= from {
        return Ok(Vec::new());
    }
    let (bucket_seconds, n_buckets, usable_to) = series::series_window(from, to, bucket_secs);
    let rows = storage
        .jobs
        .metric_buckets(None, series::DB_METRICS, from, usable_to)
        .await?;
    Ok(series::aggregate_db_health(
        &rows,
        from,
        bucket_seconds,
        n_buckets,
    ))
}

// ── cron ─────────────────────────────────────────────────────────────

/// `GET /cron` — all cron schedules.
pub async fn cron_list(storage: &Storage) -> Result<Vec<CronScheduleDto>, Error> {
    let rows = storage.cron.list_schedules().await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

/// `POST /cron/{name}/enabled`.
pub async fn cron_set_enabled(storage: &Storage, name: &str, enabled: bool) -> Result<(), Error> {
    storage.cron.set_enabled(name, enabled).await?;
    Ok(())
}

/// `POST /cron/{name}/expr` — validates the expression before storing.
pub async fn cron_set_expr(storage: &Storage, name: &str, expr: &str) -> Result<(), Error> {
    forge_jobs::parse_cron(expr)
        .map_err(|e| Error::validation("cron_expr", format!("invalid cron expression: {e}")))?;
    storage.cron.set_expr(name, expr).await?;
    Ok(())
}

/// `POST /cron/{name}/dedupe` — toggle skip-if-in-flight. `true` sets the
/// schedule's dedupe key to its name (so a tick landing while the previous
/// run is still active is a no-op); `false` clears it.
pub async fn cron_set_dedupe(storage: &Storage, name: &str, dedupe: bool) -> Result<(), Error> {
    let key = dedupe.then(|| name.to_string());
    storage.cron.set_dedupe_key(name, key).await?;
    Ok(())
}

/// `POST /cron/{name}/trigger` — fire a schedule immediately. Returns
/// the enqueued job id.
pub async fn cron_trigger_now(storage: &Storage, name: &str) -> Result<String, Error> {
    use forge_jobs::Router as _;
    let row = storage
        .cron
        .get_schedule(name)
        .await?
        .ok_or_else(|| Error::not_found(format!("cron schedule `{name}` not found")))?;
    let now = Utc::now();
    // Match the regular cron-tick path: when the row doesn't pin a
    // queue, route by kind prefix (else the enqueue rejects with
    // "queue_name required").
    let queue_name = row.queue_name.clone().map_or_else(
        || std::borrow::Cow::Borrowed(forge_jobs::KindPrefixRouter.route(row.kind.as_str())),
        std::borrow::Cow::Owned,
    );
    let req = EnqueueRequest {
        kind: std::borrow::Cow::Owned(row.kind.clone()),
        payload: row.payload.clone(),
        queue_name: Some(queue_name),
        dedupe_key: None,
        max_attempts: row.max_attempts,
        run_at: None,
        priority: 0,
    };
    let outcome = storage.jobs.enqueue(req).await?;
    // Advance next_fire_at so the regular tick doesn't immediately re-fire.
    if let Ok(sched) = forge_jobs::parse_cron(&row.cron_expr)
        && let Some(next) = sched
            .after(&now.with_timezone(&chrono::Local))
            .next()
            .map(|dt| dt.with_timezone(&Utc))
    {
        let _ = storage.cron.record_fire(name, now, next).await;
    }
    Ok(outcome.id().as_str().to_owned())
}
