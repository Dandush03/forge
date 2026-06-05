//! Metrics roller.
//!
//! Pre-aggregates per-`(queue, metric)` rollup rows into `metric_bucket`
//! so the dashboard's per-queue + resource charts read a small indexed
//! table instead of re-scanning the hot jobs tables on every poll. See
//! `docs/adr/0009-metrics-rollup.md`.
//!
//! Counts (`enqueued`/`completed`/`failed`) come from the event log;
//! latency percentiles (`proc_ms`/`total_ms`) from `completed_latencies`.
//! CPU/RAM gauges are fed by the sampler (a later commit). The roller
//! re-rolls the last few closed minutes each tick — upserts are
//! idempotent, so that self-heals a missed/jittered tick without
//! double-counting.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use sysinfo::{Disks, Pid, ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};
use tokio_util::sync::CancellationToken;

use super::cron::CRON_LEASE_TTL;
use crate::storage::Storage;
use crate::storage::error::Result;
use crate::storage::types::{JobLatency, MetricBucket, TimelineEvent, TimelineEventType, metric};

/// Base rollup granularity in seconds — one row per `(queue, metric)`
/// per minute. See the ADR for why 60s.
pub const METRICS_BUCKET_SECS: i64 = 60;
/// How often the roller runs.
pub const METRICS_TICK: Duration = Duration::from_mins(1);
/// Days of rollup history kept; older rows are swept by `cleanup_once`.
pub(super) const METRIC_RETENTION_DAYS: i64 = 30;
/// Trailing buckets (re)rolled each tick. Re-rolling closed minutes is
/// idempotent and self-heals a missed tick + catches completions stamped
/// just after a boundary.
const ROLL_LOOKBACK_BUCKETS: i64 = 3;
/// Cap on latency rows scanned per queue per roll window.
const LATENCY_CAP: usize = 50_000;

/// Coordinator loop. Only the cron-lease holder rolls, so exactly one
/// pod scans per tick (upserts are idempotent regardless). `SQLite` is
/// single-process, so its lease always grants.
pub(super) async fn metrics_loop(storage: Storage, host_id: String, shutdown: CancellationToken) {
    let mut sampler = MetricsSampler::new();
    let mut tick = tokio::time::interval(METRICS_TICK);
    tick.tick().await;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {
                // Refresh the process sample every tick on every pod so
                // the CPU-usage delta baseline stays warm even before this
                // pod becomes leader. Same reason we drain the db
                // operation samples here: the backend's recorder buffer
                // would grow unbounded on non-leader pods if we only
                // drained inside the lease-locked block.
                let sample = sampler.sample();
                let drained = storage.jobs.drain_op_samples();
                if drained.dropped > 0 {
                    // Bucket-full drops. Either this tick ran way
                    // long, or the metrics loop was down and is just
                    // catching up. Logged at warn so it's visible
                    // without scraping a separate metric.
                    tracing::warn!(
                        dropped = drained.dropped,
                        "metrics: db_timing samples dropped at bucket cap"
                    );
                }
                // DB-sourced health gauges: SQLite returns file/WAL
                // bytes, Postgres returns server-side connection counts
                // + DB size. The backend only emits what it can query
                // truthfully; nothing here is derived from sqlx
                // bookkeeping. The query is awaited pre-lease for the
                // same reason as `drain_op_samples`: every pod needs
                // its own data flowing even when not leader.
                let db_health = storage.jobs.db_health_snapshot().await;

                match storage.cron.try_cron_lease(&host_id, CRON_LEASE_TTL).await {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        tracing::warn!(?e, %host_id, "metrics: lease check failed");
                        continue;
                    }
                }
                match metrics_roll_once(&storage, Utc::now()).await {
                    Ok(n) => tracing::debug!(rows = n, "metrics rolled"),
                    Err(e) => tracing::warn!(?e, "metrics: roll failed"),
                }
                // Per-pod resource gauges, keyed by host_id (queue column).
                // Each pod samples *itself* and the leader writes; in a
                // cluster every pod is its own row → one line per pod. CPU
                // is normalized to % of all cores. See ADR 0009.
                if let Some(s) = sample {
                    let rows = gauge_rows(&host_id, Utc::now(), &s);
                    if let Err(e) = storage.jobs.upsert_metric_buckets(&rows).await {
                        tracing::warn!(?e, "metrics: gauge upsert failed");
                    }
                }
                // DB health: latency percentiles + whatever DB-sourced
                // gauges the backend produced (file/WAL bytes on SQLite,
                // server connection counts on Postgres).
                let db_rows = db_health_rows(
                    &host_id,
                    Utc::now(),
                    &drained.read,
                    &drained.write,
                    &db_health,
                );
                if !db_rows.is_empty()
                    && let Err(e) = storage.jobs.upsert_metric_buckets(&db_rows).await
                {
                    tracing::warn!(?e, "metrics: db-health upsert failed");
                }
            }
        }
    }
}

/// Roll the last [`ROLL_LOOKBACK_BUCKETS`] closed minutes into
/// `metric_bucket`. Returns the number of rows upserted. Exposed so
/// tests and ops tooling can trigger a roll directly.
///
/// # Errors
///
/// Surfaces storage errors from the source scans or the upsert.
pub async fn metrics_roll_once(storage: &Storage, now: DateTime<Utc>) -> Result<usize> {
    let cur = floor_to_bucket(now, METRICS_BUCKET_SECS);
    let from = cur - TimeDelta::seconds(METRICS_BUCKET_SECS * ROLL_LOOKBACK_BUCKETS);
    if from >= cur {
        return Ok(0);
    }
    let n_buckets = usize::try_from(ROLL_LOOKBACK_BUCKETS).unwrap_or(0);

    // Counts from the event log (carries queue_name + event_type).
    let events = storage.jobs.list_for_timeline(from, cur).await?;
    let mut rows = count_rows(&events, from, METRICS_BUCKET_SECS, n_buckets);

    // Latency percentiles, one scan per queue.
    for q in storage.config.list_queues().await? {
        let lats = storage
            .jobs
            .completed_latencies(Some(&q.name), from, cur, LATENCY_CAP)
            .await?;
        latency_rows(
            &q.name,
            &lats,
            from,
            METRICS_BUCKET_SECS,
            n_buckets,
            &mut rows,
        );
    }

    let n = rows.len();
    storage.jobs.upsert_metric_buckets(&rows).await?;
    Ok(n)
}

/// Bucket the event log into per-`(queue, bucket)` enqueued/completed/
/// failed counts. Started/Retried events aren't rolled. Emits only
/// non-zero counts (a missing row reads as 0).
fn count_rows(
    events: &[TimelineEvent],
    from: DateTime<Utc>,
    bucket_secs: i64,
    n_buckets: usize,
) -> Vec<MetricBucket> {
    // (queue, bucket_idx) -> [enqueued, completed, failed]
    let mut acc: HashMap<(String, usize), [i64; 3]> = HashMap::new();
    for e in events {
        let Some(idx) = bucket_index(e.at, from, bucket_secs, n_buckets) else {
            continue;
        };
        let slot = match e.event_type {
            TimelineEventType::Enqueued => 0,
            TimelineEventType::Completed => 1,
            TimelineEventType::Failed => 2,
            TimelineEventType::Started | TimelineEventType::Retried => continue,
        };
        acc.entry((e.queue_name.clone(), idx)).or_default()[slot] += 1;
    }
    let mut rows = Vec::new();
    for ((queue, idx), counts) in acc {
        let bucket_start = bucket_start_at(from, bucket_secs, idx);
        for (slot, name) in [
            (0, metric::ENQUEUED),
            (1, metric::COMPLETED),
            (2, metric::FAILED),
        ] {
            if counts[slot] > 0 {
                rows.push(count_bucket(
                    queue.clone(),
                    name,
                    bucket_start,
                    counts[slot],
                ));
            }
        }
    }
    rows
}

/// Bucket one queue's latency samples and append `proc_ms` + `total_ms`
/// percentile rows for each non-empty bucket to `out`.
fn latency_rows(
    queue: &str,
    lats: &[JobLatency],
    from: DateTime<Utc>,
    bucket_secs: i64,
    n_buckets: usize,
    out: &mut Vec<MetricBucket>,
) {
    // bucket_idx -> (proc_ms, total_ms) samples
    let mut by_bucket: HashMap<usize, (Vec<i64>, Vec<i64>)> = HashMap::new();
    for l in lats {
        let Some(idx) = bucket_index(l.completed_at, from, bucket_secs, n_buckets) else {
            continue;
        };
        let entry = by_bucket.entry(idx).or_default();
        entry.0.push(l.processing_ms.max(0));
        entry.1.push(l.total_ms.max(0));
    }
    for (idx, (mut proc, mut total)) in by_bucket {
        let bucket_start = bucket_start_at(from, bucket_secs, idx);
        proc.sort_unstable();
        total.sort_unstable();
        out.push(latency_bucket(queue, metric::PROC_MS, bucket_start, &proc));
        out.push(latency_bucket(
            queue,
            metric::TOTAL_MS,
            bucket_start,
            &total,
        ));
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "counts are small non-negative tallies; exact as f64 for display"
)]
fn count_bucket(
    queue: String,
    name: &str,
    bucket_start: DateTime<Utc>,
    count: i64,
) -> MetricBucket {
    MetricBucket {
        queue,
        metric: name.to_owned(),
        bucket_start,
        count,
        sum: count as f64,
        p50: None,
        p95: None,
        p99: None,
        max: count as f64,
    }
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    reason = "latency-ms + sample counts are small, non-negative; exact-enough as f64 for a monitoring rollup"
)]
fn latency_bucket(
    queue: &str,
    name: &str,
    bucket_start: DateTime<Utc>,
    sorted: &[i64],
) -> MetricBucket {
    MetricBucket {
        queue: queue.to_owned(),
        metric: name.to_owned(),
        bucket_start,
        count: sorted.len() as i64,
        sum: sorted.iter().sum::<i64>() as f64,
        p50: Some(percentile(sorted, 50)),
        p95: Some(percentile(sorted, 95)),
        p99: Some(percentile(sorted, 99)),
        max: sorted.last().copied().unwrap_or(0) as f64,
    }
}

/// Floor `t` down to a `secs`-aligned boundary.
fn floor_to_bucket(t: DateTime<Utc>, secs: i64) -> DateTime<Utc> {
    let ts = t.timestamp();
    DateTime::from_timestamp(ts - ts.rem_euclid(secs), 0).unwrap_or(t)
}

/// Index of `at` within `[from, from + n_buckets*bucket_secs)`, or
/// `None` if it falls outside.
fn bucket_index(
    at: DateTime<Utc>,
    from: DateTime<Utc>,
    bucket_secs: i64,
    n_buckets: usize,
) -> Option<usize> {
    let offset = at.timestamp() - from.timestamp();
    if offset < 0 {
        return None;
    }
    let idx = usize::try_from(offset / bucket_secs).ok()?;
    (idx < n_buckets).then_some(idx)
}

#[allow(
    clippy::cast_possible_wrap,
    reason = "n_buckets is tiny (single digits); idx never wraps i64"
)]
fn bucket_start_at(from: DateTime<Utc>, bucket_secs: i64, idx: usize) -> DateTime<Utc> {
    from + TimeDelta::seconds(bucket_secs * idx as i64)
}

/// One resource sample for this process: CPU (% of all cores), resident
/// memory, disk bytes read/written since the last sample, and the data
/// volume's fullness.
#[derive(Clone, Copy)]
struct ResourceSample {
    cpu_pct: f64,
    rss_bytes: u64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
    disk_used_pct: f64,
}

/// Samples this process's resource usage via `sysinfo`. Holds the
/// `System`/`Disks` across ticks because CPU + disk I/O are reported
/// *since the last refresh* — they need a prior refresh as a baseline.
struct MetricsSampler {
    sys: System,
    disks: Disks,
    pid: Option<Pid>,
    /// Logical core count — CPU usage is divided by this so the gauge is
    /// "% of the whole box" rather than summing past 100% per core.
    cores: f64,
}

impl MetricsSampler {
    #[allow(
        clippy::cast_precision_loss,
        reason = "core count is tiny; exact as f64"
    )]
    fn new() -> Self {
        let cores = std::thread::available_parallelism().map_or(1.0, |n| n.get() as f64);
        let mut s = Self {
            sys: System::new(),
            disks: Disks::new_with_refreshed_list(),
            pid: get_current_pid().ok(),
            cores,
        };
        // Prime so the first real sample has a CPU/disk baseline.
        let _ = s.sample();
        s
    }

    /// Refresh + read this process's resources, or `None` if the pid
    /// isn't readable (locked-down sandbox). CPU is normalized to % of
    /// all cores and averaged over the interval since the last refresh
    /// (~the tick spacing).
    fn sample(&mut self) -> Option<ResourceSample> {
        let pid = self.pid?;
        self.sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing()
                .with_cpu()
                .with_memory()
                .with_disk_usage(),
        );
        let p = self.sys.process(pid)?;
        let io = p.disk_usage();
        self.disks.refresh(true);
        Some(ResourceSample {
            cpu_pct: f64::from(p.cpu_usage()) / self.cores.max(1.0),
            rss_bytes: p.memory(),
            disk_read_bytes: io.read_bytes,
            disk_write_bytes: io.written_bytes,
            disk_used_pct: data_volume_used_pct(&self.disks),
        })
    }
}

/// Fullness (% used) of the volume the process runs on. Picks the disk
/// whose mount point is the longest prefix of the current dir (the data
/// dir lives under it); falls back to the longest mount overall (`/`).
/// 0.0 if no disk is readable.
#[allow(
    clippy::cast_precision_loss,
    reason = "disk byte counts fit f64 exactly past any real volume size"
)]
fn data_volume_used_pct(disks: &Disks) -> f64 {
    let cwd = std::env::current_dir().unwrap_or_default();
    let best = disks
        .list()
        .iter()
        .filter(|d| cwd.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .or_else(|| {
            disks
                .list()
                .iter()
                .max_by_key(|d| d.mount_point().as_os_str().len())
        });
    match best {
        Some(d) if d.total_space() > 0 => {
            let used = d.total_space().saturating_sub(d.available_space());
            (used as f64 / d.total_space() as f64) * 100.0
        }
        _ => 0.0,
    }
}

/// Per-pod resource gauge rows for the minute containing `now`, keyed by
/// `host` (the queue column) so each pod is its own series.
#[allow(
    clippy::cast_precision_loss,
    reason = "byte counts fit f64 exactly well past any real disk/memory size"
)]
fn gauge_rows(host: &str, now: DateTime<Utc>, s: &ResourceSample) -> Vec<MetricBucket> {
    let at = floor_to_bucket(now, METRICS_BUCKET_SECS);
    [
        (metric::CPU_PCT, s.cpu_pct),
        (metric::RSS_BYTES, s.rss_bytes as f64),
        (metric::DISK_READ_BYTES, s.disk_read_bytes as f64),
        (metric::DISK_WRITE_BYTES, s.disk_write_bytes as f64),
        (metric::DISK_USED_PCT, s.disk_used_pct),
    ]
    .into_iter()
    .map(|(name, value)| gauge_bucket(host, name, at, value))
    .collect()
}

/// Build the DB-health rollup rows for the minute containing `now`.
/// Emits `db_read_ms` + `db_write_ms` latency rows (each skipped when
/// its sample set is empty so a quiet minute doesn't pin the chart to
/// zero), plus one gauge row per DB-sourced `(metric_name, value)`
/// the backend produced.
fn db_health_rows(
    host: &str,
    now: DateTime<Utc>,
    read_samples: &[i64],
    write_samples: &[i64],
    db_health: &[(&'static str, f64)],
) -> Vec<MetricBucket> {
    let at = floor_to_bucket(now, METRICS_BUCKET_SECS);
    let mut rows = Vec::with_capacity(2 + db_health.len());
    for (kind, samples) in [
        (metric::DB_READ_MS, read_samples),
        (metric::DB_WRITE_MS, write_samples),
    ] {
        if !samples.is_empty() {
            let mut sorted: Vec<i64> = samples.to_vec();
            sorted.sort_unstable();
            rows.push(latency_bucket(host, kind, at, &sorted));
        }
    }
    for (name, value) in db_health {
        rows.push(gauge_bucket(host, name, at, *value));
    }
    rows
}

fn gauge_bucket(host: &str, name: &str, bucket_start: DateTime<Utc>, value: f64) -> MetricBucket {
    MetricBucket {
        queue: host.to_owned(),
        metric: name.to_owned(),
        bucket_start,
        count: 1,
        sum: value,
        p50: None,
        p95: None,
        p99: None,
        max: value,
    }
}

/// Nearest-rank percentile over an ascending slice. `p` in `1..=100`.
/// Returns 0 for an empty slice.
#[allow(
    clippy::cast_precision_loss,
    reason = "latency ms are within f64's exact integer range for any realistic value"
)]
fn percentile(sorted: &[i64], p: u8) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    let rank = (usize::from(p) * n).div_ceil(100).clamp(1, n);
    sorted[rank - 1] as f64
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::float_cmp,
        reason = "unit tests crash loudly on setup failure; the f64 values compared are exact integers"
    )]
    use super::*;

    fn ev(at: DateTime<Utc>, queue: &str, t: TimelineEventType) -> TimelineEvent {
        TimelineEvent {
            at,
            kind: "k".into(),
            queue_name: queue.into(),
            event_type: t,
        }
    }

    #[test]
    fn floor_to_bucket_aligns_to_minute() {
        let t = DateTime::from_timestamp(1_000_037, 0).unwrap();
        assert_eq!(floor_to_bucket(t, 60).timestamp(), 1_000_020);
    }

    #[test]
    fn bucket_index_in_and_out_of_range() {
        let from = DateTime::from_timestamp(1_000_000, 0).unwrap();
        assert_eq!(bucket_index(from, from, 60, 3), Some(0));
        let mid = DateTime::from_timestamp(1_000_130, 0).unwrap(); // +130s → bucket 2
        assert_eq!(bucket_index(mid, from, 60, 3), Some(2));
        let past = DateTime::from_timestamp(999_999, 0).unwrap();
        assert_eq!(bucket_index(past, from, 60, 3), None);
        let beyond = DateTime::from_timestamp(1_000_200, 0).unwrap(); // bucket 3 ≥ n
        assert_eq!(bucket_index(beyond, from, 60, 3), None);
    }

    #[test]
    fn percentile_nearest_rank() {
        let v: Vec<i64> = (1..=100).collect();
        assert_eq!(percentile(&v, 50), 50.0);
        assert_eq!(percentile(&v, 99), 99.0);
        assert_eq!(percentile(&[], 50), 0.0);
        assert_eq!(percentile(&[42], 99), 42.0);
    }

    #[test]
    fn count_rows_tallies_per_queue_per_bucket_nonzero_only() {
        let from = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let b0 = from; // bucket 0
        let b1 = DateTime::from_timestamp(1_000_060, 0).unwrap(); // bucket 1
        let events = vec![
            ev(b0, "gh", TimelineEventType::Enqueued),
            ev(b0, "gh", TimelineEventType::Enqueued),
            ev(b0, "gh", TimelineEventType::Completed),
            ev(b0, "gh", TimelineEventType::Started), // ignored
            ev(b1, "slack", TimelineEventType::Failed),
        ];
        let rows = count_rows(&events, from, 60, 3);
        // gh@b0: enqueued=2, completed=1 (no failed row); slack@b1: failed=1
        assert_eq!(rows.len(), 3, "no zero-count rows, Started ignored");
        let enq = rows
            .iter()
            .find(|r| r.queue == "gh" && r.metric == metric::ENQUEUED)
            .unwrap();
        assert_eq!(enq.count, 2);
        assert_eq!(enq.bucket_start, b0);
        assert!(rows.iter().any(|r| r.queue == "slack"
            && r.metric == metric::FAILED
            && r.count == 1
            && r.bucket_start == b1));
        assert!(
            !rows
                .iter()
                .any(|r| r.queue == "gh" && r.metric == metric::FAILED),
            "gh had no failures → no failed row"
        );
    }

    #[test]
    fn gauge_rows_builds_per_host_resource_rows() {
        let now = DateTime::from_timestamp(1_000_037, 0).unwrap();
        let s = ResourceSample {
            cpu_pct: 12.5,
            rss_bytes: 4096,
            disk_read_bytes: 100,
            disk_write_bytes: 200,
            disk_used_pct: 73.0,
        };
        let rows = gauge_rows("pod-1", now, &s);
        assert_eq!(rows.len(), 5, "cpu, rss, disk read/write, disk used");
        assert!(rows.iter().all(|r| r.queue == "pod-1"), "keyed by host");
        assert!(rows.iter().all(|r| r.bucket_start.timestamp() == 1_000_020));
        let cpu = rows.iter().find(|r| r.metric == metric::CPU_PCT).unwrap();
        assert_eq!(cpu.sum, 12.5);
        assert_eq!(cpu.count, 1);
        assert!(cpu.p50.is_none());
        let write = rows
            .iter()
            .find(|r| r.metric == metric::DISK_WRITE_BYTES)
            .unwrap();
        assert_eq!(write.sum, 200.0);
        let used = rows
            .iter()
            .find(|r| r.metric == metric::DISK_USED_PCT)
            .unwrap();
        assert_eq!(used.sum, 73.0);
    }

    #[test]
    fn sampler_reads_self_process() {
        let mut s = MetricsSampler::new();
        // On a normal host the current process is visible with non-zero
        // RSS. In a locked-down sandbox sample() returns None and the
        // gauge is simply skipped — not a failure.
        if let Some(sample) = s.sample() {
            assert!(sample.rss_bytes > 0, "self RSS should be > 0");
            assert!(sample.cpu_pct >= 0.0, "cpu% is non-negative");
            assert!(sample.disk_used_pct >= 0.0);
        }
    }

    #[test]
    fn db_health_rows_emits_read_and_write_latency_separately() {
        let now = DateTime::from_timestamp(1_000_037, 0).unwrap();
        let sqlite_gauges: Vec<(&'static str, f64)> = vec![
            (metric::DB_SIZE_BYTES, 65_536.0),
            (metric::DB_WAL_BYTES, 4_096.0),
        ];
        // Mixed read + write samples + sqlite gauges → 4 rows
        // (db_read_ms + db_write_ms + 2 gauges).
        let rows = db_health_rows("pod-1", now, &[3, 4, 5], &[20, 30], &sqlite_gauges);
        assert_eq!(rows.len(), 4);
        let read = rows
            .iter()
            .find(|r| r.metric == metric::DB_READ_MS)
            .unwrap();
        assert_eq!(read.count, 3);
        assert_eq!(read.p99, Some(5.0));
        let write = rows
            .iter()
            .find(|r| r.metric == metric::DB_WRITE_MS)
            .unwrap();
        assert_eq!(write.count, 2);
        assert_eq!(write.p99, Some(30.0));
        let size = rows
            .iter()
            .find(|r| r.metric == metric::DB_SIZE_BYTES)
            .unwrap();
        assert_eq!(size.sum, 65_536.0);

        // Quiet read minute (writes only) → no db_read_ms row.
        let writes_only = db_health_rows("pod-1", now, &[], &[10], &sqlite_gauges);
        assert!(
            writes_only.iter().all(|r| r.metric != metric::DB_READ_MS),
            "no read samples → no db_read_ms row"
        );
        assert!(writes_only.iter().any(|r| r.metric == metric::DB_WRITE_MS));

        // Fully quiet minute → only the gauges; no latency rows.
        let quiet = db_health_rows("pod-1", now, &[], &[], &sqlite_gauges);
        assert_eq!(quiet.len(), 2);
        assert!(
            quiet
                .iter()
                .all(|r| r.metric != metric::DB_READ_MS && r.metric != metric::DB_WRITE_MS)
        );
    }

    #[test]
    fn latency_rows_emits_proc_and_total_percentiles() {
        let from = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let b0 = from;
        let lats = vec![
            JobLatency {
                completed_at: b0,
                processing_ms: 100,
                total_ms: 300,
            },
            JobLatency {
                completed_at: b0,
                processing_ms: 200,
                total_ms: 400,
            },
        ];
        let mut out = Vec::new();
        latency_rows("gh", &lats, from, 60, 3, &mut out);
        assert_eq!(out.len(), 2, "one proc_ms + one total_ms row");
        let proc = out.iter().find(|r| r.metric == metric::PROC_MS).unwrap();
        assert_eq!(proc.count, 2);
        assert_eq!(proc.max, 200.0);
        assert_eq!(proc.p99, Some(200.0));
        let total = out.iter().find(|r| r.metric == metric::TOTAL_MS).unwrap();
        assert_eq!(total.max, 400.0);
    }
}
