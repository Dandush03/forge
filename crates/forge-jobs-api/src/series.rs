//! Pure bucketing + aggregation helpers behind the timeline and the
//! per-queue / per-pod metric series. Lifted verbatim from the Tauri
//! plugin so the two transports compute identical numbers; the unit
//! tests came with it.
//!
//! Everything here is `pub(crate)` — the handlers in [`crate::handlers`]
//! are the public surface. Aggregation is over the pre-rolled
//! `metric_bucket` table (see `docs/adr/0009-metrics-rollup.md`), never
//! the hot jobs tables.

use chrono::{DateTime, Utc};
use forge_jobs::{METRICS_BUCKET_SECS, MetricBucket, metric};

use crate::dto::{
    DbHealthBucket, DbHealthHostSeries, MetricSeriesBucket, ResourceBucket, ResourceHostSeries,
};

/// Hard cap on how many buckets one call can return. The frontend
/// targets ~60-100 buckets per view, so this only trips on
/// pathological input.
pub const TIMELINE_MAX_BUCKETS: usize = 2_000;

/// Cap on latency rows scanned per timeline call — a recent sample is
/// enough for monitoring percentiles.
pub const TIMELINE_LATENCY_CAP: usize = 50_000;

/// Per-queue throughput + latency metrics read in one scan.
pub const QUEUE_METRICS: &[&str] = &[
    metric::ENQUEUED,
    metric::COMPLETED,
    metric::FAILED,
    metric::PROC_MS,
    metric::TOTAL_MS,
];

/// Per-pod resource gauges read in one scan.
pub const RESOURCE_METRICS: &[&str] = &[
    metric::CPU_PCT,
    metric::RSS_BYTES,
    metric::DISK_READ_BYTES,
    metric::DISK_WRITE_BYTES,
    metric::DISK_USED_PCT,
];

/// Per-pod DB-health metrics read in one scan.
pub const DB_METRICS: &[&str] = &[
    metric::DB_READ_MS,
    metric::DB_WRITE_MS,
    metric::DB_POOL_ACTIVE,
    metric::DB_POOL_IDLE,
    metric::DB_POOL_MAX,
    metric::DB_SIZE_BYTES,
    metric::DB_WAL_BYTES,
];

/// Nearest-rank percentile over an ascending-sorted slice. `p` is in
/// `1..=100`. Returns 0 for an empty slice (no completions → flat zero
/// line, which the chart reads as "no data").
pub fn percentile(sorted: &[u64], p: u8) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let n = sorted.len();
    let rank = (usize::from(p) * n).div_ceil(100).max(1).min(n);
    sorted[rank - 1]
}

/// Index of the bucket `at` falls into, or `None` when out of range.
pub fn bucket_index(
    at: DateTime<Utc>,
    from: DateTime<Utc>,
    bucket_seconds: i64,
    n_buckets: usize,
) -> Option<usize> {
    if at < from {
        return None;
    }
    let offset = (at - from).num_seconds();
    if offset < 0 {
        return None;
    }
    let idx = usize::try_from(offset / bucket_seconds).ok()?;
    (idx < n_buckets).then_some(idx)
}

/// Resolve `(bucket_seconds, n_buckets, usable_to)` for a rollup query.
/// `bucket_secs` is clamped up to the rollup's 60s base — it can't go
/// finer.
pub fn series_window(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket_secs: u32,
) -> (i64, usize, DateTime<Utc>) {
    let bucket_seconds = i64::from(bucket_secs).max(METRICS_BUCKET_SECS);
    let total_secs = (to - from).num_seconds().max(0);
    let raw = ((total_secs + bucket_seconds - 1) / bucket_seconds).max(1);
    let n_buckets = usize::try_from(raw)
        .unwrap_or(TIMELINE_MAX_BUCKETS)
        .min(TIMELINE_MAX_BUCKETS);
    let usable_to =
        from + chrono::Duration::seconds(bucket_seconds * i64::try_from(n_buckets).unwrap_or(0));
    (bucket_seconds, n_buckets, usable_to)
}

/// Per-output-bucket accumulator for the per-queue series. Counts sum;
/// latency p50 is count-weighted, p95/p99 take the worst sub-bucket
/// (percentiles don't merge exactly — ADR 0009).
#[derive(Default, Clone)]
struct Accum {
    enqueued: u64,
    completed: u64,
    failed: u64,
    proc_count: u64,
    proc_p50_wsum: f64,
    proc_p95_max: f64,
    proc_p99_max: f64,
    total_count: u64,
    total_p50_wsum: f64,
    total_p95_max: f64,
    total_p99_max: f64,
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "rollup counts/latency-ms are small non-negative values; rounding for display is exact enough"
)]
pub fn aggregate_series(
    rows: &[MetricBucket],
    from: DateTime<Utc>,
    bucket_seconds: i64,
    n_buckets: usize,
) -> Vec<MetricSeriesBucket> {
    let mut acc = vec![Accum::default(); n_buckets];
    for r in rows {
        let Some(i) = bucket_index(r.bucket_start, from, bucket_seconds, n_buckets) else {
            continue;
        };
        let a = &mut acc[i];
        let count = r.count.max(0) as u64;
        match r.metric.as_str() {
            metric::ENQUEUED => a.enqueued += count,
            metric::COMPLETED => a.completed += count,
            metric::FAILED => a.failed += count,
            metric::PROC_MS => {
                a.proc_count += count;
                a.proc_p50_wsum = r.p50.unwrap_or(0.0).mul_add(count as f64, a.proc_p50_wsum);
                a.proc_p95_max = a.proc_p95_max.max(r.p95.unwrap_or(0.0));
                a.proc_p99_max = a.proc_p99_max.max(r.p99.unwrap_or(0.0));
            }
            metric::TOTAL_MS => {
                a.total_count += count;
                a.total_p50_wsum = r.p50.unwrap_or(0.0).mul_add(count as f64, a.total_p50_wsum);
                a.total_p95_max = a.total_p95_max.max(r.p95.unwrap_or(0.0));
                a.total_p99_max = a.total_p99_max.max(r.p99.unwrap_or(0.0));
            }
            _ => {}
        }
    }
    acc.into_iter()
        .enumerate()
        .map(|(i, a)| {
            let at =
                from + chrono::Duration::seconds(bucket_seconds * i64::try_from(i).unwrap_or(0));
            let wavg = |wsum: f64, n: u64| {
                if n > 0 {
                    (wsum / n as f64).round() as u64
                } else {
                    0
                }
            };
            MetricSeriesBucket {
                at,
                enqueued: a.enqueued,
                completed: a.completed,
                failed: a.failed,
                proc_p50_ms: wavg(a.proc_p50_wsum, a.proc_count),
                proc_p95_ms: a.proc_p95_max.round() as u64,
                proc_p99_ms: a.proc_p99_max.round() as u64,
                total_p50_ms: wavg(a.total_p50_wsum, a.total_count),
                total_p95_ms: a.total_p95_max.round() as u64,
                total_p99_ms: a.total_p99_max.round() as u64,
            }
        })
        .collect()
}

/// Running gauge averages for one (host, bucket): Σvalue / Σcount.
#[derive(Default, Clone)]
struct GaugeAccum {
    cpu_sum: f64,
    cpu_n: u64,
    rss_sum: f64,
    rss_n: u64,
    read_sum: f64,
    read_n: u64,
    write_sum: f64,
    write_n: u64,
    used_sum: f64,
    used_n: u64,
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "rollup gauge values (bytes/percent) are non-negative; averaging + rounding for display is exact enough"
)]
pub fn aggregate_resources(
    rows: &[MetricBucket],
    from: DateTime<Utc>,
    bucket_seconds: i64,
    n_buckets: usize,
) -> Vec<ResourceHostSeries> {
    use std::collections::BTreeMap;
    let mut by_host: BTreeMap<String, Vec<GaugeAccum>> = BTreeMap::new();
    for r in rows {
        // Skip the legacy "" sentinel — older runs wrote resources under
        // PROCESS_WIDE_QUEUE before the per-host redesign; those rows
        // would otherwise dominate `.first()` on the consumer.
        if r.queue.is_empty() {
            continue;
        }
        let Some(i) = bucket_index(r.bucket_start, from, bucket_seconds, n_buckets) else {
            continue;
        };
        let slot = by_host
            .entry(r.queue.clone())
            .or_insert_with(|| vec![GaugeAccum::default(); n_buckets]);
        let a = &mut slot[i];
        let count = r.count.max(0) as u64;
        match r.metric.as_str() {
            metric::CPU_PCT => {
                a.cpu_sum += r.sum;
                a.cpu_n += count;
            }
            metric::RSS_BYTES => {
                a.rss_sum += r.sum;
                a.rss_n += count;
            }
            metric::DISK_READ_BYTES => {
                a.read_sum += r.sum;
                a.read_n += count;
            }
            metric::DISK_WRITE_BYTES => {
                a.write_sum += r.sum;
                a.write_n += count;
            }
            metric::DISK_USED_PCT => {
                a.used_sum += r.sum;
                a.used_n += count;
            }
            _ => {}
        }
    }
    let avg = |sum: f64, n: u64| if n > 0 { sum / n as f64 } else { 0.0 };
    by_host
        .into_iter()
        .map(|(host, slots)| {
            let buckets = slots
                .into_iter()
                .enumerate()
                .map(|(i, a)| ResourceBucket {
                    at: from
                        + chrono::Duration::seconds(bucket_seconds * i64::try_from(i).unwrap_or(0)),
                    cpu_pct: avg(a.cpu_sum, a.cpu_n),
                    rss_bytes: avg(a.rss_sum, a.rss_n).round() as u64,
                    disk_read_bytes: avg(a.read_sum, a.read_n).round() as u64,
                    disk_write_bytes: avg(a.write_sum, a.write_n).round() as u64,
                    disk_used_pct: avg(a.used_sum, a.used_n),
                })
                .collect();
            ResourceHostSeries { host, buckets }
        })
        .collect()
}

/// Per-output-bucket accumulator for DB health. `db_op_ms` percentiles
/// merge by ADR 0009 convention: p50 count-weighted, p95/p99 worst.
/// Pool gauges average sub-bucket values (sum / count).
#[derive(Default, Clone)]
struct DbAccum {
    read_count: u64,
    read_p50_wsum: f64,
    read_p95_max: f64,
    read_p99_max: f64,
    write_count: u64,
    write_p50_wsum: f64,
    write_p95_max: f64,
    write_p99_max: f64,
    active_sum: f64,
    active_n: u64,
    idle_sum: f64,
    idle_n: u64,
    max_sum: f64,
    max_n: u64,
    db_size_sum: f64,
    db_size_n: u64,
    wal_sum: f64,
    wal_n: u64,
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "rollup counts/latency-ms/pool gauges are small non-negative values; rounding for display is exact enough"
)]
pub fn aggregate_db_health(
    rows: &[MetricBucket],
    from: DateTime<Utc>,
    bucket_seconds: i64,
    n_buckets: usize,
) -> Vec<DbHealthHostSeries> {
    use std::collections::BTreeMap;
    let mut by_host: BTreeMap<String, Vec<DbAccum>> = BTreeMap::new();
    for r in rows {
        if r.queue.is_empty() {
            continue;
        }
        let Some(i) = bucket_index(r.bucket_start, from, bucket_seconds, n_buckets) else {
            continue;
        };
        let slot = by_host
            .entry(r.queue.clone())
            .or_insert_with(|| vec![DbAccum::default(); n_buckets]);
        let a = &mut slot[i];
        let count = r.count.max(0) as u64;
        match r.metric.as_str() {
            metric::DB_READ_MS => {
                a.read_count += count;
                a.read_p50_wsum = r.p50.unwrap_or(0.0).mul_add(count as f64, a.read_p50_wsum);
                a.read_p95_max = a.read_p95_max.max(r.p95.unwrap_or(0.0));
                a.read_p99_max = a.read_p99_max.max(r.p99.unwrap_or(0.0));
            }
            metric::DB_WRITE_MS => {
                a.write_count += count;
                a.write_p50_wsum = r.p50.unwrap_or(0.0).mul_add(count as f64, a.write_p50_wsum);
                a.write_p95_max = a.write_p95_max.max(r.p95.unwrap_or(0.0));
                a.write_p99_max = a.write_p99_max.max(r.p99.unwrap_or(0.0));
            }
            metric::DB_POOL_ACTIVE => {
                a.active_sum += r.sum;
                a.active_n += count;
            }
            metric::DB_POOL_IDLE => {
                a.idle_sum += r.sum;
                a.idle_n += count;
            }
            metric::DB_POOL_MAX => {
                a.max_sum += r.sum;
                a.max_n += count;
            }
            metric::DB_SIZE_BYTES => {
                a.db_size_sum += r.sum;
                a.db_size_n += count;
            }
            metric::DB_WAL_BYTES => {
                a.wal_sum += r.sum;
                a.wal_n += count;
            }
            _ => {}
        }
    }
    let avg = |sum: f64, n: u64| if n > 0 { sum / n as f64 } else { 0.0 };
    let wavg = |wsum: f64, n: u64| {
        if n > 0 {
            (wsum / n as f64).round() as u64
        } else {
            0
        }
    };
    by_host
        .into_iter()
        .map(|(host, slots)| {
            let buckets = slots
                .into_iter()
                .enumerate()
                .map(|(i, a)| {
                    let active = avg(a.active_sum, a.active_n);
                    let pool_max = avg(a.max_sum, a.max_n);
                    let bucket_mins = (bucket_seconds / 60).max(1) as u64;
                    let reads_per_min = a.read_count / bucket_mins;
                    let writes_per_min = a.write_count / bucket_mins;
                    let pool_used_pct = if pool_max > 0.0 {
                        (active / pool_max) * 100.0
                    } else {
                        0.0
                    };
                    DbHealthBucket {
                        at: from
                            + chrono::Duration::seconds(
                                bucket_seconds * i64::try_from(i).unwrap_or(0),
                            ),
                        read_p50_ms: wavg(a.read_p50_wsum, a.read_count),
                        read_p95_ms: a.read_p95_max.round() as u64,
                        read_p99_ms: a.read_p99_max.round() as u64,
                        reads_per_min,
                        write_p50_ms: wavg(a.write_p50_wsum, a.write_count),
                        write_p95_ms: a.write_p95_max.round() as u64,
                        write_p99_ms: a.write_p99_max.round() as u64,
                        writes_per_min,
                        pool_active: active.round() as u64,
                        pool_idle: avg(a.idle_sum, a.idle_n).round() as u64,
                        pool_max: pool_max.round() as u64,
                        pool_used_pct,
                        db_size_bytes: avg(a.db_size_sum, a.db_size_n).round() as u64,
                        wal_bytes: avg(a.wal_sum, a.wal_n).round() as u64,
                    }
                })
                .collect();
            DbHealthHostSeries { host, buckets }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::float_cmp,
        clippy::unwrap_used,
        reason = "unit tests: f64 asserts are exact integers; unwrap crashes loudly on setup failure"
    )]
    use super::{aggregate_db_health, aggregate_resources, aggregate_series, percentile};
    use chrono::{DateTime, Utc};
    use forge_jobs::{MetricBucket, metric};

    fn row(
        metric: &str,
        bucket_start: DateTime<Utc>,
        count: i64,
        sum: f64,
        p: Option<f64>,
    ) -> MetricBucket {
        res_row("gh", metric, bucket_start, count, sum, p)
    }

    fn res_row(
        host: &str,
        metric: &str,
        bucket_start: DateTime<Utc>,
        count: i64,
        sum: f64,
        p: Option<f64>,
    ) -> MetricBucket {
        MetricBucket {
            queue: host.into(),
            metric: metric.into(),
            bucket_start,
            count,
            sum,
            p50: p,
            p95: p,
            p99: p,
            max: sum,
        }
    }

    #[test]
    fn aggregate_series_one_to_one_at_base_granularity() {
        let from = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let b1 = from + chrono::Duration::seconds(60);
        let rows = vec![
            row(metric::COMPLETED, from, 3, 3.0, None),
            row(metric::PROC_MS, from, 3, 600.0, Some(100.0)),
            row(metric::COMPLETED, b1, 1, 1.0, None),
        ];
        let out = aggregate_series(&rows, from, 60, 3);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].completed, 3);
        assert_eq!(out[0].proc_p50_ms, 100);
        assert_eq!(out[1].completed, 1);
        assert_eq!(out[2].completed, 0, "empty bucket reads as zero");
    }

    #[test]
    fn aggregate_series_coarsens_counts_sum_latency_p99_max() {
        let from = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let m1 = from + chrono::Duration::seconds(60);
        let rows = vec![
            row(metric::COMPLETED, from, 2, 2.0, None),
            row(metric::COMPLETED, m1, 5, 5.0, None),
            row(metric::PROC_MS, from, 2, 200.0, Some(100.0)),
            row(metric::PROC_MS, m1, 5, 2500.0, Some(500.0)),
        ];
        let out = aggregate_series(&rows, from, 120, 1);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].completed, 7, "counts sum across sub-buckets");
        assert_eq!(out[0].proc_p99_ms, 500);
        assert_eq!(out[0].proc_p50_ms, 386);
    }

    #[test]
    fn aggregate_resources_averages_gauges_per_host() {
        let from = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let m1 = from + chrono::Duration::seconds(60);
        let rows = vec![
            res_row("pod-a", metric::CPU_PCT, from, 1, 20.0, None),
            res_row("pod-a", metric::CPU_PCT, m1, 1, 40.0, None),
            res_row("pod-a", metric::RSS_BYTES, from, 1, 1000.0, None),
            res_row("pod-a", metric::DISK_WRITE_BYTES, from, 1, 4096.0, None),
            res_row("pod-b", metric::CPU_PCT, from, 1, 5.0, None),
        ];
        let out = aggregate_resources(&rows, from, 120, 1);
        assert_eq!(out.len(), 2, "one series per host, sorted");
        assert_eq!(out[0].host, "pod-a");
        assert_eq!(out[0].buckets.len(), 1);
        assert_eq!(out[0].buckets[0].cpu_pct, 30.0, "cpu avg across minutes");
        assert_eq!(out[0].buckets[0].rss_bytes, 1000);
        assert_eq!(out[0].buckets[0].disk_write_bytes, 4096);
        assert_eq!(out[1].host, "pod-b");
        assert_eq!(out[1].buckets[0].cpu_pct, 5.0);
    }

    #[test]
    fn aggregate_db_health_normalizes_throughput_split_by_kind() {
        let from = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let m1 = from + chrono::Duration::seconds(60);
        let rows = vec![
            res_row("pod-a", metric::DB_READ_MS, from, 50, 500.0, Some(5.0)),
            res_row("pod-a", metric::DB_READ_MS, m1, 30, 600.0, Some(10.0)),
            res_row("pod-a", metric::DB_WRITE_MS, from, 10, 200.0, Some(20.0)),
            res_row("pod-a", metric::DB_POOL_ACTIVE, from, 1, 4.0, None),
            res_row("pod-a", metric::DB_POOL_ACTIVE, m1, 1, 6.0, None),
            res_row("pod-a", metric::DB_POOL_IDLE, from, 1, 5.0, None),
            res_row("pod-a", metric::DB_POOL_MAX, from, 1, 9.0, None),
            res_row("pod-a", metric::DB_POOL_MAX, m1, 1, 9.0, None),
        ];
        let out = aggregate_db_health(&rows, from, 120, 1);
        assert_eq!(out.len(), 1);
        let b = &out[0].buckets[0];
        assert_eq!(b.reads_per_min, 40, "80 reads / 2 min = 40");
        assert_eq!(b.writes_per_min, 5, "10 writes / 2 min = 5");
        assert_eq!(b.read_p99_ms, 10);
        assert_eq!(b.read_p50_ms, 7);
        assert_eq!(b.write_p99_ms, 20, "single write sample → p99 = its value");
        assert_eq!(b.pool_active, 5, "avg(4, 6) = 5");
        assert_eq!(b.pool_max, 9);
        assert!((b.pool_used_pct - 55.555_555).abs() < 0.01);
    }

    #[test]
    fn aggregate_db_health_no_op_samples_zeros_latency_but_keeps_pool() {
        let from = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let rows = vec![
            res_row("pod-a", metric::DB_POOL_ACTIVE, from, 1, 0.0, None),
            res_row("pod-a", metric::DB_POOL_IDLE, from, 1, 9.0, None),
            res_row("pod-a", metric::DB_POOL_MAX, from, 1, 9.0, None),
        ];
        let out = aggregate_db_health(&rows, from, 60, 1);
        let b = &out[0].buckets[0];
        assert_eq!(b.reads_per_min, 0);
        assert_eq!(b.writes_per_min, 0);
        assert_eq!(b.read_p50_ms, 0);
        assert_eq!(b.write_p99_ms, 0);
        assert_eq!(b.pool_idle, 9);
        assert_eq!(b.pool_used_pct, 0.0, "0 active / 9 max → 0%");
    }

    #[test]
    fn percentile_empty_is_zero() {
        assert_eq!(percentile(&[], 50), 0);
        assert_eq!(percentile(&[], 99), 0);
    }

    #[test]
    fn percentile_single_value() {
        assert_eq!(percentile(&[42], 50), 42);
        assert_eq!(percentile(&[42], 99), 42);
    }

    #[test]
    fn percentile_nearest_rank() {
        let v: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&v, 50), 50);
        assert_eq!(percentile(&v, 95), 95);
        assert_eq!(percentile(&v, 99), 99);
        assert_eq!(percentile(&v, 100), 100);
    }

    #[test]
    fn percentile_p99_picks_tail() {
        let v: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 1000];
        assert_eq!(percentile(&v, 99), 1000);
        assert_eq!(percentile(&v, 50), 5);
    }
}
