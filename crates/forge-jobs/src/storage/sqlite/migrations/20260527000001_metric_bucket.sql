-- Pre-aggregated metrics rollup. See docs/adr/0009-metrics-rollup.md.
--
-- A periodic roller writes one row per (queue, metric) per 60-second
-- bucket so the dashboard's per-queue + CPU/RAM charts read a tiny
-- indexed table instead of re-scanning the hot `sync_queue` /
-- `queue_event` tables on every poll.
--
--  * `queue`        — queue name; "" (empty) = process-wide gauge
--                     (CPU/RAM can't be split per-queue for in-process
--                     work — only cmd_exec subprocesses attribute).
--  * `metric`       — enqueued | completed | failed (counts);
--                     proc_ms | total_ms (latency); cpu_pct | rss_bytes
--                     (gauges).
--  * `bucket_start` — RFC3339, aligned to the 60s base granularity.
--  * `count`/`sum`  — sample count + Σ value (avg = sum/count; counts
--                     are additive across coarser windows).
--  * `p50/p95/p99`  — per-bucket percentiles, latency metrics only
--                     (NULL otherwise). Do NOT merge across buckets.
--  * `max`          — peak in the bucket (gauges).
--
-- Idempotent: the roller upserts on the PK so a re-run overwrites
-- rather than double-counts.

CREATE TABLE metric_bucket (
    queue        TEXT NOT NULL,
    metric       TEXT NOT NULL,
    bucket_start TEXT NOT NULL,
    count        INTEGER NOT NULL DEFAULT 0,
    sum          REAL NOT NULL DEFAULT 0,
    p50          REAL,
    p95          REAL,
    p99          REAL,
    max          REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, metric, bucket_start)
);

-- Read path: WHERE metric IN (...) [AND queue = ?] AND bucket_start
-- BETWEEN ? AND ? ORDER BY bucket_start.
CREATE INDEX metric_bucket_range ON metric_bucket (metric, queue, bucket_start);

-- Retention sweep prunes by bucket_start across all queues/metrics.
CREATE INDEX metric_bucket_age ON metric_bucket (bucket_start);
