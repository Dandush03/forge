-- Pre-aggregated metrics rollup. See docs/adr/0009-metrics-rollup.md.
-- Postgres sibling of the SQLite metric_bucket table — same shape with
-- pg-native types (TIMESTAMPTZ, BIGINT, DOUBLE PRECISION).
--
-- A periodic roller (cron-leader-gated on Postgres so only one replica
-- scans) writes one row per (queue, metric) per 60-second bucket.
-- See the SQLite migration for the column semantics.

CREATE TABLE metric_bucket (
    queue        TEXT NOT NULL,
    metric       TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    count        BIGINT NOT NULL DEFAULT 0,
    sum          DOUBLE PRECISION NOT NULL DEFAULT 0,
    p50          DOUBLE PRECISION,
    p95          DOUBLE PRECISION,
    p99          DOUBLE PRECISION,
    max          DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, metric, bucket_start)
);

CREATE INDEX metric_bucket_range ON metric_bucket (metric, queue, bucket_start);
CREATE INDEX metric_bucket_age ON metric_bucket (bucket_start);
