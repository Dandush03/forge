-- Per-table storage + autovacuum tuning for high-volume deployments.
--
-- forge-jobs on Postgres is relentlessly UPDATE-heavy on sync_queue:
-- every claim flips status, every in-flight job heartbeats (an UPDATE)
-- on a ~10s tick, and every finalize transitions the row — each one
-- leaves a dead tuple. At millions of jobs/day the default autovacuum
-- (which only triggers at 20% dead tuples) falls far behind, the table
-- and its indexes bloat, HOT updates stop fitting on-page, and claim
-- latency creeps up. These settings keep the churn in check. See
-- docs/operating-at-scale.md for the monitoring + reasoning.
--
-- NOTE on fillfactor: ALTER TABLE ... SET (fillfactor) only governs
-- pages written *after* this runs — it's immediate on a fresh deploy,
-- but an already-populated table needs a `VACUUM FULL sync_queue`
-- (exclusive lock) or pg_repack (online) to repack existing pages at
-- the new factor. The autovacuum settings, by contrast, take effect on
-- the next autovacuum cycle with no rewrite.

-- sync_queue: leave 15% per-page free space so the heartbeat/claim/
-- finalize UPDATEs land as in-page HOT updates (no index churn, cheaper
-- to vacuum), and vacuum aggressively so dead tuples from the same churn
-- are reclaimed long before they bloat the table.
ALTER TABLE sync_queue SET (
    fillfactor = 85,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_analyze_threshold = 1000
);

-- queue_event: append-only (INSERT + periodic bulk DELETE on cleanup),
-- so fillfactor is irrelevant — there are no in-place updates to keep
-- on-page. It does need eager autovacuum to reclaim the dead tuples the
-- retention sweep leaves behind, otherwise the table and its three
-- indexes bloat between cleanups.
ALTER TABLE queue_event SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 5000,
    autovacuum_analyze_scale_factor = 0.05
);
