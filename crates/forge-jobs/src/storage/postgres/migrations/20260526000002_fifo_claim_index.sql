-- Rebuild jq_claim with `id` as the trailing column so claim_next's
-- FIFO tiebreaker walks the index without a sort step.
--
-- See the matching SQLite migration
-- (20260526000001_fifo_claim_index.sql) for the rationale — same
-- shape, same reasoning. The Postgres impl additionally pays for
-- this in the SKIP LOCKED claim path: without a stable tiebreaker,
-- racing workers can claim rows in arbitrary order even when their
-- scheduled_at differs.
--
-- For a live-production cutover on a busy table use
-- `CREATE INDEX CONCURRENTLY` to avoid an exclusive lock — sqlx's
-- migrate! macro doesn't support CONCURRENTLY in a tx, so cold-cut
-- environments only. Production cutover: pause workers, run the
-- migration, resume.

DROP INDEX jq_claim;
CREATE INDEX jq_claim ON sync_queue (queue_name, status, priority, scheduled_at, id);
