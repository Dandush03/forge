-- Enforce dedupe with a UNIQUE partial index instead of relying on the
-- SELECT-then-INSERT check alone.
--
-- The old `jq_dedupe` was a plain index, so dedupe was only as strong
-- as the read-then-write in enqueue_in_tx. On SQLite (single writer)
-- that holds, but on Postgres two concurrent enqueues with the same key
-- both see "no active row" and both insert. A UNIQUE index over the
-- *active* rows (the same predicate the dedupe lookup uses) makes the
-- second insert conflict, so enqueue can fall back to Deduped.
--
-- Scoped to active statuses so a fresh enqueue is still allowed once the
-- previous job with that key has completed (done/dead/failed leave the
-- index). Assumes no pre-existing active duplicates (true on the
-- single-writer SQLite store).

DROP INDEX IF EXISTS jq_dedupe;

CREATE UNIQUE INDEX jq_dedupe ON sync_queue (dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'in_progress');
