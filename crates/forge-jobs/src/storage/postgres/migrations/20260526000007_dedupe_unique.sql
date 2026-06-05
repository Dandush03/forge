-- UNIQUE partial index for dedupe — see the SQLite migration of the
-- same name. This is the load-bearing fix on Postgres: without it, two
-- concurrent enqueues with the same key race past the SELECT check and
-- both insert. Scoped to active statuses so re-enqueue after completion
-- still works. Assumes no pre-existing active duplicates.

DROP INDEX IF EXISTS jq_dedupe;

CREATE UNIQUE INDEX jq_dedupe ON sync_queue (dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'in_progress');
