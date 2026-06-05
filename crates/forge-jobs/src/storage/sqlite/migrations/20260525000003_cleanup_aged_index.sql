-- cleanup_aged filters by (queue_name, status, completed_at). Without
-- queue_name in the index prefix the prior `jq_status_completed`
-- index seeks on status but full-scans the rest of the table — so
-- cleanup_aged holds the writer lock for ~1-2s during a busy
-- bootstrap. That blocks every concurrent worker's finalize past
-- busy_timeout(5s), surfacing as "database is locked" failures
-- everywhere. This composite index lets the planner seek straight
-- to the candidate rows.
--
-- Keeps the prior jq_status_completed in place for any future query
-- that filters on status alone.

CREATE INDEX jq_queue_status_completed
    ON sync_queue (queue_name, status, completed_at);
