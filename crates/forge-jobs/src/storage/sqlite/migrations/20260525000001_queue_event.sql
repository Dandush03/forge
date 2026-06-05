-- Append-only event log for the timeline chart.
--
-- The Mission Control timeline used to derive its buckets from
-- `sync_queue.enqueued_at` / `completed_at`, but those rows are
-- purged by `cleanup_aged` after retention (7d done / 30d dead).
-- That meant the historical chart lost data the moment we purged.
--
-- This table is append-only and survives the cleanup pass. We write
-- one row per:
--   - enqueue (event_type = 'enqueued')
--   - claim by a worker (event_type = 'started') — lets the chart
--     show concurrency-over-time: in-flight = cumulative(started) -
--     cumulative(completed) - cumulative(failed)
--   - terminal Done (event_type = 'completed')
--   - terminal Dead (event_type = 'failed')
--
-- Retention is a separate concern (no cleanup today; the table is
-- small — a few bytes per row — and can grow for years before
-- becoming inconvenient).
--
-- Indexed by `at` so range scans for the chart are O(log n).

CREATE TABLE queue_event (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    at           TEXT NOT NULL,
    kind         TEXT NOT NULL,
    queue_name   TEXT NOT NULL,
    event_type   TEXT NOT NULL CHECK (event_type IN ('enqueued', 'started', 'completed', 'failed'))
);

CREATE INDEX qe_at ON queue_event (at);
CREATE INDEX qe_queue ON queue_event (queue_name, at);
