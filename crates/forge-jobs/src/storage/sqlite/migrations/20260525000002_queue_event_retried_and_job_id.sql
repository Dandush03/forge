-- Add `retried` event type + `job_id` column to queue_event.
--
-- `retried` is emitted whenever a worker-claimed row goes back to the
-- schedulable pool without finishing (Throttled, retryable Failed,
-- non-terminal reap). It counterweights the prior `started` so the
-- chart's running in-flight gauge — `cumulative(started)
-- - cumulative(completed) - cumulative(failed) - cumulative(retried)`
-- — stays accurate across retry cycles.
--
-- `job_id` lets purge / cleanup_aged cascade-delete the events of a
-- deleted job row instead of orphaning them in the log. Legacy rows
-- (from before this migration) get job_id = NULL and are left to age
-- out naturally as their time window rolls past the chart's preset.
--
-- SQLite can't ALTER the CHECK constraint in place, so this is a
-- standard 12-step rebuild: new table, copy data, swap names,
-- recreate indexes.

CREATE TABLE queue_event_new (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    at           TEXT NOT NULL,
    kind         TEXT NOT NULL,
    queue_name   TEXT NOT NULL,
    event_type   TEXT NOT NULL CHECK (event_type IN ('enqueued', 'started', 'retried', 'completed', 'failed')),
    job_id       TEXT
);

INSERT INTO queue_event_new (id, at, kind, queue_name, event_type, job_id)
SELECT id, at, kind, queue_name, event_type, NULL
  FROM queue_event;

DROP TABLE queue_event;
ALTER TABLE queue_event_new RENAME TO queue_event;

CREATE INDEX qe_at ON queue_event (at);
CREATE INDEX qe_queue ON queue_event (queue_name, at);
CREATE INDEX qe_job ON queue_event (job_id);
