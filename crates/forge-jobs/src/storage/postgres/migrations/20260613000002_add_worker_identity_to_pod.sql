-- Worker identity + queue responsibilities, published on each pod heartbeat.
-- Mirrors the SQLite migration; see it for model details.
-- worker_name: optional human-friendly label (FORGE_WORKER_NAME); NULL falls
--   back to host_id in the monitoring view.
-- queues: CSV of queue names this worker consumes. NULL only on rows from a
--   pre-upgrade binary (deprecated) — treated as "eligible for no queue".
ALTER TABLE pod ADD COLUMN worker_name TEXT;
ALTER TABLE pod ADD COLUMN queues TEXT;
