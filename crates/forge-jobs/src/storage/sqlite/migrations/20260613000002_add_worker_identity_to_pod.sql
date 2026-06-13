-- Worker identity + queue responsibilities, published on each pod heartbeat.
-- worker_name: optional human-friendly label (FORGE_WORKER_NAME); NULL falls
--   back to host_id in the monitoring view.
-- queues: CSV of queue names this worker consumes. New workers always set this
--   (declaring queues is mandatory). NULL only appears on a row written by a
--   pre-upgrade binary (deprecated) and is treated as "eligible for no queue"
--   by the rebalancer until that pod re-heartbeats with the new code.
ALTER TABLE pod ADD COLUMN worker_name TEXT;
ALTER TABLE pod ADD COLUMN queues TEXT;
