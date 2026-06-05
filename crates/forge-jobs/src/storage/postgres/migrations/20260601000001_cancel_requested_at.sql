-- Per-job cancellation flag. SQLite twin migration of the same date.
--
-- `JobQueue::delete(job_id)` on an `in_progress` row sets this
-- column instead of removing the row, so the worker's heartbeat
-- tick can observe and signal the in-process cancel token.
-- The runtime clears it on `claim_next` so a row that's been
-- requeued after a previous cancel starts the next attempt clean.
ALTER TABLE sync_queue ADD COLUMN cancel_requested_at TIMESTAMPTZ;  -- NULL = no cancel pending
