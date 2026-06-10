-- Ordered-claim index for claim_next. Parity with the Postgres adapter
-- (postgres/migrations/20260610000002_claim_ready_index.sql) — see there
-- for the full rationale.
--
-- claim_next orders by (priority, scheduled_at, id) over rows with
-- `status IN ('pending','failed')`. With status as a key column in
-- jq_claim, that two-value IN forces the planner to sort the eligible set
-- rather than walk it in order. This partial index moves `status` into the
-- predicate so the remaining keys are exactly the claim order — an ordered
-- index walk + LIMIT 1, no sort, independent of how deep the backlog is.
--
-- Partial on the claimable statuses, so it stays small (excludes
-- done/dead). jq_claim is kept for the queue_name+status prefix lookups.
CREATE INDEX jq_claim_ready ON sync_queue (queue_name, priority, scheduled_at, id)
    WHERE status IN ('pending', 'failed');
