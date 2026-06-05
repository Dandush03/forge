-- Rebuild jq_claim with `id` as the trailing column so claim_next's
-- FIFO tiebreaker walks the index without a sort step.
--
-- Before: ORDER BY priority ASC, scheduled_at ASC against an index
-- on (queue_name, status, priority, scheduled_at). With many rows
-- tied on (priority, scheduled_at) — common during bursty enqueues
-- where scheduled_at is microseconds-identical — the planner picks
-- whichever row it sees first in the index leaf. Under
-- SKIP LOCKED-style concurrency across multiple workers this
-- amplifies into visible FIFO violations.
--
-- After: ORDER BY priority ASC, scheduled_at ASC, id ASC against an
-- index on (queue_name, status, priority, scheduled_at, id). ULIDs
-- are monotonically time-sortable so `id ASC` is true insertion
-- order. The five-column index lets the planner walk in exact ORDER
-- BY order, zero sort.
--
-- Size cost: ~12 bytes more per index entry. At 10M rows: ~250MB →
-- ~360MB. Negligible.

DROP INDEX jq_claim;
CREATE INDEX jq_claim ON sync_queue (queue_name, status, priority, scheduled_at, id);
