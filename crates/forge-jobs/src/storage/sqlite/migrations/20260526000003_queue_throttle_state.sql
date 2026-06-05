-- Queue-wide throttle cool-down state.
--
-- GitHub-style rate limits are per-token, not per-job: when one job
-- hits a 429/403, every sibling worker on the queue will too. The
-- per-job `throttle_attempts` curve (20260526000002) can't see that —
-- each fresh job resets the exponent, so workers march through the
-- whole pending backlog hammering the limiter. These two columns move
-- the throttle decision to the queue level:
--
--   `throttle_attempts` — consecutive queue-wide throttles. Drives the
--     backoff exponent; reset to 0 the moment any job completes.
--   `throttled_until`   — cool-down deadline. While set in the future,
--     `claim_next` refuses to hand out rows for this queue, so the
--     whole fleet (every worker, every replica) backs off together.
--
-- Only populated when the queue has `backoff_enabled = true`; with
-- backoff off these stay 0 / NULL and the gate is inert (legacy
-- per-job-only behavior).

ALTER TABLE queue ADD COLUMN throttle_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queue ADD COLUMN throttled_until    TEXT;  -- RFC3339; NULL = not throttled
