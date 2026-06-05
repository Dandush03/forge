-- Per-queue throttle backoff config + per-job throttle counter.
--
-- `backoff_enabled` opts a queue into the configurable exponential
-- curve. When disabled (the default), the runtime falls back to the
-- previous flat 60s throttle delay, so this migration is a no-op for
-- behavior until a user toggles a queue on from the UI.
--
-- `throttle_attempts` lives on `sync_queue` because the curve grows
-- per *consecutive* throttle on a single job — the runtime increments
-- on `FinalizeOutcome::Throttled` and resets on `Done`.

ALTER TABLE queue       ADD COLUMN backoff_enabled      INTEGER NOT NULL DEFAULT 0;  -- bool: 0/1
ALTER TABLE queue       ADD COLUMN backoff_base_seconds INTEGER NOT NULL DEFAULT 60;
ALTER TABLE queue       ADD COLUMN backoff_max_seconds  INTEGER NOT NULL DEFAULT 1800;
ALTER TABLE sync_queue  ADD COLUMN throttle_attempts    INTEGER NOT NULL DEFAULT 0;
