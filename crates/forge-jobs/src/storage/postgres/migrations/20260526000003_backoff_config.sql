-- Per-queue throttle backoff config + per-job throttle counter.
--
-- Mirrors the SQLite migration of the same name. `backoff_enabled`
-- uses Postgres-native BOOLEAN (same shape as `paused`).

ALTER TABLE queue       ADD COLUMN backoff_enabled      BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE queue       ADD COLUMN backoff_base_seconds INTEGER NOT NULL DEFAULT 60;
ALTER TABLE queue       ADD COLUMN backoff_max_seconds  INTEGER NOT NULL DEFAULT 1800;
ALTER TABLE sync_queue  ADD COLUMN throttle_attempts    INTEGER NOT NULL DEFAULT 0;
