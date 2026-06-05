-- Queue-wide throttle cool-down state. Mirrors the SQLite migration
-- of the same name; see it for the rationale. Postgres-native types:
-- TIMESTAMPTZ for the deadline (same shape as `throttled_until`'s
-- SQLite TEXT, but timezone-aware).

ALTER TABLE queue ADD COLUMN throttle_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queue ADD COLUMN throttled_until    TIMESTAMPTZ;  -- NULL = not throttled
