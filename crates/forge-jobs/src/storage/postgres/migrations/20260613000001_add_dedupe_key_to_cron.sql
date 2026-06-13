-- Optional per-schedule dedupe key. When set, each cron firing enqueues
-- with this key, so a tick landing while the previous run is still
-- pending/in-progress collapses to a no-op (skip-if-in-flight) instead of
-- stacking the queue. NULL preserves the fire-every-tick default.
ALTER TABLE cron_schedule ADD COLUMN dedupe_key TEXT;
