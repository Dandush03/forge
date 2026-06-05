-- Single-row cron leadership lease.
--
-- One process at a time holds the lease (`holder` = its host_id) until
-- `lease_until`; only the holder fires cron schedules. On SQLite this
-- is effectively always-grant (single process), but the table keeps
-- the leader-election logic identical across both backends and lets it
-- be unit-tested without a Postgres container.
--
-- Timestamps use SQLite's `datetime()` text format (not the RFC3339
-- `iso()` used elsewhere) — this column is only ever compared against
-- `datetime('now')`, never parsed into a DateTime, so the format is
-- self-contained.

CREATE TABLE cron_leader (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    holder      TEXT NOT NULL,
    lease_until TEXT NOT NULL
);
