-- Single-row cron leadership lease. Mirrors the SQLite migration.
-- The leader (holder = host_id) renews before `lease_until`; if it
-- crashes, another replica claims the lease once it expires, so cron
-- recovers ~ttl after a leader dies. Lease-based rather than a
-- pg_advisory_lock so it survives connection pooling (a pooled
-- advisory lock never auto-releases on conn return).

CREATE TABLE cron_leader (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    holder      TEXT NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL
);
