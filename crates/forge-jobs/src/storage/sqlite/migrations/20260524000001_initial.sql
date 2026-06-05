-- Initial schema for the SQLite-backed queue.
--
-- One file is enough today; later migrations will live alongside
-- this one and sqlx picks them up in lexicographic order. Naming
-- convention: <YYYYMMDDHHMMSS>_<short_name>.sql.
--
-- Design notes:
--  * All ids are TEXT (ULIDs minted by the Rust side). Sortable,
--    monotonically increasing, no need for AUTOINCREMENT.
--  * JSON columns (payload, error_history) stored as TEXT, serialized
--    via serde_json. SQLite's JSON1 functions are available but we
--    don't query inside the payload — it's opaque to the queue.
--  * Datetimes stored as ISO-8601 strings. sqlx maps them to/from
--    chrono::DateTime<Utc> via the `chrono` feature.
--  * `status` is a TEXT with a CHECK constraint — keeps bad values
--    out at write time without dragging in an enum table.

-- ── sync_queue — the job rows ────────────────────────────────────────

CREATE TABLE sync_queue (
    id              TEXT PRIMARY KEY NOT NULL,
    queue_name      TEXT NOT NULL,
    kind            TEXT NOT NULL,
    payload         TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'done', 'failed', 'dead')),
    priority        INTEGER NOT NULL DEFAULT 0,
    enqueued_at     TEXT NOT NULL,
    scheduled_at    TEXT NOT NULL,
    started_at      TEXT,
    completed_at    TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    last_error      TEXT,
    error_history   TEXT NOT NULL DEFAULT '[]',
    process_id      TEXT,
    heartbeat_at    TEXT,
    dedupe_key      TEXT
);

-- The hot path: every claim_next SELECT filters by queue + status +
-- scheduled_at and orders by (priority, scheduled_at). Composite
-- index in exactly that shape so the planner does an index-only seek.
CREATE INDEX jq_claim ON sync_queue (queue_name, status, priority, scheduled_at);

-- Dedupe lookups: enqueue does WHERE dedupe_key = ? AND status IN (...).
CREATE INDEX jq_dedupe ON sync_queue (dedupe_key, status) WHERE dedupe_key IS NOT NULL;

-- "Jobs of kind X" — used by Mission Control's filter by kind.
CREATE INDEX jq_kind ON sync_queue (kind);

-- "Recently enqueued failures" / timeline panel scrolls.
CREATE INDEX jq_status_enqueued ON sync_queue (status, enqueued_at);

-- "Aged completions" — cleanup_aged sweeps by (status, completed_at).
CREATE INDEX jq_status_completed ON sync_queue (status, completed_at);

-- Reaper: WHERE status = 'in_progress' AND heartbeat_at < ?.
CREATE INDEX jq_inflight_heartbeat ON sync_queue (status, heartbeat_at);

-- ── queue — per-queue config ─────────────────────────────────────────

CREATE TABLE queue (
    name                    TEXT PRIMARY KEY NOT NULL,
    max_workers             INTEGER NOT NULL DEFAULT 1,
    paused                  INTEGER NOT NULL DEFAULT 0,  -- bool: 0/1
    retain_done_for_days    INTEGER NOT NULL DEFAULT 7,
    retain_dead_for_days    INTEGER NOT NULL DEFAULT 30,
    updated_at              TEXT NOT NULL
);

-- ── queue_process — live worker registry ─────────────────────────────

CREATE TABLE queue_process (
    process_id      TEXT PRIMARY KEY NOT NULL,
    queue_name      TEXT NOT NULL,
    host_id         TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    heartbeat_at    TEXT NOT NULL,
    current_job     TEXT  -- references sync_queue.id when set, but
                          -- enforced only by the Rust side; no FK so
                          -- a job delete doesn't cascade-fail a
                          -- legitimate worker row.
);

CREATE INDEX qp_queue ON queue_process (queue_name);
CREATE INDEX qp_host ON queue_process (host_id);
CREATE INDEX qp_heartbeat ON queue_process (heartbeat_at);

-- ── cron_schedule — recurring triggers ───────────────────────────────

CREATE TABLE cron_schedule (
    name            TEXT PRIMARY KEY NOT NULL,
    kind            TEXT NOT NULL,
    payload         TEXT NOT NULL DEFAULT '{}',
    queue_name      TEXT,
    cron_expr       TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    max_attempts    INTEGER,
    last_fired_at   TEXT,
    next_fire_at    TEXT,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX cron_next_fire ON cron_schedule (enabled, next_fire_at);
