-- Initial schema for the Postgres-backed queue.
--
-- Mirrors the SQLite schema's domain model but uses pg-native types
-- where they're a better fit:
--  * payload / error_history → JSONB (server-side validation, partial
--    index potential, pg_jsonb_pretty in psql).
--  * timestamps → TIMESTAMPTZ (timezone-aware; no string round-trips).
--  * status → TEXT with CHECK constraint (same shape as SQLite — no
--    enum type so a status add is one ALTER instead of two).
--  * queue_event.job_id is TEXT (not UUID) to match sync_queue.id,
--    which the Rust side mints as ULID strings.
--
-- Naming convention: <YYYYMMDDHHMMSS>_<short_name>.sql, alphabetical
-- load order via sqlx::migrate!. Same as the SQLite tree.

-- ── sync_queue — the job rows ────────────────────────────────────────

CREATE TABLE sync_queue (
    id              TEXT PRIMARY KEY NOT NULL,
    queue_name      TEXT NOT NULL,
    kind            TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'in_progress', 'done', 'failed', 'dead')),
    priority        INTEGER NOT NULL DEFAULT 0,
    enqueued_at     TIMESTAMPTZ NOT NULL,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    last_error      TEXT,
    error_history   JSONB NOT NULL DEFAULT '[]'::jsonb,
    process_id      TEXT,
    heartbeat_at    TIMESTAMPTZ,
    dedupe_key      TEXT
);

-- Hot path: `claim_next` uses SELECT … FOR UPDATE SKIP LOCKED on
-- (queue_name, status IN ('pending','failed'), scheduled_at <= now)
-- ordered by (priority ASC, scheduled_at ASC). Composite index in
-- that shape so the planner does an index-only seek + skip-locked.
CREATE INDEX jq_claim ON sync_queue (queue_name, status, priority, scheduled_at);

-- Partial index for dedupe lookups: enqueue does WHERE dedupe_key = ?
-- AND status IN ('pending', 'in_progress'). Postgres lets us narrow
-- the index to active rows only — done/dead/failed rows never block
-- a dedupe.
CREATE INDEX jq_dedupe ON sync_queue (dedupe_key, status)
    WHERE dedupe_key IS NOT NULL
      AND status IN ('pending', 'in_progress');

-- Mission Control's "filter by kind" dropdown.
CREATE INDEX jq_kind ON sync_queue (kind);

-- Status-scrolling panels (Retries, Dead, recently-enqueued, etc.).
CREATE INDEX jq_status_enqueued ON sync_queue (status, enqueued_at);

-- cleanup_aged: WHERE queue_name = ? AND status = ? AND completed_at < ?.
-- Composite covers all three predicates so the planner can seek.
CREATE INDEX jq_queue_status_completed ON sync_queue (queue_name, status, completed_at);

-- Reaper: WHERE status = 'in_progress' AND heartbeat_at < ?.
CREATE INDEX jq_inflight_heartbeat ON sync_queue (status, heartbeat_at);

-- ── queue_event — append-only timeline log ───────────────────────────

CREATE TABLE queue_event (
    id           BIGSERIAL PRIMARY KEY,
    at           TIMESTAMPTZ NOT NULL,
    kind         TEXT NOT NULL,
    queue_name   TEXT NOT NULL,
    event_type   TEXT NOT NULL CHECK (event_type IN ('enqueued', 'started', 'retried', 'completed', 'failed')),
    job_id       TEXT
);

CREATE INDEX qe_at ON queue_event (at);
CREATE INDEX qe_queue ON queue_event (queue_name, at);
CREATE INDEX qe_job ON queue_event (job_id);

-- ── queue — per-queue config ─────────────────────────────────────────

CREATE TABLE queue (
    name                    TEXT PRIMARY KEY NOT NULL,
    max_workers             INTEGER NOT NULL DEFAULT 1,
    paused                  BOOLEAN NOT NULL DEFAULT FALSE,
    retain_done_for_days    INTEGER NOT NULL DEFAULT 7,
    retain_dead_for_days    INTEGER NOT NULL DEFAULT 30,
    updated_at              TIMESTAMPTZ NOT NULL
);

-- ── queue_process — live worker registry ─────────────────────────────

CREATE TABLE queue_process (
    process_id      TEXT PRIMARY KEY NOT NULL,
    queue_name      TEXT NOT NULL,
    host_id         TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    heartbeat_at    TIMESTAMPTZ NOT NULL,
    current_job     TEXT  -- references sync_queue.id when set; no FK
                          -- so a job delete can't cascade-fail a
                          -- legitimate worker row.
);

CREATE INDEX qp_queue ON queue_process (queue_name);
CREATE INDEX qp_host ON queue_process (host_id);
CREATE INDEX qp_heartbeat ON queue_process (heartbeat_at);

-- ── cron_schedule — recurring triggers ───────────────────────────────

CREATE TABLE cron_schedule (
    name            TEXT PRIMARY KEY NOT NULL,
    kind            TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    queue_name      TEXT,
    cron_expr       TEXT NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    max_attempts    INTEGER,
    last_fired_at   TIMESTAMPTZ,
    next_fire_at    TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX cron_next_fire ON cron_schedule (enabled, next_fire_at);
