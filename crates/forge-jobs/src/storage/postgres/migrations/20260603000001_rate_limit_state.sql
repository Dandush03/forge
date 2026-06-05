-- Cluster-wide rate-limit budget. SQLite twin migration of the same date.
--
-- See the SQLite migration for the token-bucket rationale. On PG
-- the row lock from `UPDATE … WHERE … RETURNING` serializes
-- concurrent acquires across replicas — that's the entire point of
-- moving the limiter to a DB-backed table instead of an in-process
-- counter (which would let each pod independently spend the
-- full budget).
CREATE TABLE rate_limit_state (
    scope            TEXT PRIMARY KEY NOT NULL,
    tokens           DOUBLE PRECISION NOT NULL,
    capacity         BIGINT           NOT NULL,
    refill_per_sec   DOUBLE PRECISION NOT NULL,
    last_refilled_at TIMESTAMPTZ      NOT NULL
);
