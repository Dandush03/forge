-- Cluster-wide rate-limit budget.
--
-- One row per `scope` (today: per queue name — "slack", "gh", …).
-- Token-bucket semantics: `tokens` is a REAL so fractional refill
-- between sub-second `acquire` calls accumulates correctly;
-- `capacity` is the integer ceiling. `last_refilled_at` advances on
-- every successful acquire so the next call sees only the new
-- elapsed window.
--
-- Atomic acquire is `UPDATE … WHERE … >= 1.0 RETURNING tokens` —
-- the WHERE clause re-computes the post-refill amount so a row that
-- crosses the threshold via passive refill is observably claimable
-- in the same statement.
CREATE TABLE rate_limit_state (
    scope            TEXT PRIMARY KEY NOT NULL,
    tokens           REAL    NOT NULL,
    capacity         INTEGER NOT NULL,
    refill_per_sec   REAL    NOT NULL,
    last_refilled_at TEXT    NOT NULL  -- RFC3339, parsed via unixepoch()
);
