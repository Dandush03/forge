-- Cluster worker rebalancing: pod presence + per-pod slot assignment.
-- Mirrors the SQLite migration of the same name; see it for the model.

CREATE TABLE pod (
    host_id      TEXT PRIMARY KEY,
    heartbeat_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE pod_slot_assignment (
    queue_name TEXT NOT NULL,
    host_id    TEXT NOT NULL,
    slots      INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_name, host_id)
);
