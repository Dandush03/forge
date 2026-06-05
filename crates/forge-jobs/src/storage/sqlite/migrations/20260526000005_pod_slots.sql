-- Cluster worker rebalancing: pod presence + per-pod slot assignment.
--
-- `max_workers` is a *cluster total*. A leader-elected rebalancer
-- splits it fairly across live pods and writes each pod's share to
-- `pod_slot_assignment`; every supervisor reads its own row and scales
-- local workers to match (4×3×3 for 10 workers over 3 pods).
--
-- `pod` is a presence heartbeat independent of workers: a pod assigned
-- 0 slots for every queue still runs no workers (so no `queue_process`
-- rows), but must stay visible to the rebalancer so it can be handed
-- slots when totals grow. Workers heartbeat per-row; the pod heartbeats
-- its own existence here.
--
-- On SQLite (single process) the rebalancer trivially assigns the whole
-- total to the one pod — same effective behavior as before.

CREATE TABLE pod (
    host_id      TEXT PRIMARY KEY,
    heartbeat_at TEXT NOT NULL   -- RFC3339
);

CREATE TABLE pod_slot_assignment (
    queue_name TEXT NOT NULL,
    host_id    TEXT NOT NULL,
    slots      INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (queue_name, host_id)
);
