//! Cluster worker rebalancing.
//!
//! `queue.max_workers` is a *cluster total*. Without coordination, N
//! replicas would each run `max_workers` workers (3 pods × 10 = 30, not
//! the 10 the operator asked for). The rebalancer — run only by the
//! elected coordinator (it reuses the cron leadership lease) — splits
//! each queue's total fairly across live pods and writes each pod's
//! share to `pod_slot_assignment`. Every supervisor reads its own row
//! and scales local workers to match (10 over 3 pods → 4 / 3 / 3).
//!
//! Two background pieces live here:
//!  - [`pod_heartbeat_loop`]: every pod stamps its liveness so the
//!    rebalancer can see it even when it holds 0 slots.
//!  - [`rebalance_loop`]: the coordinator recomputes assignments.
//!
//! On `SQLite` (single process) the lone pod wins the lease and is
//! handed every queue's full total — same effective behavior as before.

use std::time::Duration;

use chrono::Utc;
use tokio_util::sync::CancellationToken;

use super::cron::CRON_LEASE_TTL;
use crate::storage::Storage;

/// How often the coordinator recomputes pod→slot assignments.
pub const REBALANCE_TICK: Duration = Duration::from_secs(5);

/// Split `total` slots across `pods` pods: each gets `total / pods`,
/// and the first `total % pods` (by the caller's sort order) get one
/// extra. `total = 10, pods = 3` → `[4, 3, 3]`. `pods = 0` → empty.
fn fair_shares(total: usize, pods: usize) -> Vec<usize> {
    if pods == 0 {
        return Vec::new();
    }
    let base = total / pods;
    let extra = total % pods;
    (0..pods)
        .map(|i| if i < extra { base + 1 } else { base })
        .collect()
}

/// Per-pod liveness heartbeat. Independent of workers so a pod assigned
/// 0 slots stays visible to the rebalancer and can be handed slots when
/// totals grow.
pub(super) async fn pod_heartbeat_loop(
    storage: Storage,
    host_id: String,
    worker_name: Option<String>,
    queues: Vec<String>,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(super::SUPERVISOR_TICK);
    tick.tick().await;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {
                if let Err(e) = storage
                    .procs
                    .pod_heartbeat(&host_id, worker_name.as_deref(), &queues)
                    .await
                {
                    tracing::warn!(?e, %host_id, "rebalance: pod heartbeat failed");
                }
            }
        }
    }
}

/// Coordinator loop. Only the cron-lease holder rebalances, so exactly
/// one pod writes assignments per tick.
pub(super) async fn rebalance_loop(storage: Storage, host_id: String, shutdown: CancellationToken) {
    let mut tick = tokio::time::interval(REBALANCE_TICK);
    tick.tick().await;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {
                match storage.cron.try_cron_lease(&host_id, CRON_LEASE_TTL).await {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        tracing::warn!(?e, %host_id, "rebalance: lease check failed");
                        continue;
                    }
                }
                if let Err(e) = rebalance_once(&storage).await {
                    tracing::warn!(?e, "rebalance: tick failed");
                }
            }
        }
    }
}

/// One rebalance pass: for every queue, split `max_workers` across the
/// live pods and persist each pod's share. Run by the coordinator loop;
/// exposed so tests and ops tooling can trigger a pass directly.
///
/// # Errors
///
/// Surfaces storage errors from listing pods/queues. Per-pod `set_slots`
/// failures are logged and skipped, not propagated.
pub async fn rebalance_once(storage: &Storage) -> crate::storage::error::Result<()> {
    let stale_before = Utc::now() - super::STALE_THRESHOLD;
    let pods = storage.procs.list_live_pods(stale_before).await?;
    if pods.is_empty() {
        return Ok(());
    }
    let queues = storage.config.list_queues().await?;
    let assignments_snapshot = storage.procs.list_slot_assignments().await;
    // Snapshot current assignments once so the zero-out pass below only
    // writes pods that actually carry a positive stale slot — without this,
    // every non-eligible (queue, pod) pair got a no-op `set_slots(…, 0)`
    // every tick (O(pods × queues) redundant upserts in steady state). The
    // assign pass only touches eligible pods, so this pre-assign snapshot
    // stays accurate for the non-eligible pods the zero-out targets. A read
    // failure just defers zeroing to the next tick; assigns still run.
    let positive: std::collections::HashSet<(&str, &str)> = match assignments_snapshot {
        Ok(ref a) => a
            .iter()
            .filter(|s| s.slots > 0)
            .map(|s| (s.queue_name.as_str(), s.host_id.as_str()))
            .collect(),
        Err(ref e) => {
            tracing::warn!(
                ?e,
                "rebalance: slot-assignment read failed; skipping zero-out this tick"
            );
            std::collections::HashSet::new()
        }
    };
    for q in queues {
        // Only pods eligible for this queue run it (declared it, or a
        // legacy empty-set pod mid-rollout — see PodRecord::handles). Pods
        // sort by host_id from list_live_pods, so fair_shares' remainder
        // distribution stays deterministic.
        let eligible: Vec<&str> = pods
            .iter()
            .filter(|p| p.handles(&q.name))
            .map(|p| p.host_id.as_str())
            .collect();
        let total = usize::try_from(q.max_workers).unwrap_or(0);
        // A configured queue with workers but no live pod to run them is a
        // silent stall (jobs sit pending forever) — make it loud so it
        // reaches logs/alerts, not just the Workers-tab banner.
        if eligible.is_empty() && total > 0 {
            tracing::warn!(
                queue = %q.name,
                "rebalance: no live worker declares this queue; its jobs will not run \
                 until a worker lists it in FORGE_QUEUES / with_queues",
            );
        }
        let shares = fair_shares(total, eligible.len());
        for (host, slots) in eligible.iter().zip(shares) {
            let slots = i32::try_from(slots).unwrap_or(0);
            if let Err(e) = storage.procs.set_slots(&q.name, host, slots).await {
                tracing::warn!(?e, queue = %q.name, %host, "rebalance: set_slots failed");
            }
        }
        // Zero out live pods that are NOT eligible for this queue but still
        // carry a *positive* stale assignment (e.g. redeployed without the
        // queue), so their supervisor — if any lingered — winds down and the
        // monitoring view doesn't show phantom slots. Pods with no row or an
        // already-zero row are left untouched (a non-eligible pod has no
        // supervisor for this queue, so absent and 0 are equivalent).
        for p in &pods {
            if eligible.iter().any(|h| *h == p.host_id) {
                continue;
            }
            if !positive.contains(&(q.name.as_str(), p.host_id.as_str())) {
                continue;
            }
            if let Err(e) = storage.procs.set_slots(&q.name, &p.host_id, 0).await {
                tracing::warn!(?e, queue = %q.name, host = %p.host_id, "rebalance: zero set_slots failed");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::fair_shares;

    #[test]
    fn fair_shares_distributes_remainder_to_leaders() {
        assert_eq!(fair_shares(10, 3), vec![4, 3, 3]);
        assert_eq!(fair_shares(9, 3), vec![3, 3, 3]);
        assert_eq!(fair_shares(2, 3), vec![1, 1, 0]);
        assert_eq!(fair_shares(0, 3), vec![0, 0, 0]);
        assert_eq!(fair_shares(7, 1), vec![7]);
    }

    #[test]
    fn fair_shares_zero_pods_is_empty() {
        assert!(fair_shares(10, 0).is_empty());
    }

    #[test]
    fn fair_shares_conserves_the_total() {
        for total in 0..50 {
            for pods in 1..8 {
                let sum: usize = fair_shares(total, pods).iter().sum();
                assert_eq!(sum, total, "total={total} pods={pods}");
            }
        }
    }
}
