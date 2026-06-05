//! Runtime test — `rebalance_once`.
//!
//! Exercises the cluster worker rebalancer against an in-memory
//! `SQLite` store: register pods, set a queue total, run a pass, and
//! assert each pod got its fair share (4×3×3 for 10 over 3).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests crash loudly on setup/assert failures; that's the point"
)]

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use forge_jobs::storage::sqlite::SqliteStorage;
use forge_jobs::{Storage as JobStorage, rebalance_once};

async fn fresh() -> JobStorage {
    let s = Arc::new(
        SqliteStorage::open_in_memory()
            .await
            .expect("open_in_memory"),
    );
    JobStorage::from_one(s)
}

#[tokio::test]
async fn splits_total_across_live_pods() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 10).await.unwrap();
    for host in ["pod-a", "pod-b", "pod-c"] {
        s.procs.pod_heartbeat(host).await.unwrap();
    }

    rebalance_once(&s).await.unwrap();

    // 10 over 3, remainder to the sorted-first pod → 4 / 3 / 3.
    assert_eq!(s.procs.get_slots("gh", "pod-a").await.unwrap(), Some(4));
    assert_eq!(s.procs.get_slots("gh", "pod-b").await.unwrap(), Some(3));
    assert_eq!(s.procs.get_slots("gh", "pod-c").await.unwrap(), Some(3));
}

#[tokio::test]
async fn redistributes_when_a_pod_goes_stale() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 9).await.unwrap();
    for host in ["pod-a", "pod-b", "pod-c"] {
        s.procs.pod_heartbeat(host).await.unwrap();
    }
    rebalance_once(&s).await.unwrap();
    assert_eq!(s.procs.get_slots("gh", "pod-a").await.unwrap(), Some(3));

    // pod-c drops out (graceful deregister); the next pass splits 9
    // across the two survivors.
    s.procs.delete_for_host("pod-c").await.unwrap();
    rebalance_once(&s).await.unwrap();

    let live = s
        .procs
        .list_live_pods(Utc::now() - ChronoDuration::seconds(60))
        .await
        .unwrap();
    assert_eq!(live, vec!["pod-a".to_owned(), "pod-b".to_owned()]);
    // 9 over 2 → 5 / 4.
    assert_eq!(s.procs.get_slots("gh", "pod-a").await.unwrap(), Some(5));
    assert_eq!(s.procs.get_slots("gh", "pod-b").await.unwrap(), Some(4));
}

#[tokio::test]
async fn reap_stale_evicts_dead_pods_and_orphan_assignments() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 6).await.unwrap();
    s.procs.pod_heartbeat("pod-a").await.unwrap();
    s.procs.set_slots("gh", "pod-a", 6).await.unwrap();

    // Reap with a cutoff in the future → the (just-heartbeated) pod
    // looks stale and is evicted, along with its orphaned assignment.
    s.procs
        .reap_stale(Utc::now() + ChronoDuration::seconds(60))
        .await
        .unwrap();

    let live = s
        .procs
        .list_live_pods(Utc::now() - ChronoDuration::seconds(60))
        .await
        .unwrap();
    assert!(live.is_empty(), "dead pod evicted from the pod table");
    assert_eq!(
        s.procs.get_slots("gh", "pod-a").await.unwrap(),
        None,
        "orphaned slot assignment swept"
    );
}

#[tokio::test]
async fn stale_pods_are_excluded() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 6).await.unwrap();
    s.procs.pod_heartbeat("pod-a").await.unwrap();

    // A cutoff in the future makes pod-a's just-written heartbeat look
    // stale, so no pod is live and nothing is assigned.
    let future_cutoff = Utc::now() + ChronoDuration::seconds(60);
    let live = s.procs.list_live_pods(future_cutoff).await.unwrap();
    assert!(live.is_empty());
}
