//! Runtime test — `rebalance_once`.
//!
//! Exercises the cluster worker rebalancer against an in-memory
//! `SQLite` store: register pods, set a queue total, run a pass, and
//! assert each *eligible* pod got its fair share (4×3×3 for 10 over 3).
//! Eligibility = the pod declared the queue on its heartbeat.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests crash loudly on setup/assert failures; that's the point"
)]

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use forge_jobs::SqliteStorage;
use forge_jobs::{Storage as JobStorage, rebalance_once};

async fn fresh() -> JobStorage {
    let s = Arc::new(
        SqliteStorage::open_in_memory()
            .await
            .expect("open_in_memory"),
    );
    JobStorage::from_one(s)
}

/// Heartbeat a pod declaring it consumes `queues`.
async fn heartbeat(s: &JobStorage, host: &str, queues: &[&str]) {
    let queues: Vec<String> = queues.iter().map(|q| (*q).to_owned()).collect();
    s.procs.pod_heartbeat(host, None, &queues).await.unwrap();
}

/// Sorted live host ids (drops the rest of the `PodRecord`).
async fn live_hosts(s: &JobStorage) -> Vec<String> {
    let mut hosts: Vec<String> = s
        .procs
        .list_live_pods(Utc::now() - ChronoDuration::seconds(60))
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.host_id)
        .collect();
    hosts.sort();
    hosts
}

#[tokio::test]
async fn splits_total_across_eligible_pods() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 10).await.unwrap();
    for host in ["pod-a", "pod-b", "pod-c"] {
        heartbeat(&s, host, &["gh"]).await;
    }

    rebalance_once(&s).await.unwrap();

    // 10 over 3, remainder to the sorted-first pod → 4 / 3 / 3.
    assert_eq!(s.procs.get_slots("gh", "pod-a").await.unwrap(), Some(4));
    assert_eq!(s.procs.get_slots("gh", "pod-b").await.unwrap(), Some(3));
    assert_eq!(s.procs.get_slots("gh", "pod-c").await.unwrap(), Some(3));

    // list_slot_assignments surfaces every written row.
    let slots = s.procs.list_slot_assignments().await.unwrap();
    let total: i32 = slots.iter().filter(|s| s.queue_name == "gh").map(|s| s.slots).sum();
    assert_eq!(total, 10, "all 10 slots accounted for across the assignments");
}

#[tokio::test]
async fn only_pods_that_declared_the_queue_get_slots() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 4).await.unwrap();
    s.config.ensure_queue("default", 2).await.unwrap();
    // tom runs gh+slack; jerry runs default only.
    heartbeat(&s, "tom", &["gh", "slack"]).await;
    heartbeat(&s, "jerry", &["default"]).await;

    rebalance_once(&s).await.unwrap();

    // All of gh goes to tom; jerry isn't eligible (and is zeroed).
    assert_eq!(s.procs.get_slots("gh", "tom").await.unwrap(), Some(4));
    assert_eq!(s.procs.get_slots("gh", "jerry").await.unwrap(), Some(0));
    // All of default goes to jerry; tom isn't eligible (and is zeroed).
    assert_eq!(s.procs.get_slots("default", "jerry").await.unwrap(), Some(2));
    assert_eq!(s.procs.get_slots("default", "tom").await.unwrap(), Some(0));
}

#[tokio::test]
async fn queue_with_no_eligible_pod_gets_no_positive_slots() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 5).await.unwrap();
    // The only live pod runs `default`, not `gh`.
    heartbeat(&s, "pod-a", &["default"]).await;

    rebalance_once(&s).await.unwrap();

    // pod-a is live but not eligible for gh, so the zero-out pass pins it
    // at 0 — gh has no worker actually serving it (the API surfaces this
    // queue in `unassigned_queues`, computed from declarations not slots).
    assert_eq!(s.procs.get_slots("gh", "pod-a").await.unwrap(), Some(0));
    let serving: i32 = s
        .procs
        .list_slot_assignments()
        .await
        .unwrap()
        .iter()
        .filter(|a| a.queue_name == "gh")
        .map(|a| a.slots)
        .sum();
    assert_eq!(serving, 0, "no slots are actually serving gh");
}

#[tokio::test]
async fn dropping_a_queue_zeroes_the_stale_assignment() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 6).await.unwrap();
    heartbeat(&s, "pod-a", &["gh"]).await;
    rebalance_once(&s).await.unwrap();
    assert_eq!(s.procs.get_slots("gh", "pod-a").await.unwrap(), Some(6));

    // Redeploy: same host, no longer declares gh. The next pass zeroes it.
    heartbeat(&s, "pod-a", &["default"]).await;
    s.config.ensure_queue("default", 1).await.unwrap();
    rebalance_once(&s).await.unwrap();
    assert_eq!(s.procs.get_slots("gh", "pod-a").await.unwrap(), Some(0));
}

#[tokio::test]
async fn redistributes_when_a_pod_goes_stale() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 9).await.unwrap();
    for host in ["pod-a", "pod-b", "pod-c"] {
        heartbeat(&s, host, &["gh"]).await;
    }
    rebalance_once(&s).await.unwrap();
    assert_eq!(s.procs.get_slots("gh", "pod-a").await.unwrap(), Some(3));

    // pod-c drops out (graceful deregister); the next pass splits 9
    // across the two survivors.
    s.procs.delete_for_host("pod-c").await.unwrap();
    rebalance_once(&s).await.unwrap();

    assert_eq!(live_hosts(&s).await, vec!["pod-a".to_owned(), "pod-b".to_owned()]);
    // 9 over 2 → 5 / 4.
    assert_eq!(s.procs.get_slots("gh", "pod-a").await.unwrap(), Some(5));
    assert_eq!(s.procs.get_slots("gh", "pod-b").await.unwrap(), Some(4));
}

#[tokio::test]
async fn reap_stale_evicts_dead_pods_and_orphan_assignments() {
    let s = fresh().await;
    s.config.ensure_queue("gh", 6).await.unwrap();
    heartbeat(&s, "pod-a", &["gh"]).await;
    s.procs.set_slots("gh", "pod-a", 6).await.unwrap();

    // Reap with a cutoff in the future → the (just-heartbeated) pod
    // looks stale and is evicted, along with its orphaned assignment.
    s.procs
        .reap_stale(Utc::now() + ChronoDuration::seconds(60))
        .await
        .unwrap();

    assert!(live_hosts(&s).await.is_empty(), "dead pod evicted from the pod table");
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
    heartbeat(&s, "pod-a", &["gh"]).await;

    // A cutoff in the future makes pod-a's just-written heartbeat look
    // stale, so no pod is live and nothing is assigned.
    let future_cutoff = Utc::now() + ChronoDuration::seconds(60);
    let live = s.procs.list_live_pods(future_cutoff).await.unwrap();
    assert!(live.is_empty());
}

#[tokio::test]
async fn pod_heartbeat_round_trips_name_and_queues() {
    let s = fresh().await;
    s.procs
        .pod_heartbeat("tom", Some("tom"), &["gh".to_owned(), "slack".to_owned()])
        .await
        .unwrap();
    let pods = s
        .procs
        .list_live_pods(Utc::now() - ChronoDuration::seconds(60))
        .await
        .unwrap();
    assert_eq!(pods.len(), 1);
    assert_eq!(pods[0].host_id, "tom");
    assert_eq!(pods[0].worker_name.as_deref(), Some("tom"));
    assert_eq!(pods[0].queues, vec!["gh".to_owned(), "slack".to_owned()]);
}
