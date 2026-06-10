//! Runtime test — `cleanup_once` retention sweep.
//!
//! `cleanup_once(storage)` iterates `list_queues()` and deletes rows
//! whose `completed_at` is older than the queue's per-status retention
//! window. The storage trait doesn't let us backdate `completed_at` on
//! a real row, so these tests drive `JobQueue::cleanup_aged(queue,
//! status, threshold)` directly with chosen thresholds — that's the
//! method `cleanup_once` calls internally. One smoke test wires the
//! full `cleanup_once` path to confirm the queue iteration + report
//! aggregation still hangs together.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests crash loudly on setup/assert failures; that's the point"
)]

use std::sync::Arc;

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use forge_jobs::SqliteStorage;
use forge_jobs::storage::{EnqueueRequest, FinalizeOutcome, JobStatus};
use forge_jobs::{Storage as JobStorage, cleanup_once};
use serde_json::json;

async fn fresh() -> JobStorage {
    let s = Arc::new(
        SqliteStorage::open_in_memory()
            .await
            .expect("open_in_memory"),
    );
    JobStorage::from_one(s)
}

/// Enqueue → claim → finalize so the row ends in the requested
/// terminal status with `completed_at = now`. Returns the row id.
async fn run_to_terminal(
    s: &JobStorage,
    kind: &str,
    queue: &str,
    outcome: FinalizeOutcome,
) -> forge_jobs::JobId {
    let enq = s
        .jobs
        .enqueue(
            EnqueueRequest::new(std::borrow::Cow::Owned(kind.to_owned()), json!({}))
                .on_queue(queue.to_owned()),
        )
        .await
        .expect("enqueue");
    let id = enq.id().clone();
    let claimed = s
        .jobs
        .claim_next(queue, "w-0")
        .await
        .expect("claim_next")
        .expect("a row to claim");
    assert_eq!(claimed.id, id);
    s.jobs.finalize(&id, None, outcome).await.expect("finalize");
    id
}

#[tokio::test]
async fn deletes_done_rows_past_threshold() {
    let s = fresh().await;
    s.config
        .ensure_queue("retain", 1)
        .await
        .expect("ensure_queue");
    let _id = run_to_terminal(&s, "noop_done", "retain", FinalizeOutcome::Done).await;

    // `completed_at` was stamped at `now` during finalize. A future
    // threshold makes the row look "older than threshold" → delete.
    let threshold = Utc::now() + ChronoDuration::hours(1);
    let deleted = s
        .jobs
        .cleanup_aged("retain", JobStatus::Done, threshold)
        .await
        .expect("cleanup_aged Done");
    assert_eq!(deleted, 1, "one done row should be deleted");
}

#[tokio::test]
async fn keeps_done_rows_within_threshold() {
    let s = fresh().await;
    s.config
        .ensure_queue("retain", 1)
        .await
        .expect("ensure_queue");
    let _id = run_to_terminal(&s, "noop_done2", "retain", FinalizeOutcome::Done).await;

    // Past threshold → the row's `completed_at = now` is fresher,
    // so nothing should be deleted.
    let threshold = Utc::now() - ChronoDuration::hours(1);
    let deleted = s
        .jobs
        .cleanup_aged("retain", JobStatus::Done, threshold)
        .await
        .expect("cleanup_aged Done");
    assert_eq!(deleted, 0, "fresh done rows must not be deleted");
}

#[tokio::test]
async fn deletes_dead_rows_past_threshold() {
    let s = fresh().await;
    s.config
        .ensure_queue("retain", 1)
        .await
        .expect("ensure_queue");
    let _id = run_to_terminal(
        &s,
        "noop_dead",
        "retain",
        FinalizeOutcome::Dead {
            message: "test".into(),
        },
    )
    .await;

    let threshold = Utc::now() + ChronoDuration::hours(1);
    let deleted = s
        .jobs
        .cleanup_aged("retain", JobStatus::Dead, threshold)
        .await
        .expect("cleanup_aged Dead");
    assert_eq!(deleted, 1, "one dead row should be deleted");
}

#[tokio::test]
async fn cleanup_once_wrapper_aggregates_report() {
    // End-to-end: `cleanup_once` iterates queues, runs both Done +
    // Dead sweeps per queue, and aggregates counts in a
    // `CleanupReport`. With a fresh store there are no queue rows
    // and nothing happens, so the report comes back zeroed.
    let s = fresh().await;
    let report = cleanup_once(&s).await.expect("cleanup_once");
    assert_eq!(report.done_deleted, 0);
    assert_eq!(report.dead_deleted, 0);
    assert_eq!(report.total(), 0);
}

#[tokio::test]
async fn cleanup_lease_grants_exactly_one_pod_per_window() {
    // The cleanup_loop gates `cleanup_once` behind `try_cron_lease`
    // so N pods don't all redundantly DELETE the same retention
    // rows every tick (idempotent, but multiplies writer-lock
    // contention by N). This test proves the gating primitive:
    // first pod gets the lease, a sibling racing inside the TTL
    // window gets denied.
    let s = fresh().await;
    let ttl = Duration::from_secs(15);

    let leader_first = s
        .cron
        .try_cron_lease("pod-leader", ttl)
        .await
        .expect("lease ok");
    assert!(leader_first, "first acquirer must grant");

    let follower = s
        .cron
        .try_cron_lease("pod-follower", ttl)
        .await
        .expect("lease ok");
    assert!(
        !follower,
        "a non-leader request inside the TTL window must be denied — cleanup_loop sees this as `Ok(false)` and skips the tick"
    );

    // The leader renews its own lease on every tick (lease holders
    // don't bounce themselves out). cleanup_loop relies on this so
    // the same pod keeps doing the work across ticks rather than
    // flapping leadership.
    let leader_renew = s
        .cron
        .try_cron_lease("pod-leader", ttl)
        .await
        .expect("lease ok");
    assert!(leader_renew, "leader must keep renewing");
}
