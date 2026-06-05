//! Runtime test — the reaper revival path.
//!
//! `reap_stale_jobs(&storage)` uses a fixed `STALE_THRESHOLD` and
//! computes `stale_before = now - STALE_THRESHOLD`. Tests drive
//! `JobQueue::revive_stale(stale_before)` directly with a chosen
//! threshold so we can pin the boundary without waiting wall-clock
//! time. One test calls `reap_stale_jobs` end-to-end to prove the
//! wrapper still wires up.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests crash loudly on setup/assert failures; that's the point"
)]

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use forge_jobs::SqliteStorage;
use forge_jobs::storage::{EnqueueRequest, JobStatus};
use forge_jobs::{Storage as JobStorage, reap_stale_jobs};
use serde_json::json;

async fn fresh() -> JobStorage {
    let s = Arc::new(
        SqliteStorage::open_in_memory()
            .await
            .expect("open_in_memory"),
    );
    JobStorage::from_one(s)
}

/// Enqueue + claim one job so it lands in `in_progress` with a
/// `heartbeat_at = now`. Returns the claimed id so the caller can
/// look it back up.
async fn enqueue_and_claim(s: &JobStorage, kind: &str) -> forge_jobs::JobId {
    let outcome = s
        .jobs
        .enqueue(
            EnqueueRequest::new(std::borrow::Cow::Owned(kind.to_owned()), json!({}))
                .on_queue("default"),
        )
        .await
        .expect("enqueue");
    let id = outcome.id().clone();
    let claimed = s
        .jobs
        .claim_next("default", "w-0")
        .await
        .expect("claim_next")
        .expect("a row to claim");
    assert_eq!(
        claimed.id, id,
        "claim returned a different row than enqueue"
    );
    id
}

#[tokio::test]
async fn revives_in_progress_job_with_stale_heartbeat() {
    let s = fresh().await;
    let _id = enqueue_and_claim(&s, "noop_stale").await;

    // Pick a threshold in the future so EVERY in-progress row counts
    // as stale. This is the inverse of how `reap_stale_jobs` works in
    // production (it passes `now - STALE_THRESHOLD`), but the
    // contract of `revive_stale` is "any in_progress with
    // heartbeat_at < threshold", and a future threshold gives us
    // deterministic 'definitely stale' rows without sleeping.
    let threshold = Utc::now() + ChronoDuration::hours(1);
    let revived = s.jobs.revive_stale(threshold).await.expect("revive_stale");
    assert_eq!(
        revived, 1,
        "the one in-progress row should have been revived"
    );

    let pending = s
        .jobs
        .list_by_status(None, JobStatus::Failed, 100)
        .await
        .expect("list failed");
    assert_eq!(
        pending.len(),
        1,
        "revived rows transition to Failed (with backoff), not Pending; got {pending:?}"
    );
}

#[tokio::test]
async fn leaves_fresh_in_progress_jobs_alone() {
    let s = fresh().await;
    let _id = enqueue_and_claim(&s, "noop_fresh").await;

    // Threshold in the past → the row's `heartbeat_at = now` is
    // newer than the threshold, so revive_stale must skip it.
    let threshold = Utc::now() - ChronoDuration::hours(1);
    let revived = s.jobs.revive_stale(threshold).await.expect("revive_stale");
    assert_eq!(revived, 0, "fresh in-progress rows must not be revived");

    let in_progress = s
        .jobs
        .list_by_status(None, JobStatus::InProgress, 100)
        .await
        .expect("list in_progress");
    assert_eq!(in_progress.len(), 1, "still in-progress");
}

#[tokio::test]
async fn revived_job_skips_delay_when_backoff_disabled() {
    // Default queue config has backoff_enabled = false; the revived
    // row's scheduled_at should be ~now, not now + 2min (the old
    // hardcoded 2^attempts curve).
    let s = fresh().await;
    let _id = enqueue_and_claim(&s, "noop_no_backoff").await;

    let threshold = Utc::now() + ChronoDuration::hours(1);
    s.jobs.revive_stale(threshold).await.expect("revive_stale");

    let failed = s
        .jobs
        .list_by_status(None, JobStatus::Failed, 100)
        .await
        .expect("list failed");
    assert_eq!(failed.len(), 1, "exactly one revived row");
    let drift = (failed[0].scheduled_at - Utc::now()).num_seconds().abs();
    assert!(
        drift < 5,
        "with backoff off, revived scheduled_at must be ~now; drift={drift}s, scheduled_at={:?}",
        failed[0].scheduled_at
    );
}

#[tokio::test]
async fn revived_job_uses_queue_backoff_when_enabled() {
    let s = fresh().await;
    // `set_backoff` is an UPDATE — the queue row has to exist first.
    s.config
        .ensure_queue("default", 1)
        .await
        .expect("ensure_queue");
    let _id = enqueue_and_claim(&s, "noop_backoff").await;

    // Turn the curve on with a custom base + max so we'd notice a
    // hardcoded fallback. attempts after claim = 1 → expected delay
    // = min(10 * 2^1, 300) = 20s.
    s.config
        .set_backoff(
            "default", /* enabled: */ true, /* base_seconds: */ 10,
            /* max_seconds: */ 300,
        )
        .await
        .expect("set_backoff");

    let threshold = Utc::now() + ChronoDuration::hours(1);
    s.jobs.revive_stale(threshold).await.expect("revive_stale");

    let failed = s
        .jobs
        .list_by_status(None, JobStatus::Failed, 100)
        .await
        .expect("list failed");
    assert_eq!(failed.len(), 1, "exactly one revived row");
    let delay_secs = (failed[0].scheduled_at - Utc::now()).num_seconds();
    // 20s expected, allow a few seconds for test scheduling drift.
    assert!(
        (15..=25).contains(&delay_secs),
        "expected ~20s delay (base=10, attempts=1); got {delay_secs}s"
    );
}

#[tokio::test]
async fn reap_stale_jobs_wrapper_runs_without_panicking() {
    // End-to-end sanity check on the public `reap_stale_jobs`
    // wrapper. The fixed `STALE_THRESHOLD` is too long to wait out,
    // so this assertion is just "no errors, returns a count" —
    // the actual revival math is covered by the direct trait tests
    // above.
    let s = fresh().await;
    let revived = reap_stale_jobs(&s).await.expect("reap_stale_jobs");
    assert_eq!(
        revived, 0,
        "no in-progress rows in a fresh store, nothing to revive"
    );
}
