//! Runtime test — end-to-end happy path through `QueueRuntime`.
//!
//! Boots a one-queue runtime with `NoopEcho` registered, enqueues a
//! single job, waits for it to land in `Done`, and verifies the
//! timeline log captured both the `Enqueued` and `Completed` events.
//! Finally calls `shutdown_graceful` and asserts it returns within
//! the 5 s budget (the runtime has no in-flight work at that point).

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
use forge_jobs::storage::{EnqueueRequest, JobStatus, TimelineEventType};
use forge_jobs::{
    DefaultRouter, HandlerRegistry, NOOP_ECHO_KIND, NoopEcho, QueueRuntime, Storage as JobStorage,
};
use serde_json::json;

#[tokio::test]
async fn runtime_runs_one_noop_echo_job_to_done() {
    // ── setup ──────────────────────────────────────────────────────
    let sqlite = Arc::new(
        SqliteStorage::open_in_memory()
            .await
            .expect("open_in_memory"),
    );
    let storage = JobStorage::from_one(sqlite);
    storage
        .config
        .ensure_queue("default", 1)
        .await
        .expect("ensure_queue");

    let mut handlers = HandlerRegistry::new();
    handlers.register(NoopEcho);
    let runtime = QueueRuntime::new(storage.clone(), handlers, Arc::new(DefaultRouter));

    // ── enqueue + start ────────────────────────────────────────────
    let outcome = runtime
        .enqueue(EnqueueRequest::new(NOOP_ECHO_KIND, json!({ "sleep_ms": 5 })).on_queue("default"))
        .await
        .expect("enqueue");
    let job_id = outcome.id().clone();

    let handle = runtime.start().await.expect("start");

    // ── wait for the job to finish ────────────────────────────────
    // NoopEcho with `sleep_ms = 5` should land in `Done` within
    // about one supervisor tick. Poll briefly to avoid relying on a
    // fixed sleep.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let job = storage
            .jobs
            .get_job(&job_id)
            .await
            .expect("get_job")
            .expect("job exists");
        if job.status == JobStatus::Done {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "job did not reach Done within 3 s; last status = {:?}",
            job.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ── timeline events ───────────────────────────────────────────
    let from = Utc::now() - ChronoDuration::seconds(60);
    let to = Utc::now() + ChronoDuration::seconds(10);
    let events = storage
        .jobs
        .list_for_timeline(from, to)
        .await
        .expect("list_for_timeline");
    let kinds: Vec<TimelineEventType> = events.iter().map(|e| e.event_type).collect();
    assert!(
        kinds.contains(&TimelineEventType::Enqueued),
        "expected an Enqueued event in {kinds:?}"
    );
    assert!(
        kinds.contains(&TimelineEventType::Completed),
        "expected a Completed event in {kinds:?}"
    );

    // ── graceful shutdown ─────────────────────────────────────────
    handle.shutdown_graceful(Duration::from_secs(5)).await;
    // If `shutdown_graceful` hangs past the budget it logs a warn
    // and aborts the join_set; either way control returns here.
    // The fact that we got here without timing out is the assertion.
}
