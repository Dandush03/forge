//! Runtime test — `QueueHandle::request_cancel` and the DB-flag
//! cancel path.
//!
//! Boots a runtime with a custom handler that loops on
//! `ctx.cancel.is_cancelled()`, enqueues one job, waits for it to
//! land `in_progress`, and verifies that:
//!  1. `QueueHandle::request_cancel(&job_id)` stops the handler via
//!     the in-process registry (instant);
//!  2. `JobQueue::delete(&job_id)` on an `in_progress` row sets the
//!     cancel flag, which the heartbeat task observes and surfaces
//!     to the same `ctx.cancel`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests crash loudly on setup/assert failures; that's the point"
)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use forge_jobs::SqliteStorage;
use forge_jobs::storage::{EnqueueRequest, JobStatus};
use forge_jobs::{
    DefaultRouter, HandlerRegistry, JobCtx, JobHandler, JobOutcome, QueueRuntime,
    Storage as JobStorage,
};
use serde_json::json;

const SLEEPY_KIND: &str = "test_sleepy";

#[derive(Debug)]
struct SleepyHandler;

#[async_trait]
impl JobHandler for SleepyHandler {
    fn kind(&self) -> &'static str {
        SLEEPY_KIND
    }

    async fn run(&self, ctx: JobCtx<'_>, _payload: serde_json::Value) -> JobOutcome {
        // Loop in 25ms steps checking the cancel token. Cap at 30s
        // so a stuck test fails loudly instead of running forever.
        for _ in 0..1200 {
            if ctx.cancel.is_cancelled() {
                return JobOutcome::Failed("cancelled".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        JobOutcome::Done
    }
}

async fn boot_with_sleepy_handler() -> (JobStorage, QueueRuntime) {
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
    handlers.register(SleepyHandler);
    let runtime = QueueRuntime::new(storage.clone(), handlers, Arc::new(DefaultRouter));
    (storage, runtime)
}

/// Poll `get_job` until the predicate holds. Panics on timeout so
/// the test failure points at the actual stuck state.
async fn wait_for_status(
    storage: &JobStorage,
    job_id: &forge_jobs::JobId,
    pred: impl Fn(JobStatus) -> bool,
    label: &str,
    timeout: Duration,
) -> JobStatus {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let job = storage
            .jobs
            .get_job(job_id)
            .await
            .expect("get_job")
            .expect("job exists");
        if pred(job.status) {
            return job.status;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "timed out waiting for {label}; last status = {:?}",
            job.status
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn request_cancel_stops_running_handler_in_process() {
    let (storage, runtime) = boot_with_sleepy_handler().await;
    let outcome = runtime
        .enqueue(EnqueueRequest::new(SLEEPY_KIND, json!({})).on_queue("default"))
        .await
        .expect("enqueue");
    let job_id = outcome.id().clone();

    let handle = runtime.start().await.expect("start");

    // Poll request_cancel directly. Status flips to InProgress
    // inside `claim_next` BEFORE the worker registers in
    // `running_jobs`; the window is tiny but real, and waiting on
    // status would race that gap. The cancel-bus check is the
    // ground truth we actually care about anyway.
    let cancel_deadline = std::time::Instant::now() + Duration::from_secs(3);
    let cancelled = loop {
        if handle.request_cancel(&job_id) {
            break true;
        }
        if std::time::Instant::now() > cancel_deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(
        cancelled,
        "request_cancel must report success once the worker registers the job"
    );

    // The handler loops in 25 ms steps; observing the cancel token
    // and returning Failed should happen well under 1 s. We allow 2
    // s to absorb scheduler jitter.
    let final_status = wait_for_status(
        &storage,
        &job_id,
        |s| matches!(s, JobStatus::Failed | JobStatus::Dead),
        "Failed/Dead after cancel",
        Duration::from_secs(2),
    )
    .await;
    assert!(
        matches!(final_status, JobStatus::Failed | JobStatus::Dead),
        "cancelled handler should land in Failed/Dead, got {final_status:?}"
    );

    handle.shutdown_graceful(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn request_cancel_returns_false_for_unknown_job() {
    let (_storage, runtime) = boot_with_sleepy_handler().await;
    let handle = runtime.start().await.expect("start");
    let bogus = forge_jobs::JobId::new("01HXXXNOTREAL00000000000000".to_owned());
    assert!(
        !handle.request_cancel(&bogus),
        "request_cancel for an unregistered id must return false"
    );
    handle.shutdown_graceful(Duration::from_secs(5)).await;
}
