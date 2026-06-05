//! End-to-end smoke test for the `SQLite` storage layer.
//!
//! Exercises the hot paths of all four traits against an in-memory
//! `SQLite` DB. The goal isn't full coverage (that's
//! `queue_runtime.rs`'s job once the runtime is on traits) — it's to
//! prove the trait surface, the SQL, and the row-mapping all line
//! up before we touch the runtime.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests crash loudly on setup/assert failures; that's the point"
)]

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;
use forge_jobs::storage::sqlite::SqliteStorage;
use forge_jobs::storage::{
    CronStorage, EnqueueOutcome, EnqueueRequest, FinalizeOutcome, JobQueue, JobStatus,
    NewCronSchedule, ProcessRegistry, QueueConfig,
};

async fn fresh() -> Arc<SqliteStorage> {
    Arc::new(
        SqliteStorage::open_in_memory()
            .await
            .expect("open_in_memory"),
    )
}

#[tokio::test]
async fn enqueue_then_claim_returns_the_row() {
    let s = fresh().await;
    let req = EnqueueRequest::new("noop_echo", json!({ "x": 1 })).on_queue("gh");
    let outcome = s.enqueue(req).await.unwrap();
    let id = match outcome {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => panic!("first enqueue can't dedupe"),
    };

    let claimed = s.claim_next("gh", "w-0").await.unwrap().expect("claim");
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.kind, "noop_echo");
    assert_eq!(claimed.status, JobStatus::InProgress);
    assert_eq!(claimed.attempts, 1, "claim increments attempts");
    assert_eq!(claimed.process_id.as_deref(), Some("w-0"));
}

#[tokio::test]
async fn dedupe_returns_existing_active_id() {
    let s = fresh().await;
    let req = || {
        EnqueueRequest::new("noop_echo", json!({}))
            .on_queue("gh")
            .with_dedupe_key("dup-1")
    };
    let a = s.enqueue(req()).await.unwrap();
    let b = s.enqueue(req()).await.unwrap();
    assert!(matches!(a, EnqueueOutcome::Enqueued(_)));
    assert!(matches!(b, EnqueueOutcome::Deduped(_)));
    assert_eq!(a.id(), b.id(), "dedupe returns same id");
}

#[tokio::test]
async fn claim_skips_future_scheduled_jobs() {
    let s = fresh().await;
    let future = Utc::now() + chrono::Duration::seconds(60);
    let _ = s
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_run_at(future),
        )
        .await
        .unwrap();
    assert!(s.claim_next("gh", "w-0").await.unwrap().is_none());
}

#[tokio::test]
async fn finalize_done_marks_completed() {
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(&id, FinalizeOutcome::Done).await.unwrap();
    let row = s.get_job(&id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Done);
    assert!(row.completed_at.is_some());
}

#[tokio::test]
async fn finalize_throttled_bumps_scheduled_at_and_un_attempts() {
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(
        &id,
        FinalizeOutcome::Throttled {
            retry_after: Duration::from_secs(30),
            cool_down_queue: false,
        },
    )
    .await
    .unwrap();
    let row = s.get_job(&id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Pending);
    assert!(row.scheduled_at > Utc::now() + chrono::Duration::seconds(20));
    assert_eq!(row.attempts, 0, "throttled doesn't burn a retry");
}

#[tokio::test]
async fn queue_cooldown_gate_blocks_siblings_then_clears_on_success() {
    let s = fresh().await;
    s.ensure_queue("gh", 1).await.unwrap();
    // Two eligible jobs so a candidate always exists for claim_next —
    // the only thing that can hold it back is the queue gate.
    let a = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let b = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };

    // Claim + throttle A with the queue cool-down engaged.
    let claimed = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    assert_eq!(claimed.id, a, "FIFO hands out A first");
    s.finalize(
        &a,
        FinalizeOutcome::Throttled {
            retry_after: Duration::from_mins(1),
            cool_down_queue: true,
        },
    )
    .await
    .unwrap();

    // B is pending + eligible, but the whole queue is gated.
    assert!(
        s.claim_next("gh", "w-0").await.unwrap().is_none(),
        "queue in cool-down hands out nothing even with an eligible row"
    );
    let cfg = s.get_queue("gh").await.unwrap().unwrap();
    assert_eq!(cfg.throttle_attempts, 1, "queue throttle counter bumped");
    assert!(cfg.throttled_until.is_some(), "cool-down deadline set");

    // A success from a still-in-flight job (started before the limit hit)
    // must NOT clear an active cool-down — otherwise the gate reopens
    // straight into the live rate limit. The window must run its course.
    s.finalize(&b, FinalizeOutcome::Done).await.unwrap();
    let cfg = s.get_queue("gh").await.unwrap().unwrap();
    assert_eq!(
        cfg.throttle_attempts, 1,
        "active cool-down survives an in-flight success"
    );
    assert!(
        cfg.throttled_until.is_some(),
        "deadline not cleared mid-window"
    );
}

#[tokio::test]
async fn queue_cooldown_clears_after_window_elapses() {
    let s = fresh().await;
    s.ensure_queue("gh", 1).await.unwrap();
    let a = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let b = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    // A zero-length cool-down → throttled_until is "now", i.e. already
    // elapsed by the time the next finalize runs.
    s.finalize(
        &a,
        FinalizeOutcome::Throttled {
            retry_after: Duration::from_secs(0),
            cool_down_queue: true,
        },
    )
    .await
    .unwrap();
    let cfg = s.get_queue("gh").await.unwrap().unwrap();
    assert_eq!(cfg.throttle_attempts, 1, "first throttle bumps the counter");

    // A success once the window has elapsed resets the curve.
    s.finalize(&b, FinalizeOutcome::Done).await.unwrap();
    let cfg = s.get_queue("gh").await.unwrap().unwrap();
    assert_eq!(
        cfg.throttle_attempts, 0,
        "counter resets after the window passes"
    );
    assert!(
        cfg.throttled_until.is_none(),
        "deadline cleared after the window"
    );
}

#[tokio::test]
async fn finalize_failed_appends_error_history() {
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(
        &id,
        FinalizeOutcome::Failed {
            retry_after: Duration::from_mins(1),
            message: "boom".into(),
        },
    )
    .await
    .unwrap();
    let row = s.get_job(&id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.last_error.as_deref(), Some("boom"));
    assert_eq!(row.error_history.len(), 1);
    assert_eq!(row.error_history[0].message, "boom");
}

#[tokio::test]
async fn requeue_batch_skips_dedupe_conflicts_instead_of_aborting() {
    // "Retry all" must not abort on the `jq_dedupe` UNIQUE index: a failed
    // job whose dedupe_key already has an ACTIVE sibling is skipped, the rest
    // are requeued.
    let store = fresh().await;

    // `conflicting`: failed job with dedupe "kb".
    let conflicting = match store
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_dedupe_key("kb"),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    store.claim_next("gh", "w-0").await.unwrap().unwrap();
    store
        .finalize(
            &conflicting,
            FinalizeOutcome::Failed {
                retry_after: Duration::from_mins(1),
                message: "x".into(),
            },
        )
        .await
        .unwrap();

    // `control`: failed job with dedupe "kc" (no active sibling).
    let control = match store
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_dedupe_key("kc"),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    store.claim_next("gh", "w-0").await.unwrap().unwrap();
    store
        .finalize(
            &control,
            FinalizeOutcome::Failed {
                retry_after: Duration::from_mins(1),
                message: "x".into(),
            },
        )
        .await
        .unwrap();

    // `active`: pending job reusing the conflicting job's dedupe "kb" —
    // allowed now that `conflicting` is 'failed' (out of the active dedupe
    // index). "kb" is active again.
    let active = match store
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_dedupe_key("kb"),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };

    // Retry all failed jobs: `conflicting` clashes with `active` on "kb" →
    // skipped; `control` has no conflict → requeued. Pre-fix this aborted the
    // whole batch with a UNIQUE error.
    let requeued = store
        .requeue_batch_by_status(None, JobStatus::Failed, 500)
        .await
        .unwrap();
    assert_eq!(
        requeued, 1,
        "only the non-conflicting failed job is requeued"
    );

    assert_eq!(
        store.get_job(&conflicting).await.unwrap().unwrap().status,
        JobStatus::Failed,
        "conflicting retry is skipped, left as failed"
    );
    assert_eq!(
        store.get_job(&control).await.unwrap().unwrap().status,
        JobStatus::Pending,
        "non-conflicting retry was requeued"
    );
    assert_eq!(
        store.get_job(&active).await.unwrap().unwrap().status,
        JobStatus::Pending,
        "active sibling untouched"
    );
}

#[tokio::test]
async fn claim_next_skips_failed_when_dedupe_sibling_is_active() {
    // Without the claim-time pre-filter, the worker would pick the failed
    // row (older id), try to flip 'failed' → 'in_progress', trip jq_dedupe
    // against the pending sibling, and loop forever on 1s backoffs.
    let store = fresh().await;

    // Failed retry, immediately eligible (retry_after = 0), dedupe "kb".
    let stuck = match store
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_dedupe_key("kb"),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    store.claim_next("gh", "w-0").await.unwrap().unwrap();
    store
        .finalize(
            &stuck,
            FinalizeOutcome::Failed {
                retry_after: Duration::from_secs(0),
                message: "x".into(),
            },
        )
        .await
        .unwrap();

    // Pending row sharing the same dedupe key (allowed because `stuck` left
    // the active index when it transitioned to 'failed').
    let active = match store
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_dedupe_key("kb"),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };

    let claimed = store
        .claim_next("gh", "w-0")
        .await
        .unwrap()
        .expect("a claimable row exists (the pending one)");
    assert_eq!(
        claimed.id, active,
        "filter skipped the conflicting failed row"
    );
    assert_eq!(
        store.get_job(&stuck).await.unwrap().unwrap().status,
        JobStatus::Failed,
        "stuck row left as failed, not flipped to in_progress"
    );
}

#[tokio::test]
async fn cleanup_superseded_retries_marks_redundant_failed_dead() {
    // A failed retry whose dedupe_key has an active sibling is redundant —
    // the sibling does the work. The boot sweep marks it dead with a reason
    // so a previously-stuck queue starts clean.
    let store = fresh().await;

    let superseded = match store
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_dedupe_key("kb"),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    store.claim_next("gh", "w-0").await.unwrap().unwrap();
    store
        .finalize(
            &superseded,
            FinalizeOutcome::Failed {
                retry_after: Duration::from_mins(1),
                message: "x".into(),
            },
        )
        .await
        .unwrap();

    // Control: a failed retry with no active sibling — must NOT be swept.
    let lone_failed = match store
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_dedupe_key("kc"),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    store.claim_next("gh", "w-0").await.unwrap().unwrap();
    store
        .finalize(
            &lone_failed,
            FinalizeOutcome::Failed {
                retry_after: Duration::from_mins(1),
                message: "x".into(),
            },
        )
        .await
        .unwrap();

    let active = match store
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_dedupe_key("kb"),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };

    let swept = store.cleanup_superseded_retries().await.unwrap();
    assert_eq!(
        swept, 1,
        "only the row whose key has an active sibling is swept"
    );

    let dead = store.get_job(&superseded).await.unwrap().unwrap();
    assert_eq!(dead.status, JobStatus::Dead, "redundant retry marked dead");
    assert_eq!(
        dead.last_error.as_deref(),
        Some("superseded by active sibling"),
        "reason recorded for the Dead tab"
    );
    assert_eq!(
        store.get_job(&lone_failed).await.unwrap().unwrap().status,
        JobStatus::Failed,
        "non-superseded failed retry left in place"
    );
    assert_eq!(
        store.get_job(&active).await.unwrap().unwrap().status,
        JobStatus::Pending,
        "the active sibling itself isn't touched"
    );
}

#[tokio::test]
async fn finalize_dead_marks_terminal() {
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(
        &id,
        FinalizeOutcome::Dead {
            message: "rip".into(),
        },
    )
    .await
    .unwrap();
    let row = s.get_job(&id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Dead);
    assert_eq!(row.last_error.as_deref(), Some("rip"));
    assert!(row.completed_at.is_some());
}

#[tokio::test]
async fn count_by_status_groups_correctly() {
    let s = fresh().await;
    // 4 pending; then claim one (it goes to in_progress) and finalize
    // *that* one as done. Final state: 3 pending, 0 in_progress, 1 done.
    for _ in 0..4 {
        s.enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
            .await
            .unwrap();
    }
    let claimed = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(&claimed.id, FinalizeOutcome::Done)
        .await
        .unwrap();

    let counts = s.count_by_status("gh").await.unwrap();
    assert_eq!(counts.pending, 3);
    assert_eq!(counts.in_progress, 0);
    assert_eq!(counts.done, 1);
}

#[tokio::test]
async fn oldest_ready_at_ignores_future_and_empty() {
    let s = fresh().await;
    // Empty queue → no ready job.
    assert!(s.oldest_ready_at("gh").await.unwrap().is_none());

    // A future-scheduled job is deferred, not lagging → still None.
    s.enqueue(
        EnqueueRequest::new("k", json!({}))
            .on_queue("gh")
            .with_run_at(Utc::now() + chrono::Duration::seconds(60)),
    )
    .await
    .unwrap();
    assert!(
        s.oldest_ready_at("gh").await.unwrap().is_none(),
        "future-scheduled rows don't count as lag"
    );

    // A ready job surfaces as the oldest-ready timestamp (in the past).
    s.enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap();
    let oldest = s.oldest_ready_at("gh").await.unwrap();
    assert!(oldest.is_some_and(|t| t <= Utc::now()));
}

#[tokio::test]
async fn completed_latencies_reports_done_jobs() {
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(&id, FinalizeOutcome::Done).await.unwrap();

    let from = Utc::now() - chrono::Duration::seconds(60);
    let to = Utc::now() + chrono::Duration::seconds(60);
    let lat = s.completed_latencies(None, from, to, 100).await.unwrap();
    assert_eq!(lat.len(), 1, "one done job in the window");
    assert!(lat[0].processing_ms >= 0);
    assert!(
        lat[0].total_ms >= lat[0].processing_ms,
        "total (enqueue→done) is at least processing (claim→done)"
    );
    // A window in the past excludes it.
    let past = Utc::now() - chrono::Duration::hours(2);
    let none = s
        .completed_latencies(None, past, past + chrono::Duration::seconds(1), 100)
        .await
        .unwrap();
    assert!(none.is_empty(), "out-of-window done jobs excluded");
}

#[tokio::test]
async fn metric_buckets_upsert_then_read_back() {
    use forge_jobs::storage::{MetricBucket, metric};

    let s = fresh().await;
    let t0 = Utc::now() - chrono::Duration::minutes(5);
    let t1 = t0 + chrono::Duration::minutes(1);

    let rows = vec![
        MetricBucket {
            queue: "gh".into(),
            metric: metric::PROC_MS.into(),
            bucket_start: t0,
            count: 3,
            sum: 600.0,
            p50: Some(100.0),
            p95: Some(250.0),
            p99: Some(300.0),
            max: 300.0,
        },
        MetricBucket {
            queue: "gh".into(),
            metric: metric::COMPLETED.into(),
            bucket_start: t0,
            count: 3,
            sum: 3.0,
            p50: None,
            p95: None,
            p99: None,
            max: 0.0,
        },
        MetricBucket {
            queue: "slack".into(),
            metric: metric::PROC_MS.into(),
            bucket_start: t1,
            count: 1,
            sum: 50.0,
            p50: Some(50.0),
            p95: Some(50.0),
            p99: Some(50.0),
            max: 50.0,
        },
    ];
    s.upsert_metric_buckets(&rows).await.unwrap();

    let from = t0 - chrono::Duration::seconds(1);
    let to = Utc::now() + chrono::Duration::seconds(1);

    // Queue filter + metric filter.
    let gh_proc = s
        .metric_buckets(Some("gh"), &[metric::PROC_MS], from, to)
        .await
        .unwrap();
    assert_eq!(gh_proc.len(), 1, "one gh proc_ms bucket");
    assert_eq!(gh_proc[0].count, 3);
    assert_eq!(gh_proc[0].p99, Some(300.0));

    // All queues, multiple metrics, ascending by bucket_start.
    let all = s
        .metric_buckets(None, &[metric::PROC_MS, metric::COMPLETED], from, to)
        .await
        .unwrap();
    assert_eq!(all.len(), 3, "two gh + one slack");
    assert!(
        all.windows(2)
            .all(|w| w[0].bucket_start <= w[1].bucket_start),
        "rows ascending by bucket_start"
    );

    // Upsert is idempotent — re-writing the same key overwrites.
    let updated = vec![MetricBucket {
        queue: "gh".into(),
        metric: metric::PROC_MS.into(),
        bucket_start: t0,
        count: 9,
        sum: 1800.0,
        p50: Some(150.0),
        p95: Some(400.0),
        p99: Some(500.0),
        max: 500.0,
    }];
    s.upsert_metric_buckets(&updated).await.unwrap();
    let after = s
        .metric_buckets(Some("gh"), &[metric::PROC_MS], from, to)
        .await
        .unwrap();
    assert_eq!(after.len(), 1, "still one row (overwrite, not insert)");
    assert_eq!(after[0].count, 9, "overwritten with new value");

    // Empty metrics slice → no rows, no query.
    let empty = s.metric_buckets(None, &[], from, to).await.unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn metrics_roll_once_aggregates_completed_job() {
    use forge_jobs::metrics_roll_once;
    use forge_jobs::storage::{Storage, metric};

    let s = fresh().await;
    s.ensure_queue("gh", 1).await.unwrap();
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(&id, FinalizeOutcome::Done).await.unwrap();

    // Roll with `now` two minutes ahead so the just-completed job's
    // minute bucket is closed and inside the lookback window.
    let storage = Storage::from_one(s.clone());
    let rolled = metrics_roll_once(&storage, Utc::now() + chrono::Duration::minutes(2))
        .await
        .unwrap();
    assert!(rolled >= 2, "at least a completed-count + proc_ms row");

    let from = Utc::now() - chrono::Duration::minutes(5);
    let to = Utc::now() + chrono::Duration::minutes(5);
    let proc = s
        .metric_buckets(Some("gh"), &[metric::PROC_MS], from, to)
        .await
        .unwrap();
    assert_eq!(proc.len(), 1, "one proc_ms bucket for gh");
    assert_eq!(proc[0].count, 1);
    assert!(proc[0].p50.is_some(), "latency percentile recorded");

    let completed = s
        .metric_buckets(Some("gh"), &[metric::COMPLETED], from, to)
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].count, 1, "one completion counted");

    // Re-rolling the same window is idempotent (upsert, not double-count).
    metrics_roll_once(&storage, Utc::now() + chrono::Duration::minutes(2))
        .await
        .unwrap();
    let proc2 = s
        .metric_buckets(Some("gh"), &[metric::PROC_MS], from, to)
        .await
        .unwrap();
    assert_eq!(proc2.len(), 1, "still one row after re-roll");
    assert_eq!(proc2[0].count, 1);
}

#[tokio::test]
async fn wait_for_work_returns_on_notify() {
    let s = fresh().await;
    // Enqueue first so the permit is stored on the Notify.
    s.enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap();
    let woke = s
        .wait_for_work("gh", Duration::from_millis(500))
        .await
        .unwrap();
    assert!(woke, "stored notify permit should be consumed");
}

#[tokio::test]
async fn wait_for_work_times_out_when_idle() {
    let s = fresh().await;
    let woke = s
        .wait_for_work("gh", Duration::from_millis(50))
        .await
        .unwrap();
    assert!(!woke, "idle wait should time out");
}

#[tokio::test]
async fn process_registry_register_then_list() {
    let s = fresh().await;
    s.register("gh-0", "gh", "host-A").await.unwrap();
    s.register("gh-1", "gh", "host-A").await.unwrap();
    s.register("slack-0", "slack", "host-A").await.unwrap();
    let gh = s.list(Some("gh")).await.unwrap();
    assert_eq!(gh.len(), 2);
    let all = s.list(None).await.unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn process_registry_heartbeat_self_heals_missing_row() {
    let s = fresh().await;
    // No prior register — heartbeat should still create a row.
    s.heartbeat("orphan-0", None).await.unwrap();
    let rows = s.list(None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].process_id, "orphan-0");
}

#[tokio::test]
async fn queue_config_ensure_then_get() {
    let s = fresh().await;
    s.ensure_queue("gh", 3).await.unwrap();
    s.ensure_queue("slack", 2).await.unwrap();
    // Idempotent + doesn't overwrite an existing row.
    s.ensure_queue("gh", 1).await.unwrap();
    let gh = s.get_queue("gh").await.unwrap().unwrap();
    assert_eq!(gh.max_workers, 3, "ensure must not overwrite");
    s.set_max_workers("gh", 5).await.unwrap();
    let gh = s.get_queue("gh").await.unwrap().unwrap();
    assert_eq!(gh.max_workers, 5);
    s.set_paused("gh", true).await.unwrap();
    assert!(s.get_queue("gh").await.unwrap().unwrap().paused);
}

#[tokio::test]
async fn cron_ensure_then_list() {
    let s = fresh().await;
    s.ensure_schedule(NewCronSchedule {
        name: "tickets_sync".into(),
        kind: "tickets_sync".into(),
        payload: json!({}),
        queue_name: None,
        cron_expr: "0 30 * * * Mon-Fri".into(),
        enabled: true,
        max_attempts: Some(3),
    })
    .await
    .unwrap();
    let rows = CronStorage::list_schedules(&*s).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "tickets_sync");
    assert!(rows[0].enabled);
}

#[tokio::test]
async fn cron_lease_grants_one_holder_and_renews() {
    let s = fresh().await;
    let ttl = Duration::from_mins(1);
    // First holder wins the empty lease.
    assert!(s.try_cron_lease("host-a", ttl).await.unwrap());
    // A different holder is denied while the lease is valid.
    assert!(
        !s.try_cron_lease("host-b", ttl).await.unwrap(),
        "second holder must not steal a live lease"
    );
    // The current holder renews freely.
    assert!(
        s.try_cron_lease("host-a", ttl).await.unwrap(),
        "lease holder renews its own lease"
    );
}

#[tokio::test]
async fn retry_cycle_emits_balanced_events() {
    // Drive one job through Throttled -> re-claim -> Done. The
    // event log should hold [Enqueued, Started, Retried, Started,
    // Completed]; with the chart's running-diff formula
    // `started - completed - failed - retried`, the gauge returns
    // to 0 at the end. This is the fix for the "in-flight drifts
    // upward" bug that retries used to cause.
    use forge_jobs::TimelineEventType;

    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(
        &id,
        FinalizeOutcome::Throttled {
            retry_after: Duration::from_millis(1),
            cool_down_queue: false,
        },
    )
    .await
    .unwrap();
    // Re-claim after the throttle window. scheduled_at is in the
    // very near future; nudge with a sleep so claim_next sees it.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(&id, FinalizeOutcome::Done).await.unwrap();

    let now = Utc::now();
    let events = s
        .list_for_timeline(
            now - chrono::Duration::seconds(60),
            now + chrono::Duration::seconds(60),
        )
        .await
        .unwrap();
    let kinds: Vec<TimelineEventType> = events.iter().map(|e| e.event_type).collect();
    assert_eq!(
        kinds,
        vec![
            TimelineEventType::Enqueued,
            TimelineEventType::Started,
            TimelineEventType::Retried,
            TimelineEventType::Started,
            TimelineEventType::Completed,
        ],
        "expected balanced retry sequence; got {kinds:?}",
    );

    // Running diff: 2 starts - 1 completed - 0 failed - 1 retried = 0.
    let count_of = |t: TimelineEventType| -> i64 {
        i64::try_from(events.iter().filter(|e| e.event_type == t).count()).unwrap_or(i64::MAX)
    };
    let started = count_of(TimelineEventType::Started);
    let completed = count_of(TimelineEventType::Completed);
    let failed = count_of(TimelineEventType::Failed);
    let retried = count_of(TimelineEventType::Retried);
    assert_eq!(started - completed - failed - retried, 0);
}

#[tokio::test]
async fn delete_cascades_to_queue_event_rows() {
    // After delete(job_id), the matching queue_event rows are gone
    // — so the chart no longer carries `started` events that have no
    // surviving row to balance them. Legacy rows (job_id = NULL,
    // inserted before this migration) aren't touched.
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    s.finalize(&id, FinalizeOutcome::Done).await.unwrap();

    // Pre-delete: 3 events (Enqueued, Started, Completed).
    let now = Utc::now();
    let pre = s
        .list_for_timeline(
            now - chrono::Duration::seconds(60),
            now + chrono::Duration::seconds(60),
        )
        .await
        .unwrap();
    assert_eq!(pre.len(), 3);

    assert!(s.delete(&id).await.unwrap());

    let post = s
        .list_for_timeline(
            now - chrono::Duration::seconds(60),
            now + chrono::Duration::seconds(60),
        )
        .await
        .unwrap();
    assert!(
        post.is_empty(),
        "expected cascade-delete to clear events; got {post:?}"
    );
}

#[tokio::test]
async fn delete_batch_by_status_processes_in_chunks() {
    // Enqueue 7 jobs, finalize 5 as Done. Batch-delete with size 2
    // should remove 2, 2, 1, then return 0 — and the matching
    // queue_event rows must cascade in the same tx.
    let s = fresh().await;
    for i in 0..7 {
        let req = EnqueueRequest::new("k", json!({ "i": i })).on_queue("gh");
        let id = match s.enqueue(req).await.unwrap() {
            EnqueueOutcome::Enqueued(id) => id,
            EnqueueOutcome::Deduped(_) => unreachable!(),
        };
        let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
        if i < 5 {
            s.finalize(&id, FinalizeOutcome::Done).await.unwrap();
        }
    }

    // First batch: 2 deleted.
    let n = s
        .delete_batch_by_status(Some("gh"), JobStatus::Done, 2)
        .await
        .unwrap();
    assert_eq!(n, 2);

    // Second batch: 2 more.
    let n = s
        .delete_batch_by_status(Some("gh"), JobStatus::Done, 2)
        .await
        .unwrap();
    assert_eq!(n, 2);

    // Third batch: last one.
    let n = s
        .delete_batch_by_status(Some("gh"), JobStatus::Done, 2)
        .await
        .unwrap();
    assert_eq!(n, 1);

    // Fourth batch: empty — caller's break signal.
    let n = s
        .delete_batch_by_status(Some("gh"), JobStatus::Done, 2)
        .await
        .unwrap();
    assert_eq!(n, 0);

    // The 2 still-in_progress rows (the unfinalized ones) remain.
    let remaining = s
        .list_by_status(Some("gh"), JobStatus::InProgress, 100)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 2);
}

#[tokio::test]
async fn requeue_batch_by_status_resets_pending() {
    // 3 jobs reach `failed` (the retry pool). requeue_batch_by_status
    // flips them all back to `pending` in one tx.
    let s = fresh().await;

    // Phase 1: enqueue 3 jobs.
    for i in 0..3 {
        let req = EnqueueRequest::new("k", json!({ "i": i })).on_queue("gh");
        let _ = s.enqueue(req).await.unwrap();
    }

    // Phase 2: claim + finalize-as-Failed each. Use the *claimed* id
    // (FIFO via claim_next) — not an enqueue id — because a
    // retry-eligible `failed` row from a prior iteration would
    // re-win the claim race. Long retry_after parks the row in
    // `failed` so subsequent claims target the next pending row.
    for _ in 0..3 {
        let claimed = s.claim_next("gh", "w-0").await.unwrap().unwrap();
        s.finalize(
            &claimed.id,
            FinalizeOutcome::Failed {
                retry_after: std::time::Duration::from_mins(1),
                message: "boom".into(),
            },
        )
        .await
        .unwrap();
    }

    let failed_before = s
        .list_by_status(Some("gh"), JobStatus::Failed, 100)
        .await
        .unwrap();
    assert_eq!(failed_before.len(), 3);

    let n = s
        .requeue_batch_by_status(Some("gh"), JobStatus::Failed, 10)
        .await
        .unwrap();
    assert_eq!(n, 3);

    let pending = s
        .list_by_status(Some("gh"), JobStatus::Pending, 100)
        .await
        .unwrap();
    assert_eq!(pending.len(), 3);
    let failed_after = s
        .list_by_status(Some("gh"), JobStatus::Failed, 100)
        .await
        .unwrap();
    assert!(failed_after.is_empty());
}

#[tokio::test]
async fn list_scheduled_after_returns_future_pending_ordered() {
    // Three jobs with explicit run_at: one in the past, one +1min,
    // one +5min. list_scheduled_after(anchor) returns the two
    // futures in ascending order; the past-row is excluded.
    // Anchor is fixed so the filter is independent of test wallclock.
    let s = fresh().await;
    let anchor = Utc::now();
    let _id_past = match s
        .enqueue(
            EnqueueRequest::new("k", json!({ "i": 0 }))
                .on_queue("gh")
                .with_run_at(anchor - chrono::Duration::seconds(60)),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _id_5m = match s
        .enqueue(
            EnqueueRequest::new("k", json!({ "i": 5 }))
                .on_queue("gh")
                .with_run_at(anchor + chrono::Duration::minutes(5)),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _id_1m = match s
        .enqueue(
            EnqueueRequest::new("k", json!({ "i": 1 }))
                .on_queue("gh")
                .with_run_at(anchor + chrono::Duration::minutes(1)),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };

    let rows = s
        .list_scheduled_after(Some("gh"), anchor, 100)
        .await
        .unwrap();
    let payload_is = rows
        .iter()
        .map(|r| {
            r.payload
                .get("i")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(-1)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        payload_is,
        vec![1, 5],
        "ordered ascending; past row excluded"
    );
}

#[tokio::test]
async fn run_now_advances_pending_scheduled_at() {
    // Schedule a job 5 min out, then run_now: scheduled_at moves to
    // ~now and claim_next picks it up immediately.
    let s = fresh().await;
    let now = Utc::now();
    let id = match s
        .enqueue(
            EnqueueRequest::new("k", json!({}))
                .on_queue("gh")
                .with_run_at(now + chrono::Duration::minutes(5)),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };

    // Pre: not yet eligible.
    assert!(s.claim_next("gh", "w-0").await.unwrap().is_none());

    // run_now → eligible.
    assert!(s.run_now(&id).await.unwrap());
    let claimed = s.claim_next("gh", "w-0").await.unwrap().expect("claim");
    assert_eq!(claimed.id, id);
}

#[tokio::test]
async fn run_now_no_op_on_non_pending() {
    // run_now only touches `pending`. An in_progress / done / failed
    // / dead row returns false without side effects.
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    // Now status = in_progress.
    assert!(!s.run_now(&id).await.unwrap(), "must not touch in_progress");
    s.finalize(&id, FinalizeOutcome::Done).await.unwrap();
    // Now status = done.
    assert!(!s.run_now(&id).await.unwrap(), "must not touch done");
}

#[tokio::test]
async fn claim_next_is_fifo_within_priority_and_scheduled_at() {
    // Enqueue 5 rows back-to-back at the same priority. claim_next
    // 5 times. Order claimed must match enqueue order — even though
    // scheduled_at may differ only by microseconds and could tie
    // under coarse clock resolution. The `id ASC` tiebreaker
    // guarantees insertion order regardless.
    let s = fresh().await;
    let mut enq_ids = Vec::new();
    for i in 0..5 {
        let id = match s
            .enqueue(EnqueueRequest::new("k", json!({ "i": i })).on_queue("gh"))
            .await
            .unwrap()
        {
            EnqueueOutcome::Enqueued(id) => id,
            EnqueueOutcome::Deduped(_) => unreachable!(),
        };
        enq_ids.push(id);
    }

    let mut claimed_ids = Vec::new();
    for _ in 0..5 {
        let claimed = s.claim_next("gh", "w-0").await.unwrap().expect("claim");
        claimed_ids.push(claimed.id);
    }

    assert_eq!(claimed_ids, enq_ids, "claim_next must be FIFO");
}

#[tokio::test]
async fn delete_in_progress_sets_cancel_flag_and_keeps_row() {
    // `delete` on an `in_progress` row must NOT remove it — instead
    // it sets `cancel_requested_at` so the owning worker's heartbeat
    // observes it and stops the handler. The row stays until the
    // worker finalizes.
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();

    // delete returns true but doesn't remove the row.
    assert!(s.delete(&id).await.unwrap());
    let job = s.get_job(&id).await.unwrap().expect("row still present");
    assert_eq!(job.status, JobStatus::InProgress);

    // Heartbeat picks up the cancel flag.
    let cancel_requested = s.heartbeat_job(&id, "w-0").await.unwrap();
    assert!(
        cancel_requested,
        "heartbeat_job must surface the cancel flag set by delete"
    );
}

#[tokio::test]
async fn delete_pending_still_removes_row() {
    // Baseline: non-running statuses keep today's behaviour.
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };

    assert!(s.delete(&id).await.unwrap());
    assert!(
        s.get_job(&id).await.unwrap().is_none(),
        "row should be gone"
    );
}

#[tokio::test]
async fn claim_next_clears_stale_cancel_flag_from_previous_attempt() {
    // A row may carry `cancel_requested_at` set from a prior
    // in-progress life (delete fired, worker finalized to failed
    // before observing, user retried). The next claim must start
    // clean — otherwise the new attempt would immediately re-cancel.
    let s = fresh().await;
    let id = match s
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        EnqueueOutcome::Deduped(_) => unreachable!(),
    };
    let _ = s.claim_next("gh", "w-0").await.unwrap().unwrap();
    assert!(s.delete(&id).await.unwrap()); // sets cancel flag

    // Worker finalizes (failed); user retries via requeue → pending.
    s.finalize(
        &id,
        FinalizeOutcome::Failed {
            retry_after: Duration::ZERO,
            message: "cancelled".to_owned(),
        },
    )
    .await
    .unwrap();
    assert!(s.requeue(&id).await.unwrap());

    // Re-claim — cancel flag must be NULL on the new attempt.
    let _ = s.claim_next("gh", "w-1").await.unwrap().unwrap();
    let cancel_requested = s.heartbeat_job(&id, "w-1").await.unwrap();
    assert!(
        !cancel_requested,
        "claim_next must clear cancel_requested_at from previous attempts"
    );
}
