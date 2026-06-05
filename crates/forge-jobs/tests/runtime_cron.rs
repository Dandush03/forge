//! Runtime test — `cron_tick_once`.
//!
//! Exercises the four cron behaviours (fire / not-yet-due / seed /
//! parse-error) against an in-memory `SQLite` store + the trivial
//! `DefaultRouter`. The handlers themselves never run — `cron_tick_once`
//! only enqueues jobs onto `sync_queue`, which we then count via
//! `JobQueue::count_by_status` to verify the fire.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests crash loudly on setup/assert failures; that's the point"
)]

use std::sync::Arc;

use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use forge_jobs::storage::NewCronSchedule;
use forge_jobs::storage::sqlite::SqliteStorage;
use forge_jobs::{DefaultRouter, Storage as JobStorage, cron_tick_once};

async fn fresh() -> JobStorage {
    let s = Arc::new(
        SqliteStorage::open_in_memory()
            .await
            .expect("open_in_memory"),
    );
    JobStorage::from_one(s)
}

fn schedule(name: &str, expr: &str) -> NewCronSchedule {
    NewCronSchedule {
        name: name.into(),
        kind: name.into(),
        payload: json!({}),
        queue_name: None,
        cron_expr: expr.into(),
        enabled: true,
        max_attempts: Some(3),
    }
}

/// Force `next_fire_at` to a specific value so a tick at `now` either
/// finds it due or not. `CronStorage::ensure_schedule` leaves
/// `next_fire_at` as `None` (seed-on-first-tick semantics), so tests
/// that want the row already-seeded call this helper afterwards.
async fn set_next_fire_at(storage: &JobStorage, name: &str, when: chrono::DateTime<Utc>) {
    // `CronStorage::set_next_fire_at` isn't on the public trait, but
    // `record_fire` does the same write as a side effect: it sets
    // `last_fired_at` (we don't care) and `next_fire_at` (we do).
    storage
        .cron
        .record_fire(name, Utc::now(), when)
        .await
        .expect("seed next_fire_at via record_fire");
}

#[tokio::test]
async fn try_advance_fire_is_a_compare_and_swap() {
    let s = fresh().await;
    s.cron
        .ensure_schedule(schedule("noop", "*/5 * * * * *"))
        .await
        .expect("ensure");
    let due = Utc::now() - ChronoDuration::seconds(30);
    set_next_fire_at(&s, "noop", due).await;

    let now = Utc::now();
    let next = now + ChronoDuration::seconds(5);
    // First claimer sees next_fire_at == due → wins.
    assert!(
        s.cron
            .try_advance_fire("noop", due, now, next)
            .await
            .unwrap(),
        "first claim of a due fire wins"
    );
    // A second leader that read the same `due` now loses — next_fire_at
    // has advanced, so the CAS fails and it won't double-enqueue.
    assert!(
        !s.cron
            .try_advance_fire("noop", due, now, next)
            .await
            .unwrap(),
        "stale claim of an already-advanced fire loses"
    );
}

#[tokio::test]
async fn fires_one_job_when_schedule_is_due() {
    let s = fresh().await;
    s.cron
        .ensure_schedule(schedule("noop_due", "*/5 * * * * *"))
        .await
        .expect("ensure");

    // Past `next_fire_at` so the tick at `now` finds it due.
    let now = Utc::now();
    set_next_fire_at(&s, "noop_due", now - ChronoDuration::seconds(30)).await;

    let router = DefaultRouter;
    let report = cron_tick_once(&s, &router, now)
        .await
        .expect("cron_tick_once");
    assert_eq!(
        report.fired, 1,
        "schedule whose next_fire_at is in the past should fire"
    );
    assert_eq!(report.seeded, 0);
    assert_eq!(report.errors, 0);

    // Verify a job actually landed. Use `list_by_status` with
    // `queue = None` so we don't depend on `ensure_queue` having been
    // called (`cron_tick_once` doesn't auto-register the destination
    // queue row — that's the host's job at boot).
    let pending = s
        .jobs
        .list_by_status(None, forge_jobs::JobStatus::Pending, 100)
        .await
        .expect("list pending");
    assert_eq!(
        pending.len(),
        1,
        "expected one pending job after fire; got {pending:?}"
    );
}

#[tokio::test]
async fn does_not_fire_when_not_due_yet() {
    let s = fresh().await;
    s.cron
        .ensure_schedule(schedule("noop_future", "*/5 * * * * *"))
        .await
        .expect("ensure");

    let now = Utc::now();
    // Far enough in the future that any reasonable tick clock won't
    // race past it before the assertion runs.
    set_next_fire_at(&s, "noop_future", now + ChronoDuration::hours(1)).await;

    let router = DefaultRouter;
    let report = cron_tick_once(&s, &router, now)
        .await
        .expect("cron_tick_once");
    assert_eq!(report.fired, 0);
    assert_eq!(report.seeded, 0);
    assert_eq!(report.errors, 0);
}

#[tokio::test]
async fn seeds_next_fire_at_for_never_fired_row() {
    let s = fresh().await;
    s.cron
        .ensure_schedule(schedule("noop_seed", "*/5 * * * * *"))
        .await
        .expect("ensure");

    // Don't call `set_next_fire_at` — the row starts with
    // `next_fire_at = None`, which is the "never fired" seed case.
    let router = DefaultRouter;
    let report = cron_tick_once(&s, &router, Utc::now())
        .await
        .expect("cron_tick_once");
    assert_eq!(report.fired, 0, "first tick should seed, not fire");
    assert_eq!(report.seeded, 1);
    assert_eq!(report.errors, 0);

    // After seeding the row should have a non-None next_fire_at.
    let row = s
        .cron
        .get_schedule("noop_seed")
        .await
        .expect("get_schedule")
        .expect("schedule exists");
    assert!(
        row.next_fire_at.is_some(),
        "seed-on-first-tick must populate next_fire_at; got {row:?}"
    );
}

#[tokio::test]
async fn records_parse_error_for_invalid_cron_expr() {
    let s = fresh().await;
    s.cron
        .ensure_schedule(schedule("noop_bad", "this is not a cron"))
        .await
        .expect("ensure");

    let router = DefaultRouter;
    let report = cron_tick_once(&s, &router, Utc::now())
        .await
        .expect("cron_tick_once");
    assert_eq!(report.fired, 0);
    assert_eq!(report.seeded, 0);
    assert_eq!(
        report.errors, 1,
        "invalid cron_expr should bump errors and be persisted"
    );

    let row = s
        .cron
        .get_schedule("noop_bad")
        .await
        .expect("get_schedule")
        .expect("schedule exists");
    assert!(
        row.last_error.is_some(),
        "record_parse_error must populate last_error; got {row:?}"
    );
}
