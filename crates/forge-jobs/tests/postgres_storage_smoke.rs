//! End-to-end smoke tests for the Postgres storage adapter.
//!
//! Gated behind `--features postgres` — the default `cargo test` doesn't
//! compile or run these. Two ways to provide a Postgres (see `bootstrap`):
//!
//! - **testcontainers (default)** — each test starts its own ephemeral
//!   `postgres` container. Needs a reachable Docker daemon (set
//!   `DOCKER_HOST` for non-default sockets, e.g. colima). Isolated but
//!   slow (~5-10s per container).
//!
//!   ```text
//!   cargo test -p forge-jobs --features postgres --test postgres_storage_smoke
//!   ```
//!
//! - **external server (`TEST_DATABASE_URL`)** — point at a standing
//!   Postgres (the repo's `docker-compose.yml`, or the CI service). Each
//!   test creates and drops its own `forge_test_*` database, so parallel
//!   tests stay isolated. Much faster (no per-test container).
//!
//!   ```text
//!   docker compose up -d postgres
//!   TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5433/postgres \
//!     cargo test -p forge-jobs --features postgres --test postgres_storage_smoke
//!   ```

#![cfg(feature = "postgres")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests crash loudly on setup/assert failures; that's the point"
)]
#![allow(
    clippy::manual_let_else,
    reason = "match-on-EnqueueOutcome with a wildcard arm is the SemVer-safe shape for a `#[non_exhaustive]` enum; `let...else` would lose the named happy-path variant"
)]

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use forge_jobs::PostgresStorage;
use forge_jobs::TimelineEventType;
use forge_jobs::storage::{
    CronStorage, DeleteOutcome, EnqueueOutcome, EnqueueRequest, FinalizeOutcome, JobQueue,
    JobStatus, NewCronSchedule, ProcessRegistry, QueueConfig,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ulid::Ulid;

/// Open storage plus whatever keeps its database alive for the test.
///
/// Two modes:
/// - **Container** (default): an ephemeral `testcontainers` Postgres,
///   isolated per test by virtue of being its own container.
/// - **External** (`TEST_DATABASE_URL` set): a standing server (the
///   `docker-compose` Postgres / the CI service). Each test gets its own
///   freshly-created database so parallel tests stay isolated, dropped on
///   completion.
///
/// `storage` is declared before `_guard` so it (and its connection pool +
/// listener task) drops first, before the guard drops the database.
struct Bootstrapped {
    storage: Arc<PostgresStorage>,
    _guard: TestDbGuard,
}

#[allow(
    dead_code,
    clippy::large_enum_variant,
    reason = "Container holds the testcontainers handle purely for its Drop (which stops the container) — never read. The size skew vs External is irrelevant for a per-test helper that constructs exactly one."
)]
enum TestDbGuard {
    /// Container teardown drops the whole server with the database.
    Container(ContainerAsync<Postgres>),
    /// A per-test database on a shared server; dropped on completion.
    External { admin_url: String, db: String },
}

impl Drop for TestDbGuard {
    fn drop(&mut self) {
        let Self::External { admin_url, db } = self else {
            return;
        };
        let admin_url = admin_url.clone();
        let db = db.clone();
        // Drop the per-test database on a dedicated OS thread with its own
        // runtime — we can't block on the test's runtime inside Drop, and
        // it may already be tearing down. `WITH (FORCE)` evicts any
        // lingering connection (e.g. the storage's listener) so the DROP
        // can't be blocked by one. Best-effort: a leaked test DB is
        // harmless and `docker compose down -v` resets the volume.
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async move {
                if let Ok(pool) = PgPoolOptions::new().connect(&admin_url).await {
                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"))
                        .execute(&pool)
                        .await;
                    pool.close().await;
                }
            });
        })
        .join();
    }
}

async fn bootstrap() -> Bootstrapped {
    match std::env::var("TEST_DATABASE_URL") {
        Ok(url) if !url.trim().is_empty() => bootstrap_external(&url).await,
        _ => bootstrap_container().await,
    }
}

/// Ephemeral container, one per test (the default, no env needed).
async fn bootstrap_container() -> Bootstrapped {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let storage = Arc::new(
        PostgresStorage::open(&url, 10)
            .await
            .expect("open PostgresStorage"),
    );
    Bootstrapped {
        storage,
        _guard: TestDbGuard::Container(container),
    }
}

/// A fresh per-test database on the server `TEST_DATABASE_URL` points at
/// (the compose / CI Postgres). Creating a database per test keeps
/// parallel tests isolated on one shared server.
async fn bootstrap_external(admin_url: &str) -> Bootstrapped {
    // Unique, lowercase, valid-identifier db name. ULID is Crockford
    // base32 (upper); lowercased + the `forge_test_` prefix → a-z0-9_
    // starting with a letter, well under PG's 63-char identifier cap.
    let db = format!(
        "forge_test_{}",
        Ulid::new().to_string().to_ascii_lowercase()
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await
        .expect("connect TEST_DATABASE_URL");
    sqlx::query(&format!("CREATE DATABASE \"{db}\""))
        .execute(&admin)
        .await
        .expect("create per-test database");
    admin.close().await;

    let url = with_database(admin_url, &db);
    let storage = Arc::new(
        PostgresStorage::open(&url, 10)
            .await
            .expect("open PostgresStorage on per-test database"),
    );
    Bootstrapped {
        storage,
        _guard: TestDbGuard::External {
            admin_url: admin_url.to_owned(),
            db,
        },
    }
}

/// Swap the database name in a `postgres://…/<db>[?query]` URL.
fn with_database(base: &str, db: &str) -> String {
    let (pre, query) = base
        .split_once('?')
        .map_or((base, None), |(p, q)| (p, Some(q)));
    let slash = pre.rfind('/').expect("postgres URL has a path slash");
    let mut out = format!("{}/{db}", &pre[..slash]);
    if let Some(q) = query {
        out.push('?');
        out.push_str(q);
    }
    out
}

#[tokio::test]
async fn describe_reports_backend_and_server_version() {
    let b = bootstrap().await;
    let info = b.storage.describe().await.unwrap();
    assert_eq!(info.backend, "postgres");
    let has_version = info
        .fields
        .iter()
        .any(|(k, v)| k == "server_version" && !v.is_empty());
    assert!(
        has_version,
        "server_version field missing: {:?}",
        info.fields
    );
}

#[tokio::test]
async fn enqueue_then_claim_returns_the_row() {
    let b = bootstrap().await;
    let req = EnqueueRequest::new("noop_echo", json!({ "x": 1 })).on_queue("gh");
    let outcome = b.storage.enqueue(req).await.unwrap();
    let id = match outcome {
        EnqueueOutcome::Enqueued(id) => id,
        _ => unreachable!(),
    };

    let claimed = b
        .storage
        .claim_next("gh", "w-0")
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(claimed.id, id);
    assert_eq!(claimed.kind, "noop_echo");
    assert_eq!(claimed.status, JobStatus::InProgress);
    assert_eq!(claimed.attempts, 1);
    assert_eq!(claimed.process_id.as_deref(), Some("w-0"));
}

#[tokio::test]
async fn dedupe_returns_existing_active_id() {
    let b = bootstrap().await;
    let req = || {
        EnqueueRequest::new("noop_echo", json!({}))
            .on_queue("gh")
            .with_dedupe_key("dup-1")
    };
    let a = b.storage.enqueue(req()).await.unwrap();
    let c = b.storage.enqueue(req()).await.unwrap();
    assert!(matches!(a, EnqueueOutcome::Enqueued(_)));
    assert!(matches!(c, EnqueueOutcome::Deduped(_)));
    assert_eq!(a.id(), c.id());
}

#[tokio::test]
async fn claim_skips_future_scheduled_jobs() {
    let b = bootstrap().await;
    let future = Utc::now() + chrono::Duration::seconds(60);
    let _ = b
        .storage
        .enqueue(
            EnqueueRequest::new("noop_echo", json!({}))
                .on_queue("gh")
                .with_run_at(future),
        )
        .await
        .unwrap();
    assert!(b.storage.claim_next("gh", "w-0").await.unwrap().is_none());
}

#[tokio::test]
async fn finalize_done_marks_completed() {
    let b = bootstrap().await;
    let id = match b
        .storage
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        _ => unreachable!(),
    };
    let _ = b.storage.claim_next("gh", "w-0").await.unwrap().unwrap();
    b.storage
        .finalize(&id, None, FinalizeOutcome::Done)
        .await
        .unwrap();
    let row = b.storage.get_job(&id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Done);
    assert!(row.completed_at.is_some());
}

#[tokio::test]
async fn finalize_failed_appends_error_history() {
    let b = bootstrap().await;
    let id = match b
        .storage
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        _ => unreachable!(),
    };
    let _ = b.storage.claim_next("gh", "w-0").await.unwrap().unwrap();
    b.storage
        .finalize(
            &id,
            None,
            FinalizeOutcome::Failed {
                retry_after: Duration::from_mins(1),
                message: "boom".into(),
            },
        )
        .await
        .unwrap();
    let row = b.storage.get_job(&id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.last_error.as_deref(), Some("boom"));
    assert_eq!(row.error_history.len(), 1);
    assert_eq!(row.error_history[0].message, "boom");
}

#[tokio::test]
async fn retry_cycle_emits_balanced_events() {
    // Drive one job through claim → Throttled → re-claim → Done.
    // Event log should contain [Enqueued, Started, Retried, Started,
    // Completed]; the running diff converges to 0.
    let b = bootstrap().await;
    let id = match b
        .storage
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        _ => unreachable!(),
    };
    let _ = b.storage.claim_next("gh", "w-0").await.unwrap().unwrap();
    b.storage
        .finalize(
            &id,
            None,
            FinalizeOutcome::Throttled {
                retry_after: Duration::from_millis(1),
                cool_down_queue: false,
            },
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = b.storage.claim_next("gh", "w-0").await.unwrap().unwrap();
    b.storage
        .finalize(&id, None, FinalizeOutcome::Done)
        .await
        .unwrap();
    // Events are buffered off the hot path; flush to read them back.
    b.storage.flush_event_buffer().await.unwrap();

    let now = Utc::now();
    let events = b
        .storage
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
}

#[tokio::test]
async fn delete_cascades_to_queue_event_rows() {
    let b = bootstrap().await;
    let id = match b
        .storage
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        _ => unreachable!(),
    };
    let _ = b.storage.claim_next("gh", "w-0").await.unwrap().unwrap();
    b.storage
        .finalize(&id, None, FinalizeOutcome::Done)
        .await
        .unwrap();
    // Flush buffered events so the cascade-delete has rows to clear.
    b.storage.flush_event_buffer().await.unwrap();

    let now = Utc::now();
    let pre = b
        .storage
        .list_for_timeline(
            now - chrono::Duration::seconds(60),
            now + chrono::Duration::seconds(60),
        )
        .await
        .unwrap();
    assert_eq!(pre.len(), 3);

    assert_ne!(
        b.storage.delete(&id).await.unwrap(),
        DeleteOutcome::NotFound
    );

    let post = b
        .storage
        .list_for_timeline(
            now - chrono::Duration::seconds(60),
            now + chrono::Duration::seconds(60),
        )
        .await
        .unwrap();
    assert!(
        post.is_empty(),
        "expected cascade-delete to clear events; got {post:?}",
    );
}

#[tokio::test]
async fn run_now_advances_pending_scheduled_at() {
    let b = bootstrap().await;
    let now = Utc::now();
    let id = match b
        .storage
        .enqueue(
            EnqueueRequest::new("k", json!({}))
                .on_queue("gh")
                .with_run_at(now + chrono::Duration::minutes(5)),
        )
        .await
        .unwrap()
    {
        EnqueueOutcome::Enqueued(id) => id,
        _ => unreachable!(),
    };
    assert!(b.storage.claim_next("gh", "w-0").await.unwrap().is_none());
    assert!(b.storage.run_now(&id).await.unwrap());
    let claimed = b
        .storage
        .claim_next("gh", "w-0")
        .await
        .unwrap()
        .expect("claim after run_now");
    assert_eq!(claimed.id, id);
}

#[tokio::test]
async fn claim_next_skips_locked_under_concurrency() {
    // Two simultaneous claim_next calls against the same row must
    // both succeed *without* claiming the same job. This is the
    // SKIP LOCKED guarantee — Postgres's defining advantage over
    // SQLite for multi-replica deploys.
    let b = bootstrap().await;
    // Enqueue 2 jobs so both claimers can succeed.
    for i in 0..2 {
        b.storage
            .enqueue(EnqueueRequest::new("k", json!({ "i": i })).on_queue("gh"))
            .await
            .unwrap();
    }

    let s1 = b.storage.clone();
    let s2 = b.storage.clone();
    let (r1, r2) = tokio::join!(
        async move { s1.claim_next("gh", "w-1").await.unwrap() },
        async move { s2.claim_next("gh", "w-2").await.unwrap() },
    );

    let a = r1.expect("w-1 claimed");
    let c = r2.expect("w-2 claimed");
    assert_ne!(a.id, c.id, "SKIP LOCKED must hand out distinct rows");
}

#[tokio::test]
async fn listen_notify_wakes_wait_for_work() {
    // wait_for_work blocks until a NOTIFY arrives. Enqueue from
    // another task; the listener should wake before the timeout.
    let b = bootstrap().await;
    let s = b.storage.clone();
    let waiter =
        tokio::spawn(async move { s.wait_for_work("gh", Duration::from_secs(5)).await.unwrap() });

    // Small delay so the listener LISTENs before we NOTIFY (otherwise
    // the notify is silently dropped — Postgres doesn't queue
    // notifications for not-yet-listening sessions).
    tokio::time::sleep(Duration::from_millis(200)).await;
    b.storage
        .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
        .await
        .unwrap();

    let woke = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("waiter didn't finish in time")
        .expect("waiter task panicked");
    assert!(woke, "LISTEN/NOTIFY should have woken the waiter");
}

#[tokio::test]
async fn process_registry_register_then_list() {
    let b = bootstrap().await;
    b.storage.register("gh-0", "gh", "host-A").await.unwrap();
    b.storage.register("gh-1", "gh", "host-A").await.unwrap();
    b.storage
        .register("slack-0", "slack", "host-A")
        .await
        .unwrap();
    let gh = b.storage.list(Some("gh")).await.unwrap();
    assert_eq!(gh.len(), 2);
    let all = b.storage.list(None).await.unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn queue_config_ensure_then_tune() {
    let b = bootstrap().await;
    b.storage.ensure_queue("gh", 3).await.unwrap();
    // Re-ensure with a different default must not overwrite a
    // user-tuned value: first set it, then re-ensure, confirm
    // the user value sticks.
    b.storage.set_max_workers("gh", 5).await.unwrap();
    b.storage.ensure_queue("gh", 3).await.unwrap();
    let row = b.storage.get_queue("gh").await.unwrap().unwrap();
    assert_eq!(row.max_workers, 5, "ensure must not overwrite user value");
    b.storage.set_paused("gh", true).await.unwrap();
    assert!(b.storage.get_queue("gh").await.unwrap().unwrap().paused);
}

#[tokio::test]
async fn cron_ensure_then_list() {
    let b = bootstrap().await;
    b.storage
        .ensure_schedule(NewCronSchedule {
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
    let rows = b.storage.list_schedules().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "tickets_sync");
    assert!(rows[0].enabled);
}

#[tokio::test]
async fn cron_lease_elects_single_leader() {
    let b = bootstrap().await;
    let ttl = Duration::from_mins(1);
    // One replica wins the lease; a sibling is denied while it's live;
    // the holder renews its own. This is the multi-pod guard that keeps
    // cron from firing every schedule once per replica.
    assert!(b.storage.try_cron_lease("pod-a", ttl).await.unwrap());
    assert!(!b.storage.try_cron_lease("pod-b", ttl).await.unwrap());
    assert!(b.storage.try_cron_lease("pod-a", ttl).await.unwrap());
}

#[tokio::test]
async fn pod_presence_and_slot_assignment_roundtrip() {
    let b = bootstrap().await;
    // Two pods announce presence.
    b.storage.pod_heartbeat("pod-a").await.unwrap();
    b.storage.pod_heartbeat("pod-b").await.unwrap();
    let live = b
        .storage
        .list_live_pods(Utc::now() - chrono::Duration::seconds(60))
        .await
        .unwrap();
    assert_eq!(live, vec!["pod-a".to_owned(), "pod-b".to_owned()]);

    // No assignment yet → None (supervisor falls back to the total).
    assert_eq!(b.storage.get_slots("gh", "pod-a").await.unwrap(), None);
    // Upsert is idempotent on (queue, host).
    b.storage.set_slots("gh", "pod-a", 5).await.unwrap();
    b.storage.set_slots("gh", "pod-a", 4).await.unwrap();
    assert_eq!(b.storage.get_slots("gh", "pod-a").await.unwrap(), Some(4));

    // delete_for_host clears presence + assignments (graceful deregister).
    b.storage.delete_for_host("pod-a").await.unwrap();
    let live = b
        .storage
        .list_live_pods(Utc::now() - chrono::Duration::seconds(60))
        .await
        .unwrap();
    assert_eq!(live, vec!["pod-b".to_owned()]);
    assert_eq!(b.storage.get_slots("gh", "pod-a").await.unwrap(), None);
}

#[tokio::test]
async fn claim_next_is_fifo_within_priority_and_scheduled_at() {
    // Same shape as the SQLite FIFO test, but here the SKIP LOCKED
    // path also gets the tiebreaker. ULID `id ASC` is the
    // deterministic insertion-order signal regardless of how close
    // the scheduled_at timestamps land.
    let b = bootstrap().await;
    let mut enq_ids = Vec::new();
    for i in 0..5 {
        let id = match b
            .storage
            .enqueue(EnqueueRequest::new("k", json!({ "i": i })).on_queue("gh"))
            .await
            .unwrap()
        {
            EnqueueOutcome::Enqueued(id) => id,
            _ => unreachable!(),
        };
        enq_ids.push(id);
    }

    let mut claimed_ids = Vec::new();
    for _ in 0..5 {
        let claimed = b
            .storage
            .claim_next("gh", "w-0")
            .await
            .unwrap()
            .expect("claim");
        claimed_ids.push(claimed.id);
    }

    assert_eq!(claimed_ids, enq_ids, "claim_next must be FIFO");
}
