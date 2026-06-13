//! Runtime test — per-worker queue affinity.
//!
//! Two `QueueRuntime`s share one in-memory store but declare disjoint
//! queue sets (the `SQLite` path is single-process, so both runtimes run
//! in one test). We assert each worker only spawns processes for the queues
//! it declared, and that starting a worker with no declared queues is a
//! hard error rather than an implicit run-everything.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration tests crash loudly on setup/assert failures; that's the point"
)]

use std::sync::Arc;
use std::time::Duration;

use forge_jobs::SqliteStorage;
use forge_jobs::{
    DefaultRouter, HandlerRegistry, NoopEcho, QueueRuntime, Storage as JobStorage, StorageError,
};

fn runtime(storage: &JobStorage, queues: &[&str]) -> QueueRuntime {
    let mut handlers = HandlerRegistry::new();
    handlers.register(NoopEcho);
    QueueRuntime::new(storage.clone(), handlers, Arc::new(DefaultRouter))
        .with_queues(queues.iter().map(|q| (*q).to_owned()))
}

#[tokio::test]
async fn start_without_declared_queues_errors() {
    let sqlite = Arc::new(SqliteStorage::open_in_memory().await.unwrap());
    let storage = JobStorage::from_one(sqlite);
    let mut handlers = HandlerRegistry::new();
    handlers.register(NoopEcho);
    let rt = QueueRuntime::new(storage, handlers, Arc::new(DefaultRouter));

    let err = rt.start().await.expect_err("must reject empty queue set");
    assert!(
        matches!(err, StorageError::Config(_)),
        "expected a Config error, got {err:?}"
    );
}

#[tokio::test]
async fn workers_only_run_their_declared_queues() {
    let sqlite = Arc::new(SqliteStorage::open_in_memory().await.unwrap());
    let storage = JobStorage::from_one(sqlite);
    storage.config.ensure_queue("gh", 1).await.unwrap();
    storage.config.ensure_queue("default", 1).await.unwrap();

    // tom owns gh; jerry owns default. Distinct names so the process_id
    // host segments differ.
    let tom = runtime(&storage, &["gh"]).with_worker_name("tom");
    let jerry = runtime(&storage, &["default"]).with_worker_name("jerry");
    let tom = tom.start().await.expect("tom start");
    let jerry = jerry.start().await.expect("jerry start");

    // Give the supervisors a couple ticks to register their worker slots.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let gh = storage.procs.list(Some("gh")).await.unwrap();
        let def = storage.procs.list(Some("default")).await.unwrap();
        if !gh.is_empty() && !def.is_empty() {
            // Every gh process must belong to a single host, and that host
            // must NOT also be serving default (disjoint ownership).
            let gh_hosts: std::collections::HashSet<_> =
                gh.iter().map(|p| p.host_id.clone()).collect();
            let def_hosts: std::collections::HashSet<_> =
                def.iter().map(|p| p.host_id.clone()).collect();
            assert!(
                gh_hosts.is_disjoint(&def_hosts),
                "gh and default are served by disjoint workers: gh={gh_hosts:?} default={def_hosts:?}"
            );
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "both queues did not get a worker within 3s (gh={}, default={})",
            gh.len(),
            def.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    tom.shutdown_graceful(Duration::from_secs(5)).await;
    jerry.shutdown_graceful(Duration::from_secs(5)).await;
}
