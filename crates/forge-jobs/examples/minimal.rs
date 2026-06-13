#![allow(
    clippy::print_stdout,
    reason = "an example program prints user-facing progress; that's the demo"
)]

//! Minimal `forge-jobs` consumer.
//!
//! Demonstrates the public API surface end-to-end:
//!  1. Implement `QueuePaths` (env-var-or-CWD fallback)
//!  2. Open storage via `DatabaseConfig::load(&paths)`
//!  3. Register a handler (the bundled `NoopEcho` demo)
//!  4. Boot the runtime, enqueue a job, shut down cleanly
//!
//! Run with:
//!
//!   cargo run --example minimal -p forge-jobs
//!
//! Defaults to an in-process `SQLite` at `./jobs/data/queue.sqlite`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use forge_jobs::storage::{DatabaseConfig, PathsError, QueuePaths};
use forge_jobs::{
    DEFAULT_SHUTDOWN_TIMEOUT, DefaultRouter, EnqueueRequest, HandlerRegistry, NOOP_ECHO_KIND,
    NoopEcho, QueueRuntime,
};

/// Env-var-or-CWD paths resolver. A real consumer plugs in their own
/// paths layer (XDG via `directories`, a hardcoded prod path, a
/// tempdir for tests). The queue crate stays agnostic.
#[derive(Debug)]
struct EnvPaths;

impl QueuePaths for EnvPaths {
    fn config_dir(&self) -> Result<PathBuf, PathsError> {
        Ok(std::env::var_os("JOBS_CONFIG_DIR")
            .map_or_else(|| PathBuf::from("./jobs/config"), PathBuf::from))
    }
    fn data_dir(&self) -> Result<PathBuf, PathsError> {
        Ok(std::env::var_os("JOBS_DATA_DIR")
            .map_or_else(|| PathBuf::from("./jobs/data"), PathBuf::from))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // 1. Load config (or default to SQLite at <data_dir>/queue.sqlite)
    //    and open storage. Migrations run idempotently as part of open.
    let paths = EnvPaths;
    let db_cfg = DatabaseConfig::load(&paths)?;
    let storage = db_cfg.open_storage(&paths).await?;

    // 2. Register handlers. NoopEcho is bundled with the crate for
    //    smoke-tests like this one.
    let mut handlers = HandlerRegistry::new();
    handlers.register(NoopEcho);

    // 3. Build the runtime. The router decides which queue an
    //    enqueued kind lands on when the caller doesn't pin one;
    //    DefaultRouter sends everything to "default". `with_queues`
    //    declares which queues THIS worker consumes — it's required
    //    (a worker with none fails at start). A real host typically
    //    reads it from the environment via `queues_from_env()`.
    let runtime = QueueRuntime::new(storage, handlers, Arc::new(DefaultRouter))
        .with_queues(["default".to_owned()]);

    // 4. Make sure the "default" queue config row exists. ensure_queue
    //    is idempotent — calling it on every boot is the expected pattern.
    //    (start() also ensures rows for every declared queue.)
    runtime.ensure_queue("default", 2).await?;

    // 5. Seed one job.
    let outcome = runtime
        .enqueue(
            EnqueueRequest::new(NOOP_ECHO_KIND, serde_json::json!({ "hello": "world" }))
                .on_queue("default"),
        )
        .await?;
    println!("enqueued: {:?}", outcome.id());

    // 6. Boot supervisors + workers + reaper + cleanup + cron + metrics.
    //    Returns a QueueHandle for orchestration; keep it alive.
    let handle = runtime.start().await?;

    // Let workers drain the one job we enqueued.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 7. Graceful shutdown. Waits up to the budget for in-flight work
    //    to finish, then aborts the rest.
    handle.shutdown_graceful(DEFAULT_SHUTDOWN_TIMEOUT).await;
    println!("done.");
    Ok(())
}
