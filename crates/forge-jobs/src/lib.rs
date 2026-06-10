#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc)]

//! Sidekiq-style job queue with embedded `SQLite` and pluggable Postgres.
//!
//! Domain crate — host-agnostic. Consumers register handlers, enqueue jobs,
//! and the runtime claims/runs/finalizes them across N worker tasks. The
//! same code path runs single-process on `SQLite` (local desktop) or
//! multi-replica on Postgres (deployed service).
//!
//! ## What it gives you
//!
//! - Backend-agnostic [`Storage`] traits (`JobQueue`, `ProcessRegistry`,
//!   `QueueConfig`, `CronStorage`, `RateLimitStorage`) — one row of
//!   indirection between the runtime and the database
//! - Per-queue worker pools, cooperative shutdown, stale-heartbeat reaper
//! - Cron schedules with lease-elected leader (only one replica fires
//!   each tick); same lease gates the retention sweep and metrics roller
//! - Per-queue exponential backoff with a configurable on/off toggle; the
//!   `Failed` and `Throttled` arms both respect it
//! - Cancellation that survives across replicas: [`QueueHandle::request_cancel`]
//!   short-circuits in-process; cross-pod cancels flow through a DB flag
//!   the heartbeat tick observes
//! - Cluster-wide rate-limit budget — handlers `acquire("slack")` /
//!   `acquire("gh")` against a server-side token bucket; one upstream
//!   429 drains the bucket so sibling pods don't each fire their own
//!
//! ## Minimal consumer
//!
//! ```ignore
//! use std::sync::Arc;
//! use forge_jobs::storage::{DatabaseConfig, PathsError, QueuePaths};
//! use forge_jobs::{
//!     DefaultRouter, EnqueueRequest, HandlerRegistry, NoopEcho, QueueRuntime,
//! };
//!
//! #[derive(Debug)]
//! struct EnvPaths;
//! impl QueuePaths for EnvPaths {
//!     fn config_dir(&self) -> Result<std::path::PathBuf, PathsError> {
//!         Ok("./jobs/config".into())
//!     }
//!     fn data_dir(&self) -> Result<std::path::PathBuf, PathsError> {
//!         Ok("./jobs/data".into())
//!     }
//! }
//!
//! # async fn run() -> forge_jobs::storage::Result<()> {
//! let paths = EnvPaths;
//! let storage = DatabaseConfig::load(&paths)?.open_storage(&paths).await?;
//! let mut handlers = HandlerRegistry::new();
//! handlers.register(NoopEcho);
//! let runtime = QueueRuntime::new(storage, handlers, Arc::new(DefaultRouter));
//! runtime.ensure_queue("default", 2).await?;
//! runtime
//!     .enqueue(
//!         EnqueueRequest::new(forge_jobs::NOOP_ECHO_KIND, serde_json::json!({}))
//!             .on_queue("default"),
//!     )
//!     .await?;
//! let handle = runtime.start().await?;
//! // ... handle.shutdown_graceful(...) at exit
//! # let _ = handle;
//! # Ok(())
//! # }
//! ```
//!
//! See `examples/minimal.rs` in the crate root for the runnable version.
//!
//! ## Handler cancellation contract
//!
//! Handlers that take longer than ~1s should periodically check
//! `ctx.cancel.is_cancelled()` between `.await` points. A user click on
//! the Mission Control "delete" button fires [`QueueHandle::request_cancel`];
//! the worker's heartbeat picks up the cross-pod variant via the DB
//! `cancel_requested_at` flag within `HEARTBEAT_INTERVAL` (10s). User-
//! cancelled jobs route straight to `Dead` without burning the retry budget.
//!
//! ## Optional features
//!
//! - **`postgres`** — enables the Postgres storage adapter. Off by
//!   default; service / k8s deploys flip this on.
//! - **`legacy-scheduler`** — re-exports a smaller cooperative
//!   recurring-job `Scheduler` (with `Job`, `JobStore`, `Clock`,
//!   `parse_cron`, etc.) that predates the queue subsystem. Used by
//!   the originating project's LLM idempotency cache pruning and a
//!   few similar internal cron tasks. New code should use
//!   [`QueueRuntime`] with a cron schedule instead.

pub mod cron_expr;
pub mod runtime;
pub mod storage;

pub use cron_expr::parse_cron;

// Legacy cron `Scheduler` modules. Only compiled when the
// `legacy-scheduler` feature is on. The shared `parse_cron` helper
// the queue's `runtime::cron` needs lives in `cron_expr` (always
// compiled).
#[cfg(feature = "legacy-scheduler")]
mod clock;
#[cfg(feature = "legacy-scheduler")]
mod error;
#[cfg(feature = "legacy-scheduler")]
mod job;
#[cfg(feature = "legacy-scheduler")]
mod scheduler;
#[cfg(feature = "legacy-scheduler")]
mod store;

#[cfg(feature = "legacy-scheduler")]
pub use clock::{Clock, SystemClock};
#[cfg(feature = "legacy-scheduler")]
pub use error::{JobError, Result};
// The legacy `JobCtx` is re-exported as `SchedulerJobCtx` so the
// queue's `JobCtx` (the one handler authors actually use) can keep
// the short name.
#[cfg(feature = "legacy-scheduler")]
pub use job::{Job, JobCtx as SchedulerJobCtx, Schedule};
#[cfg(feature = "legacy-scheduler")]
pub use scheduler::Scheduler;
#[cfg(feature = "legacy-scheduler")]
pub use store::{JobStateRecord, JobStore};

// Queue subsystem — runtime + storage trait surface.
//
// Consumers (handlers, host setup, IPC) get everything they need
// from these two re-export groups. Swapping the storage backend
// later (e.g. Redis) only changes which `Storage`-implementing type
// the host constructs in `setup()`; nothing in `runtime::*` or in
// downstream handler crates moves.
pub use runtime::{
    AcquireOutcome, CLEANUP_TICK, CMD_EXEC_KIND, CRON_TICK, CleanupReport, CmdExecHandler,
    CmdExecPayload, CronTickReport, DEFAULT_QUEUE_WORKERS, DEFAULT_RATE_LIMIT_SCOPES,
    DEFAULT_SHUTDOWN_TIMEOUT, DefaultRouter, HandlerRegistry, JobCtx, JobHandler, JobOutcome,
    KindPrefixRouter, METRICS_BUCKET_SECS, METRICS_TICK, NOOP_ECHO_KIND, NoopEcho, QueueHandle,
    QueueRuntime, REAPER_TICK, REBALANCE_TICK, RateLimiter, Router, WorkerPoolConfig,
    WorkerPoolHandler, cleanup_once, cron_tick_once, ensure_default_rate_limits, ensure_schedules,
    metrics_roll_once, reap_stale_jobs, rebalance_once,
};
#[cfg(feature = "postgres")]
pub use storage::PostgresStorage;
pub use storage::{
    CronScheduleRecord, CronStorage, DrainedSamples, EnqueueOutcome, EnqueueRequest,
    FinalizeOutcome, HeartbeatStatus, JobId, JobLatency, JobQueue, JobRecord, JobStatus,
    MetricBucket, NewCronSchedule, NewJob, PROCESS_WIDE_QUEUE, ProcessRecord, ProcessRegistry,
    QueueConfig, QueueConfigRow, QueueCounts, RateLimitOutcome, RateLimitStorage, SqliteStorage,
    Storage, StorageError, StorageInfo, TimelineEvent, TimelineEventType, metric,
};

/// Format an error with its full `Error::source()` chain as
/// `"top: middle: root"`.
///
/// `Display` only shows the outermost variant — for thiserror enums with
/// `#[from]` that drops the cause entirely (e.g. a `GhError::Octocrab`
/// reads as `"github: <generic>"` instead of `"github: <generic>: 422
/// Validation Failed: field X required"`). Use this when building the
/// `JobOutcome::Failed` / `Dead` message and when logging handler
/// failures so the cause survives into `last_error` and into the CLI.
#[must_use]
pub fn format_error_chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut src = e.source();
    while let Some(s) = src {
        out.push_str(": ");
        out.push_str(&s.to_string());
        src = s.source();
    }
    out
}

#[cfg(test)]
mod format_error_chain_tests {
    use super::format_error_chain;

    #[derive(Debug, thiserror::Error)]
    #[error("root")]
    struct Root;

    #[derive(Debug, thiserror::Error)]
    #[error("middle")]
    struct Middle(#[from] Root);

    #[derive(Debug, thiserror::Error)]
    #[error("top")]
    struct Top(#[from] Middle);

    #[test]
    fn chain_walks_every_source() {
        let e = Top(Middle(Root));
        assert_eq!(format_error_chain(&e), "top: middle: root");
    }

    #[test]
    fn chain_returns_just_top_when_no_source() {
        let e = Root;
        assert_eq!(format_error_chain(&e), "root");
    }
}
