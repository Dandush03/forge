//! `JobHandler` trait + `JobOutcome` + dispatch registry + `JobCtx`.
//!
//! Consumer crates implement [`JobHandler`] for each kind of work
//! they want to run on the queue. Handlers are stateless from the
//! runtime's perspective; per-job state lives in the `payload`
//! argument or in whatever store the consumer reaches through `ctx`.
//!
//! `JobCtx` is the only API surface a handler sees. It carries a
//! reference to the backend-agnostic `Storage` bundle (so handlers
//! can enqueue follow-up jobs) and the cancellation token (so they
//! can shut down cleanly when the supervisor signals).

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::routing::Router;
use crate::storage::Storage;
use crate::storage::error::Result;
use crate::storage::types::{EnqueueOutcome, EnqueueRequest, JobId};

/// Implementations are registered in a [`HandlerRegistry`] keyed by
/// their [`JobHandler::kind`].
#[async_trait]
pub trait JobHandler: Send + Sync + 'static {
    /// Stable identifier matched against `sync_queue.kind`.
    fn kind(&self) -> &'static str;

    /// Perform the unit of work. Returning [`JobOutcome::Done`] marks
    /// the row terminal-success; the other variants are described in
    /// the enum doc-comments.
    async fn run(&self, ctx: JobCtx<'_>, payload: serde_json::Value) -> JobOutcome;
}

/// What happened when a handler ran. The runtime maps this to a
/// `FinalizeOutcome` (with backoff applied) before calling
/// `JobQueue::finalize`.
#[derive(Debug)]
pub enum JobOutcome {
    /// Success. Row → `done`.
    Done,
    /// Rate-limited (e.g. HTTP 429). The runtime pushes
    /// `scheduled_at` forward by `retry_after` and returns the row to
    /// `pending` without burning an attempt.
    Throttled { retry_after: Duration },
    /// Application failure. The runtime applies exponential backoff
    /// against `max_attempts` and lands the row in `failed`/`dead`.
    Failed(String),
    /// Permanent application failure — retrying won't help (deleted
    /// upstream resource, 404 / `thread_not_found` / `channel_not_found`,
    /// payload references an entity that no longer exists). The runtime
    /// lands the row in `dead` directly, skipping the retry budget.
    /// Use `Failed` for transient or maybe-transient errors and `Dead`
    /// only when the handler can prove a retry would also fail.
    Dead(String),
}

/// Per-invocation context passed to a handler's `run`.
///
/// Handlers should use `ctx.enqueue(req)` to chain follow-up work —
/// it applies the runtime's router so `req.queue_name = None` is
/// resolved automatically.
pub struct JobCtx<'a> {
    /// The backend-agnostic storage bundle. Handlers usually only
    /// need `storage.jobs` for follow-up enqueues; the other Arcs
    /// are exposed for read-only inspection.
    pub storage: &'a Storage,
    /// The router used to fill in `queue_name` on enqueues.
    pub router: &'a (dyn Router + Send + Sync),
    /// Cluster-wide rate-limit budget. Handlers that talk to an
    /// external API (`acquire("slack")`, `acquire("gh")`) gate
    /// every upstream call through this so a sibling pod doesn't
    /// independently spend the same budget.
    pub rate_limit: &'a super::RateLimiter,
    /// Id of the row being processed.
    pub job_id: JobId,
    /// Worker name (`"{queue}-{slot}-{host_id}"`).
    pub process_id: &'a str,
    /// Process-boot ULID so handlers can correlate logs with the
    /// originating process across restarts.
    pub host_id: &'a str,
    /// Honor this if the handler can cooperatively shut down — the
    /// supervisor signals it when the queue is paused or the process
    /// is exiting.
    pub cancel: CancellationToken,
}

impl std::fmt::Debug for JobCtx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobCtx")
            .field("job_id", &self.job_id)
            .field("process_id", &self.process_id)
            .field("host_id", &self.host_id)
            .finish_non_exhaustive()
    }
}

/// Retry delays for handler-side writes hit by transient
/// `is_transient_conflict()` errors (typically "database is locked"
/// during peak backfill). Mirrors the schedule in `finalize` so the
/// shape is one place to look. After all delays elapse the error
/// surfaces to the caller as before.
const ENQUEUE_RETRY_DELAYS: &[Duration] = &[
    Duration::from_millis(100),
    Duration::from_millis(300),
    Duration::from_secs(1),
];

impl JobCtx<'_> {
    /// Enqueue a follow-up job from inside a handler. The router
    /// fills in `queue_name` when the request doesn't pin one.
    /// Retries up to 3× on transient writer-lock conflicts so a
    /// handler doesn't abort a long fan-out loop because of a
    /// momentary lock race against another worker.
    pub async fn enqueue(&self, req: EnqueueRequest) -> Result<EnqueueOutcome> {
        let mut req = req;
        if req.queue_name.is_none() {
            req.queue_name = Some(Cow::Borrowed(self.router.route(req.kind.as_ref())));
        }
        let mut attempt = 0usize;
        loop {
            match self.storage.jobs.enqueue(req.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) if e.is_transient_conflict() && attempt < ENQUEUE_RETRY_DELAYS.len() => {
                    tracing::warn!(
                        kind = %req.kind,
                        attempt,
                        delay_ms = ENQUEUE_RETRY_DELAYS[attempt].as_millis(),
                        err = %e,
                        "ctx.enqueue: transient conflict; retrying"
                    );
                    tokio::time::sleep(ENQUEUE_RETRY_DELAYS[attempt]).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Enqueue many follow-up jobs in a single transaction. Use this
    /// from bootstrap-style handlers that produce hundreds-to-thousands
    /// of sub-jobs in one go — otherwise we'd take and release the
    /// underlying writer lock N times and starve heartbeats. Same
    /// 3× transient-conflict retry as [`Self::enqueue`].
    pub async fn enqueue_bulk(&self, reqs: Vec<EnqueueRequest>) -> Result<Vec<EnqueueOutcome>> {
        let routed: Vec<EnqueueRequest> = reqs
            .into_iter()
            .map(|mut req| {
                if req.queue_name.is_none() {
                    req.queue_name = Some(Cow::Borrowed(self.router.route(req.kind.as_ref())));
                }
                req
            })
            .collect();
        let mut attempt = 0usize;
        loop {
            match self.storage.jobs.enqueue_bulk(routed.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) if e.is_transient_conflict() && attempt < ENQUEUE_RETRY_DELAYS.len() => {
                    tracing::warn!(
                        batch_size = routed.len(),
                        attempt,
                        delay_ms = ENQUEUE_RETRY_DELAYS[attempt].as_millis(),
                        err = %e,
                        "ctx.enqueue_bulk: transient conflict; retrying"
                    );
                    tokio::time::sleep(ENQUEUE_RETRY_DELAYS[attempt]).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Maps `kind` → handler. `Arc<dyn JobHandler>` so each worker can
/// hold its own reference without copying the trait object.
#[derive(Default)]
pub struct HandlerRegistry {
    handlers: HashMap<&'static str, Arc<dyn JobHandler>>,
}

impl HandlerRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handler. If a handler for the same kind is already
    /// present the new one wins — useful for tests overriding
    /// production handlers.
    pub fn register<H: JobHandler>(&mut self, handler: H) {
        self.handlers.insert(handler.kind(), Arc::new(handler));
    }

    #[must_use]
    pub fn get(&self, kind: &str) -> Option<Arc<dyn JobHandler>> {
        self.handlers.get(kind).cloned()
    }

    /// Snapshot the registered kinds. Used by the runtime to log
    /// "what can we run?" at boot.
    #[must_use]
    pub fn kinds(&self) -> Vec<&'static str> {
        self.handlers.keys().copied().collect()
    }
}

impl std::fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerRegistry")
            .field("kinds", &self.kinds())
            .finish()
    }
}
