use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::error::Result;

/// A schedulable unit of work.
///
/// Implementors must be `Send + Sync + 'static` because the scheduler holds
/// each job behind an `Arc<dyn Job>` and runs it from a `tokio::spawn`'d
/// task.
///
/// Avoid implementing `Job` for types that carry mutable state — store
/// per-run progress in the `JobStore` (so it survives crashes) or in a
/// shared `Arc<Mutex<…>>` owned outside the job.
#[async_trait]
pub trait Job: Send + Sync + 'static {
    /// Stable identifier; matches `job_state.job_id`. Registering two jobs
    /// with the same id returns [`crate::error::JobError::DuplicateId`].
    fn id(&self) -> &'static str;

    /// How often the scheduler should fire `run()`. Only [`Schedule::Interval`]
    /// is supported today; cron-style schedules are deferred.
    fn schedule(&self) -> Schedule;

    /// The work itself. The scheduler calls this whenever
    /// `next_run_at <= now` (and the cooldown floor has been cleared).
    /// Returning `Err(…)` records the failure and reschedules at
    /// `now + interval` — no exponential backoff in v1.
    async fn run(&self, ctx: &JobCtx) -> Result<()>;
}

/// Cadence for a job.
///
/// `Cron` carries a 6-field cron expression (`sec min hour dom mon dow`)
/// per the [`cron`](https://crates.io/crates/cron) crate's syntax —
/// covers time-windowed cadences like
/// `0 */10 10-21 * * Mon-Fri` (every 10 min, 10:00–21:59, weekdays).
/// Validated server-side before persistence; the Scheduler treats an
/// unparseable expression as "do not fire" and logs a warning.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Schedule {
    Interval(Duration),
    Cron(String),
}

/// Per-run context handed to `Job::run`.
///
/// Carries the host's cancellation token (so long-running work can
/// cooperate with shutdown) and the current attempt count from
/// `job_state` (so a job can decide to skip expensive recovery work
/// on retry).
#[derive(Debug, Clone)]
pub struct JobCtx {
    pub cancel: CancellationToken,
    pub attempt: u32,
}
