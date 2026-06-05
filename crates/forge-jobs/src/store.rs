use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Snapshot of a job's persistent scheduling state. The concrete table
/// lives in `crates/storage` (`job_state`); this struct is the trait's
/// transport-level view of one row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStateRecord {
    pub job_id: String,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub attempt: u32,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    /// When true, the Scheduler keeps writing heartbeats but skips
    /// firing the job. Pause/resume from the host commands flips this
    /// flag; the scheduler doesn't read it through any other channel.
    #[serde(default)]
    pub disabled: bool,
    /// Runtime cadence override. When `Some`, supersedes the Job's
    /// compiled-in `Schedule`. v0016 introduces this; pre-v0016 rows
    /// decode as None.
    #[serde(default)]
    pub schedule_override: Option<String>,
}

/// Persistence trait the [`Scheduler`](crate::Scheduler) writes through.
/// `tech-admin-storage` implements this for `JobStateRepo`; tests
/// implement it for an in-memory map.
#[async_trait]
pub trait JobStore: Send + Sync + 'static {
    async fn get(&self, job_id: &str) -> Result<Option<JobStateRecord>>;

    async fn list(&self) -> Result<Vec<JobStateRecord>>;

    /// Set `next_run_at` without touching success / error / attempt.
    async fn set_next_run(&self, job_id: &str, at: Option<DateTime<Utc>>) -> Result<()>;

    /// Record a successful run: writes `last_success_at`, clears
    /// `last_error`, resets `attempt` to 0, and sets `next_run_at`.
    async fn set_success(
        &self,
        job_id: &str,
        at: DateTime<Utc>,
        next: Option<DateTime<Utc>>,
    ) -> Result<()>;

    /// Record a failure: writes `last_error`, increments `attempt`, and
    /// sets `next_run_at` (no backoff is applied here — the scheduler
    /// passes its own `now + interval`).
    async fn set_error(&self, job_id: &str, error: &str, next: Option<DateTime<Utc>>)
    -> Result<()>;

    /// Update `last_heartbeat_at`. Called every poll tick regardless of
    /// whether `run()` fires; absence or staleness signals the task is
    /// dead.
    async fn set_heartbeat(&self, job_id: &str, at: DateTime<Utc>) -> Result<()>;

    /// Pause (`true`) or resume (`false`) the job. Pause additionally
    /// clears `next_run_at` so a stale schedule can't fire after
    /// resume; resume leaves `next_run_at` alone so the scheduler's
    /// init branch re-arms the cooldown floor on the next tick.
    async fn set_disabled(&self, job_id: &str, disabled: bool) -> Result<()>;

    /// Set (`Some`) or clear (`None`) the runtime cadence override.
    /// Also clears `next_run_at` so the new schedule takes effect on
    /// the next tick instead of firing against a stale timestamp.
    async fn set_schedule_override(&self, job_id: &str, expr: Option<String>) -> Result<()>;
}
