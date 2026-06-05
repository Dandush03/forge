use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Local, Utc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::clock::{Clock, SystemClock};
use crate::error::{JobError, Result};
use crate::job::{Job, JobCtx, Schedule};
use crate::store::JobStore;

/// Default scheduler poll cadence. Matches the value the legacy ad-hoc
/// loops used in [src-tauri/src/commands/tickets.rs] so behavior on the
/// happy path is identical.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(30);

pub struct Scheduler {
    store: Arc<dyn JobStore>,
    cancel: CancellationToken,
    jobs: Vec<Arc<dyn Job>>,
    poll_interval: Duration,
    opened_at: DateTime<Utc>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field(
                "jobs",
                &self.jobs.iter().map(|j| j.id()).collect::<Vec<_>>(),
            )
            .field("poll_interval", &self.poll_interval)
            .field("opened_at", &self.opened_at)
            .finish_non_exhaustive()
    }
}

impl Scheduler {
    pub fn new(store: Arc<dyn JobStore>, cancel: CancellationToken) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        Self {
            store,
            cancel,
            jobs: Vec::new(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            opened_at: clock.now(),
            clock,
        }
    }

    #[must_use]
    pub const fn with_poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Override `opened_at`. Tests use this to make the cooldown floor
    /// deterministic; production lets `Scheduler::new` capture the real
    /// launch instant.
    #[must_use]
    pub const fn with_opened_at(mut self, at: DateTime<Utc>) -> Self {
        self.opened_at = at;
        self
    }

    /// Inject a custom clock. Used by tests to drive the cooldown and
    /// `next_run_at` comparisons deterministically without sleeping.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn register(&mut self, job: Arc<dyn Job>) -> Result<()> {
        if self.jobs.iter().any(|j| j.id() == job.id()) {
            return Err(JobError::DuplicateId(job.id().to_owned()));
        }
        self.jobs.push(job);
        Ok(())
    }

    /// Consume the scheduler and spawn one task per registered job.
    /// Each task observes the shared `CancellationToken`; call
    /// [`Self::shutdown`] with the returned handles to drain cleanly.
    #[must_use = "spawned tasks need to be joined via Scheduler::shutdown at exit"]
    pub fn run(self) -> Vec<JoinHandle<()>> {
        let Self {
            store,
            cancel,
            jobs,
            poll_interval,
            opened_at,
            clock,
        } = self;
        jobs.into_iter()
            .map(|job| {
                let store = store.clone();
                let cancel = cancel.clone();
                let clock = clock.clone();
                tokio::spawn(async move {
                    run_job_loop(job, store, cancel, poll_interval, opened_at, clock).await;
                })
            })
            .collect()
    }

    /// Wait for spawned job loops to exit, up to `timeout`. Callers
    /// should cancel the token *before* calling this — `shutdown` only
    /// joins, it doesn't trigger the cancellation itself.
    pub async fn shutdown(handles: Vec<JoinHandle<()>>, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        for handle in handles {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                tracing::warn!("scheduler shutdown deadline reached; remaining tasks abandoned");
                handle.abort();
                continue;
            }
            match tokio::time::timeout(remaining, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(?e, "scheduler: join error during shutdown"),
                Err(_) => tracing::warn!("scheduler shutdown timed out; tasks remain"),
            }
        }
    }
}

async fn run_job_loop(
    job: Arc<dyn Job>,
    store: Arc<dyn JobStore>,
    cancel: CancellationToken,
    poll_interval: Duration,
    opened_at: DateTime<Utc>,
    clock: Arc<dyn Clock>,
) {
    let job_id = job.id();
    let compiled_schedule = job.schedule();
    tracing::info!(
        job_id,
        schedule = ?compiled_schedule,
        opened_at = %opened_at,
        "scheduler: job loop started"
    );

    loop {
        if cancel.is_cancelled() {
            break;
        }
        let now = clock.now();

        if let Err(e) = store.set_heartbeat(job_id, now).await {
            tracing::warn!(job_id, ?e, "scheduler: heartbeat write failed");
        }

        let record = match store.get(job_id).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(job_id, ?e, "scheduler: get failed; skipping tick");
                if wait_or_cancel(poll_interval, &cancel).await {
                    break;
                }
                continue;
            }
        };

        let attempt = record.as_ref().map_or(0, |r| r.attempt);
        let next_run_at = record.as_ref().and_then(|r| r.next_run_at);
        let disabled = record.as_ref().is_some_and(|r| r.disabled);
        let override_expr = record.as_ref().and_then(|r| r.schedule_override.clone());

        if disabled {
            // Heartbeat already written above; the panel will render
            // "paused, alive". Skip everything else this tick.
            if wait_or_cancel(poll_interval, &cancel).await {
                break;
            }
            continue;
        }

        let effective =
            EffectiveSchedule::resolve(job_id, &compiled_schedule, override_expr.as_deref());

        let should_fire = match (&effective, next_run_at) {
            (EffectiveSchedule::Invalid, _) => {
                tracing::warn!(
                    job_id,
                    "scheduler: invalid compiled schedule and no override; not firing"
                );
                false
            }
            (_, None) => {
                let initial = effective.first_fire_at(opened_at, now);
                if let Err(e) = store.set_next_run(job_id, initial).await {
                    tracing::warn!(job_id, ?e, "scheduler: initial set_next_run failed");
                }
                false
            }
            // Interval-only cooldown floor: skip if `scheduled` would
            // fire before one interval has elapsed since launch.
            (EffectiveSchedule::Interval(interval_chrono), Some(scheduled))
                if scheduled < opened_at + *interval_chrono =>
            {
                let bumped = opened_at + *interval_chrono;
                if let Err(e) = store.set_next_run(job_id, Some(bumped)).await {
                    tracing::warn!(job_id, ?e, "scheduler: cooldown bump failed");
                }
                false
            }
            (_, Some(scheduled)) => scheduled <= now,
        };

        if should_fire {
            let next_at = effective.next_fire_after(now);
            let ctx = JobCtx {
                cancel: cancel.clone(),
                attempt,
            };
            tracing::debug!(job_id, attempt, ?next_at, "scheduler: firing job");
            match job.run(&ctx).await {
                Ok(()) => {
                    if let Err(e) = store.set_success(job_id, now, next_at).await {
                        tracing::warn!(job_id, ?e, "scheduler: set_success failed");
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    tracing::warn!(job_id, error = %msg, "scheduler: job run returned error");
                    if let Err(store_err) = store.set_error(job_id, &msg, next_at).await {
                        tracing::warn!(job_id, ?store_err, "scheduler: set_error failed");
                    }
                }
            }
        }

        if wait_or_cancel(poll_interval, &cancel).await {
            break;
        }
    }

    tracing::info!(job_id, "scheduler: job loop exited");
}

/// Sleep for `d` or until `cancel` fires. Returns `true` if cancelled.
async fn wait_or_cancel(d: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = tokio::time::sleep(d) => false,
        () = cancel.cancelled() => true,
    }
}

/// Resolved schedule for one tick — compiled default or parsed override.
///
/// Built at the top of every tick from
/// `(compiled_schedule, record.schedule_override)` and thrown away after;
/// the persisted state is just the override string.
enum EffectiveSchedule {
    /// Nothing fires — compiled schedule is malformed and there is no
    /// override (or the override didn't parse either). Logged once
    /// per tick at warn level.
    Invalid,
    Interval(chrono::Duration),
    Cron(Box<cron::Schedule>),
}

impl EffectiveSchedule {
    /// Override (when present + valid) wins over the compiled default.
    /// Invalid override logs once at warn level and falls back to the
    /// compiled schedule.
    fn resolve(job_id: &str, compiled: &Schedule, override_expr: Option<&str>) -> Self {
        if let Some(expr) = override_expr {
            match parse_cron(expr) {
                Ok(sched) => return Self::Cron(Box::new(sched)),
                Err(e) => tracing::warn!(
                    job_id,
                    error = %e,
                    "scheduler: invalid schedule_override; falling back to compiled default"
                ),
            }
        }
        Self::from_compiled(compiled)
    }

    fn from_compiled(schedule: &Schedule) -> Self {
        match schedule {
            Schedule::Interval(d) => {
                chrono::Duration::from_std(*d).map_or(Self::Invalid, Self::Interval)
            }
            Schedule::Cron(expr) => {
                parse_cron(expr).map_or(Self::Invalid, |s| Self::Cron(Box::new(s)))
            }
        }
    }

    /// First `next_run_at` to write when the row has none yet. For
    /// Interval this is the cooldown floor (`opened_at + interval`);
    /// for Cron it's the next tick after `now`. Returning `None`
    /// leaves the row's `next_run_at` cleared, which prevents firing.
    fn first_fire_at(&self, opened_at: DateTime<Utc>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Self::Invalid => None,
            Self::Interval(d) => Some(opened_at + *d),
            Self::Cron(sched) => next_cron_local(sched, now),
        }
    }

    /// `next_run_at` after the current run completes. Interval: `now +
    /// interval`. Cron: next tick strictly after `now`.
    fn next_fire_after(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Self::Invalid => None,
            Self::Interval(d) => Some(now + *d),
            Self::Cron(sched) => next_cron_local(sched, now),
        }
    }
}

/// Compute the next cron fire after `now`, with the expression's
/// hour/minute/day fields interpreted in the host's **local
/// timezone**. The cron crate is generic over `TimeZone`, so handing
/// it a `DateTime<Local>` makes `0 30 8-18 * * Mon-Fri` mean
/// "8am-6pm in the operator's wall-clock time" — which is what users
/// mean on a desktop tool. Result is converted back to UTC for
/// storage.
///
/// DST: during spring-forward, fire times that fall in the skipped
/// hour are skipped to the next valid local time; during fall-back
/// the cron crate may emit a single fire across the duplicate hour.
/// Acceptable trade-off for an interactive tool.
fn next_cron_local(sched: &cron::Schedule, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let local_now = now.with_timezone(&Local);
    sched
        .after(&local_now)
        .next()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Parse a 6-field cron expression (`sec min hour dom mon dow`).
///
/// Returns the error as a `String` so callers can log it without
/// dragging the cron crate into their signatures. Host code should
/// validate at the edge with this same function so rejection reasons
/// match what the scheduler would otherwise silently log.
pub fn parse_cron(expr: &str) -> std::result::Result<cron::Schedule, String> {
    use std::str::FromStr;
    cron::Schedule::from_str(expr).map_err(|e| e.to_string())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "explicit panics make test failures point at the right assertion"
)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::clock::Clock;
    use crate::error::JobError;
    use crate::job::{Job, JobCtx, Schedule};
    use crate::store::{JobStateRecord, JobStore};

    #[derive(Debug, Default)]
    struct ManualClock {
        now: Mutex<Option<DateTime<Utc>>>,
    }

    impl ManualClock {
        fn at(t: DateTime<Utc>) -> Arc<Self> {
            Arc::new(Self {
                now: Mutex::new(Some(t)),
            })
        }
        fn advance(&self, by: chrono::Duration) {
            let mut g = self.now.lock().unwrap();
            *g = Some(g.unwrap_or_else(Utc::now) + by);
            drop(g);
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> DateTime<Utc> {
            self.now.lock().unwrap().unwrap_or_else(Utc::now)
        }
    }

    #[derive(Default)]
    struct MemStore {
        rows: Mutex<HashMap<String, JobStateRecord>>,
        heartbeats: AtomicU32,
        successes: AtomicU32,
        errors: AtomicU32,
    }

    impl MemStore {
        fn row(&self, id: &str) -> Option<JobStateRecord> {
            self.rows.lock().unwrap().get(id).cloned()
        }

        fn upsert<F: FnOnce(&mut JobStateRecord)>(&self, id: &str, f: F) {
            let mut rows = self.rows.lock().unwrap();
            let row = rows.entry(id.to_owned()).or_insert_with(|| JobStateRecord {
                job_id: id.to_owned(),
                last_success_at: None,
                last_error: None,
                attempt: 0,
                next_run_at: None,
                last_heartbeat_at: None,
                disabled: false,
                schedule_override: None,
            });
            f(row);
            drop(rows);
        }
    }

    #[async_trait]
    impl JobStore for MemStore {
        async fn get(&self, job_id: &str) -> Result<Option<JobStateRecord>> {
            Ok(self.row(job_id))
        }
        async fn list(&self) -> Result<Vec<JobStateRecord>> {
            Ok(self.rows.lock().unwrap().values().cloned().collect())
        }
        async fn set_next_run(&self, job_id: &str, at: Option<DateTime<Utc>>) -> Result<()> {
            self.upsert(job_id, |r| r.next_run_at = at);
            Ok(())
        }
        async fn set_success(
            &self,
            job_id: &str,
            at: DateTime<Utc>,
            next: Option<DateTime<Utc>>,
        ) -> Result<()> {
            self.successes.fetch_add(1, Ordering::Relaxed);
            self.upsert(job_id, |r| {
                r.last_success_at = Some(at);
                r.last_error = None;
                r.attempt = 0;
                r.next_run_at = next;
            });
            Ok(())
        }
        async fn set_error(
            &self,
            job_id: &str,
            error: &str,
            next: Option<DateTime<Utc>>,
        ) -> Result<()> {
            self.errors.fetch_add(1, Ordering::Relaxed);
            self.upsert(job_id, |r| {
                r.last_error = Some(error.to_owned());
                r.attempt = r.attempt.saturating_add(1);
                r.next_run_at = next;
            });
            Ok(())
        }
        async fn set_heartbeat(&self, job_id: &str, at: DateTime<Utc>) -> Result<()> {
            self.heartbeats.fetch_add(1, Ordering::Relaxed);
            self.upsert(job_id, |r| r.last_heartbeat_at = Some(at));
            Ok(())
        }
        async fn set_disabled(&self, job_id: &str, disabled: bool) -> Result<()> {
            self.upsert(job_id, |r| {
                r.disabled = disabled;
                if disabled {
                    r.next_run_at = None;
                }
            });
            Ok(())
        }
        async fn set_schedule_override(&self, job_id: &str, expr: Option<String>) -> Result<()> {
            self.upsert(job_id, |r| {
                r.schedule_override = expr;
                // Clear next_run_at so the new cadence takes effect on
                // the next tick instead of firing against a stale ts.
                r.next_run_at = None;
            });
            Ok(())
        }
    }

    struct CountingJob {
        id: &'static str,
        interval: Duration,
        runs: AtomicU32,
        fail_with: Option<&'static str>,
    }

    impl CountingJob {
        const fn new(id: &'static str, interval: Duration) -> Self {
            Self {
                id,
                interval,
                runs: AtomicU32::new(0),
                fail_with: None,
            }
        }
        const fn failing(id: &'static str, interval: Duration, msg: &'static str) -> Self {
            Self {
                id,
                interval,
                runs: AtomicU32::new(0),
                fail_with: Some(msg),
            }
        }
    }

    #[async_trait]
    impl Job for CountingJob {
        fn id(&self) -> &'static str {
            self.id
        }
        fn schedule(&self) -> Schedule {
            Schedule::Interval(self.interval)
        }
        async fn run(&self, _ctx: &JobCtx) -> Result<()> {
            self.runs.fetch_add(1, Ordering::Relaxed);
            self.fail_with
                .map_or(Ok(()), |msg| Err(JobError::JobFailed(msg.to_owned())))
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn fires_once_per_interval_after_cooldown() {
        let store = Arc::new(MemStore::default());
        let cancel = CancellationToken::new();
        let job = Arc::new(CountingJob::new("count", Duration::from_mins(1)));
        let opened_at = Utc::now();
        let clock = ManualClock::at(opened_at);

        let mut sched = Scheduler::new(store.clone(), cancel.clone())
            .with_poll_interval(Duration::from_secs(5))
            .with_opened_at(opened_at)
            .with_clock(clock.clone());
        sched.register(job.clone()).unwrap();
        let handles = sched.run();

        // First poll: initializes next_run_at = opened_at + 60s. No fire.
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        assert_eq!(job.runs.load(Ordering::Relaxed), 0);
        assert!(store.row("count").unwrap().next_run_at.is_some());

        // Advance the scheduler's clock past the cooldown floor, then
        // advance tokio time so the next poll-tick wakes up.
        clock.advance(chrono::Duration::seconds(70));
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;

        let runs_after = job.runs.load(Ordering::Relaxed);
        assert!(
            runs_after >= 1,
            "expected at least one run after cooldown; got {runs_after}"
        );

        cancel.cancel();
        Scheduler::shutdown(handles, Duration::from_secs(1)).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn writes_heartbeat_every_poll_tick() {
        let store = Arc::new(MemStore::default());
        let cancel = CancellationToken::new();
        let job = Arc::new(CountingJob::new("hb", Duration::from_hours(1)));

        let mut sched = Scheduler::new(store.clone(), cancel.clone())
            .with_poll_interval(Duration::from_secs(10))
            .with_opened_at(Utc::now());
        sched.register(job).unwrap();
        let handles = sched.run();

        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        // Two more poll ticks.
        for _ in 0..2 {
            tokio::time::advance(Duration::from_secs(10)).await;
            tokio::task::yield_now().await;
        }

        let hb = store.heartbeats.load(Ordering::Relaxed);
        assert!(hb >= 3, "expected >=3 heartbeats, got {hb}");
        assert!(store.row("hb").unwrap().last_heartbeat_at.is_some());

        cancel.cancel();
        Scheduler::shutdown(handles, Duration::from_secs(1)).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn cancellation_stops_loop_promptly() {
        let store = Arc::new(MemStore::default());
        let cancel = CancellationToken::new();
        let job = Arc::new(CountingJob::new("cancellable", Duration::from_mins(1)));

        let mut sched = Scheduler::new(store, cancel.clone())
            .with_poll_interval(Duration::from_secs(30))
            .with_opened_at(Utc::now());
        sched.register(job).unwrap();
        let handles = sched.run();

        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        cancel.cancel();

        // shutdown should resolve well within the timeout — the sleep
        // future is awoken by the cancellation branch.
        Scheduler::shutdown(handles, Duration::from_secs(2)).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn run_error_calls_set_error_and_schedules_retry() {
        let store = Arc::new(MemStore::default());
        let cancel = CancellationToken::new();
        let job = Arc::new(CountingJob::failing(
            "broken",
            Duration::from_mins(1),
            "boom",
        ));
        let opened_at = Utc::now();
        let clock = ManualClock::at(opened_at);

        let mut sched = Scheduler::new(store.clone(), cancel.clone())
            .with_poll_interval(Duration::from_secs(5))
            .with_opened_at(opened_at)
            .with_clock(clock.clone());
        sched.register(job).unwrap();
        let handles = sched.run();

        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        clock.advance(chrono::Duration::seconds(70));
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;

        let errors = store.errors.load(Ordering::Relaxed);
        assert!(errors >= 1, "expected >=1 set_error call, got {errors}");
        let row = store.row("broken").unwrap();
        assert_eq!(row.last_error.as_deref(), Some("job failed: boom"));
        assert!(row.attempt >= 1);
        assert!(row.next_run_at.is_some());

        cancel.cancel();
        Scheduler::shutdown(handles, Duration::from_secs(1)).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn disabled_flag_suppresses_fire_then_resumes() {
        let store = Arc::new(MemStore::default());
        let cancel = CancellationToken::new();
        let job = Arc::new(CountingJob::new("pauseable", Duration::from_mins(1)));
        let opened_at = Utc::now();
        let clock = ManualClock::at(opened_at);

        let mut sched = Scheduler::new(store.clone(), cancel.clone())
            .with_poll_interval(Duration::from_secs(5))
            .with_opened_at(opened_at)
            .with_clock(clock.clone());
        sched.register(job.clone()).unwrap();
        let handles = sched.run();

        // Get past the cooldown floor on the unpaused job (initial tick
        // + cooldown bump): set disabled = true mid-stream.
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        store.set_disabled("pauseable", true).await.expect("pause");

        // Advance well past what would otherwise have fired the job.
        clock.advance(chrono::Duration::seconds(120));
        for _ in 0..4 {
            tokio::time::advance(Duration::from_secs(6)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            job.runs.load(Ordering::Relaxed),
            0,
            "disabled job must not fire"
        );
        let hb_paused = store.heartbeats.load(Ordering::Relaxed);
        assert!(
            hb_paused >= 4,
            "heartbeat should keep ticking while paused; got {hb_paused}"
        );
        assert!(
            store.row("pauseable").unwrap().next_run_at.is_none(),
            "pause should leave next_run_at cleared"
        );

        // Resume: the scheduler's init branch will re-arm the cooldown
        // floor on the next tick. To make the job actually fire under
        // the test clock, also push the clock past that floor.
        store
            .set_disabled("pauseable", false)
            .await
            .expect("resume");
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        clock.advance(chrono::Duration::seconds(120));
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(6)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            job.runs.load(Ordering::Relaxed) >= 1,
            "job should fire again after resume + cooldown elapses"
        );

        cancel.cancel();
        Scheduler::shutdown(handles, Duration::from_secs(1)).await;
    }

    #[test]
    fn resolve_picks_valid_override_over_compiled() {
        let compiled = Schedule::Interval(Duration::from_mins(1));
        let resolved = EffectiveSchedule::resolve("j", &compiled, Some("0/5 * * * * *"));
        assert!(matches!(resolved, EffectiveSchedule::Cron(_)));
    }

    #[test]
    fn resolve_falls_back_to_compiled_when_override_invalid() {
        let compiled = Schedule::Interval(Duration::from_mins(1));
        let resolved = EffectiveSchedule::resolve("j", &compiled, Some("not a cron"));
        assert!(matches!(resolved, EffectiveSchedule::Interval(_)));
    }

    #[test]
    fn resolve_falls_back_when_no_override() {
        let compiled = Schedule::Interval(Duration::from_mins(1));
        let resolved = EffectiveSchedule::resolve("j", &compiled, None);
        assert!(matches!(resolved, EffectiveSchedule::Interval(_)));
    }

    #[test]
    fn cron_first_fire_lands_after_now() {
        let compiled = Schedule::Cron("0/5 * * * * *".to_owned());
        let sched = EffectiveSchedule::from_compiled(&compiled);
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        let opened = now - chrono::Duration::seconds(30);
        let fire = sched.first_fire_at(opened, now).unwrap();
        assert!(fire > now);
        assert!(fire - now <= chrono::Duration::seconds(5));
    }

    #[test]
    fn cron_next_fire_uses_now_not_opened_at() {
        let compiled = Schedule::Cron("0/5 * * * * *".to_owned());
        let sched = EffectiveSchedule::from_compiled(&compiled);
        let now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        let fire = sched.next_fire_after(now).unwrap();
        assert!(fire > now);
        assert!(fire - now <= chrono::Duration::seconds(5));
    }

    #[test]
    fn invalid_compiled_cron_resolves_to_invalid_without_override() {
        let compiled = Schedule::Cron("garbage".to_owned());
        let resolved = EffectiveSchedule::resolve("j", &compiled, None);
        assert!(matches!(resolved, EffectiveSchedule::Invalid));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn schedule_override_makes_job_fire_under_compiled_floor() {
        // Job compiled with a 1-hour interval should fire much sooner
        // once a "every 2 seconds" cron override is set.
        let store = Arc::new(MemStore::default());
        let cancel = CancellationToken::new();
        let job = Arc::new(CountingJob::new("cron-overridden", Duration::from_hours(1)));
        let opened_at = Utc::now();
        let clock = ManualClock::at(opened_at);

        let mut sched = Scheduler::new(store.clone(), cancel.clone())
            .with_poll_interval(Duration::from_secs(1))
            .with_opened_at(opened_at)
            .with_clock(clock.clone());
        sched.register(job.clone()).unwrap();
        let handles = sched.run();

        // Initial tick fires before override is set; nothing else.
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        assert_eq!(job.runs.load(Ordering::Relaxed), 0);

        // Install the override. set_schedule_override clears next_run_at
        // so the next tick reinitializes from the cron schedule.
        store
            .set_schedule_override("cron-overridden", Some("0/2 * * * * *".to_owned()))
            .await
            .unwrap();

        // Drive the scheduler past the next cron boundary. Each tick:
        // advance the clock 1s + advance tokio time so the poll wakes.
        for _ in 0..6 {
            clock.advance(chrono::Duration::seconds(1));
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        assert!(
            job.runs.load(Ordering::Relaxed) >= 1,
            "cron override should have made the job fire within 6s"
        );

        cancel.cancel();
        Scheduler::shutdown(handles, Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn duplicate_id_rejected() {
        let store = Arc::new(MemStore::default());
        let cancel = CancellationToken::new();
        let mut sched = Scheduler::new(store, cancel);
        sched
            .register(Arc::new(CountingJob::new("same", Duration::from_secs(10))))
            .unwrap();
        let err = sched
            .register(Arc::new(CountingJob::new("same", Duration::from_secs(10))))
            .unwrap_err();
        assert!(matches!(err, JobError::DuplicateId(ref id) if id == "same"));
    }
}
