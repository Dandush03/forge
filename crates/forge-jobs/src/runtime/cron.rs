//! Cron service — turns `cron_schedule` rows into queue jobs.
//!
//! Every tick (default 5s) the service:
//!
//! 1. Reads every enabled schedule via `CronStorage::list_schedules`.
//! 2. For rows whose `next_fire_at` is in the past, enqueues a fresh
//!    job from `(kind, payload, queue_name)`, then advances
//!    `next_fire_at` from `cron_expr`.
//! 3. For rows whose `next_fire_at` is `None` (newly registered or
//!    just re-enabled), seeds `next_fire_at` without firing.
//!
//! Tick failures are logged but never propagated; one bad cron
//! expression must not stop the rest of the schedules from firing.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

use super::routing::Router;
use crate::cron_expr::parse_cron;
use crate::storage::Storage;
use crate::storage::error::Result;
use crate::storage::types::{CronScheduleRecord, EnqueueRequest};

/// How often the cron service evaluates schedules.
pub const CRON_TICK: Duration = Duration::from_secs(5);

/// Cron leadership lease TTL. Must exceed [`CRON_TICK`] by enough that
/// a leader missing a tick (GC pause, scheduling jitter) doesn't lose
/// leadership spuriously — 3× the tick. On a leader crash, another
/// replica takes over within roughly this window. Also serves as the
/// coordinator lease for the worker rebalancer (`super::rebalance`).
pub(super) const CRON_LEASE_TTL: Duration = Duration::from_secs(15);

/// Floor on the cron loop's sleep so a schedule due "right now" doesn't
/// spin the loop. The loop sleeps `min(time_until_next_schedule,
/// CRON_TICK)` clamped to at least this — see [`cron_loop`].
const CRON_MIN_SLEEP: Duration = Duration::from_millis(500);

/// Counts returned by one tick. Surfaced for tests and ops.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CronTickReport {
    pub fired: u64,
    pub seeded: u64,
    pub errors: u64,
    /// Soonest upcoming fire across all enabled schedules after this
    /// tick (UTC), or `None` if nothing is scheduled. The loop sleeps
    /// until this instant (capped at [`CRON_TICK`]) so a schedule fires
    /// on time instead of at the next fixed tick boundary.
    pub next_due: Option<DateTime<Utc>>,
}

/// One pass over the cron schedules. Exposed so tests can drive the
/// service deterministically without running the loop.
#[tracing::instrument(skip(storage, router))]
pub async fn cron_tick_once(
    storage: &Storage,
    router: &dyn Router,
    now: DateTime<Utc>,
) -> Result<CronTickReport> {
    let rows = storage.cron.list_schedules().await?;
    let total = rows.len();
    let enabled = rows.iter().filter(|r| r.enabled).count();
    tracing::debug!(total, enabled, "cron: evaluating schedules");
    let mut report = CronTickReport::default();

    for row in rows {
        if !row.enabled {
            continue;
        }
        if let Some(when) = process_row(storage, router, &row, now, &mut report).await {
            report.next_due = Some(report.next_due.map_or(when, |cur| cur.min(when)));
        }
    }
    Ok(report)
}

/// Process one schedule row. Returns the row's next fire time (so the
/// caller can compute the soonest loop wake-up), or `None` if the row
/// has no valid upcoming fire (unparseable expression, or the cron
/// yields no next occurrence).
async fn process_row(
    storage: &Storage,
    router: &dyn Router,
    row: &CronScheduleRecord,
    now: DateTime<Utc>,
    report: &mut CronTickReport,
) -> Option<DateTime<Utc>> {
    let sched = match parse_cron(&row.cron_expr) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(name = row.name, error = %e, "cron: invalid expression");
            if let Err(persist_err) = storage
                .cron
                .record_parse_error(&row.name, &format!("invalid cron expression: {e}"))
                .await
            {
                tracing::warn!(
                    name = row.name,
                    ?persist_err,
                    "cron: failed to persist parse error"
                );
            }
            report.errors += 1;
            return None;
        }
    };
    let next = next_cron_after(&sched, now)?;

    match row.next_fire_at {
        None => {
            // Newly enabled / never fired — seed by recording a
            // "fire" at the original last_fired_at (or `now` if
            // unset) with the next_at scheduled forward. This sets
            // next_fire_at without enqueueing.
            let fired_at = row.last_fired_at.unwrap_or(now);
            if let Err(e) = storage.cron.record_fire(&row.name, fired_at, next).await {
                tracing::warn!(name = row.name, ?e, "cron: seed record_fire failed");
                report.errors += 1;
            } else {
                report.seeded += 1;
            }
            Some(next)
        }
        Some(at) if at <= now => {
            // Claim the fire by advancing next_fire_at atomically — only
            // the winner enqueues. Without this, a leadership handoff
            // mid-tick (the old leader stalled past its lease) could let
            // a second replica fire the same schedule. We advance before
            // enqueueing, so a rare enqueue error skips this occurrence
            // rather than risking a double-fire.
            match storage
                .cron
                .try_advance_fire(&row.name, at, now, next)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    tracing::debug!(name = row.name, "cron: fire already claimed elsewhere");
                    return Some(next);
                }
                Err(e) => {
                    tracing::warn!(name = row.name, ?e, "cron: claim fire failed");
                    report.errors += 1;
                    return Some(next);
                }
            }
            tracing::info!(name = row.name, kind = row.kind, "cron: firing schedule");
            let queue_name = row.queue_name.clone().map_or_else(
                || Cow::Borrowed(router.route(row.kind.as_str())),
                Cow::Owned,
            );
            let req = EnqueueRequest {
                kind: Cow::Owned(row.kind.clone()),
                payload: row.payload.clone(),
                queue_name: Some(queue_name),
                dedupe_key: None,
                max_attempts: row.max_attempts,
                run_at: None,
                priority: 0,
            };
            match storage.jobs.enqueue(req).await {
                Ok(_) => report.fired += 1,
                Err(e) => {
                    tracing::warn!(name = row.name, error = %e, "cron: enqueue failed");
                    report.errors += 1;
                    if let Err(persist_err) = storage
                        .cron
                        .record_parse_error(&row.name, &format!("enqueue failed: {e}"))
                        .await
                    {
                        tracing::warn!(name = row.name, ?persist_err, "cron: persist err failed");
                    }
                }
            }
            // Next occurrence after this fire.
            Some(next)
        }
        // Not yet due — its stored fire time is the next wake-up.
        Some(at) => Some(at),
    }
}

/// Periodic loop. Spawned by `QueueRuntime::start` alongside the
/// reaper and cleanup tasks.
pub(super) async fn cron_loop(
    storage: Storage,
    router: Arc<dyn Router>,
    host_id: String,
    shutdown: CancellationToken,
) {
    tracing::debug!(%host_id, "cron: start");
    // Sleep until the next schedule is due, capped at CRON_TICK. The
    // cap keeps the lease renewed (CRON_TICK << CRON_LEASE_TTL) and lets
    // a non-leader contest leadership promptly if the leader dies, while
    // still firing schedules on time rather than at fixed boundaries.
    // First wake is immediate so schedules seed at startup.
    let mut next_sleep = Duration::ZERO;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::debug!("cron: shutdown");
                return;
            }
            () = tokio::time::sleep(next_sleep) => {
                // Cluster-wide leader election: only the lease holder
                // fires schedules, so N replicas don't each enqueue
                // every schedule N times. A non-leader still wakes (to
                // contest the lease if the leader dies) but does no
                // work. SQLite is single-process and always wins.
                match storage.cron.try_cron_lease(&host_id, CRON_LEASE_TTL).await {
                    Ok(false) => {
                        tracing::trace!(%host_id, "cron: not leader this tick");
                        next_sleep = CRON_TICK;
                        continue;
                    }
                    Ok(true) => {}
                    Err(e) => {
                        tracing::warn!(?e, %host_id, "cron: lease check failed; skipping tick");
                        next_sleep = CRON_TICK;
                        continue;
                    }
                }
                let now = Utc::now();
                let report = match cron_tick_once(&storage, router.as_ref(), now).await {
                    Ok(report) => {
                        if report.fired > 0 || report.seeded > 0 || report.errors > 0 {
                            tracing::info!(
                                fired = report.fired,
                                seeded = report.seeded,
                                errors = report.errors,
                                "cron: tick"
                            );
                        }
                        report
                    }
                    Err(e) => {
                        tracing::warn!(?e, "cron: tick failed");
                        CronTickReport::default()
                    }
                };
                next_sleep = sleep_until_next(report.next_due, now);
            }
        }
    }
}

/// How long to sleep before the next cron evaluation: until the soonest
/// upcoming schedule, floored at [`CRON_MIN_SLEEP`] (so a due-now
/// schedule doesn't spin) and capped at [`CRON_TICK`] (so the lease
/// stays renewed and leadership failover stays prompt).
fn sleep_until_next(next_due: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Duration {
    // `to_std` errors when the instant is already past (overdue) — fall
    // back to the floor so we re-evaluate promptly. `None` (nothing
    // scheduled) idles at the cap, which still renews the lease.
    next_due.map_or(CRON_TICK, |t| {
        (t - now)
            .to_std()
            .unwrap_or(CRON_MIN_SLEEP)
            .clamp(CRON_MIN_SLEEP, CRON_TICK)
    })
}

/// Next firing strictly after `now`, evaluated in **UTC**.
///
/// M3: cron expressions are evaluated against UTC, not the server's local
/// timezone, so every replica computes the same `next_fire_at` for a given
/// expression regardless of its `TZ` — leadership handoff can't shift the
/// cadence, and there's no DST skip/double. The UI renders these instants
/// in the operator's local zone for display; the schedule itself is
/// timezone-stable. (A future per-schedule IANA timezone column could
/// restore local-time semantics deterministically if a host needs it.)
fn next_cron_after(sched: &cron::Schedule, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    sched.after(&now).next()
}

/// Ensure a list of cron schedules exists on `storage`.
///
/// Idempotent — re-running on subsequent boots is safe;
/// `CronStorage::ensure_schedule` preserves any user-edited fields
/// (enabled flag, cron expression) from previous runs.
///
/// Hosts pass their full schedule list once at boot. The actual host
/// schedules are kind-specific (they reference `KIND` constants from
/// the host's own handler modules), so the list of `NewCronSchedule`s
/// is built host-side and passed in.
pub async fn ensure_schedules(
    storage: &crate::Storage,
    schedules: &[crate::NewCronSchedule],
) -> crate::storage::error::Result<()> {
    for schedule in schedules {
        storage.cron.ensure_schedule(schedule.clone()).await?;
    }
    tracing::info!(count = schedules.len(), "cron: default schedules ensured");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sleep_until_next_clamps_to_bounds() {
        let now = Utc::now();
        // Far future → capped at the tick (keeps the lease renewed).
        assert_eq!(
            sleep_until_next(Some(now + chrono::Duration::hours(1)), now),
            CRON_TICK
        );
        // Overdue → floored (re-evaluate promptly).
        assert_eq!(
            sleep_until_next(Some(now - chrono::Duration::seconds(5)), now),
            CRON_MIN_SLEEP
        );
        // Nothing scheduled → idle at the cap.
        assert_eq!(sleep_until_next(None, now), CRON_TICK);
        // Within the window → sleep exactly that long.
        assert_eq!(
            sleep_until_next(Some(now + chrono::Duration::seconds(2)), now),
            Duration::from_secs(2)
        );
    }
}
