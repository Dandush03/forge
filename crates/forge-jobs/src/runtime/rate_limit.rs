//! Runtime adapter over [`crate::storage::RateLimitStorage`].
//!
//! Wraps the storage-layer token bucket so handlers reach for a
//! single typed call (`RateLimiter::acquire`) and get back an
//! `AcquireOutcome` that carries the right `retry_after` hint for
//! `JobOutcome::Throttled`. The hint is computed from the
//! per-scope refill rate baked in at boot — no extra DB round-trip
//! per acquire.

use std::collections::HashMap;
use std::time::Duration;

use crate::storage::error::Result;
use crate::storage::{RateLimitOutcome, Storage};

/// Per-scope refill rate registry built at boot. Same `&'static str`
/// scope keys the handlers use when calling `acquire`. Two-token
/// breathing room (`2.0 / refill_per_sec`) clamps to [1s, 60s] so
/// the runtime's queue cool-down has a sensible delay shape.
const RETRY_AFTER_FLOOR_SECS: f64 = 1.0;
const RETRY_AFTER_CEILING_SECS: f64 = 60.0;

/// Outcome of a single `RateLimiter::acquire` call. Mirrors
/// [`crate::storage::RateLimitOutcome`] but carries a per-scope
/// `retry_after` so handlers can plug it straight into
/// `JobOutcome::Throttled`.
#[derive(Debug, Clone, Copy)]
pub enum AcquireOutcome {
    /// Token spent; handler should proceed.
    Granted,
    /// Bucket empty. `retry_after` is sized so the next acquire on
    /// this scope is very likely to succeed (two-token cushion over
    /// the configured refill).
    Throttled { retry_after: Duration },
}

/// Cluster-wide rate limiter. Constructed at host boot from the
/// `Storage` bundle + the same default table that seeds the DB rows
/// via [`ensure_default_rate_limits`].
#[derive(Debug, Clone)]
pub struct RateLimiter {
    storage: Storage,
    refill_per_sec: HashMap<&'static str, f64>,
}

impl RateLimiter {
    /// Build a limiter that knows how to size `retry_after` for the
    /// given default scopes. Scopes not in `defaults` still work
    /// (the DB has the row, `acquire` returns the right decision)
    /// but `Throttled.retry_after` falls back to the ceiling so an
    /// unconfigured scope still throttles politely.
    #[must_use]
    pub fn new(storage: Storage, defaults: &[(&'static str, i64, f64)]) -> Self {
        let refill_per_sec = defaults.iter().map(|(s, _, r)| (*s, *r)).collect();
        Self {
            storage,
            refill_per_sec,
        }
    }

    /// Consume one token from `scope`'s bucket.
    pub async fn acquire(&self, scope: &str) -> Result<AcquireOutcome> {
        match self.storage.rate_limit.acquire(scope).await? {
            RateLimitOutcome::Granted => Ok(AcquireOutcome::Granted),
            RateLimitOutcome::Throttled => Ok(AcquireOutcome::Throttled {
                retry_after: self.retry_after(scope),
            }),
        }
    }

    /// Zero out `scope`'s bucket. Call this when the upstream
    /// returns a real 429: the next acquire in the window will see
    /// the bucket empty and force throttle, instead of optimistically
    /// firing another doomed call.
    pub async fn drain(&self, scope: &str) -> Result<()> {
        self.storage.rate_limit.drain(scope).await
    }

    fn retry_after(&self, scope: &str) -> Duration {
        let refill = self
            .refill_per_sec
            .get(scope)
            .copied()
            .filter(|r| r.is_finite() && *r > 0.0);
        let secs = refill
            .map_or(RETRY_AFTER_CEILING_SECS, |r| 2.0 / r)
            .clamp(RETRY_AFTER_FLOOR_SECS, RETRY_AFTER_CEILING_SECS);
        Duration::from_secs_f64(secs)
    }
}

/// Seed default rate-limit rows at boot.
///
/// Call once from the host's startup, alongside `ensure_queue` and
/// `ensure_default_cron_schedules`. `ON CONFLICT DO NOTHING`
/// semantics in the storage layer mean existing operator-tuned
/// rows survive.
///
/// # Errors
///
/// Surfaces any storage-layer error on the first failing scope.
pub async fn ensure_default_rate_limits(
    storage: &Storage,
    defaults: &[(&'static str, i64, f64)],
) -> Result<()> {
    for (scope, capacity, refill) in defaults {
        storage
            .rate_limit
            .ensure_default(scope, *capacity, *refill)
            .await?;
    }
    Ok(())
}

/// Default rate-limit budgets seeded at boot.
///
/// - **slack**: Tier-3 endpoint budget = 50/min. ~0.833/sec refill.
/// - **gh**: REST + GraphQL share a 5000/hour primary budget.
///   ~1.389/sec refill.
///
/// Per-handler 429 observations call `RateLimiter::drain` to zero
/// the bucket — these defaults are the optimistic ceiling, not the
/// upstream's published quota verbatim.
pub const DEFAULT_RATE_LIMIT_SCOPES: &[(&str, i64, f64)] =
    &[("slack", 50, 50.0 / 60.0), ("gh", 5000, 5000.0 / 3600.0)];

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unnecessary_semicolon,
        clippy::unreadable_literal,
        reason = "unit tests crash loudly on setup/assert failures; that's the point"
    )]

    use std::sync::Arc;

    use super::*;
    use crate::storage::Storage as JobStorage;
    use crate::storage::sqlite::SqliteStorage;

    async fn fresh() -> JobStorage {
        let s = Arc::new(
            SqliteStorage::open_in_memory()
                .await
                .expect("open_in_memory"),
        );
        JobStorage::from_one(s)
    }

    #[tokio::test]
    async fn acquire_returns_granted_then_throttled_with_sane_retry_after() {
        let storage = fresh().await;
        ensure_default_rate_limits(&storage, &[("test", 1, 1.0)])
            .await
            .unwrap();
        let limiter = RateLimiter::new(storage, &[("test", 1, 1.0)]);

        let first = limiter.acquire("test").await.unwrap();
        assert!(
            matches!(first, AcquireOutcome::Granted),
            "first acquire grants"
        );
        let second = limiter.acquire("test").await.unwrap();
        match second {
            AcquireOutcome::Throttled { retry_after } => {
                // 2.0 / 1.0 = 2.0s; well within the [1s, 60s] clamp.
                assert_eq!(retry_after, Duration::from_secs(2));
            }
            AcquireOutcome::Granted => panic!("second acquire must throttle on capacity=1"),
        }
    }

    #[tokio::test]
    async fn unknown_scope_throttles_with_ceiling_retry_after() {
        let storage = fresh().await;
        // No ensure_default for "ghost"; limiter doesn't know its
        // refill rate either. Storage acquire returns Throttled
        // (no row); runtime falls back to the ceiling.
        let limiter = RateLimiter::new(storage, &[]);
        match limiter.acquire("ghost").await.unwrap() {
            AcquireOutcome::Throttled { retry_after } => {
                assert_eq!(retry_after, Duration::from_mins(1));
            }
            AcquireOutcome::Granted => panic!("unknown scope must throttle"),
        }
    }

    #[tokio::test]
    async fn retry_after_clamps_below_floor_and_above_ceiling() {
        let storage = fresh().await;
        // Very fast refill (1000/sec) → 2/1000 = 0.002s would
        // underflow the 1s floor; very slow (0.01/sec) → 200s
        // would overflow the 60s ceiling. Both must clamp.
        ensure_default_rate_limits(&storage, &[("fast", 1, 1000.0), ("slow", 1, 0.01)])
            .await
            .unwrap();
        let limiter = RateLimiter::new(storage.clone(), &[("fast", 1, 1000.0), ("slow", 1, 0.01)]);
        let _ = limiter.acquire("fast").await.unwrap();
        match limiter.acquire("fast").await.unwrap() {
            AcquireOutcome::Throttled { retry_after } => {
                assert_eq!(retry_after, Duration::from_secs(1), "must clamp to floor");
            }
            AcquireOutcome::Granted => {}
        }
        let _ = limiter.acquire("slow").await.unwrap();
        match limiter.acquire("slow").await.unwrap() {
            AcquireOutcome::Throttled { retry_after } => {
                assert_eq!(retry_after, Duration::from_mins(1), "must clamp to ceiling");
            }
            AcquireOutcome::Granted => {}
        }
    }
}
