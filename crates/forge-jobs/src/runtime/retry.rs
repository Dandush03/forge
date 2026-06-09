//! Retry / backoff helpers.
//!
//! Pure functions — no I/O — so they're trivially unit-testable
//! independent of the runtime.

use std::time::Duration;

/// Fallback throttle delay used when a queue has `backoff_enabled =
/// false`. Matches the pre-toggle behaviour every queue had before
/// the per-queue backoff config landed.
pub(super) const FALLBACK_THROTTLE_SECS: u64 = 60;

/// Grace period before a post-window success decays the throttle
/// exponent back to zero.
///
/// The cool-down *gate* reopens at `throttled_until` (throughput
/// resumes immediately), but the exponent only resets once the limiter
/// has stayed quiet for this long *past* the last window. Without it, a
/// single success in the gap between a window ending and the limiter
/// flapping back to 429 resets the curve to `base` — so a flapping
/// limiter never escalates, it just oscillates at `base`. In short:
/// "did we throttle in the last couple of minutes?" — if so, keep the
/// exponent.
// `pub(crate)` (re-exported via `runtime`) so the storage adapters can
// reach it from their `clear_queue_cooldown`. Same rationale as
// `failed_delay` below — `pub(super)` only reaches `runtime`, and `pub`
// would leak it as public API.
#[allow(clippy::redundant_pub_crate, reason = "see comment above")]
pub(crate) const THROTTLE_DECAY_GRACE_SECS: i64 = 120;

/// Per-queue throttle delay curve. Used by `map_outcome` for the
/// `JobOutcome::Throttled` arm. When `enabled = false`, returns a
/// flat [`FALLBACK_THROTTLE_SECS`] regardless of `throttle_attempts`
/// (legacy behaviour). When `enabled = true`, returns
/// `min(base * 2^throttle_attempts, max)`.
#[must_use]
pub(super) fn throttle_delay(
    throttle_attempts: i32,
    enabled: bool,
    base_seconds: i32,
    max_seconds: i32,
) -> Duration {
    if !enabled {
        return Duration::from_secs(FALLBACK_THROTTLE_SECS);
    }
    let base = u64::try_from(base_seconds.max(1)).unwrap_or(1);
    let max = u64::try_from(max_seconds.max(1)).unwrap_or(FALLBACK_THROTTLE_SECS);
    // Clamp exponent so `1 << exp` stays well inside u64; the max
    // cap below catches anything that grows past `max` anyway.
    let exp = u32::try_from(throttle_attempts.max(0)).unwrap_or(0).min(30);
    let secs = base.saturating_mul(1u64 << exp).min(max);
    Duration::from_secs(secs)
}

/// Per-queue failure backoff curve. Used by `map_outcome` for the
/// `JobOutcome::Failed` arm.
///
/// When `enabled = false`, returns [`Duration::ZERO`] — the failed
/// job is immediately re-claimable (`max_attempts` still gates the
/// trip to `dead`). When `enabled = true`, returns
/// `min(base * 2^attempts, max)` — same shape as [`throttle_delay`],
/// just keyed on the job's own attempt counter rather than the
/// queue-wide throttle counter.
// `pub(crate)` so storage adapters (sqlite/postgres `revive_stale`)
// can reach this for stale-heartbeat revives. Lint thinks `pub(crate)`
// inside a private module is redundant, but `pub(super)` only reaches
// `runtime`, and `pub` would surface this as public API surface we
// don't want.
#[allow(clippy::redundant_pub_crate, reason = "see comment above")]
#[must_use]
pub(crate) fn failed_delay(
    attempts: i32,
    enabled: bool,
    base_seconds: i32,
    max_seconds: i32,
) -> Duration {
    if !enabled {
        return Duration::ZERO;
    }
    let base = u64::try_from(base_seconds.max(1)).unwrap_or(1);
    let max = u64::try_from(max_seconds.max(1)).unwrap_or(FALLBACK_THROTTLE_SECS);
    let exp = u32::try_from(attempts.max(0)).unwrap_or(0).min(30);
    let secs = base.saturating_mul(1u64 << exp).min(max);
    Duration::from_secs(secs)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn throttle_disabled_returns_flat_fallback() {
        let d = throttle_delay(0, false, 60, 1800);
        assert_eq!(d.as_secs(), FALLBACK_THROTTLE_SECS);
        // Even after many consecutive throttle hits.
        let d = throttle_delay(20, false, 60, 1800);
        assert_eq!(d.as_secs(), FALLBACK_THROTTLE_SECS);
    }

    #[test]
    fn throttle_enabled_grows_exponentially() {
        assert_eq!(throttle_delay(0, true, 60, 1800).as_secs(), 60);
        assert_eq!(throttle_delay(1, true, 60, 1800).as_secs(), 120);
        assert_eq!(throttle_delay(2, true, 60, 1800).as_secs(), 240);
        assert_eq!(throttle_delay(5, true, 60, 1800).as_secs(), 1800);
    }

    #[test]
    fn throttle_enabled_caps_at_max() {
        // 60 * 2^30 would overflow without saturation; the cap holds.
        assert_eq!(throttle_delay(30, true, 60, 1800).as_secs(), 1800);
        assert_eq!(throttle_delay(100, true, 60, 1800).as_secs(), 1800);
    }

    #[test]
    fn failed_disabled_returns_zero() {
        // With backoff off, a failure is immediately re-claimable;
        // `max_attempts` still gates the trip to `dead`.
        assert_eq!(failed_delay(0, false, 60, 1800), Duration::ZERO);
        assert_eq!(failed_delay(1, false, 60, 1800), Duration::ZERO);
        assert_eq!(failed_delay(20, false, 60, 1800), Duration::ZERO);
        // Even with bogus config values, off means off.
        assert_eq!(failed_delay(5, false, 0, 0), Duration::ZERO);
    }

    #[test]
    fn failed_enabled_grows_exponentially() {
        assert_eq!(failed_delay(0, true, 60, 1800).as_secs(), 60);
        assert_eq!(failed_delay(1, true, 60, 1800).as_secs(), 120);
        assert_eq!(failed_delay(2, true, 60, 1800).as_secs(), 240);
        assert_eq!(failed_delay(5, true, 60, 1800).as_secs(), 1800);
    }

    #[test]
    fn failed_enabled_caps_at_max() {
        assert_eq!(failed_delay(30, true, 60, 1800).as_secs(), 1800);
        assert_eq!(failed_delay(100, true, 60, 1800).as_secs(), 1800);
    }

    #[test]
    fn failed_enabled_honours_custom_base_and_max() {
        // Custom base (10s) + custom max (300s) — the curve respects both.
        assert_eq!(failed_delay(0, true, 10, 300).as_secs(), 10);
        assert_eq!(failed_delay(3, true, 10, 300).as_secs(), 80);
        assert_eq!(failed_delay(10, true, 10, 300).as_secs(), 300);
    }

    #[test]
    fn failed_negative_attempts_clamp_to_zero() {
        // Defensive: stored i32 should never be negative, but if it
        // somehow is, we treat it as the first failure (no exponent).
        assert_eq!(failed_delay(-5, true, 60, 1800).as_secs(), 60);
    }
}
