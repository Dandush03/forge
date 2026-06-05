//! 6-field cron-expression parser, shared by the queue's
//! `runtime::cron` and (when the `legacy-scheduler` feature is on)
//! the cooperative `Scheduler`.
//!
//! Wraps `cron::Schedule::from_str` so callers don't drag the `cron`
//! crate into their signatures. Returns the error as a `String` so
//! host code can log + display rejection reasons consistently.

/// Parse a 6-field cron expression (`sec min hour dom mon dow`) and
/// return the compiled schedule on success.
///
/// Host code should validate at the edge with this same function so
/// rejection reasons match what the queue / scheduler would
/// otherwise silently log.
///
/// # Errors
///
/// Returns the `cron` crate's error as a `String` when the expression
/// is malformed.
pub fn parse_cron(expr: &str) -> std::result::Result<cron::Schedule, String> {
    use std::str::FromStr;
    cron::Schedule::from_str(expr).map_err(|e| e.to_string())
}
