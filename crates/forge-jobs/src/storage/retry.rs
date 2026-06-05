//! Transient-conflict retry primitive for storage operations.
//!
//! Two backends produce the same class of error under load — `SQLite`
//! when the writer pool serializes contending writes ("database is
//! locked"), Postgres when a deadlock or serialization-failure trips.
//! Both surface through [`crate::storage::StorageError::is_transient_conflict`]
//! and both heal on retry. Adapters wrap their hot write paths in
//! [`with_transient_retry`] so a transient surfaces as a brief
//! `tracing::warn` + retry instead of a spurious caller-facing error.

use std::future::Future;
use std::time::Duration;

use crate::storage::error::Result;

/// Exponential schedule: 100ms → 300ms → 1s, then surface the error.
/// Matches what `finalize` used inline before the helper landed; the
/// schedule is gentle enough that a healthy queue doesn't notice it
/// and short enough that a genuinely-stuck DB still surfaces in ~1.5s.
const DELAYS: &[Duration] = &[
    Duration::from_millis(100),
    Duration::from_millis(300),
    Duration::from_secs(1),
];

/// Run `op`, retrying on [`StorageError::is_transient_conflict`].
///
/// `label` is the `tracing` event identifier so retried operations
/// are filterable in logs (e.g. `"finalize"`, `"delete_batch"`).
/// Non-transient errors surface immediately; transients past the
/// last delay surface with the final error.
///
/// **TODO(claude):** callers that open a transaction inside `op`
/// retry the whole `BEGIN → work → COMMIT` cycle on a transient.
/// That's the correct semantics (a partial commit on retry is
/// worse) but costs one extra `BEGIN` per retry. If a hot batch
/// path ever shows up in profiles as `BEGIN`-heavy under deadlock
/// pressure, push the retry *inside* each statement and keep the
/// outer transaction.
#[allow(
    clippy::redundant_pub_crate,
    reason = "pub(crate) is the intended surface — adapters reach it via `use crate::storage::retry::with_transient_retry`. `pub(super)` would only reach `storage`."
)]
pub(crate) async fn with_transient_retry<T, F, Fut>(label: &'static str, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_transient_conflict() && attempt < DELAYS.len() => {
                tracing::warn!(
                    label,
                    attempt,
                    delay_ms = DELAYS[attempt].as_millis(),
                    err = %e,
                    "storage: transient conflict; retrying"
                );
                tokio::time::sleep(DELAYS[attempt]).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "unit tests crash loudly on setup failure"
)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::storage::error::StorageError;

    #[tokio::test(start_paused = true)]
    async fn ok_returns_without_retry() {
        let calls = AtomicUsize::new(0);
        let r: Result<i32> = with_transient_retry("test", || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(7) }
        })
        .await;
        assert_eq!(r.unwrap(), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_retries_until_success() {
        let calls = AtomicUsize::new(0);
        let r: Result<i32> = with_transient_retry("test", || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(StorageError::Conflict("locked".to_owned()))
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 42);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "two transients + one success = 3 calls"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn transient_past_last_delay_surfaces_error() {
        let calls = AtomicUsize::new(0);
        let r: Result<i32> = with_transient_retry("test", || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(StorageError::Conflict("locked".to_owned())) }
        })
        .await;
        assert!(r.is_err(), "exhausted retries must surface the error");
        // 1 initial + DELAYS.len() retries
        assert_eq!(calls.load(Ordering::SeqCst), 1 + DELAYS.len());
    }

    #[tokio::test(start_paused = true)]
    async fn non_transient_returns_immediately() {
        let calls = AtomicUsize::new(0);
        let r: Result<i32> = with_transient_retry("test", || {
            calls.fetch_add(1, Ordering::SeqCst);
            // `NotFound` doesn't match any substring in
            // is_transient_conflict — surfaces immediately.
            async { Err(StorageError::NotFound("missing".to_owned())) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "non-transient must not retry"
        );
    }
}
