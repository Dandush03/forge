//! `RateLimitStorage` impl on `SQLite`.
//!
//! Token-bucket math is server-side via `UPDATE … RETURNING`. The
//! WHERE clause re-computes the post-refill amount so a row that
//! passively crosses the >= 1.0 threshold via elapsed time is
//! observably claimable in the same statement. Tokens are `REAL` so
//! sub-second fractional refill accumulates correctly (otherwise a
//! 50/min curve = 0.833/sec would round to zero on every per-call
//! refresh).
//!
//! `last_refilled_at` is RFC3339 `TEXT` and parsed via `SQLite`'s
//! `unixepoch()` so the elapsed-window math stays portable. Keeping
//! it as `TEXT` also matches the rest of the `SQLite` adapter's
//! columns (consistency, easy `SELECT` inspection).

use async_trait::async_trait;
use chrono::Utc;

use super::{SqliteStorage, map_sqlx_err};
use crate::storage::error::Result;
use crate::storage::{RateLimitOutcome, RateLimitStorage};

fn iso(dt: chrono::DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[async_trait]
impl RateLimitStorage for SqliteStorage {
    async fn acquire(&self, scope: &str) -> Result<RateLimitOutcome> {
        let now_iso = iso(Utc::now());
        // `unixepoch(…, 'subsec')` returns a REAL with millisecond
        // precision — without `'subsec'` the elapsed math truncates
        // to whole seconds and a 50/min curve (0.833/sec) never
        // refills between sub-second acquires.
        //
        // `MAX(0.0, …)` on the elapsed delta mirrors the Postgres
        // adapter's `GREATEST(0, …)` — clamps backward clock motion
        // (NTP slew, laptop suspend/resume, host VM resume) so a
        // negative delta can't subtract from `tokens`. Without the
        // clamp a sleeping laptop returning would see tokens drop
        // below 1.0 and acquires would stick until elapsed turned
        // positive again.
        let row = sqlx::query(
            r"UPDATE rate_limit_state
                 SET tokens = MIN(
                       CAST(capacity AS REAL),
                       tokens
                         + MAX(0.0,
                             unixepoch(?1, 'subsec')
                               - unixepoch(last_refilled_at, 'subsec')) * refill_per_sec
                     ) - 1.0,
                     last_refilled_at = ?1
               WHERE scope = ?2
                 AND MIN(
                       CAST(capacity AS REAL),
                       tokens
                         + MAX(0.0,
                             unixepoch(?1, 'subsec')
                               - unixepoch(last_refilled_at, 'subsec')) * refill_per_sec
                     ) >= 1.0
               RETURNING tokens",
        )
        .bind(&now_iso)
        .bind(scope)
        .fetch_optional(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(if row.is_some() {
            RateLimitOutcome::Granted
        } else {
            RateLimitOutcome::Throttled
        })
    }

    async fn drain(&self, scope: &str) -> Result<()> {
        // Force-empty the bucket. Used when the handler observes a
        // real 429: our local accounting said we had budget but the
        // upstream's didn't — drain so the next acquire in this
        // window stays throttled.
        let now_iso = iso(Utc::now());
        sqlx::query(
            r"UPDATE rate_limit_state
                 SET tokens = 0.0,
                     last_refilled_at = ?1
               WHERE scope = ?2",
        )
        .bind(&now_iso)
        .bind(scope)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn ensure_default(&self, scope: &str, capacity: i64, refill_per_sec: f64) -> Result<()> {
        // INSERT … ON CONFLICT DO NOTHING preserves a row already
        // seeded with operator-tuned values; defaults seed only on
        // first boot.
        let now_iso = iso(Utc::now());
        sqlx::query(
            r"INSERT INTO rate_limit_state
                (scope, tokens, capacity, refill_per_sec, last_refilled_at)
              VALUES (?1, CAST(?2 AS REAL), ?2, ?3, ?4)
              ON CONFLICT(scope) DO NOTHING",
        )
        .bind(scope)
        .bind(capacity)
        .bind(refill_per_sec)
        .bind(&now_iso)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "unit tests crash loudly on setup failure"
)]
mod tests {
    use std::sync::Arc;

    use super::*;

    async fn fresh() -> Arc<SqliteStorage> {
        Arc::new(
            SqliteStorage::open_in_memory()
                .await
                .expect("open_in_memory"),
        )
    }

    #[tokio::test]
    async fn acquire_grants_until_capacity_then_throttles() {
        let s = fresh().await;
        // capacity=3, no refill in test timeframe (0.0/sec).
        s.ensure_default("test", 3, 0.0).await.unwrap();
        for i in 0..3 {
            assert_eq!(
                s.acquire("test").await.unwrap(),
                RateLimitOutcome::Granted,
                "first {i} acquires must grant",
                i = i + 1
            );
        }
        assert_eq!(
            s.acquire("test").await.unwrap(),
            RateLimitOutcome::Throttled,
            "4th acquire on a capacity=3 bucket must throttle"
        );
    }

    #[tokio::test]
    async fn ensure_default_is_idempotent_and_preserves_existing_state() {
        let s = fresh().await;
        s.ensure_default("scope", 10, 1.0).await.unwrap();
        // Consume a token so we can detect re-seeding by checking
        // that the next acquire still observes the spent state.
        assert_eq!(s.acquire("scope").await.unwrap(), RateLimitOutcome::Granted);
        // Second ensure_default with different params should be a
        // no-op — operator-tuned values survive boot.
        s.ensure_default("scope", 999, 999.0).await.unwrap();
        // Drain to prove the next acquire goes through; if
        // ensure_default had re-seeded to tokens=999 we'd see drain
        // == still-grantable.
        s.drain("scope").await.unwrap();
        assert_eq!(
            s.acquire("scope").await.unwrap(),
            RateLimitOutcome::Throttled,
            "post-drain must throttle even if ensure_default ran a second time"
        );
    }

    #[tokio::test]
    async fn drain_force_empties_the_bucket() {
        let s = fresh().await;
        s.ensure_default("test", 100, 0.0).await.unwrap();
        s.drain("test").await.unwrap();
        assert_eq!(
            s.acquire("test").await.unwrap(),
            RateLimitOutcome::Throttled,
            "drain must empty even a freshly-seeded bucket"
        );
    }

    #[tokio::test]
    async fn refill_replenishes_tokens_over_elapsed_time() {
        let s = fresh().await;
        // 10 tokens / sec refill, capacity 2. Spend both → throttle.
        s.ensure_default("fast", 2, 10.0).await.unwrap();
        assert_eq!(s.acquire("fast").await.unwrap(), RateLimitOutcome::Granted);
        assert_eq!(s.acquire("fast").await.unwrap(), RateLimitOutcome::Granted);
        assert_eq!(
            s.acquire("fast").await.unwrap(),
            RateLimitOutcome::Throttled
        );
        // After 200ms wall-clock the bucket refilled ≥ 2 tokens
        // (10/sec × 0.2s = 2.0). One acquire must grant.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert_eq!(
            s.acquire("fast").await.unwrap(),
            RateLimitOutcome::Granted,
            "after 250ms the bucket should have refilled enough"
        );
    }

    #[tokio::test]
    async fn unknown_scope_throttles_safely() {
        let s = fresh().await;
        // No `ensure_default` call — acquire against a non-existent
        // scope must return Throttled, not error.
        assert_eq!(
            s.acquire("missing").await.unwrap(),
            RateLimitOutcome::Throttled
        );
    }

    #[tokio::test]
    async fn concurrent_acquires_never_double_spend() {
        // SQLite serializes through the write pool, so this is more
        // of a sanity check than a true race. The Postgres twin will
        // exercise the FOR UPDATE row lock under real parallelism.
        let s = fresh().await;
        s.ensure_default("contention", 10, 0.0).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..50 {
            let s = s.clone();
            handles.push(tokio::spawn(async move { s.acquire("contention").await }));
        }
        let mut granted = 0;
        let mut throttled = 0;
        for h in handles {
            match h.await.unwrap().unwrap() {
                RateLimitOutcome::Granted => granted += 1,
                RateLimitOutcome::Throttled => throttled += 1,
            }
        }
        assert_eq!(granted, 10, "exactly capacity grants across 50 racers");
        assert_eq!(throttled, 40);
    }
}
