//! `QueueConfig` impl on `SQLite`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;

use super::{SqliteStorage, map_sqlx_err};
use crate::storage::QueueConfig;
use crate::storage::error::{Result, StorageError};
use crate::storage::types::QueueConfigRow;

#[async_trait]
impl QueueConfig for SqliteStorage {
    async fn ensure_queue(&self, name: &str, default_max_workers: i32) -> Result<()> {
        let now = iso(Utc::now());
        // INSERT … ON CONFLICT DO NOTHING — preserves user-tuned values.
        // Backoff defaults: disabled, base=60s, max=1800s. Defaults
        // also live on the column DEFAULTs so an ALTER-only path still
        // works; spelling them here keeps the row well-formed.
        sqlx::query(
            r"INSERT INTO queue
                (name, max_workers, paused, retain_done_for_days, retain_dead_for_days,
                 backoff_enabled, backoff_base_seconds, backoff_max_seconds, updated_at)
              VALUES (?1, ?2, 0, 7, 30, 0, 60, 1800, ?3)
              ON CONFLICT(name) DO NOTHING",
        )
        .bind(name)
        .bind(i64::from(default_max_workers))
        .bind(&now)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn get_queue(&self, name: &str) -> Result<Option<QueueConfigRow>> {
        let row = sqlx::query("SELECT * FROM queue WHERE name = ?1")
            .bind(name)
            .fetch_optional(&self.read_pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_config).transpose()
    }

    async fn list_queues(&self) -> Result<Vec<QueueConfigRow>> {
        let rows = sqlx::query("SELECT * FROM queue ORDER BY name ASC")
            .fetch_all(&self.read_pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_config).collect()
    }

    async fn set_max_workers(&self, name: &str, n: i32) -> Result<()> {
        let clamped = n.clamp(0, 64);
        let now = iso(Utc::now());
        sqlx::query("UPDATE queue SET max_workers = ?1, updated_at = ?2 WHERE name = ?3")
            .bind(i64::from(clamped))
            .bind(&now)
            .bind(name)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn set_paused(&self, name: &str, paused: bool) -> Result<()> {
        let now = iso(Utc::now());
        sqlx::query("UPDATE queue SET paused = ?1, updated_at = ?2 WHERE name = ?3")
            .bind(i64::from(paused))
            .bind(&now)
            .bind(name)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn set_retention(&self, name: &str, done_days: i32, dead_days: i32) -> Result<()> {
        let now = iso(Utc::now());
        sqlx::query(
            r"UPDATE queue
                 SET retain_done_for_days = ?1,
                     retain_dead_for_days = ?2,
                     updated_at = ?3
               WHERE name = ?4",
        )
        .bind(i64::from(done_days.max(0)))
        .bind(i64::from(dead_days.max(0)))
        .bind(&now)
        .bind(name)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn set_backoff(
        &self,
        name: &str,
        enabled: bool,
        base_seconds: i32,
        max_seconds: i32,
    ) -> Result<()> {
        let base = base_seconds.clamp(1, 86_400);
        let max = max_seconds.clamp(1, 86_400);
        let now = iso(Utc::now());
        sqlx::query(
            r"UPDATE queue
                 SET backoff_enabled      = ?1,
                     backoff_base_seconds = ?2,
                     backoff_max_seconds  = ?3,
                     updated_at           = ?4
               WHERE name = ?5",
        )
        .bind(i64::from(enabled))
        .bind(i64::from(base))
        .bind(i64::from(max))
        .bind(&now)
        .bind(name)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }
}

fn iso(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Parse a nullable RFC3339 timestamp column into `Option<DateTime>`.
fn parse_opt_dt(raw: Option<String>) -> Result<Option<DateTime<Utc>>> {
    raw.map(|s| {
        DateTime::parse_from_rfc3339(&s)
            .map(|d| d.with_timezone(&Utc))
            .map_err(|e| StorageError::Backend(format!("bad datetime {s:?}: {e}")))
    })
    .transpose()
}

fn row_to_config(r: &sqlx::sqlite::SqliteRow) -> Result<QueueConfigRow> {
    let updated_at: String = r.try_get("updated_at").map_err(map_sqlx_err)?;
    let updated_at = DateTime::parse_from_rfc3339(&updated_at)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| StorageError::Backend(format!("bad datetime {updated_at:?}: {e}")))?;
    Ok(QueueConfigRow {
        name: r.try_get("name").map_err(map_sqlx_err)?,
        max_workers: r
            .try_get::<i64, _>("max_workers")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(1),
        paused: r.try_get::<i64, _>("paused").map_err(map_sqlx_err)? != 0,
        retain_done_for_days: r
            .try_get::<i64, _>("retain_done_for_days")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(7),
        retain_dead_for_days: r
            .try_get::<i64, _>("retain_dead_for_days")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(30),
        backoff_enabled: r
            .try_get::<i64, _>("backoff_enabled")
            .map_err(map_sqlx_err)?
            != 0,
        backoff_base_seconds: r
            .try_get::<i64, _>("backoff_base_seconds")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(60),
        backoff_max_seconds: r
            .try_get::<i64, _>("backoff_max_seconds")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(1800),
        throttle_attempts: r
            .try_get::<i64, _>("throttle_attempts")
            .map_err(map_sqlx_err)?
            .try_into()
            .unwrap_or(0),
        throttled_until: parse_opt_dt(r.try_get("throttled_until").map_err(map_sqlx_err)?)?,
        updated_at,
    })
}
