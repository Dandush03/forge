//! `CronStorage` impl on `SQLite`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;

use super::{SqliteStorage, map_sqlx_err};
use crate::storage::CronStorage;
use crate::storage::error::{Result, StorageError};
use crate::storage::types::{CronScheduleRecord, NewCronSchedule};

#[async_trait]
impl CronStorage for SqliteStorage {
    async fn ensure_schedule(&self, schedule: NewCronSchedule) -> Result<()> {
        let now = iso(Utc::now());
        let payload_s = serde_json::to_string(&schedule.payload)?;
        // INSERT or do-nothing — preserves user-edited rows.
        sqlx::query(
            r"INSERT INTO cron_schedule (
                name, kind, payload, queue_name, cron_expr, enabled,
                max_attempts, created_at, updated_at
              ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
              ON CONFLICT(name) DO NOTHING",
        )
        .bind(&schedule.name)
        .bind(&schedule.kind)
        .bind(&payload_s)
        .bind(schedule.queue_name.as_deref())
        .bind(&schedule.cron_expr)
        .bind(i64::from(schedule.enabled))
        .bind(schedule.max_attempts.map(i64::from))
        .bind(&now)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn list_schedules(&self) -> Result<Vec<CronScheduleRecord>> {
        let rows = sqlx::query("SELECT * FROM cron_schedule ORDER BY name ASC")
            .fetch_all(&self.read_pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_cron).collect()
    }

    async fn record_fire(
        &self,
        name: &str,
        fired_at: DateTime<Utc>,
        next_at: DateTime<Utc>,
    ) -> Result<()> {
        let now = iso(Utc::now());
        sqlx::query(
            r"UPDATE cron_schedule
                 SET last_fired_at = ?1,
                     next_fire_at  = ?2,
                     last_error    = NULL,
                     updated_at    = ?3
               WHERE name = ?4",
        )
        .bind(iso(fired_at))
        .bind(iso(next_at))
        .bind(&now)
        .bind(name)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn try_advance_fire(
        &self,
        name: &str,
        expected: DateTime<Utc>,
        fired_at: DateTime<Utc>,
        next_at: DateTime<Utc>,
    ) -> Result<bool> {
        let now = iso(Utc::now());
        let res = sqlx::query(
            r"UPDATE cron_schedule
                 SET last_fired_at = ?1,
                     next_fire_at  = ?2,
                     last_error    = NULL,
                     updated_at    = ?3
               WHERE name = ?4 AND next_fire_at = ?5",
        )
        .bind(iso(fired_at))
        .bind(iso(next_at))
        .bind(&now)
        .bind(name)
        .bind(iso(expected))
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected() == 1)
    }

    async fn record_parse_error(&self, name: &str, message: &str) -> Result<()> {
        let now = iso(Utc::now());
        sqlx::query(
            r"UPDATE cron_schedule
                 SET last_error = ?1,
                     enabled    = 0,
                     updated_at = ?2
               WHERE name = ?3",
        )
        .bind(message)
        .bind(&now)
        .bind(name)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn set_enabled(&self, name: &str, enabled: bool) -> Result<()> {
        let now = iso(Utc::now());
        // Re-enabling clears any stale next_fire_at so the cron loop
        // recomputes from the schedule expression on the next tick.
        if enabled {
            sqlx::query(
                r"UPDATE cron_schedule
                     SET enabled = 1, next_fire_at = NULL, last_error = NULL, updated_at = ?1
                   WHERE name = ?2",
            )
            .bind(&now)
            .bind(name)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        } else {
            sqlx::query(
                r"UPDATE cron_schedule
                     SET enabled = 0, next_fire_at = NULL, updated_at = ?1
                   WHERE name = ?2",
            )
            .bind(&now)
            .bind(name)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        }
        Ok(())
    }

    async fn set_expr(&self, name: &str, expr: &str) -> Result<()> {
        let now = iso(Utc::now());
        sqlx::query(
            r"UPDATE cron_schedule
                 SET cron_expr = ?1, next_fire_at = NULL, last_error = NULL, updated_at = ?2
               WHERE name = ?3",
        )
        .bind(expr)
        .bind(&now)
        .bind(name)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn delete_schedule(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM cron_schedule WHERE name = ?1")
            .bind(name)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn get_schedule(&self, name: &str) -> Result<Option<CronScheduleRecord>> {
        let row = sqlx::query("SELECT * FROM cron_schedule WHERE name = ?1")
            .bind(name)
            .fetch_optional(&self.read_pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_cron).transpose()
    }

    async fn try_cron_lease(&self, holder: &str, ttl: std::time::Duration) -> Result<bool> {
        let ttl_secs = i64::try_from(ttl.as_secs()).unwrap_or(15).max(1);
        // Atomic upsert: claim the lease only if it's unheld/expired or
        // already ours. RETURNING yields a row exactly when we won, so
        // `is_some()` is the leadership answer.
        let row = sqlx::query(
            r"INSERT INTO cron_leader (id, holder, lease_until)
              VALUES (1, ?1, datetime('now', '+' || ?2 || ' seconds'))
              ON CONFLICT(id) DO UPDATE
                 SET holder      = excluded.holder,
                     lease_until = excluded.lease_until
               WHERE cron_leader.lease_until < datetime('now')
                  OR cron_leader.holder = ?1
              RETURNING 1",
        )
        .bind(holder)
        .bind(ttl_secs)
        .fetch_optional(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(row.is_some())
    }
}

fn iso(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn parse_dt(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| StorageError::Backend(format!("bad datetime {s:?}: {e}")))
}

fn row_to_cron(r: &sqlx::sqlite::SqliteRow) -> Result<CronScheduleRecord> {
    let payload_s: String = r.try_get("payload").map_err(map_sqlx_err)?;
    let payload: serde_json::Value =
        serde_json::from_str(&payload_s).unwrap_or(serde_json::Value::Null);
    Ok(CronScheduleRecord {
        name: r.try_get("name").map_err(map_sqlx_err)?,
        kind: r.try_get("kind").map_err(map_sqlx_err)?,
        payload,
        queue_name: r.try_get("queue_name").map_err(map_sqlx_err)?,
        cron_expr: r.try_get("cron_expr").map_err(map_sqlx_err)?,
        enabled: r.try_get::<i64, _>("enabled").map_err(map_sqlx_err)? != 0,
        max_attempts: r
            .try_get::<Option<i64>, _>("max_attempts")
            .map_err(map_sqlx_err)?
            .map(|n| i32::try_from(n).unwrap_or(5)),
        last_fired_at: r
            .try_get::<Option<String>, _>("last_fired_at")
            .map_err(map_sqlx_err)?
            .as_deref()
            .map(parse_dt)
            .transpose()?,
        next_fire_at: r
            .try_get::<Option<String>, _>("next_fire_at")
            .map_err(map_sqlx_err)?
            .as_deref()
            .map(parse_dt)
            .transpose()?,
        last_error: r.try_get("last_error").map_err(map_sqlx_err)?,
        created_at: parse_dt(&r.try_get::<String, _>("created_at").map_err(map_sqlx_err)?)?,
        updated_at: parse_dt(&r.try_get::<String, _>("updated_at").map_err(map_sqlx_err)?)?,
    })
}
