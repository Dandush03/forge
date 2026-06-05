//! `ProcessRegistry` impl on `SQLite`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;

use super::{SqliteStorage, map_sqlx_err};
use crate::storage::ProcessRegistry;
use crate::storage::error::{Result, StorageError};
use crate::storage::types::{JobId, ProcessRecord};

#[async_trait]
impl ProcessRegistry for SqliteStorage {
    async fn register(&self, process_id: &str, queue: &str, host: &str) -> Result<()> {
        let now = iso(Utc::now());
        // INSERT OR REPLACE — process_id is the PK. Replaces any
        // existing partial row stamped by heartbeat() during a
        // restart, healing the row to the right shape.
        sqlx::query(
            r"INSERT OR REPLACE INTO queue_process
                (process_id, queue_name, host_id, started_at, heartbeat_at, current_job)
              VALUES (?1, ?2, ?3, ?4, ?4, NULL)",
        )
        .bind(process_id)
        .bind(queue)
        .bind(host)
        .bind(&now)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn heartbeat(&self, process_id: &str, current_job: Option<JobId>) -> Result<()> {
        let now = iso(Utc::now());
        let current_job_str = current_job.as_ref().map(JobId::as_str);
        // UPDATE first; if no row touched, INSERT a partial row that
        // the next `register` heals. Same self-healing semantics as
        // the surrealkv version.
        let res = sqlx::query(
            r"UPDATE queue_process
                 SET heartbeat_at = ?1, current_job = ?2
               WHERE process_id = ?3",
        )
        .bind(&now)
        .bind(current_job_str)
        .bind(process_id)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        if res.rows_affected() > 0 {
            return Ok(());
        }
        sqlx::query(
            r"INSERT OR REPLACE INTO queue_process
                (process_id, queue_name, host_id, started_at, heartbeat_at, current_job)
              VALUES (?1, '', '', ?2, ?2, ?3)",
        )
        .bind(process_id)
        .bind(&now)
        .bind(current_job_str)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn deregister(&self, process_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM queue_process WHERE process_id = ?1")
            .bind(process_id)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn reap_stale(&self, stale_before: DateTime<Utc>) -> Result<u64> {
        let cutoff = iso(stale_before);
        let res = sqlx::query("DELETE FROM queue_process WHERE heartbeat_at < ?1")
            .bind(&cutoff)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        // Evict crashed pods + their orphaned slot assignments. Without
        // this, an ungraceful exit (delete_for_host only runs on a clean
        // shutdown) leaks one `pod` row + N assignment rows per dead host
        // forever — every rollout mints a fresh host_id.
        sqlx::query("DELETE FROM pod WHERE heartbeat_at < ?1")
            .bind(&cutoff)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        sqlx::query(
            "DELETE FROM pod_slot_assignment
              WHERE host_id NOT IN (SELECT host_id FROM pod)",
        )
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected())
    }

    async fn list(&self, queue: Option<&str>) -> Result<Vec<ProcessRecord>> {
        let rows = if let Some(q) = queue {
            sqlx::query("SELECT * FROM queue_process WHERE queue_name = ?1 ORDER BY process_id ASC")
                .bind(q)
                .fetch_all(&self.read_pool)
                .await
        } else {
            sqlx::query("SELECT * FROM queue_process ORDER BY queue_name ASC, process_id ASC")
                .fetch_all(&self.read_pool)
                .await
        }
        .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_proc).collect()
    }

    async fn delete_for_host(&self, host: &str) -> Result<u64> {
        // queue_process + pod presence + slot assignments. A graceful
        // exit frees the pod from the cluster view immediately so the
        // next rebalance redistributes its slots without waiting out
        // the stale window.
        let res = sqlx::query("DELETE FROM queue_process WHERE host_id = ?1")
            .bind(host)
            .execute(&self.write_pool)
            .await
            .map_err(map_sqlx_err)?;
        for sql in [
            "DELETE FROM pod WHERE host_id = ?1",
            "DELETE FROM pod_slot_assignment WHERE host_id = ?1",
        ] {
            sqlx::query(sql)
                .bind(host)
                .execute(&self.write_pool)
                .await
                .map_err(map_sqlx_err)?;
        }
        Ok(res.rows_affected())
    }

    async fn pod_heartbeat(&self, host: &str) -> Result<()> {
        let now = iso(Utc::now());
        sqlx::query(
            r"INSERT INTO pod (host_id, heartbeat_at) VALUES (?1, ?2)
              ON CONFLICT(host_id) DO UPDATE SET heartbeat_at = excluded.heartbeat_at",
        )
        .bind(host)
        .bind(&now)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn list_live_pods(&self, stale_before: DateTime<Utc>) -> Result<Vec<String>> {
        let rows =
            sqlx::query("SELECT host_id FROM pod WHERE heartbeat_at >= ?1 ORDER BY host_id ASC")
                .bind(iso(stale_before))
                .fetch_all(&self.read_pool)
                .await
                .map_err(map_sqlx_err)?;
        rows.iter()
            .map(|r| r.try_get::<String, _>("host_id").map_err(map_sqlx_err))
            .collect()
    }

    async fn set_slots(&self, queue: &str, host: &str, slots: i32) -> Result<()> {
        let now = iso(Utc::now());
        sqlx::query(
            r"INSERT INTO pod_slot_assignment (queue_name, host_id, slots, updated_at)
              VALUES (?1, ?2, ?3, ?4)
              ON CONFLICT(queue_name, host_id) DO UPDATE
                 SET slots = excluded.slots, updated_at = excluded.updated_at",
        )
        .bind(queue)
        .bind(host)
        .bind(i64::from(slots.max(0)))
        .bind(&now)
        .execute(&self.write_pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(())
    }

    async fn get_slots(&self, queue: &str, host: &str) -> Result<Option<i32>> {
        let row = sqlx::query(
            "SELECT slots FROM pod_slot_assignment WHERE queue_name = ?1 AND host_id = ?2",
        )
        .bind(queue)
        .bind(host)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(map_sqlx_err)?;
        row.map(|r| {
            r.try_get::<i64, _>("slots")
                .map_err(map_sqlx_err)
                .map(|n| i32::try_from(n).unwrap_or(0))
        })
        .transpose()
    }
}

fn iso(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn row_to_proc(r: &sqlx::sqlite::SqliteRow) -> Result<ProcessRecord> {
    let parse_dt = |s: String| -> Result<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(&s)
            .map(|d| d.with_timezone(&Utc))
            .map_err(|e| StorageError::Backend(format!("bad datetime {s:?}: {e}")))
    };
    Ok(ProcessRecord {
        process_id: r.try_get("process_id").map_err(map_sqlx_err)?,
        queue_name: r.try_get("queue_name").map_err(map_sqlx_err)?,
        host_id: r.try_get("host_id").map_err(map_sqlx_err)?,
        started_at: parse_dt(r.try_get("started_at").map_err(map_sqlx_err)?)?,
        heartbeat_at: parse_dt(r.try_get("heartbeat_at").map_err(map_sqlx_err)?)?,
        current_job: r
            .try_get::<Option<String>, _>("current_job")
            .map_err(map_sqlx_err)?
            .map(JobId::new),
    })
}
