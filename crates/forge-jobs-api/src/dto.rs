//! Request/response shapes shared by the Tauri plugin and the
//! HTTP transport.
//!
//! This first commit only ports the DTOs needed for the scaffold's
//! one demonstration endpoint (queue overview). The remaining DTOs
//! (job rows / inspect / timeline / cron / cleanup-report) follow
//! in subsequent commits as their handlers move over from
//! `tauri-plugin-queue`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use forge_jobs::{ProcessRecord, QueueConfigRow, QueueCounts};

/// One queue's snapshot for the Mission Control overview card.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueOverviewDto {
    pub name: String,
    pub paused: bool,
    pub max_workers: i32,
    pub counts: StatusCountsDto,
    pub processes: Vec<QueueProcessDto>,
    pub retain_done_days: u32,
    pub retain_dead_days: u32,
    pub backoff_enabled: bool,
    pub backoff_base_seconds: u32,
    pub backoff_max_seconds: u32,
    /// Cool-down deadline while the queue is throttled; `None` otherwise.
    pub throttled_until: Option<DateTime<Utc>>,
    /// Age (seconds) of the oldest ready job — the queue lag.
    pub oldest_pending_age_seconds: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusCountsDto {
    pub pending: u64,
    pub scheduled: u64,
    pub in_progress: u64,
    pub done: u64,
    pub failed: u64,
    pub dead: u64,
}

impl From<QueueCounts> for StatusCountsDto {
    fn from(c: QueueCounts) -> Self {
        Self {
            pending: c.pending,
            scheduled: c.scheduled,
            in_progress: c.in_progress,
            done: c.done,
            failed: c.failed,
            dead: c.dead,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueProcessDto {
    pub process_id: String,
    pub queue_name: String,
    pub host_id: String,
    pub started_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    pub current_job_id: Option<String>,
}

impl From<ProcessRecord> for QueueProcessDto {
    fn from(p: ProcessRecord) -> Self {
        Self {
            process_id: p.process_id,
            queue_name: p.queue_name,
            host_id: p.host_id,
            started_at: p.started_at,
            heartbeat_at: p.heartbeat_at,
            current_job_id: p.current_job.map(|id| id.as_str().to_owned()),
        }
    }
}

/// Helper: build a `QueueOverviewDto` from the storage-layer pieces.
/// Lives here (rather than `From<>`) because building it needs three
/// independent storage calls — the handler does the orchestration.
#[must_use]
pub fn overview_dto(
    cfg: QueueConfigRow,
    counts: QueueCounts,
    processes: Vec<ProcessRecord>,
    oldest_pending_age_seconds: u64,
) -> QueueOverviewDto {
    QueueOverviewDto {
        name: cfg.name,
        paused: cfg.paused,
        max_workers: cfg.max_workers,
        counts: counts.into(),
        processes: processes.into_iter().map(Into::into).collect(),
        retain_done_days: u32::try_from(cfg.retain_done_for_days).unwrap_or(7),
        retain_dead_days: u32::try_from(cfg.retain_dead_for_days).unwrap_or(30),
        backoff_enabled: cfg.backoff_enabled,
        backoff_base_seconds: u32::try_from(cfg.backoff_base_seconds).unwrap_or(60),
        backoff_max_seconds: u32::try_from(cfg.backoff_max_seconds).unwrap_or(1800),
        throttled_until: cfg.throttled_until,
        oldest_pending_age_seconds,
    }
}

/// Request body for `POST /queue/:name/backoff`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetBackoffRequest {
    pub enabled: bool,
    pub base_seconds: i32,
    pub max_seconds: i32,
}

/// `GET /storage/info` response — surfaces the backend's
/// `describe()` output. Useful for ops checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageInfoDto {
    pub backend: String,
    pub fields: Vec<(String, String)>,
}

impl From<forge_jobs::StorageInfo> for StorageInfoDto {
    fn from(info: forge_jobs::StorageInfo) -> Self {
        Self {
            backend: info.backend,
            fields: info.fields,
        }
    }
}
