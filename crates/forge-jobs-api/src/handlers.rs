//! Pure async fns over `&Storage`. Single source of truth for what
//! each queue operation does at the storage layer; the Tauri plugin
//! commands and the Axum HTTP routes both call into these.
//!
//! This first commit ports the queue-overview path. Remaining
//! handlers (job listing / retry / inspect / cron / cleanup) land in
//! subsequent commits.

use forge_jobs::Storage;

use crate::Error;
use crate::dto::{QueueOverviewDto, StorageInfoDto, overview_dto};

/// `GET /queue/overview` — one DTO per registered queue with
/// status counts + live workers + retention settings.
///
/// Three storage round-trips per queue (config + counts + live
/// processes). Cheap; the UI polls this every ~5s.
///
/// # Errors
///
/// Surfaces any storage-layer error untransformed — see [`Error`]
/// for the kinds.
pub async fn queue_overview(storage: &Storage) -> Result<Vec<QueueOverviewDto>, Error> {
    let queues = storage.config.list_queues().await?;
    let now = chrono::Utc::now();
    let mut out = Vec::with_capacity(queues.len());
    for cfg in queues {
        let counts = storage.jobs.count_by_status(&cfg.name).await?;
        let processes = storage.procs.list(Some(&cfg.name)).await?;
        let lag = storage
            .jobs
            .oldest_ready_at(&cfg.name)
            .await?
            .map_or(0, |t| u64::try_from((now - t).num_seconds()).unwrap_or(0));
        out.push(overview_dto(cfg, counts, processes, lag));
    }
    Ok(out)
}

/// `GET /storage/info` — adapter identifier + key/value facts.
/// Same data the boot banner logs.
///
/// # Errors
///
/// Surfaces any storage-layer error untransformed — see [`Error`]
/// for the kinds.
pub async fn storage_info(storage: &Storage) -> Result<StorageInfoDto, Error> {
    let info = storage.jobs.describe().await?;
    Ok(info.into())
}

/// `POST /queue/:name/backoff` — set the per-queue throttle backoff
/// curve (enable flag + base/max seconds).
///
/// `base_seconds` and `max_seconds` are clamped to `[1, 86400]` by
/// the storage layer.
///
/// # Errors
///
/// Surfaces any storage-layer error untransformed — see [`Error`]
/// for the kinds.
pub async fn queue_set_backoff(
    storage: &Storage,
    name: &str,
    enabled: bool,
    base_seconds: i32,
    max_seconds: i32,
) -> Result<(), Error> {
    storage
        .config
        .set_backoff(name, enabled, base_seconds, max_seconds)
        .await?;
    Ok(())
}
