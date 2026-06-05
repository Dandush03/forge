//! Integration test — `metrics::render` against an in-memory `SQLite`
//! store. Proves the collect path (config + counts + lag) and the
//! Prometheus text shape end-to-end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test crashes loudly on setup/assert failures"
)]

use std::sync::Arc;

use forge_jobs::SqliteStorage;
use forge_jobs::{EnqueueRequest, Storage};
use serde_json::json;

#[tokio::test]
async fn metrics_render_reports_per_queue_depth() {
    let inner = Arc::new(SqliteStorage::open_in_memory().await.unwrap());
    let storage = Storage::from_one(inner);
    storage.config.ensure_queue("gh", 5).await.unwrap();
    for _ in 0..3 {
        storage
            .jobs
            .enqueue(EnqueueRequest::new("k", json!({})).on_queue("gh"))
            .await
            .unwrap();
    }

    let body = forge_jobs_api::metrics_render(&storage).await.unwrap();
    assert!(body.contains("# TYPE queue_pending_jobs gauge"));
    assert!(body.contains("queue_pending_jobs{queue=\"gh\"} 3"));
    assert!(body.contains("queue_max_workers{queue=\"gh\"} 5"));
    // The lag gauge is always present (0 when nothing is overdue).
    assert!(body.contains("queue_oldest_pending_age_seconds{queue=\"gh\"}"));
}
