//! Integration test — `handlers::cron_set_dedupe` against an in-memory
//! `SQLite` store. Proves the boolean toggle maps to the schedule's
//! dedupe key (`true` → name, `false` → cleared), the bit the storage
//! and runtime tests don't cover.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test crashes loudly on setup/assert failures"
)]

use std::sync::Arc;

use forge_jobs::SqliteStorage;
use forge_jobs::Storage;
use forge_jobs::storage::NewCronSchedule;
use serde_json::json;

#[tokio::test]
async fn set_dedupe_toggles_key_to_schedule_name() {
    let inner = Arc::new(SqliteStorage::open_in_memory().await.unwrap());
    let storage = Storage::from_one(inner);
    storage
        .cron
        .ensure_schedule(NewCronSchedule {
            name: "tickets_sync".into(),
            kind: "tickets_sync".into(),
            payload: json!({}),
            queue_name: None,
            cron_expr: "*/5 * * * * *".into(),
            enabled: true,
            max_attempts: Some(3),
            dedupe_key: None,
        })
        .await
        .unwrap();

    // true → key is set to the schedule name (skip-if-in-flight on).
    forge_jobs_api::handlers::cron_set_dedupe(&storage, "tickets_sync", true)
        .await
        .unwrap();
    let row = storage
        .cron
        .get_schedule("tickets_sync")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.dedupe_key.as_deref(), Some("tickets_sync"));

    // false → key cleared (back to fire-every-tick).
    forge_jobs_api::handlers::cron_set_dedupe(&storage, "tickets_sync", false)
        .await
        .unwrap();
    let row = storage
        .cron
        .get_schedule("tickets_sync")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.dedupe_key, None);
}
