//! `HttpQueueIpc` — a [`QueueIpc`] that talks to a `forge-jobs-api`
//! server over `fetch` (browser or webview).
//!
//! Enable with the `http` feature. The host constructs it with the API
//! base URL (and optionally a bearer-token provider) and hands it to the
//! panel via Leptos context:
//!
//! ```ignore
//! use std::sync::Arc;
//! use forge_jobs_ui::{HttpQueueIpc, IpcCtx};
//! let ipc = HttpQueueIpc::with_bearer(
//!     "/api/operator/v1/jobs",
//!     || local_storage_token(), // Option<String>, read fresh per call
//! );
//! provide_context::<IpcCtx>(Arc::new(ipc));
//! ```
//!
//! The browser and a Tauri webview both use this same impl — only the
//! base URL (and how the token is sourced) differ.

#![allow(
    clippy::future_not_send,
    reason = "CSR/WASM is single-threaded; these fetch futures never cross threads — the same reason QueueIpc is declared #[async_trait(?Send)]"
)]

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gloo_net::http::{Request, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::ipc::{
    CleanupReport, CronSchedule, DbHealthHostSeries, IpcError, JobInspect, JobRow, JobsEnqueueReq,
    JobsFilter, JobsPage, MetricSeriesBucket, QueueIpc, QueueOverview, QueueProcess,
    ResourceHostSeries, TimelineBucket, WorkersOverview,
};

/// Reads the current bearer token (e.g. from `localStorage`) fresh on
/// every request, so a refreshed session is picked up without
/// reconstructing the client. `Send + Sync` because Leptos stores the
/// `QueueIpc` in a globally-shareable context even in CSR.
type TokenProvider = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// HTTP [`QueueIpc`] over a `forge-jobs-api` server.
pub struct HttpQueueIpc {
    base_url: String,
    token: Option<TokenProvider>,
}

impl std::fmt::Debug for HttpQueueIpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpQueueIpc")
            .field("base_url", &self.base_url)
            .field("auth", &self.token.is_some())
            .finish()
    }
}

impl HttpQueueIpc {
    /// Build an unauthenticated client. `base_url` is the mount prefix
    /// of the `forge-jobs-api` router (no trailing slash needed), e.g.
    /// `"/api/operator/v1/jobs"` or `"http://127.0.0.1:8787"`.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: trim_trailing_slash(base_url.into()),
            token: None,
        }
    }

    /// Build a client that sends `Authorization: Bearer <token>` on
    /// every request, reading the token fresh via `provider` (so a
    /// rotated session token is honoured without rebuilding the client).
    #[must_use]
    pub fn with_bearer(
        base_url: impl Into<String>,
        provider: impl Fn() -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            base_url: trim_trailing_slash(base_url.into()),
            token: Some(Arc::new(provider)),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn bearer(&self) -> Option<String> {
        self.token.as_ref().and_then(|f| f())
    }

    /// `GET path?query`, returning the deserialized body.
    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, IpcError> {
        let mut req =
            Request::get(&self.url(path)).query(query.iter().map(|(k, v)| (*k, v.as_str())));
        if let Some(tok) = self.bearer() {
            req = req.header("Authorization", &format!("Bearer {tok}"));
        }
        let resp = req.send().await.map_err(net_err)?;
        decode(resp).await
    }

    /// `POST path` with a JSON body, returning the deserialized body.
    async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, IpcError> {
        decode(self.post_raw(path, body).await?).await
    }

    /// `POST path` with a JSON body, ignoring the (empty) response body.
    /// For the mutating endpoints that return `204`/`200` with no JSON.
    async fn post_unit<B: Serialize>(&self, path: &str, body: &B) -> Result<(), IpcError> {
        self.post_raw(path, body).await.map(|_| ())
    }

    async fn post_raw<B: Serialize>(&self, path: &str, body: &B) -> Result<Response, IpcError> {
        let mut req = Request::post(&self.url(path));
        if let Some(tok) = self.bearer() {
            req = req.header("Authorization", &format!("Bearer {tok}"));
        }
        let resp = req
            .json(body)
            .map_err(net_err)?
            .send()
            .await
            .map_err(net_err)?;
        ok_or_err(resp).await
    }
}

fn trim_trailing_slash(mut s: String) -> String {
    while s.ends_with('/') {
        s.pop();
    }
    s
}

fn net_err(e: gloo_net::Error) -> IpcError {
    IpcError::internal(e.to_string())
}

/// Map a non-2xx response into an [`IpcError`]. The server emits the
/// same tagged shape (`{"kind": ...}`), so a clean parse preserves the
/// variant; anything else falls back to `Internal` with the status.
async fn ok_or_err(resp: Response) -> Result<Response, IpcError> {
    if resp.ok() {
        return Ok(resp);
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    Err(serde_json::from_str::<IpcError>(&body)
        .unwrap_or_else(|_| IpcError::internal(format!("HTTP {status}: {body}"))))
}

/// Check status, then deserialize the JSON body.
async fn decode<T: DeserializeOwned>(resp: Response) -> Result<T, IpcError> {
    let resp = ok_or_err(resp).await?;
    resp.json::<T>().await.map_err(net_err)
}

/// `from`/`to`/`bucket_secs` triple shared by the timeline + series GETs.
fn window_query(
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket_secs: u32,
) -> Vec<(&'static str, String)> {
    vec![
        ("from", from.to_rfc3339()),
        ("to", to.to_rfc3339()),
        ("bucket_secs", bucket_secs.to_string()),
    ]
}

#[async_trait(?Send)]
impl QueueIpc for HttpQueueIpc {
    // ── reads ──
    async fn queue_overview(&self) -> Result<Vec<QueueOverview>, IpcError> {
        self.get("/queue/overview", &[]).await
    }

    async fn queue_processes(
        &self,
        queue_name: Option<&str>,
    ) -> Result<Vec<QueueProcess>, IpcError> {
        let q: Vec<(&str, String)> = queue_name
            .map(|n| vec![("queue_name", n.to_owned())])
            .unwrap_or_default();
        self.get("/queue/processes", &q).await
    }

    async fn queue_workers(&self) -> Result<WorkersOverview, IpcError> {
        self.get("/queue/workers", &[]).await
    }

    async fn queue_timeline_range(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket_secs: u32,
    ) -> Result<Vec<TimelineBucket>, IpcError> {
        self.get("/queue/timeline", &window_query(from, to, bucket_secs))
            .await
    }

    async fn queue_metric_series(
        &self,
        queue: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket_secs: u32,
    ) -> Result<Vec<MetricSeriesBucket>, IpcError> {
        let mut q = window_query(from, to, bucket_secs);
        q.push(("queue", queue.to_owned()));
        self.get("/queue/metric-series", &q).await
    }

    async fn queue_resource_series(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket_secs: u32,
    ) -> Result<Vec<ResourceHostSeries>, IpcError> {
        self.get(
            "/queue/resource-series",
            &window_query(from, to, bucket_secs),
        )
        .await
    }

    async fn queue_db_series(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket_secs: u32,
    ) -> Result<Vec<DbHealthHostSeries>, IpcError> {
        self.get("/queue/db-series", &window_query(from, to, bucket_secs))
            .await
    }

    async fn jobs_list(
        &self,
        filter: JobsFilter,
        limit: u32,
        offset: u32,
    ) -> Result<JobsPage, IpcError> {
        self.post(
            "/jobs/list",
            &json!({ "filter": filter, "limit": limit, "offset": offset }),
        )
        .await
    }

    async fn jobs_failed(&self, limit: u32) -> Result<Vec<JobRow>, IpcError> {
        self.get("/jobs/failed", &[("limit", limit.to_string())])
            .await
    }

    async fn jobs_kinds(&self, queue_name: Option<&str>) -> Result<Vec<String>, IpcError> {
        let q: Vec<(&str, String)> = queue_name
            .map(|n| vec![("queue_name", n.to_owned())])
            .unwrap_or_default();
        self.get("/jobs/kinds", &q).await
    }

    async fn job_inspect(&self, id: &str) -> Result<JobInspect, IpcError> {
        self.get(&format!("/jobs/inspect/{id}"), &[]).await
    }

    // ── mutations ──
    async fn queue_set_max_workers(&self, queue_name: &str, n: i32) -> Result<(), IpcError> {
        self.post_unit(
            &format!("/queue/{queue_name}/max-workers"),
            &json!({ "n": n }),
        )
        .await
    }

    async fn queue_set_paused(&self, queue_name: &str, paused: bool) -> Result<(), IpcError> {
        self.post_unit(
            &format!("/queue/{queue_name}/paused"),
            &json!({ "paused": paused }),
        )
        .await
    }

    async fn queue_set_retention(
        &self,
        queue_name: &str,
        done_days: i32,
        dead_days: i32,
    ) -> Result<(), IpcError> {
        self.post_unit(
            &format!("/queue/{queue_name}/retention"),
            &json!({ "done_days": done_days, "dead_days": dead_days }),
        )
        .await
    }

    async fn queue_set_backoff(
        &self,
        queue_name: &str,
        enabled: bool,
        base_seconds: i32,
        max_seconds: i32,
    ) -> Result<(), IpcError> {
        self.post_unit(
            &format!("/queue/{queue_name}/backoff"),
            &json!({
                "enabled": enabled,
                "base_seconds": base_seconds,
                "max_seconds": max_seconds,
            }),
        )
        .await
    }

    async fn queue_cleanup_now(&self) -> Result<CleanupReport, IpcError> {
        self.post("/queue/cleanup", &json!({})).await
    }

    async fn queue_enqueue_demo(&self, payload: serde_json::Value) -> Result<String, IpcError> {
        self.post("/jobs/enqueue-demo", &json!({ "payload": payload }))
            .await
    }

    async fn jobs_enqueue(&self, req: JobsEnqueueReq) -> Result<String, IpcError> {
        self.post("/jobs/enqueue", &req).await
    }

    async fn jobs_scheduled(&self, queue_name: Option<&str>) -> Result<Vec<JobRow>, IpcError> {
        let q: Vec<(&str, String)> = queue_name
            .map(|n| vec![("queue_name", n.to_owned())])
            .unwrap_or_default();
        self.get("/jobs/scheduled", &q).await
    }

    async fn jobs_run_now(&self, id: &str) -> Result<bool, IpcError> {
        self.post(&format!("/jobs/run-now/{id}"), &json!({})).await
    }

    async fn jobs_retry(&self, ids: &[String]) -> Result<u64, IpcError> {
        self.post("/jobs/retry", &json!({ "ids": ids })).await
    }

    async fn jobs_retry_all_failed(&self) -> Result<u64, IpcError> {
        self.post("/jobs/retry-all-failed", &json!({})).await
    }

    async fn jobs_retry_all_by_status(&self, status: &str) -> Result<u64, IpcError> {
        self.post("/jobs/retry-all-by-status", &json!({ "status": status }))
            .await
    }

    async fn jobs_delete(&self, ids: &[String]) -> Result<u64, IpcError> {
        self.post("/jobs/delete", &json!({ "ids": ids })).await
    }

    async fn jobs_requeue(&self, ids: &[String]) -> Result<u64, IpcError> {
        self.post("/jobs/requeue", &json!({ "ids": ids })).await
    }

    async fn jobs_delete_done_older_than(
        &self,
        days: u32,
        queue_name: Option<&str>,
    ) -> Result<u64, IpcError> {
        self.post(
            "/jobs/delete-done-older-than",
            &json!({ "days": days, "queue_name": queue_name }),
        )
        .await
    }

    async fn jobs_delete_by_status(
        &self,
        status: &str,
        queue_name: Option<&str>,
    ) -> Result<u64, IpcError> {
        self.post(
            "/jobs/delete-by-status",
            &json!({ "status": status, "queue_name": queue_name }),
        )
        .await
    }

    // ── cron ──
    async fn cron_list(&self) -> Result<Vec<CronSchedule>, IpcError> {
        self.get("/cron", &[]).await
    }

    async fn cron_set_enabled(&self, name: &str, enabled: bool) -> Result<(), IpcError> {
        self.post_unit(
            &format!("/cron/{name}/enabled"),
            &json!({ "enabled": enabled }),
        )
        .await
    }

    async fn cron_set_expr(&self, name: &str, expr: &str) -> Result<(), IpcError> {
        self.post_unit(&format!("/cron/{name}/expr"), &json!({ "expr": expr }))
            .await
    }

    async fn cron_set_dedupe(&self, name: &str, dedupe: bool) -> Result<(), IpcError> {
        self.post_unit(
            &format!("/cron/{name}/dedupe"),
            &json!({ "dedupe": dedupe }),
        )
        .await
    }

    async fn cron_trigger_now(&self, name: &str) -> Result<String, IpcError> {
        self.post(&format!("/cron/{name}/trigger"), &json!({}))
            .await
    }
}
