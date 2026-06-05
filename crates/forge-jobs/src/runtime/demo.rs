//! `NoopEcho` — a demo `JobHandler` for end-to-end verification.

use std::time::Duration;

use async_trait::async_trait;

use super::handler::{JobCtx, JobHandler, JobOutcome};

pub const NOOP_ECHO_KIND: &str = "noop_echo";

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopEcho;

#[async_trait]
impl JobHandler for NoopEcho {
    fn kind(&self) -> &'static str {
        NOOP_ECHO_KIND
    }

    async fn run(&self, _ctx: JobCtx<'_>, payload: serde_json::Value) -> JobOutcome {
        let sleep_ms = payload
            .get("sleep_ms")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(100);
        let should_fail = payload
            .get("fail")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let throttle_ms = payload
            .get("throttle_ms")
            .and_then(serde_json::Value::as_u64);

        tracing::info!(sleep_ms, should_fail, throttle_ms, "noop_echo: running");
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

        if should_fail {
            return JobOutcome::Failed("demo-fail".into());
        }
        if let Some(ms) = throttle_ms {
            return JobOutcome::Throttled {
                retry_after: Duration::from_millis(ms),
            };
        }
        JobOutcome::Done
    }
}
