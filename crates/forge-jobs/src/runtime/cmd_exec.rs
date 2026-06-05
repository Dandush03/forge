//! `cmd_exec` — a generic command-execution `JobHandler`.
//!
//! Lets the queue accept "run this thing" jobs whose payload describes
//! the work — a shell command, a script path, eventually a Ruby class
//! invocation. The Rust runtime spawns the child process and gives it
//! queue semantics (retry, dedupe, scheduling, observability,
//! cancellation); the child does the actual work.
//!
//! This is the foundation under any future Ruby / cross-language
//! interop. Phase 5 will layer typed Ruby-job wrappers on top, but the
//! plain `argv` shape is enough to drive a Rake task or a long-lived
//! worker subprocess today.
//!
//! Routing is by kind prefix as usual: `cmd_exec` lands on the
//! `default` queue. Override via `EnqueueRequest::on_queue` if you
//! want a dedicated lane.
//!
//! ## Payload shape
//!
//! ```ignore
//! {
//!   "argv":  ["bundle", "exec", "rake", "queue:run[42]"],   // required, non-empty
//!   "env":   { "RAILS_ENV": "production" },                  // optional
//!   "cwd":   "/srv/app",                                     // optional
//!   "stdin": "...",                                          // optional
//!   "timeout_secs": 300                                      // optional, default 600
//! }
//! ```
//!
//! ## Exit-code mapping
//!
//! - `0` → [`JobOutcome::Done`]
//! - non-zero → [`JobOutcome::Failed`] with a short summary
//!   `"exit N: <last 256 bytes of stderr || stdout>"`
//! - process killed by signal / never started → `JobOutcome::Failed`
//! - timeout hit → `JobOutcome::Failed("timeout after Ns")`
//!
//! ## Sandboxing
//!
//! v1 keeps it minimal: an optional `cwd_root` constructor argument
//! refuses any payload `cwd` that doesn't canonicalize *inside* that
//! root. Default (no root) accepts any `cwd`. Executable allowlist is
//! a follow-up — today's job-enqueue surface is operator-controlled.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use super::handler::{JobCtx, JobHandler, JobOutcome};

pub const CMD_EXEC_KIND: &str = "cmd_exec";

/// Default per-job wall-clock cap (10 min). Override via the payload's
/// `timeout_secs` field.
const DEFAULT_TIMEOUT_SECS: u64 = 600;

/// How many bytes of stdout/stderr to surface in the [`JobOutcome::Failed`]
/// message. Anything longer is truncated with a "…" sentinel. Full
/// streams still hit `tracing` for the operator to inspect.
const OUTPUT_TAIL_BYTES: usize = 256;

#[derive(Debug, Deserialize, Serialize)]
pub struct CmdExecPayload {
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub stdin: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// Generic command-execution `JobHandler`. Construct once at boot and
/// register on the `HandlerRegistry`.
pub struct CmdExecHandler {
    cwd_root: Option<PathBuf>,
}

impl std::fmt::Debug for CmdExecHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CmdExecHandler")
            .field("cwd_root", &self.cwd_root)
            .finish()
    }
}

impl CmdExecHandler {
    /// Build a handler with no `cwd` restriction. Any payload `cwd` is
    /// honored. Use in trusted contexts (the Tauri app where the
    /// operator controls every enqueue).
    #[must_use]
    pub const fn new_unrestricted() -> Self {
        Self { cwd_root: None }
    }

    /// Build a handler that refuses payloads whose `cwd` doesn't
    /// canonicalize inside `root`. Use in cross-tenant deploys where
    /// untrusted job sources might try to escape the work directory.
    pub fn with_cwd_root(root: impl Into<PathBuf>) -> Self {
        Self {
            cwd_root: Some(root.into()),
        }
    }

    fn validate_cwd(&self, cwd: &str) -> Result<PathBuf, String> {
        let target = Path::new(cwd);
        let resolved = target
            .canonicalize()
            .map_err(|e| format!("cwd canonicalize {cwd}: {e}"))?;
        if let Some(root) = &self.cwd_root {
            let root_resolved = root
                .canonicalize()
                .map_err(|e| format!("cwd_root canonicalize {}: {e}", root.display()))?;
            if !resolved.starts_with(&root_resolved) {
                return Err(format!(
                    "cwd {} escapes configured cwd_root {}",
                    resolved.display(),
                    root_resolved.display()
                ));
            }
        }
        Ok(resolved)
    }
}

#[async_trait]
impl JobHandler for CmdExecHandler {
    fn kind(&self) -> &'static str {
        CMD_EXEC_KIND
    }

    #[tracing::instrument(skip(self, ctx, payload), fields(kind = CMD_EXEC_KIND))]
    async fn run(&self, ctx: JobCtx<'_>, payload: serde_json::Value) -> JobOutcome {
        execute(payload, ctx.cancel.clone(), ctx.job_id.as_str(), self).await
    }
}

/// Pure execution body. Public to the crate so tests + the (future)
/// `jobs-api` HTTP handler can drive the same logic without
/// constructing a full `JobCtx`. Takes the bits it actually uses —
/// the cancel token, a job-id label for tracing, and the handler
/// config — instead of the whole context.
async fn execute(
    payload: serde_json::Value,
    cancel: tokio_util::sync::CancellationToken,
    job_id_label: &str,
    handler: &CmdExecHandler,
) -> JobOutcome {
    let parsed: CmdExecPayload = match serde_json::from_value(payload) {
        Ok(p) => p,
        Err(e) => return JobOutcome::Failed(format!("payload: {e}")),
    };
    let Some((program, args)) = parsed.argv.split_first() else {
        return JobOutcome::Failed("payload.argv is empty".into());
    };

    let mut cmd = Command::new(program);
    cmd.args(args)
        .envs(&parsed.env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(if parsed.stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });

    if let Some(cwd_raw) = parsed.cwd.as_deref() {
        match handler.validate_cwd(cwd_raw) {
            Ok(resolved) => {
                cmd.current_dir(resolved);
            }
            Err(e) => return JobOutcome::Failed(format!("cwd rejected: {e}")),
        }
    }

    let timeout = Duration::from_secs(parsed.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS).max(1));

    tracing::info!(
        program,
        arg_count = args.len(),
        env_count = parsed.env.len(),
        cwd = ?parsed.cwd,
        timeout_secs = timeout.as_secs(),
        job_id = %job_id_label,
        "cmd_exec: spawning"
    );

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return JobOutcome::Failed(format!("spawn {program:?}: {e}")),
    };

    if let Some(stdin_data) = parsed.stdin.as_deref()
        && let Some(mut stdin) = child.stdin.take()
    {
        if let Err(e) = stdin.write_all(stdin_data.as_bytes()).await {
            tracing::warn!(?e, "cmd_exec: failed writing stdin (continuing)");
        }
        drop(stdin);
    }

    let wait = child.wait_with_output();
    let output = tokio::select! {
        () = cancel.cancelled() => {
            tracing::info!(job_id = %job_id_label, "cmd_exec: cancelled; child orphaned");
            return JobOutcome::Failed("cancelled by supervisor".into());
        }
        res = tokio::time::timeout(timeout, wait) => match res {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => return JobOutcome::Failed(format!("wait child: {e}")),
            Err(_) => return JobOutcome::Failed(format!("timeout after {}s", timeout.as_secs())),
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stdout.is_empty() {
        tracing::info!(stream = "stdout", body = %stdout, "cmd_exec: child output");
    }
    if !stderr.is_empty() {
        tracing::info!(stream = "stderr", body = %stderr, "cmd_exec: child output");
    }

    match output.status.code() {
        Some(0) => JobOutcome::Done,
        Some(code) => {
            let tail = if stderr.is_empty() { stdout } else { stderr };
            let summary = tail_chars(&tail, OUTPUT_TAIL_BYTES);
            JobOutcome::Failed(format!("exit {code}: {summary}"))
        }
        None => JobOutcome::Failed("killed by signal".into()),
    }
}

/// Truncate to the last `max_bytes` characters (UTF-8-safe). Inserts
/// a single `…` sentinel when truncation happens so the operator
/// knows the message was clipped.
fn tail_chars(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_owned();
    }
    let mut start = s.len() - max_bytes;
    while start > 0 && !s.is_char_boundary(start) {
        start -= 1;
    }
    format!("…{}", &s[start..])
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests crash loudly on setup or assertion failure; that's the point"
)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    fn payload(argv: &[&str]) -> serde_json::Value {
        serde_json::json!({ "argv": argv })
    }

    fn unrestricted() -> CmdExecHandler {
        CmdExecHandler::new_unrestricted()
    }

    async fn run(handler: &CmdExecHandler, payload: serde_json::Value) -> JobOutcome {
        execute(payload, CancellationToken::new(), "test-job", handler).await
    }

    #[tokio::test]
    async fn empty_argv_rejected() {
        let out = run(&unrestricted(), serde_json::json!({ "argv": [] })).await;
        match out {
            JobOutcome::Failed(msg) => assert!(msg.contains("argv"), "got: {msg}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn successful_command_returns_done() {
        let out = run(&unrestricted(), payload(&["true"])).await;
        assert!(matches!(out, JobOutcome::Done), "got: {out:?}");
    }

    #[tokio::test]
    async fn non_zero_exit_returns_failed_with_code() {
        let out = run(&unrestricted(), payload(&["false"])).await;
        match out {
            JobOutcome::Failed(msg) => assert!(msg.contains("exit 1"), "got: {msg}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn nonexistent_program_returns_failed() {
        let out = run(
            &unrestricted(),
            payload(&["this-program-does-not-exist-xyz"]),
        )
        .await;
        match out {
            JobOutcome::Failed(msg) => assert!(msg.contains("spawn"), "got: {msg}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn timeout_exceeded_returns_failed() {
        let out = run(
            &unrestricted(),
            serde_json::json!({ "argv": ["sleep", "5"], "timeout_secs": 1 }),
        )
        .await;
        match out {
            JobOutcome::Failed(msg) => assert!(msg.contains("timeout"), "got: {msg}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cwd_outside_root_rejected() {
        let handler = CmdExecHandler::with_cwd_root("/tmp");
        let out = run(
            &handler,
            serde_json::json!({ "argv": ["true"], "cwd": "/etc" }),
        )
        .await;
        match out {
            JobOutcome::Failed(msg) => assert!(msg.contains("cwd"), "got: {msg}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_orphans_child_and_returns_failed() {
        let cancel = CancellationToken::new();
        let cancel_inner = cancel.clone();
        let handler = unrestricted();
        let join = tokio::spawn(async move {
            execute(
                serde_json::json!({ "argv": ["sleep", "5"] }),
                cancel_inner,
                "cancel-test",
                &handler,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        let out = join.await.unwrap();
        match out {
            JobOutcome::Failed(msg) => assert!(msg.contains("cancelled"), "got: {msg}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }
}
