//! `WorkerPoolHandler` — a `JobHandler` backed by a pool of long-lived
//! subprocesses.
//!
//! Where [`super::cmd_exec`] spawns one process *per job* (fine for
//! slow jobs, fatal at 10M/day because of per-spawn + interpreter-boot
//! cost), this keeps N warm child processes and streams jobs to them
//! over stdin/stdout. The intended shape is a Ruby/Rails worker booted
//! once (`AR` + eager-loaded models, no Puma) that reads job envelopes
//! and runs them in-process — Sidekiq's architecture with Rust as the
//! queueing layer. Nothing here is Ruby-specific; the child is any
//! program that speaks the line protocol below.
//!
//! ## Protocol (line-delimited JSON, one request → one response)
//!
//! The pool writes one request line to a child's stdin:
//! ```text
//! {"id":"<job-id>","payload":{...}}\n
//! ```
//! and reads exactly one response line from its stdout:
//! ```text
//! {"id":"<job-id>","status":"ok"}
//! {"id":"<job-id>","status":"error","message":"..."}
//! {"id":"<job-id>","status":"throttled","retry_after_secs":30}
//! ```
//! → [`JobOutcome::Done`] / [`JobOutcome::Failed`] /
//! [`JobOutcome::Throttled`]. The child must emit exactly one line per
//! request and should echo the request `id`: if the echoed id doesn't
//! match (the child got out of sync by emitting an extra line, so we'd
//! otherwise read a stale response as this job's result) the child is
//! killed and respawned. A child that omits the id is trusted (no
//! validation). The child's stderr is inherited so its logs flow to the
//! host.
//!
//! ## Sizing
//!
//! One child per Rust worker slot on the queue (set `size` to the
//! queue's worker count): a Rust worker calling `run` checks out a
//! child for the duration of the job, so fewer children than workers
//! means workers block waiting for a free child.
//!
//! ## Failure handling
//!
//! A child that dies, times out, or is cancelled mid-request is killed
//! and dropped (its slot respawns lazily on next use) — never reused,
//! since a leftover late response would desync the next job. The job
//! that hit the failure returns [`JobOutcome::Failed`].

use std::collections::HashMap;
use std::io;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::handler::{JobCtx, JobHandler, JobOutcome};

/// Default per-job wall-clock cap (10 min). The child is killed if a
/// response doesn't arrive in time.
const DEFAULT_TIMEOUT_SECS: u64 = 600;

/// How to launch + size the subprocess pool.
#[derive(Debug, Clone)]
pub struct WorkerPoolConfig {
    /// Job `kind` this pool handles. Matched against `sync_queue.kind`.
    pub kind: &'static str,
    /// Program + args to launch each child (e.g.
    /// `["bundle", "exec", "ruby", "worker.rb"]`). Must be non-empty.
    pub argv: Vec<String>,
    /// Extra environment for each child.
    pub env: HashMap<String, String>,
    /// Working directory for each child.
    pub cwd: Option<String>,
    /// Number of warm child processes. Set to the queue's worker count.
    pub size: usize,
    /// Per-job response timeout. `None` → 600 seconds.
    pub timeout_secs: Option<u64>,
}

/// One live child plus its framed I/O.
struct ChildIo {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
}

impl ChildIo {
    /// Send one request line and read exactly one response line. An EOF
    /// (child closed stdout) surfaces as an error so the caller drops
    /// the child.
    async fn exchange(&mut self, request: &str) -> io::Result<String> {
        self.stdin.write_all(request.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        self.lines.next_line().await?.map_or_else(
            || {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "worker closed stdout",
                ))
            },
            Ok,
        )
    }
}

#[derive(Serialize)]
struct WorkerRequest<'a> {
    id: &'a str,
    payload: &'a serde_json::Value,
}

#[derive(Deserialize)]
struct WorkerResponse {
    /// Echo of the request `id`. The child should always echo it; when
    /// present and mismatched it means a desync (the child emitted an
    /// extra line, so we're reading a stale response) and the child is
    /// discarded. Optional for back-compat with children that don't echo.
    #[serde(default)]
    id: Option<String>,
    #[serde(flatten)]
    body: WorkerResponseBody,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum WorkerResponseBody {
    Ok,
    Error {
        #[serde(default)]
        message: Option<String>,
    },
    Throttled {
        #[serde(default)]
        retry_after_secs: Option<u64>,
    },
}

/// A `JobHandler` that dispatches each job to a pooled long-lived
/// subprocess. Construct with [`WorkerPoolHandler::spawn`] at boot and
/// register on the `HandlerRegistry`.
pub struct WorkerPoolHandler {
    config: WorkerPoolConfig,
    /// One slot per child. `None` means "needs (re)spawn"; the per-slot
    /// `Mutex` guarantees one in-flight request per child.
    slots: Vec<Mutex<Option<ChildIo>>>,
    next: AtomicUsize,
}

impl std::fmt::Debug for WorkerPoolHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPoolHandler")
            .field("kind", &self.config.kind)
            .field("size", &self.slots.len())
            .finish_non_exhaustive()
    }
}

impl WorkerPoolHandler {
    /// Build the pool and eagerly spawn `config.size` children (warm on
    /// first job). Fails if `argv` is empty or the first child can't
    /// spawn.
    ///
    /// # Errors
    ///
    /// Returns a message if `argv` is empty or a child fails to spawn.
    #[allow(
        clippy::unused_async,
        reason = "process-spawning constructor — kept async for a future readiness handshake and so callers needn't change if spawn gains awaits"
    )]
    pub async fn spawn(config: WorkerPoolConfig) -> Result<Self, String> {
        if config.argv.is_empty() {
            return Err("worker pool argv is empty".to_owned());
        }
        let size = config.size.max(1);
        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            let io = spawn_child(&config)?;
            slots.push(Mutex::new(Some(io)));
        }
        Ok(Self {
            config,
            slots,
            next: AtomicUsize::new(0),
        })
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(
            self.config
                .timeout_secs
                .unwrap_or(DEFAULT_TIMEOUT_SECS)
                .max(1),
        )
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the per-slot guard is intentionally held across the request/response exchange — one in-flight request per child"
    )]
    async fn dispatch(
        &self,
        job_id: &str,
        payload: &serde_json::Value,
        cancel: &CancellationToken,
    ) -> JobOutcome {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let mut guard = self.slots[idx].lock().await;

        // Take the child out (or spawn a fresh one if the slot is
        // empty). We put it back only if it stays healthy.
        let mut io = match guard.take() {
            Some(io) => io,
            None => match spawn_child(&self.config) {
                Ok(io) => io,
                Err(e) => return JobOutcome::Failed(format!("respawn worker: {e}")),
            },
        };

        let request = match serde_json::to_string(&WorkerRequest {
            id: job_id,
            payload,
        }) {
            Ok(s) => s,
            Err(e) => {
                // Encoding failed, not the child's fault — keep it.
                *guard = Some(io);
                return JobOutcome::Failed(format!("encode request: {e}"));
            }
        };

        let exchanged = tokio::select! {
            () = cancel.cancelled() => {
                // Child may be mid-work; killing avoids a late response
                // desyncing the next job. Slot left empty → respawn.
                let _ = io.child.start_kill();
                return JobOutcome::Failed("cancelled by supervisor".to_owned());
            }
            res = tokio::time::timeout(self.timeout(), io.exchange(&request)) => res,
        };

        match exchanged {
            Ok(Ok(line)) => match parse_response(&line, job_id) {
                // Healthy round-trip — return the child to its slot.
                Ok(outcome) => {
                    *guard = Some(io);
                    outcome
                }
                // Desync / unparseable line — the child is out of sync
                // with the request stream; discard it rather than risk
                // mapping the next job to this stale response.
                Err(reason) => {
                    let _ = io.child.start_kill();
                    JobOutcome::Failed(reason)
                }
            },
            Ok(Err(e)) => {
                let _ = io.child.start_kill();
                JobOutcome::Failed(format!("worker io: {e}"))
            }
            Err(_) => {
                let _ = io.child.start_kill();
                JobOutcome::Failed(format!(
                    "worker timeout after {}s",
                    self.timeout().as_secs()
                ))
            }
        }
    }
}

#[async_trait]
impl JobHandler for WorkerPoolHandler {
    fn kind(&self) -> &'static str {
        self.config.kind
    }

    async fn run(&self, ctx: JobCtx<'_>, payload: serde_json::Value) -> JobOutcome {
        self.dispatch(ctx.job_id.as_str(), &payload, &ctx.cancel)
            .await
    }
}

/// Spawn one child with piped stdin/stdout (stderr inherited so the
/// child's own logs reach the host). `kill_on_drop` so a dropped
/// `ChildIo` doesn't leak a process.
fn spawn_child(config: &WorkerPoolConfig) -> Result<ChildIo, String> {
    let (program, args) = config
        .argv
        .split_first()
        .ok_or_else(|| "worker pool argv is empty".to_owned())?;
    let mut cmd = Command::new(program);
    cmd.args(args)
        .envs(&config.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    if let Some(cwd) = &config.cwd {
        cmd.current_dir(cwd);
    }
    let mut child = cmd.spawn().map_err(|e| format!("spawn {program:?}: {e}"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "child stdin not piped".to_owned())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "child stdout not piped".to_owned())?;
    Ok(ChildIo {
        child,
        stdin,
        lines: BufReader::new(stdout).lines(),
    })
}

/// Map a child's response line to an outcome. `Err` means the line is
/// unusable (unparseable, or its echoed `id` doesn't match the request)
/// — the caller treats that as a desync and discards the child. `Ok`
/// means a clean response for *this* job.
fn parse_response(line: &str, expected_id: &str) -> Result<JobOutcome, String> {
    let resp: WorkerResponse =
        serde_json::from_str(line).map_err(|e| format!("bad worker response {line:?}: {e}"))?;
    // Reject a stale response (the child emitted an extra line for an
    // earlier job) so it can't be mapped to this job.
    if let Some(id) = &resp.id
        && id != expected_id
    {
        return Err(format!(
            "worker response id mismatch (desync): got {id:?}, want {expected_id:?}"
        ));
    }
    Ok(match resp.body {
        WorkerResponseBody::Ok => JobOutcome::Done,
        WorkerResponseBody::Error { message } => {
            JobOutcome::Failed(message.unwrap_or_else(|| "worker reported error".to_owned()))
        }
        WorkerResponseBody::Throttled { retry_after_secs } => JobOutcome::Throttled {
            retry_after: Duration::from_secs(retry_after_secs.unwrap_or(60)),
        },
    })
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

    /// A pool whose children are a `sh` read-loop echoing one response
    /// line per request line. `reply` is the literal JSON each request
    /// gets back.
    async fn pool_echoing(reply: &str, size: usize) -> WorkerPoolHandler {
        // `read` consumes one stdin line per loop; `printf` emits one
        // response. Single-quoted so the reply isn't shell-expanded.
        let script = format!("while IFS= read -r _line; do printf '%s\\n' '{reply}'; done");
        WorkerPoolHandler::spawn(WorkerPoolConfig {
            kind: "worker_pool_test",
            argv: vec!["sh".into(), "-c".into(), script],
            env: HashMap::new(),
            cwd: None,
            size,
            timeout_secs: Some(5),
        })
        .await
        .expect("spawn pool")
    }

    fn cancel() -> CancellationToken {
        CancellationToken::new()
    }

    #[tokio::test]
    async fn ok_response_maps_to_done() {
        let pool = pool_echoing(r#"{"status":"ok"}"#, 1).await;
        let out = pool
            .dispatch("job-1", &serde_json::json!({"x":1}), &cancel())
            .await;
        assert!(matches!(out, JobOutcome::Done), "got: {out:?}");
    }

    #[tokio::test]
    async fn error_response_maps_to_failed() {
        let pool = pool_echoing(r#"{"status":"error","message":"boom"}"#, 1).await;
        let out = pool
            .dispatch("job-2", &serde_json::json!({}), &cancel())
            .await;
        match out {
            JobOutcome::Failed(msg) => assert_eq!(msg, "boom"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn throttled_response_maps_to_throttled() {
        let pool = pool_echoing(r#"{"status":"throttled","retry_after_secs":12}"#, 1).await;
        let out = pool
            .dispatch("job-3", &serde_json::json!({}), &cancel())
            .await;
        match out {
            JobOutcome::Throttled { retry_after } => assert_eq!(retry_after.as_secs(), 12),
            other => panic!("expected Throttled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn id_mismatch_is_treated_as_failure() {
        // Child echoes a fixed wrong id → desync → Failed (and the child
        // is discarded). Proves the response can't be mapped to a job it
        // wasn't for.
        let pool = pool_echoing(r#"{"id":"stale","status":"ok"}"#, 1).await;
        let out = pool
            .dispatch("job-X", &serde_json::json!({}), &cancel())
            .await;
        match out {
            JobOutcome::Failed(msg) => assert!(msg.contains("mismatch"), "got: {msg}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn matching_id_is_accepted() {
        // Child that echoes the right id is accepted as Done.
        let pool = pool_echoing(r#"{"id":"job-Y","status":"ok"}"#, 1).await;
        let out = pool
            .dispatch("job-Y", &serde_json::json!({}), &cancel())
            .await;
        assert!(matches!(out, JobOutcome::Done), "got: {out:?}");
    }

    #[tokio::test]
    async fn garbage_response_maps_to_failed() {
        let pool = pool_echoing("not json", 1).await;
        let out = pool
            .dispatch("job-4", &serde_json::json!({}), &cancel())
            .await;
        assert!(matches!(out, JobOutcome::Failed(_)), "got: {out:?}");
    }

    #[tokio::test]
    async fn reuses_the_same_warm_child_across_jobs() {
        // size=1 forces every job onto the one child; 3 sequential
        // round-trips prove the child stays alive and framed between
        // jobs (no per-job respawn).
        let pool = pool_echoing(r#"{"status":"ok"}"#, 1).await;
        for i in 0..3 {
            let out = pool
                .dispatch(&format!("job-{i}"), &serde_json::json!({}), &cancel())
                .await;
            assert!(matches!(out, JobOutcome::Done), "iter {i}: {out:?}");
        }
    }

    #[tokio::test]
    async fn empty_argv_rejected() {
        let err = WorkerPoolHandler::spawn(WorkerPoolConfig {
            kind: "x",
            argv: vec![],
            env: HashMap::new(),
            cwd: None,
            size: 1,
            timeout_secs: None,
        })
        .await;
        assert!(err.is_err());
    }
}
