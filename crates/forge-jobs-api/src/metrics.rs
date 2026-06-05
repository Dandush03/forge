//! Prometheus exposition for the queue (`GET /metrics`).
//!
//! Per-queue gauges an autoscaler (Prometheus + HPA, or KEDA) scales
//! on. The load-bearing one is `queue_oldest_pending_age_seconds` — the
//! lag — but depth + capacity are exposed too so dashboards can show
//! the whole picture. All values are integers, rendered as gauges.
//!
//! Only gauges for now: throughput counters / duration histograms need
//! hot-path instrumentation (a storage decorator) and land separately.
//! These gauges are derived from cheap indexed reads at scrape time.

use std::fmt::Write as _;

use chrono::Utc;
use forge_jobs::Storage;

use crate::Error;

/// One queue's scrape sample. Module-private; the renderer and its
/// unit tests live here so no storage backend is needed to test it.
struct QueueSample {
    queue: String,
    pending: u64,
    scheduled: u64,
    in_flight: u64,
    done: u64,
    failed: u64,
    dead: u64,
    oldest_pending_age_seconds: u64,
    max_workers: u64,
    paused: bool,
    throttled: bool,
}

/// Collect per-queue samples and render the Prometheus text body.
///
/// # Errors
///
/// Surfaces storage errors from the per-queue count / lag reads.
pub async fn render(storage: &Storage) -> Result<String, Error> {
    let now = Utc::now();
    let queues = storage.config.list_queues().await?;
    let mut samples = Vec::with_capacity(queues.len());
    for cfg in queues {
        let counts = storage.jobs.count_by_status(&cfg.name).await?;
        let lag = storage
            .jobs
            .oldest_ready_at(&cfg.name)
            .await?
            .map_or(0, |t| u64::try_from((now - t).num_seconds()).unwrap_or(0));
        samples.push(QueueSample {
            queue: cfg.name,
            pending: counts.pending,
            scheduled: counts.scheduled,
            in_flight: counts.in_progress,
            done: counts.done,
            failed: counts.failed,
            dead: counts.dead,
            oldest_pending_age_seconds: lag,
            max_workers: u64::try_from(cfg.max_workers.max(0)).unwrap_or(0),
            paused: cfg.paused,
            throttled: cfg.throttled_until.is_some_and(|t| t > now),
        });
    }
    Ok(render_text(&samples))
}

fn render_text(samples: &[QueueSample]) -> String {
    let mut out = String::new();
    gauge(
        &mut out,
        "queue_pending_jobs",
        "Jobs ready to claim now (status=pending, scheduled_at<=now).",
        samples,
        |s| s.pending,
    );
    gauge(
        &mut out,
        "queue_scheduled_jobs",
        "Deferred jobs (status=pending, scheduled_at>now).",
        samples,
        |s| s.scheduled,
    );
    gauge(
        &mut out,
        "queue_in_flight_jobs",
        "Jobs currently being processed (status=in_progress).",
        samples,
        |s| s.in_flight,
    );
    gauge(
        &mut out,
        "queue_done_jobs",
        "Completed jobs retained on the queue.",
        samples,
        |s| s.done,
    );
    gauge(
        &mut out,
        "queue_failed_jobs",
        "Jobs in retry backoff (status=failed).",
        samples,
        |s| s.failed,
    );
    gauge(
        &mut out,
        "queue_dead_jobs",
        "Jobs that exhausted retries (status=dead).",
        samples,
        |s| s.dead,
    );
    gauge(
        &mut out,
        "queue_oldest_pending_age_seconds",
        "Age of the oldest ready job — the queue lag. Scale on this.",
        samples,
        |s| s.oldest_pending_age_seconds,
    );
    gauge(
        &mut out,
        "queue_max_workers",
        "Configured cluster-wide worker total.",
        samples,
        |s| s.max_workers,
    );
    gauge(
        &mut out,
        "queue_paused",
        "1 if the queue is paused, else 0.",
        samples,
        |s| u64::from(s.paused),
    );
    gauge(
        &mut out,
        "queue_throttled",
        "1 if the queue is in a backoff cool-down, else 0.",
        samples,
        |s| u64::from(s.throttled),
    );
    out
}

/// Emit one gauge metric: `# HELP` + `# TYPE` then one line per queue.
fn gauge(
    out: &mut String,
    name: &str,
    help: &str,
    samples: &[QueueSample],
    value: impl Fn(&QueueSample) -> u64,
) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    for s in samples {
        let _ = writeln!(
            out,
            "{name}{{queue=\"{}\"}} {}",
            escape_label(&s.queue),
            value(s)
        );
    }
}

/// Escape a Prometheus label value: backslash, double-quote, newline.
fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(queue: &str) -> QueueSample {
        QueueSample {
            queue: queue.to_owned(),
            pending: 42,
            scheduled: 3,
            in_flight: 5,
            done: 100,
            failed: 2,
            dead: 1,
            oldest_pending_age_seconds: 30,
            max_workers: 10,
            paused: false,
            throttled: true,
        }
    }

    #[test]
    fn render_text_emits_help_type_and_per_queue_lines() {
        let out = render_text(&[sample("gh")]);
        assert!(out.contains("# TYPE queue_pending_jobs gauge"));
        assert!(out.contains("queue_pending_jobs{queue=\"gh\"} 42"));
        assert!(out.contains("queue_oldest_pending_age_seconds{queue=\"gh\"} 30"));
        assert!(out.contains("queue_throttled{queue=\"gh\"} 1"));
        assert!(out.contains("queue_paused{queue=\"gh\"} 0"));
    }

    #[test]
    fn render_text_escapes_label_values() {
        let out = render_text(&[sample(r#"a"b\c"#)]);
        assert!(out.contains(r#"queue="a\"b\\c""#), "got: {out}");
    }
}
