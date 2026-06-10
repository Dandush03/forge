//! `loadgen` — Postgres load / soak harness for `forge-jobs`.
//!
//! Drives sustained enqueue → claim → finalize against a real Postgres
//! instance and reports claim-latency percentiles, throughput, and a
//! table-bloat snapshot. This is the tool that answers "does `claim_next`
//! stay flat as `sync_queue` grows to millions of rows, and does
//! autovacuum keep the churn in check" — i.e. it validates the
//! `queue_event` buffering and the storage tuning under real volume.
//! See `docs/operating-at-scale.md`.
//!
//! Postgres only — build with `--features postgres`.
//!
//! ```text
//! docker compose up -d postgres
//! DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5433/postgres \
//!   cargo run -p forge-jobs --features postgres --bin loadgen --release
//! ```
//!
//! Config via env (all optional):
//!
//! - `DATABASE_URL` / `TEST_DATABASE_URL`: PG connection string (required).
//! - `LOADGEN_SEED`: rows to pre-seed before the run (default `100_000`).
//! - `LOADGEN_WORKERS`: concurrent claim/finalize tasks (default 8).
//! - `LOADGEN_FEED`: concurrent enqueue feeders during the run (default 0 =
//!   drain-only). Above 0 keeps the queue full so the table grows under load.
//! - `LOADGEN_DURATION_SECS`: run length (default 30).
//! - `LOADGEN_QUEUE`: queue name (default `loadgen`).
//! - `LOADGEN_MAX_CONN`: PG pool size (default workers + feeders + 4).
//! - `LOADGEN_PROBE`: set to `1` for probe mode — measure enqueue→pickup
//!   latency through the `wait_for_work` (LISTEN/NOTIFY) path on a
//!   not-saturated queue, instead of claim throughput under a backlog.
//! - `LOADGEN_PROBE_RATE`: probe enqueue rate, jobs/s (default 50).
//!
//! To see how claim latency scales with table size, run it a few times
//! with `LOADGEN_SEED=1000000`, `10000000`, … and compare the p99. The
//! drained `done` rows are left in the table (no cleanup), so the table
//! stays large for the whole run — exactly the condition under test.

// CLI binary: stdout IS the user-facing surface; the workspace lints
// flag prints in library code, not in a load-test tool.
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "load-test output goes to stdout by design"
)]
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "latency math + percentile reporting; approximate by design"
)]
#![allow(
    clippy::too_many_lines,
    reason = "main() is a linear top-to-bottom harness: config, seed, run, report"
)]

use std::env;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use forge_jobs::{
    EnqueueRequest, FinalizeOutcome, JobQueue, PostgresStorage, QueueConfig, QueueCounts,
};
use serde_json::json;
use sqlx::Row;

type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

// ── microsecond latency histogram (HdrHistogram-lite) ───────────────────
// Exponential buckets with 8 linear sub-buckets per octave → percentile
// error within ~12%, O(1) memory regardless of sample count (so it's safe
// for a multi-hour soak). Bounded array covers up to 2^48 µs (~3 years).
const SUB_BITS: u32 = 3;
const SUB: usize = 1 << SUB_BITS;
const N_OCTAVES: usize = 48;

struct Hist {
    buckets: Vec<u64>,
    count: u64,
}

impl Hist {
    fn new() -> Self {
        Self {
            buckets: vec![0; N_OCTAVES * SUB],
            count: 0,
        }
    }

    fn index(v: u64) -> usize {
        if (v as usize) < SUB {
            return v as usize;
        }
        let octave = v.ilog2();
        let sub = ((v >> (octave - SUB_BITS)) & (SUB as u64 - 1)) as usize;
        let idx = (octave as usize) * SUB + sub;
        idx.min(N_OCTAVES * SUB - 1)
    }

    // Upper edge (µs) of a bucket — the value we report for a percentile.
    const fn value(index: usize) -> u64 {
        if index < SUB {
            return index as u64;
        }
        let octave = (index / SUB) as u32;
        let sub = (index % SUB) as u64;
        let base = 1u64 << octave;
        base + (sub + 1) * (base >> SUB_BITS)
    }

    fn record(&mut self, micros: u64) {
        let i = Self::index(micros);
        self.buckets[i] += 1;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        for (a, b) in self.buckets.iter_mut().zip(&other.buckets) {
            *a += *b;
        }
        self.count += other.count;
    }

    fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = (p / 100.0 * self.count as f64).ceil() as u64;
        let mut cum = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            cum += c;
            if cum >= target {
                return Self::value(i);
            }
        }
        Self::value(self.buckets.len() - 1)
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Probe mode: a slow producer enqueues at `rate`/s while idle workers
/// block on `wait_for_work` (the LISTEN/NOTIFY path) and claim as soon as
/// they're woken. We report the *pickup* latency — wall time from a row's
/// `enqueued_at` to a worker claiming it — which is what NOTIFY drives down
/// versus a queue that polls on an interval. Workers are kept mostly idle
/// (slow producer) so this isolates wake+claim, not queue wait.
async fn run_probe(
    storage: Arc<PostgresStorage>,
    queue: String,
    workers: u64,
    duration: Duration,
    rate: u64,
) -> AnyResult<()> {
    println!("probe mode: enqueue→pickup latency via wait_for_work (NOTIFY)");
    println!(
        "  target rate  {rate}/s   workers {workers}   duration {}s",
        duration.as_secs()
    );
    println!();

    let stop = Arc::new(AtomicBool::new(false));
    let picked = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for w in 0..workers {
        let storage = Arc::clone(&storage);
        let stop = Arc::clone(&stop);
        let picked = Arc::clone(&picked);
        let queue = queue.clone();
        let pid = format!("probe-{w}");
        handles.push(tokio::spawn(async move {
            let mut pickup = Hist::new();
            let mut e2e = Hist::new();
            while !stop.load(Ordering::Relaxed) {
                // Block until a NOTIFY wakes us (or a 1s safety timeout).
                let _ = storage.wait_for_work(&queue, Duration::from_secs(1)).await;
                while let Ok(Some(job)) = storage.claim_next(&queue, &pid).await {
                    let enq = job.enqueued_at;
                    let since = |t: chrono::DateTime<Utc>| {
                        (Utc::now() - t).num_microseconds().unwrap_or(0).max(0) as u64
                    };
                    // pickup = enqueue→claim; e2e = enqueue→durably finalized.
                    pickup.record(since(enq));
                    picked.fetch_add(1, Ordering::Relaxed);
                    let _ = storage
                        .finalize(&job.id, Some(&pid), FinalizeOutcome::Done)
                        .await;
                    e2e.record(since(enq));
                }
            }
            (pickup, e2e)
        }));
    }

    // Producer: one enqueue every 1/rate seconds.
    let interval = Duration::from_micros(1_000_000 / rate);
    let prod_stop = Arc::clone(&stop);
    let prod_storage = Arc::clone(&storage);
    let prod_queue = queue.clone();
    let producer = tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while !prod_stop.load(Ordering::Relaxed) {
            tick.tick().await;
            let _ = prod_storage
                .enqueue(EnqueueRequest::new("loadgen", json!({})).on_queue(prod_queue.clone()))
                .await;
        }
    });

    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);
    let _ = producer.await;

    let mut pickup = Hist::new();
    let mut e2e = Hist::new();
    for h in handles {
        if let Ok((wp, we)) = h.await {
            pickup.merge(&wp);
            e2e.merge(&we);
        }
    }
    let _ = storage.flush_event_buffer().await;

    let n = picked.load(Ordering::Relaxed);
    println!("picked up {n} jobs");
    let line = |label: &str, h: &Hist| {
        println!(
            "  {label}  p50 {}µs   p95 {}µs   p99 {}µs   max {}µs",
            h.percentile(50.0),
            h.percentile(95.0),
            h.percentile(99.0),
            h.percentile(100.0),
        );
    };
    line("pickup latency (enqueue→claim)        ", &pickup);
    line("end-to-end latency (enqueue→finalized)", &e2e);
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> AnyResult<()> {
    let url = env::var("DATABASE_URL")
        .or_else(|_| env::var("TEST_DATABASE_URL"))
        .map_err(|_| "set DATABASE_URL or TEST_DATABASE_URL to the Postgres connection string")?;

    let seed = env_u64("LOADGEN_SEED", 100_000);
    let workers = env_u64("LOADGEN_WORKERS", 8).max(1);
    let feeders = env_u64("LOADGEN_FEED", 0);
    let duration = Duration::from_secs(env_u64("LOADGEN_DURATION_SECS", 30));
    let queue = env::var("LOADGEN_QUEUE").unwrap_or_else(|_| "loadgen".to_owned());
    let max_conn = env_u64("LOADGEN_MAX_CONN", workers + feeders + 4) as u32;

    println!("forge-jobs loadgen");
    println!("  url        {}", redact(&url));
    println!("  queue      {queue}");
    println!("  seed       {seed} rows");
    println!("  workers    {workers}");
    println!("  feeders    {feeders}");
    println!("  duration   {}s", duration.as_secs());
    println!("  pool       {max_conn}");
    println!();

    let storage = Arc::new(PostgresStorage::open(&url, max_conn).await?);
    storage.ensure_queue(&queue, workers as i32).await?;

    // Probe mode: measure enqueue→pickup latency through the NOTIFY wake
    // path on a *not*-saturated queue (the metric that beats a polling
    // queue), rather than claim throughput under a saturated backlog.
    if env::var("LOADGEN_PROBE").is_ok_and(|v| v == "1" || v == "true") {
        let rate = env_u64("LOADGEN_PROBE_RATE", 50).max(1);
        return run_probe(storage, queue, workers, duration, rate).await;
    }

    // ── seed ────────────────────────────────────────────────────────────
    if seed > 0 {
        let t0 = Instant::now();
        let mut remaining = seed;
        while remaining > 0 {
            let batch = remaining.min(1000);
            let reqs: Vec<EnqueueRequest> = (0..batch)
                .map(|_| EnqueueRequest::new("loadgen", json!({})).on_queue(queue.clone()))
                .collect();
            storage.enqueue_bulk(reqs).await?;
            remaining -= batch;
        }
        let secs = t0.elapsed().as_secs_f64();
        println!(
            "seeded {seed} rows in {secs:.1}s ({:.0} enqueue/s)",
            seed as f64 / secs,
        );
        // A freshly bulk-loaded table has no planner statistics, so
        // claim_next plans a seq-scan + sort instead of using jq_claim —
        // claims run orders of magnitude slower until autovacuum's
        // auto-analyze fires (~1 naptime later). A real deployment gets
        // incremental inserts + ongoing auto-analyze; ANALYZE here puts
        // the table in that steady state so we measure the queue, not the
        // cold-stats artifact of bulk loading.
        let _ = sqlx::query("ANALYZE sync_queue")
            .execute(storage.pool())
            .await;
    }

    // ── run ──────────────────────────────────────────────────────────────
    let stop = Arc::new(AtomicBool::new(false));
    let claimed = Arc::new(AtomicU64::new(0));
    let fed = Arc::new(AtomicU64::new(0));

    let mut feed_handles = Vec::new();
    for _ in 0..feeders {
        let storage = Arc::clone(&storage);
        let stop = Arc::clone(&stop);
        let fed = Arc::clone(&fed);
        let queue = queue.clone();
        feed_handles.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if storage
                    .enqueue(EnqueueRequest::new("loadgen", json!({})).on_queue(queue.clone()))
                    .await
                    .is_ok()
                {
                    fed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    let mut worker_handles = Vec::new();
    for w in 0..workers {
        let storage = Arc::clone(&storage);
        let stop = Arc::clone(&stop);
        let claimed = Arc::clone(&claimed);
        let queue = queue.clone();
        let pid = format!("loadgen-{w}");
        worker_handles.push(tokio::spawn(async move {
            let mut hist = Hist::new();
            let mut empties = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                match storage.claim_next(&queue, &pid).await {
                    Ok(Some(job)) => {
                        hist.record(t.elapsed().as_micros() as u64);
                        empties = 0;
                        // Finalize Done (untimed — we report the claim path).
                        let _ = storage
                            .finalize(&job.id, Some(&pid), FinalizeOutcome::Done)
                            .await;
                        claimed.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        // Backlog drained. In drain-only mode, give up after a
                        // short grace; with feeders running, keep polling.
                        empties += 1;
                        if empties > 200 {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            }
            hist
        }));
    }

    let run_start = Instant::now();
    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);

    let mut hist = Hist::new();
    for h in worker_handles {
        if let Ok(wh) = h.await {
            hist.merge(&wh);
        }
    }
    for h in feed_handles {
        let _ = h.await;
    }
    let elapsed = run_start.elapsed().as_secs_f64();

    // Flush the buffered timeline events this run produced.
    let _ = storage.flush_event_buffer().await;

    // ── report ─────────────────────────────────────────────────────────
    let total_claimed = claimed.load(Ordering::Relaxed);
    let total_fed = fed.load(Ordering::Relaxed);
    println!();
    println!("run: {elapsed:.1}s");
    println!(
        "  claimed+finalized  {total_claimed}  ({:.0}/s)",
        total_claimed as f64 / elapsed,
    );
    if feeders > 0 {
        println!(
            "  enqueued (feed)    {total_fed}  ({:.0}/s)",
            total_fed as f64 / elapsed
        );
    }
    println!(
        "  claim latency      p50 {}µs   p95 {}µs   p99 {}µs   max {}µs",
        hist.percentile(50.0),
        hist.percentile(95.0),
        hist.percentile(99.0),
        hist.percentile(100.0),
    );

    if let Ok(c) = storage.count_by_status(&queue).await {
        print_counts(&c);
    }
    print_bloat(storage.pool()).await;

    Ok(())
}

fn print_counts(c: &QueueCounts) {
    println!(
        "  queue depth        pending {} | in_progress {} | done {} | failed {} | dead {}",
        c.pending, c.in_progress, c.done, c.failed, c.dead,
    );
}

async fn print_bloat(pool: &sqlx::PgPool) {
    let sql = r"SELECT relname,
                       n_live_tup,
                       n_dead_tup,
                       pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
                       last_autovacuum
                  FROM pg_stat_user_tables
                 WHERE relname IN ('sync_queue', 'queue_event')
                 ORDER BY relname";
    match sqlx::query(sql).fetch_all(pool).await {
        Ok(rows) => {
            println!();
            println!("bloat snapshot (pg_stat_user_tables):");
            for r in rows {
                let name: String = r.try_get("relname").unwrap_or_default();
                let live: i64 = r.try_get("n_live_tup").unwrap_or(0);
                let dead: i64 = r.try_get("n_dead_tup").unwrap_or(0);
                let size: String = r.try_get("total_size").unwrap_or_default();
                let ratio = if live > 0 {
                    dead as f64 / live as f64
                } else {
                    0.0
                };
                let last_av: Option<chrono::DateTime<chrono::Utc>> =
                    r.try_get("last_autovacuum").ok();
                println!(
                    "  {name:<12} live {live:>10}  dead {dead:>9}  dead_ratio {ratio:.3}  size {size:<8}  last_autovacuum {}",
                    last_av.map_or_else(|| "never".to_owned(), |t| t.to_rfc3339()),
                );
            }
        }
        Err(e) => println!("bloat snapshot failed: {e}"),
    }
}

/// Hide the password in a connection string for the echoed config line.
fn redact(url: &str) -> String {
    match (url.find("://"), url.find('@')) {
        (Some(s), Some(a)) if a > s + 3 => {
            let scheme = &url[..s + 3];
            let tail = &url[a..];
            format!("{scheme}***{tail}")
        }
        _ => url.to_owned(),
    }
}
