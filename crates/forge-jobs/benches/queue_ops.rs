//! Storage hot-path microbenchmarks for the Postgres backend.
//!
//! Measures the three operations that gate queue throughput — `enqueue`,
//! `claim_next`, and `finalize` — in isolation, and how `claim_next`
//! holds up as the table accumulates `done` history (the realistic
//! large-table condition). Companion to `src/bin/loadgen.rs`, which
//! measures the whole system under sustained load; this is for tracking
//! per-op regressions.
//!
//! Postgres only, and needs a live database:
//!
//! ```text
//! docker compose up -d postgres
//! DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5433/postgres \
//!   cargo bench -p forge-jobs --features postgres
//! ```
//!
//! With no `DATABASE_URL` / `TEST_DATABASE_URL` set, each benchmark
//! prints a skip notice and returns, so `cargo bench` stays green.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "benchmark setup crashes loudly on failure; not production code"
)]
#![allow(
    clippy::print_stderr,
    reason = "skip notices when no database is configured go to stderr"
)]

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use forge_jobs::{EnqueueRequest, FinalizeOutcome, JobQueue, PostgresStorage, QueueConfig};
use serde_json::json;
use tokio::runtime::Runtime;

fn database_url() -> Option<String> {
    env::var("DATABASE_URL")
        .or_else(|_| env::var("TEST_DATABASE_URL"))
        .ok()
}

/// Build a tokio runtime + an open storage handle on a unique queue, or
/// `None` if no database is configured (so the bench skips cleanly).
fn setup(rt: &Runtime, label: &str) -> Option<(Arc<PostgresStorage>, String)> {
    let url = database_url()?;
    let queue = format!(
        "bench_{label}_{}",
        ulid::Ulid::new().to_string().to_lowercase()
    );
    let storage = rt
        .block_on(PostgresStorage::open(&url, 16))
        .expect("open PostgresStorage");
    rt.block_on(storage.ensure_queue(&queue, 1))
        .expect("ensure_queue");
    Some((Arc::new(storage), queue))
}

/// Bulk-enqueue `n` rows into `queue`.
async fn seed_pending(storage: &PostgresStorage, queue: &str, n: u64) {
    let mut remaining = n;
    while remaining > 0 {
        let batch = remaining.min(1000);
        let reqs: Vec<EnqueueRequest> = (0..batch)
            .map(|_| EnqueueRequest::new("bench", json!({})).on_queue(queue.to_owned()))
            .collect();
        storage.enqueue_bulk(reqs).await.expect("seed enqueue_bulk");
        remaining -= batch;
    }
}

fn bench_enqueue(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let Some((storage, queue)) = setup(&rt, "enqueue") else {
        eprintln!("skip bench_enqueue: set DATABASE_URL / TEST_DATABASE_URL");
        return;
    };
    c.bench_function("enqueue", |b| {
        b.to_async(&rt).iter(|| {
            let storage = Arc::clone(&storage);
            let queue = queue.clone();
            async move {
                storage
                    .enqueue(EnqueueRequest::new("bench", json!({})).on_queue(queue.clone()))
                    .await
                    .expect("enqueue");
            }
        });
    });
}

fn bench_claim_finalize(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("claim_finalize");
    // `size` = number of inert `done` rows already in the table, standing
    // in for accumulated history — so this measures whether a large
    // done-backlog slows claims (it shouldn't: done rows are outside the
    // claimable status filter).
    for size in [1_000u64, 50_000] {
        let Some((storage, queue)) = setup(&rt, "claim") else {
            eprintln!("skip bench_claim_finalize: set DATABASE_URL / TEST_DATABASE_URL");
            return;
        };
        // Seed `size` rows and mark them done (fast, via one UPDATE) so
        // they sit in the table/index without being claimable.
        rt.block_on(async {
            seed_pending(&storage, &queue, size).await;
            sqlx::query(
                "UPDATE sync_queue SET status = 'done', completed_at = now(), \
                 process_id = NULL, heartbeat_at = NULL \
                 WHERE queue_name = $1 AND status = 'pending'",
            )
            .bind(&queue)
            .execute(storage.pool())
            .await
            .expect("mark done");
            // Refresh planner stats after the bulk load so claim_next uses
            // jq_claim — without this it seq-scans on cold stats and the
            // numbers measure the bulk-load artifact, not steady state.
            sqlx::query("ANALYZE sync_queue")
                .execute(storage.pool())
                .await
                .expect("analyze");
        });

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.to_async(&rt).iter_custom(|iters| {
                let storage = Arc::clone(&storage);
                let queue = queue.clone();
                async move {
                    // Re-seed the working set (untimed) so we always have
                    // exactly `iters` claimable rows on top of the done backlog.
                    seed_pending(&storage, &queue, iters).await;
                    let pid = "bench-w";
                    let start = Instant::now();
                    for _ in 0..iters {
                        if let Ok(Some(job)) = storage.claim_next(&queue, pid).await {
                            let _ = storage
                                .finalize(&job.id, Some(pid), FinalizeOutcome::Done)
                                .await;
                        }
                    }
                    start.elapsed()
                }
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    // Modest sample count + measurement time: each iter does real DB
    // round-trips and re-seeds a working set, so the defaults would run
    // for many minutes.
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(Duration::from_secs(8));
    targets = bench_enqueue, bench_claim_finalize
}
criterion_main!(benches);
