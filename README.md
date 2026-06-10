# forge

A small family of Rust crates for building production job queues and the UI
to run them. Extracted from a real Tauri desktop app — every crate has been
running in production against ~2K Slack threads + GitHub issues for months
before being broken out.

## Screenshots

The `forge-jobs-ui` Mission Control panel, in production. Every tab below
is a route in the panel; charts come from `forge-charts` and update live
as the queue churns. Run yours wherever you mount the consumer-implemented
`QueueIpc` trait — Tauri desktop, plain HTTP, an in-process mock for tests.

### Tabs

**Overview** — workload timeline (enqueued / retried / completed / failed
buckets), processing-time and end-to-end latency percentiles, status-counts
strip, plus the live worker processes underneath.

![Overview tab](docs/screenshots/overview_tab.png)

**Jobs** — filterable job list with inline payload + last-error inspector.
Filter by queue, status, kind, or substring across the payload.

![Jobs tab](docs/screenshots/jobs_tab.png)

**Scheduled** — future-dated work (throttle backoffs, `with_run_at`
deferrals). Same table shape as Jobs but ordered by `scheduled_at`.

![Scheduled tab](docs/screenshots/scheduled_tab.png)

**Retries** — jobs that failed once and are waiting on their next
attempt. Useful for spotting clusters of correlated failures.

![Retries tab](docs/screenshots/retries_tab.png)

**Dead** — dead-letter inspector. Jobs past `max_attempts` or routed
straight to Dead by a handler (`JobOutcome::Dead`). Read the error
chain that landed them here; re-enqueue or drop one at a time.

![Dead tab](docs/screenshots/dead_tab.png)

**Cron** — per-schedule status. Cron expression, next/last fire, the
job kind it enqueues, enabled / disabled toggle. Lease-elected so only
one replica fires each tick.

![Cron tab](docs/screenshots/cron_tab.png)

**Queues** — per-queue knobs. Worker count, retention windows, backoff
curve (`backoff_enabled` / `base_seconds` / `max_seconds`). Edits take
effect on the next tick — no worker restart.

![Queues tab](docs/screenshots/queues_tab.png)

### Detail panels

**Live processes** — per-process strip on the Overview tab. Each worker
shows its current job + last heartbeat. The reaper revives jobs whose
worker stops heartbeating within `HEARTBEAT_INTERVAL`.

![Live processes](docs/screenshots/live_processes.png)

**Resources** — this-process CPU + memory + disk I/O + disk space.
Sampled every `METRICS_TICK` and aggregated into per-minute buckets
by the metrics roller (ADR 0009).

![Resources](docs/screenshots/resource_container.png)

**DB health** — storage backend visibility: read/write latency
percentiles, ops-per-minute throughput, connection pool saturation.
Same shape on SQLite and Postgres.

![DB health](docs/screenshots/db_metrics.png)

**Per-queue metrics** — per-queue latency + throughput on a configurable
time range (5m / 30m / 1h / 3h / 6h / 24h / 7d, with drag-to-zoom).
Mini chart per queue expands to the same `AreaChart` the overview uses.

![Per-queue metrics](docs/screenshots/queue_metrics.png)

## Crates

| Crate | crates.io | docs.rs | Description |
|---|---|---|---|
| [`forge-jobs`](crates/forge-jobs/) | TBD | TBD | Sidekiq-style queue with embedded SQLite + pluggable Postgres. Per-queue workers, cron, cluster-wide rate-limit budget, cancellation that survives across replicas. |
| [`forge-jobs-api`](crates/forge-jobs-api/) | TBD | TBD | HTTP transport for `forge-jobs` (Axum routes + JSON DTOs). Drop-in for a deployed multi-replica service. |
| [`forge-jobs-ui`](crates/forge-jobs-ui/) | TBD | TBD | Reusable Leptos panel for the queue — overview, timeline, per-queue charts, cron, scheduled jobs, dead-letter inspector. Host-agnostic via a `QueueIpc` trait. |
| [`forge-charts`](crates/forge-charts/) | TBD | TBD | Pure-Rust + SVG interactive charts for Leptos CSR. No JS, no canvas, no Tailwind. Used by `forge-jobs-ui` but independent. |

## Design goals

- **Local-first, then deployable.** The same code path runs single-process on
  embedded SQLite (a desktop app, a CLI) or multi-replica on Postgres (a
  cluster). Backend traits live in `forge-jobs::storage`; pick the impl
  that fits.
- **Correctness over flash.** Every claim is atomic
  (`SELECT … FOR UPDATE SKIP LOCKED` on Postgres, single-writer pool on
  SQLite), cron schedules are lease-elected (one replica fires per tick),
  rate-limit buckets serialize across pods.
- **Cooperative cancellation.** `QueueHandle::request_cancel(&JobId)` stops
  a running job in-process; cross-replica cancels flow through a DB flag
  the worker's heartbeat observes within `HEARTBEAT_INTERVAL`. User-cancel
  routes straight to `Dead` — no retry-budget waste.
- **Per-queue exponential backoff with a clean toggle.** `Failed` and
  `Throttled` both honor the same per-queue `backoff_enabled` /
  `base_seconds` / `max_seconds`. No hardcoded constants.

## Quick start

`forge-jobs` is the core. A minimal consumer:

```rust,ignore
use std::sync::Arc;
use forge_jobs::storage::{DatabaseConfig, PathsError, QueuePaths};
use forge_jobs::{
    DefaultRouter, EnqueueRequest, HandlerRegistry, NoopEcho, QueueRuntime,
};

// Implement QueuePaths for your project's paths layer — env vars,
// `directories`, a hardcoded prod path, a tempdir for tests, etc.
#[derive(Debug)]
struct EnvPaths;
impl QueuePaths for EnvPaths {
    fn config_dir(&self) -> Result<std::path::PathBuf, PathsError> {
        Ok("./jobs/config".into())
    }
    fn data_dir(&self) -> Result<std::path::PathBuf, PathsError> {
        Ok("./jobs/data".into())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let paths = EnvPaths;
    let storage = DatabaseConfig::load(&paths)?.open_storage(&paths).await?;
    let mut handlers = HandlerRegistry::new();
    handlers.register(NoopEcho);
    let runtime = QueueRuntime::new(storage, handlers, Arc::new(DefaultRouter));
    runtime.ensure_queue("default", 2).await?;
    let handle = runtime.start().await?;
    // ... handle.shutdown_graceful(timeout) at exit
    Ok(())
}
```

See [`crates/forge-jobs/examples/minimal.rs`](crates/forge-jobs/examples/minimal.rs) for the runnable version and each crate's README for more detail.

## Architecture

```
                          ┌──────────────┐
            (your app) ──▶│ QueueRuntime │ ──▶ N worker tasks
                          └──────┬───────┘     + reaper + cleanup
                                 │              + cron + metrics
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
          ┌─────────┐    ┌──────────────┐   ┌──────────────────┐
          │JobQueue │    │ QueueConfig  │   │ RateLimitStorage │
          │         │    │              │   │                  │   …+ ProcessRegistry
          │         │    │              │   │                  │     CronStorage
          └─────────┘    └──────────────┘   └──────────────────┘
                   \           |           /
                    \          |          /
                     ▼         ▼         ▼
                     ┌──────────────────────┐
                     │   SqliteStorage  /   │
                     │   PostgresStorage    │
                     └──────────────────────┘

                          ┌────────────┐
            (your IPC) ──▶│  QueueIpc  │ (forge-jobs-ui consumer trait)
                          └─────┬──────┘
                                ▼
                          ┌──────────────┐
                          │forge-jobs-ui │  Leptos panel
                          └──────┬───────┘  ─ overview / timeline / cron / …
                                 ▼
                          ┌──────────────┐
                          │ forge-charts │  AreaChart + tooltip + zoom + theming
                          └──────────────┘
```

The storage layer is *six* traits (queue, processes, config, cron, rate-limit,
paths) and one bundle struct. Swap the backend by swapping trait impls —
nothing in the runtime moves.

## Status

`0.1` — internal API mostly stable, a few naming + restructure passes still
likely before `1.0`. Pin a specific commit if you want byte-for-byte
reproducibility during this window.

## Choosing crates

- **Just need a queue?** `forge-jobs`. Works as a library with no other
  forge-* deps.
- **Building a multi-replica service?** `forge-jobs` + `forge-jobs-api`.
  The HTTP transport lets ops poke the queue without an in-process
  binding.
- **Building a desktop app or admin panel?** `forge-jobs` +
  `forge-jobs-ui`. The UI handles overview / timeline / cron / scheduled
  / dead-letter screens. You implement the `QueueIpc` trait once for your
  host (Tauri, plain HTTP, etc.).
- **Just want the chart library?** `forge-charts` is independent. No
  jobs/queue dependency.

## Running on Postgres at scale

The embedded SQLite defaults are tuned for the single-process /
desktop case. Before pointing a multi-replica cluster or a
high-throughput workload at the Postgres backend, read
[docs/operating-at-scale.md](docs/operating-at-scale.md) — it covers the
backend-choice threshold, the vacuum/bloat tuning (and how to apply
`fillfactor` to an already-populated table), connection-pool sizing, the
single-coordinator background-work ceiling, and rate-limit-scope
contention.

## Repository layout

```
forge/
├── Cargo.toml              workspace
├── README.md               (this file)
├── LICENSE-MIT
├── LICENSE-APACHE
└── crates/
    ├── forge-jobs/         the queue
    ├── forge-jobs-api/     Axum HTTP transport
    ├── forge-jobs-ui/      Leptos panel
    └── forge-charts/       SVG charts
```

## Contributing

Bug reports and PRs welcome. Each crate carries its own quality bar in
its `Cargo.toml` `[lints]` (inherits from the workspace's
`workspace.lints` — `clippy::pedantic + nursery`, `unsafe_code = "deny"`,
`unwrap_used = "deny"`).

Before opening a PR:

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p forge-jobs --features postgres --all-targets -- -D warnings
cargo test --workspace
```

The frontend crates (`forge-jobs-ui`, `forge-charts`) are WASM-only;
their clippy invocation needs `--target wasm32-unknown-unknown`.

## License

Dual-licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project by you, as defined in the Apache-2.0
license, shall be dual-licensed as above, without any additional terms or
conditions.

## Acknowledgments

Built and battle-tested inside [`tech-admin`](https://github.com/dandush03/tech-admin) — a Tauri cockpit for a 3K-person engineering org. The
queue carried ~19K activities + ~2K tickets across Slack and GitHub for
months before being broken out.
