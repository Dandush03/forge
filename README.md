# forge

A small family of Rust crates for building production job queues and the UI
to run them. Extracted from a real Tauri desktop app — every crate has been
running in production against ~2K Slack threads + GitHub issues for months
before being broken out.

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
