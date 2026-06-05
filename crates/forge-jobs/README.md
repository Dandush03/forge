# forge-jobs

[![crates.io](https://img.shields.io/crates/v/forge-jobs.svg)](https://crates.io/crates/forge-jobs)
[![docs.rs](https://img.shields.io/docsrs/forge-jobs)](https://docs.rs/forge-jobs)
[![license](https://img.shields.io/crates/l/forge-jobs.svg)](https://github.com/dandush03/forge#license)

Sidekiq-style job queue for Rust with embedded `SQLite` and pluggable
Postgres. Register handlers, enqueue jobs; the runtime claims / runs /
finalizes them across N worker tasks. **The same code path runs
single-process on `SQLite` (local desktop, CLI tools) or multi-replica
on Postgres (deployed service).** Battle-tested against ~2K tickets +
~19K activities for months before breaking out into its own crate.

## Install

```toml
[dependencies]
forge-jobs = "0.1"

# Enable the optional Postgres adapter for deployed multi-replica use:
forge-jobs = { version = "0.1", features = ["postgres"] }
```

Pre-publish (using the workspace directly):

```toml
[dependencies]
forge-jobs = { git = "https://github.com/dandush03/forge" }
```

## Status

`0.1` — internal API mostly stable. A few naming passes may happen
pre-`1.0`. Pin a specific version if you want byte-for-byte
reproducibility during this window.

## Features

- **Two backends**, one runtime: `SQLite` for local-first single-process
  apps; Postgres (`--features postgres`) for multi-replica deploys with
  `SELECT … FOR UPDATE SKIP LOCKED` claims and `LISTEN/NOTIFY` wakeups.
- **Per-queue worker pools** with cooperative shutdown, stale-heartbeat
  reaper, scheduled-job retention sweep.
- **Cron schedules** with a lease-elected leader so only one replica
  fires each tick. The same lease gates the retention sweep and metrics
  roller, so N pods don't each redundantly delete the same rows.
- **Configurable backoff**: per-queue exponential curve with a clean
  on/off toggle. `backoff_enabled = false` means failures retry
  immediately (bounded by `max_attempts`); `backoff_enabled = true`
  reads `base_seconds` and `max_seconds` from the queue's row.
- **Cancellation**: `QueueHandle::request_cancel(&JobId)` instant-cancels
  in-process; cross-replica cancels flow through a `cancel_requested_at`
  DB flag that the worker's heartbeat tick observes within `HEARTBEAT_INTERVAL`.
  User-cancelled jobs route straight to `Dead` (no retry budget waste).
- **Cluster-wide rate-limit budget**: handlers call
  `ctx.rate_limit.acquire("slack")` (or `"gh"`, or any scope) against a
  DB-backed token bucket. Two replicas can't both spend the same last
  token. A real upstream 429 drains the bucket so siblings observe
  empty on their next acquire instead of each firing their own
  redundant 429.
- **`JobOutcome::Dead(msg)` for terminal failures**: handlers that can
  prove a retry would also fail (`thread_not_found`, 404, deleted
  upstream resource) skip the retry budget entirely. No more burning
  five attempts × backoff curve for a permanently-gone resource.

## What it doesn't give you

- An HTTP transport — see the sibling crate
  [`forge-jobs-api`](../forge-jobs-api/) for Axum routes + DTOs.
- A UI — see [`forge-jobs-ui`](../forge-jobs-ui/) for a Leptos panel
  that consumes a small `QueueIpc` trait.
- A built-in paths resolver: you implement the small
  [`QueuePaths`](src/storage/paths.rs) trait so the queue stays
  reusable across hosts. See [`examples/minimal.rs`](examples/minimal.rs)
  for the canonical pattern.

## Architecture

Four pieces, all on the storage traits — swap the backend by swapping
trait impls, the rest of the crate doesn't change.

```
                ┌──────────────┐
                │ QueueRuntime │  per-queue supervisor + N worker tasks +
                └──────┬───────┘  reaper + cleanup + cron + metrics
                       │
   ┌───────────────────┼───────────────────┐
   ▼                   ▼                   ▼
┌─────────┐    ┌────────────┐    ┌──────────────────┐
│ JobQueue│    │QueueConfig │    │ RateLimitStorage │   …+ ProcessRegistry,
│         │    │            │    │                  │     CronStorage
└─────────┘    └────────────┘    └──────────────────┘
       \           |           /
        \          |          /
         ▼         ▼         ▼
         ┌──────────────────────┐
         │   SqliteStorage  /   │
         │   PostgresStorage    │
         └──────────────────────┘
```

## Minimal consumer

See [`examples/minimal.rs`](examples/minimal.rs). The 30-line
shape:

1. Implement `QueuePaths` for your project's paths layer (or use env
   vars + CWD-relative fallbacks, like the bundled `jobs-db` CLI does)
2. `DatabaseConfig::load(&paths)?.open_storage(&paths).await?`
3. Build a `HandlerRegistry`, `register` your handlers
4. `QueueRuntime::new(storage, handlers, router)`
5. `runtime.ensure_queue("default", N).await?` for each queue you'll use
6. `runtime.enqueue(req).await?` to seed work
7. `runtime.start().await?` to spawn workers; keep the `QueueHandle`
   so you can `shutdown_graceful(...)` at exit

## Handler cancellation contract

Handlers that take longer than ~1 second should periodically check
`ctx.cancel.is_cancelled()` between `.await` points. The runtime fires
the cancel token when:

- The user clicks delete on a running job (in-process via
  `request_cancel`, or cross-pod via the DB flag the heartbeat
  observes)
- The supervisor shuts down or scales the queue's worker pool down

A user-initiated cancel routes the job straight to `Dead` with
`"cancelled by user"`, bypassing the retry budget. A supervisor-
initiated cancel leaves the row in_progress for the reaper.

Pure-Rust handlers that wrap a `client.call().await` are fine as-is —
at worst one extra upstream call happens after the user clicked
delete. Handlers that loop over paginated upstream results should add
`if ctx.cancel.is_cancelled() { return JobOutcome::Failed("cancelled".into()); }`
at the loop head.

## Backends

### `SQLite` (default)

`SqliteStorage` opens a WAL-mode file with a single-writer pool +
multi-reader pool. Migrations run idempotently on open. Suitable for
single-process desktop apps; the lease-elected coordinator paths are
no-ops (the lone process always wins).

### Postgres (`--features postgres`)

`PostgresStorage` uses `SELECT … FOR UPDATE SKIP LOCKED` for atomic
claims, `LISTEN/NOTIFY` for instant wake-up on enqueue, and per-host
process registry rows with heartbeat-based liveness. The cron / cleanup
/ metrics loops gate their work behind a `cron_leader` lease so N pods
don't each redundantly run them.

## Configuration

A `queue_database.toml` file at either `<config_dir>/queue_database.toml`
(per your `QueuePaths` impl) or any parent directory of CWD (up to 4
levels) selects the backend. Missing file → `SQLite` at
`<data_dir>/queue.sqlite`.

```toml
# SQLite (default)
adapter = "sqlite"
# path = "/custom/path/to/queue.sqlite"
```

```toml
# Postgres
adapter = "postgres"
host = "db.internal"
database = "tech_admin"
username = "tech_admin"
password_env = "TECH_ADMIN_DB_PASSWORD"  # reads the env var by name
max_connections = 30
```

## Admin CLI

`cargo run --bin jobs-db -- <create | drop | migrate | reset | status>`

Uses `JOBS_CONFIG_DIR` / `JOBS_DATA_DIR` env vars for paths (falls
back to `./jobs/config` / `./jobs/data` when unset).

## License

Same as the parent repo. Inquire if you'd like a different license for
external use.
