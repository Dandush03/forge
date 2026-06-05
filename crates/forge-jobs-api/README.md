# forge-jobs-api

[![crates.io](https://img.shields.io/crates/v/forge-jobs-api.svg)](https://crates.io/crates/forge-jobs-api)
[![docs.rs](https://img.shields.io/docsrs/forge-jobs-api)](https://docs.rs/forge-jobs-api)
[![license](https://img.shields.io/crates/l/forge-jobs-api.svg)](https://github.com/dandush03/forge#license)

HTTP transport for [`forge-jobs`](../forge-jobs/) — Axum routes over
the same handler bodies the in-process Tauri / desktop binding would
call. Lets ops poke a deployed queue without an in-process Rust
binding and gives sidecar tooling a stable JSON contract.

## Install

```toml
[dependencies]
forge-jobs     = "0.1"
forge-jobs-api = "0.1"

# For Postgres-backed deploys:
forge-jobs     = { version = "0.1", features = ["postgres"] }
forge-jobs-api = { version = "0.1", features = ["postgres"] }
```

## Use

The crate exposes:

- `forge_jobs_api::router()` — Axum `Router` you mount under your
  service. Routes are versioned-flat (no `/v1/` prefix today; the
  internal API is still settling).
- `forge_jobs_api::handlers::*` — pure async functions over
  `&Storage`. Same bodies the Tauri plugin's IPC commands call, so
  the two transports can't drift apart. Test against an in-memory
  SQLite — no router or HTTP container needed.
- `forge_jobs_api::dto::*` — request/response shapes shared between
  HTTP and IPC consumers.
- `forge_jobs_api::Error` — handler error type. Implements
  `axum::response::IntoResponse` so each variant maps to a sensible
  HTTP status code (400 / 404 / 409 / 429 / 500).

### Wiring it up

```rust,ignore
use std::sync::Arc;
use forge_jobs::storage::{DatabaseConfig, QueuePaths};
use forge_jobs::{HandlerRegistry, QueueRuntime};
use forge_jobs_api::router;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let paths = /* your QueuePaths impl */;
    let storage = DatabaseConfig::load(&paths)?.open_storage(&paths).await?;

    // Spawn workers (queue background).
    let runtime = QueueRuntime::new(storage.clone(), HandlerRegistry::new(), Arc::new(forge_jobs::DefaultRouter));
    runtime.ensure_queue("default", 4).await?;
    let _handle = runtime.start().await?;

    // Mount the HTTP API.
    let app = router(storage);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

### `jobs-server` binary

The crate also ships a small reference binary:

```bash
# Default: SQLite via $JOBS_DATA_DIR / $JOBS_CONFIG_DIR env vars.
cargo run -p forge-jobs-api --bin jobs-server

# Postgres:
cargo run -p forge-jobs-api --bin jobs-server --features postgres
```

Bind address and worker count are env-configured — see
`src/bin/jobs-server.rs`.

## Routes

| Method | Path | Description |
|---|---|---|
| `GET` | `/queue/overview` | All queues' status counts + live workers + retention settings |
| `POST` | `/queue/:name/backoff` | Set per-queue exponential backoff curve |
| `GET` | `/storage/info` | Backend identifier + boot-time facts |
| `GET` | `/metrics` | Prometheus exposition (queue depth, latency percentiles) |

More routes land as the in-process IPC surface formalizes. The
Tauri-plugin's commands and these handlers share the same DTOs, so
divergence is structurally impossible.

## Status

`0.1` — tracks `forge-jobs` major version. The handler bodies are
stable; route shapes may evolve before `1.0` as we tighten the
externally-facing contract.

## License

Dual-licensed under either [Apache-2.0](../../LICENSE-APACHE) or
[MIT](../../LICENSE-MIT) at your option.
