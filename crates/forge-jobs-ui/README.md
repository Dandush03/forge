# forge-jobs-ui

[![crates.io](https://img.shields.io/crates/v/forge-jobs-ui.svg)](https://crates.io/crates/forge-jobs-ui)
[![docs.rs](https://img.shields.io/docsrs/forge-jobs-ui)](https://docs.rs/forge-jobs-ui)
[![license](https://img.shields.io/crates/l/forge-jobs-ui.svg)](https://github.com/dandush03/forge#license)

Reusable [Leptos](https://leptos.dev) CSR panel for a
[`forge-jobs`](../forge-jobs/) queue. Drop-in Mission Control for your
desktop app, admin UI, or ops console — overview, timeline, per-queue
charts, cron, scheduled jobs, dead-letter inspector, bulk-purge
controls.

Host-agnostic via a consumer-implemented `QueueIpc` trait: the panel
doesn't know whether your IPC is Tauri, plain HTTP, in-process,
WebSocket, or something else. You implement the trait once for your
host and pass it in.

## Install

```toml
[dependencies]
forge-jobs-ui = "0.1"
leptos        = { version = "0.8", features = ["csr"] }
```

You also need the bundled stylesheet served alongside your bundle.
With [Trunk](https://trunkrs.dev/), copy or symlink `src/panel.css`
to your dist path and add to `index.html`:

```html
<link data-trunk rel="css" href="vendor/forge-jobs-ui/panel.css" />
```

Alternatively, inject at runtime via the `PANEL_CSS` constant:

```rust,ignore
use leptos_meta::Stylesheet;
use forge_jobs_ui::PANEL_CSS;

view! { <Stylesheet text=PANEL_CSS /> }
```

## Quick start

```rust,ignore
use leptos::prelude::*;
use forge_jobs_ui::{QueueIpc, QueuePanel};
use std::sync::Arc;

// Implement QueueIpc once for your host. Every call returns
// Result<T, IpcError> so the panel can render error states.
#[derive(Clone)]
struct MyTauriIpc;

#[async_trait::async_trait(?Send)]
impl QueueIpc for MyTauriIpc {
    async fn queue_overview(&self) -> Result<Vec<QueueOverview>, IpcError> {
        // … invoke your Tauri command, decode JSON, return.
        todo!()
    }
    // … other trait methods (queue_set_backoff, jobs_list,
    //     job_inspect, cron_list, queue_metric_series, etc.)
}

#[component]
fn App() -> impl IntoView {
    let ipc = Arc::new(MyTauriIpc);
    view! { <QueuePanel ipc=ipc /> }
}
```

See [`forge-jobs-ui/src/ipc.rs`](src/ipc.rs) for the full `QueueIpc`
trait surface; matching implementations exist in the `forge-jobs-api`
Axum routes and the [`tauri-plugin-queue`](https://github.com/dandush03/tech-admin/tree/main/crates/tauri-plugin-queue) crate (Tauri command bindings).

## What you get

- **Overview tab** — per-queue cards with status counts, paused
  state, live workers, lag, retention settings, backoff config
  editor, throttle countdown.
- **Timeline tab** — real-time stacked area chart of enqueued /
  started / completed / failed counts. Preset time ranges (5m / 30m
  / 1h / 6h / 24h / 7d) + drag-to-zoom. Latency percentile overlays
  (processing p50/p95/p99, end-to-end p50/p95/p99).
- **Live metrics tab** — per-host CPU / RSS / DB-op latency, drawn
  on the same chart framework as the timeline.
- **Jobs tab** — paginated job list with status filter, kind
  filter, payload search. Click a row to inspect.
- **Inspector** — payload pretty-print, error history, scheduled
  status, retry / requeue / delete buttons.
- **Cron tab** — schedule list with enable / disable / edit-expr /
  trigger-now actions.
- **Scheduled tab** — view + run-now scheduled jobs that haven't
  fired yet.
- **Dead tab** — bulk-purge / bulk-requeue dead letters.

All wired to the `QueueIpc` trait — you implement once, every screen
works.

## Companion crates

- [`forge-jobs`](../forge-jobs/) — the queue itself. You'll need
  this in your host binary even if the UI lives in a separate WASM
  bundle (the storage types and DTOs cross the IPC).
- [`forge-jobs-api`](../forge-jobs-api/) — Axum HTTP transport. If
  your IPC is HTTP, point `QueueIpc` at it.
- [`forge-charts`](../forge-charts/) — pulled in transitively; powers
  the timeline + live-metrics + per-queue charts. Themeable via CSS
  vars.

## Status

`0.1` — the IPC trait shape is mostly stable but may evolve as we
finalize the externally-facing contract. All screens have been
running in production against a real queue for months.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option. Contributions intentionally submitted for inclusion
in this crate shall be dual-licensed as above, without any additional
terms or conditions.
