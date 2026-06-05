//! Collapsible "Resources" panel — this process's CPU/RAM/disk read
//! from the `metric_bucket` rollup (per-pod, keyed by `host_id`).
//!
//! Lives in its own section (separate from per-queue metrics) because
//! resources are per-process, not per-queue. On the Tauri app there's
//! one process / one host series; the data layer is per-pod-ready, so
//! when a cluster UI ships, the same `queue_resource_series` command
//! returns one series per pod.

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;
use forge_charts::{AreaChart, Series};

use crate::chart_fmt::{
    BUCKET_SECS, TipRows, bytes_y_format, cpu_cores, fmt_bytes, pct_y_format, tooltip_for,
};
use crate::ipc::{IpcCtx, ResourceBucket, ResourceHostSeries};
use crate::timeline::bucket_label;

const WINDOW_SECS: i64 = 60 * 60;
const POLL_MS: u64 = 5_000;

#[component]
pub fn ResourcesPanel() -> impl IntoView {
    let expanded = RwSignal::new(false);
    let resources = RwSignal::new(Vec::<ResourceHostSeries>::new());
    let load_err = RwSignal::new(Option::<String>::None);
    // Section-wide crosshair: every chart in this panel shares window +
    // bucket count, so a hovered/pinned index maps to the same instant.
    let crosshair = RwSignal::new(Option::<usize>::None);
    let pinned = RwSignal::new(Option::<usize>::None);
    // This process's resource buckets. The data layer is keyed per-
    // pod by `host_id` (ULID, fresh per process start). After a
    // restart the rollup carries rows for both the old and new
    // ULIDs; the aggregator sorts ascending so `first()` returns the
    // *previous* run's data — making the chart look dead. ULIDs are
    // time-sortable, so `last()` is the most recent restart and the
    // right pick for a single-process desktop app.
    let res_data = Signal::derive(move || {
        resources.with(|r| r.last().map(|s| s.buckets.clone()).unwrap_or_default())
    });
    let ipc_ctx = expect_context::<IpcCtx>();

    let refresh = {
        let ipc_ctx = ipc_ctx.clone();
        move || {
            if !expanded.get_untracked() {
                return;
            }
            let to = Utc::now();
            let from = to - ChronoDuration::seconds(WINDOW_SECS);
            let ipc = ipc_ctx.clone();
            spawn_local(async move {
                match ipc.queue_resource_series(from, to, BUCKET_SECS).await {
                    Ok(rows) => {
                        resources.set(rows);
                        load_err.set(None);
                    }
                    Err(e) => load_err.set(Some(e.to_string())),
                }
            });
        }
    };

    let refresh_for_effect = refresh.clone();
    Effect::new(move |_| {
        let _ = expanded.get();
        refresh_for_effect();
    });
    let handle = set_interval_with_handle(refresh, Duration::from_millis(POLL_MS)).ok();
    on_cleanup(move || {
        if let Some(h) = handle {
            h.clear();
        }
    });

    let toggle = move |_| expanded.update(|e| *e = !*e);
    let chevron = move || if expanded.get() { "▾" } else { "▸" };

    view! {
        <section class="queue-perqueue">
            <header class="queue-perqueue-head" on:click=toggle>
                <button class="queue-perqueue-toggle" type="button">{ chevron }</button>
                <h3>"Resources"</h3>
                <span class="queue-metrics-sub">
                    { format!("this process · CPU normalized to % of {} cores · click to expand", cpu_cores()) }
                </span>
            </header>

            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "Resources: " }{ e }</div>
            }) }

            <Show when=move || expanded.get() fallback=|| ()>
                <div class="queue-perqueue-body queue-section-grid queue-section-grid-4">
                    <section class="queue-metrics-card">
                        <div class="queue-metrics-label">"CPU %"</div>
                        <AreaChart
                            data=res_data
                            x_label=|b: &ResourceBucket| bucket_label(b.at, BUCKET_SECS)
                            y_values=|b: &ResourceBucket| vec![b.cpu_pct]
                            series=vec![Series::area("CPU %", "queue-p95")]
                            height=180
                            legend=false
                            class="queue-perqueue-chart"
                            y_format=pct_y_format()
                            tooltip=res_tooltip(res_data, cpu_rows)
                            crosshair=crosshair
                            pinned=pinned
                        />
                    </section>
                    <section class="queue-metrics-card">
                        <div class="queue-metrics-label">"Memory (RSS)"</div>
                        <AreaChart
                            data=res_data
                            x_label=|b: &ResourceBucket| bucket_label(b.at, BUCKET_SECS)
                            y_values=|b: &ResourceBucket| vec![b.rss_bytes as f64]
                            series=vec![Series::area("RSS", "queue-completed")]
                            height=180
                            legend=false
                            class="queue-perqueue-chart"
                            y_format=bytes_y_format()
                            tooltip=res_tooltip(res_data, rss_rows)
                            crosshair=crosshair
                            pinned=pinned
                        />
                    </section>
                    <section class="queue-metrics-card">
                        <div class="queue-metrics-label">"Disk I/O (per min)"</div>
                        <AreaChart
                            data=res_data
                            x_label=|b: &ResourceBucket| bucket_label(b.at, BUCKET_SECS)
                            y_values=|b: &ResourceBucket| vec![
                                b.disk_read_bytes as f64,
                                b.disk_write_bytes as f64,
                            ]
                            series=vec![
                                Series::area("Read", "queue-completed"),
                                Series::area("Write", "queue-enqueued"),
                            ]
                            height=180
                            class="queue-perqueue-chart"
                            y_format=bytes_y_format()
                            tooltip=res_tooltip(res_data, disk_io_rows)
                            crosshair=crosshair
                            pinned=pinned
                        />
                    </section>
                    <section class="queue-metrics-card">
                        <div class="queue-metrics-label">"Disk space used"</div>
                        <AreaChart
                            data=res_data
                            x_label=|b: &ResourceBucket| bucket_label(b.at, BUCKET_SECS)
                            y_values=|b: &ResourceBucket| vec![b.disk_used_pct]
                            series=vec![Series::area("Used %", "queue-p99")]
                            height=180
                            legend=false
                            class="queue-perqueue-chart"
                            y_format=pct_y_format()
                            tooltip=res_tooltip(res_data, disk_used_rows)
                            crosshair=crosshair
                            pinned=pinned
                        />
                    </section>
                </div>
            </Show>
        </section>
    }
}

fn res_tooltip(
    data: Signal<Vec<ResourceBucket>>,
    rows: TipRows<ResourceBucket>,
) -> forge_charts::TooltipSlot {
    tooltip_for(data, |b| b.at, rows)
}

fn cpu_rows(b: &ResourceBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![("CPU", "p95", format!("{:.1}%", b.cpu_pct))]
}

fn rss_rows(b: &ResourceBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![("RSS", "completed", fmt_bytes(b.rss_bytes as f64))]
}

fn disk_io_rows(b: &ResourceBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![
        ("Read", "completed", fmt_bytes(b.disk_read_bytes as f64)),
        ("Write", "enqueued", fmt_bytes(b.disk_write_bytes as f64)),
    ]
}

fn disk_used_rows(b: &ResourceBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![("Used", "p99", format!("{:.1}%", b.disk_used_pct))]
}
