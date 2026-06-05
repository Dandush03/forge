//! Collapsible "Per-queue metrics" panel.
//!
//! One card per registered queue, each with three mini-charts:
//! Processing latency, Total latency, Throughput. Reads the per-queue
//! rollup via `queue_metric_series`. Process-wide resources (CPU/RAM/
//! disk) are a separate panel — see [`crate::resources`].

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;
use forge_charts::{AreaChart, Series, TooltipSlot};

use crate::chart_fmt::{BUCKET_SECS, TipRows, tooltip_for};
use crate::ipc::{IpcCtx, MetricSeriesBucket, QueueOverview};
use crate::timeline::{bucket_label, fmt_ms, latency_y_format};

const WINDOW_SECS: i64 = 60 * 60;
const POLL_MS: u64 = 5_000;

type QueueSeries = Vec<(String, Vec<MetricSeriesBucket>)>;

#[component]
pub fn PerQueueMetrics(queues: RwSignal<Vec<QueueOverview>>) -> impl IntoView {
    let expanded = RwSignal::new(false);
    let series = RwSignal::new(QueueSeries::new());
    let load_err = RwSignal::new(Option::<String>::None);
    // Section-wide crosshair: every chart in this panel shares window +
    // bucket count, so a hovered/pinned index lines up across queues.
    let crosshair = RwSignal::new(Option::<usize>::None);
    let pinned = RwSignal::new(Option::<usize>::None);
    let ipc_ctx = expect_context::<IpcCtx>();

    let refresh = {
        let ipc_ctx = ipc_ctx.clone();
        move || {
            if !expanded.get_untracked() {
                return;
            }
            let names: Vec<String> = queues.get_untracked().into_iter().map(|q| q.name).collect();
            let to = Utc::now();
            let from = to - ChronoDuration::seconds(WINDOW_SECS);
            let ipc = ipc_ctx.clone();
            spawn_local(async move {
                let mut out = QueueSeries::with_capacity(names.len());
                for name in names {
                    match ipc.queue_metric_series(&name, from, to, BUCKET_SECS).await {
                        Ok(rows) => out.push((name, rows)),
                        Err(e) => {
                            load_err.set(Some(e.to_string()));
                            return;
                        }
                    }
                }
                series.set(out);
                load_err.set(None);
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
    // Hoisted out of the view! macro — a `::<Vec<_>>` turbofish inside
    // the macro is misparsed (the `<` reads as a tag open).
    let queue_names = Signal::derive(move || {
        queues
            .get()
            .into_iter()
            .map(|q| q.name)
            .collect::<Vec<String>>()
    });

    view! {
        <section class="queue-perqueue">
            <header class="queue-perqueue-head" on:click=toggle>
                <button class="queue-perqueue-toggle" type="button">{ chevron }</button>
                <h3>"Per-queue metrics"</h3>
                <span class="queue-metrics-sub">
                    "latency + throughput per queue · last hour · click to expand"
                </span>
            </header>

            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "Per-queue metrics: " }{ e }</div>
            }) }

            <Show when=move || expanded.get() fallback=|| ()>
                <div class="queue-perqueue-body">
                    <For
                        each=move || queue_names.get()
                        key=|name: &String| name.clone()
                        children=move |name: String| queue_row(name, series, crosshair, pinned)
                    />
                </div>
            </Show>
        </section>
    }
}

/// One queue's card: processing-latency, total-latency, and throughput
/// mini-charts. Each chart's `data` is derived from the shared `series`
/// signal by queue name, so polling updates them in place (a keyed `For`
/// alone wouldn't re-render the row when only its buckets change).
fn queue_row(
    name: String,
    series: RwSignal<QueueSeries>,
    crosshair: RwSignal<Option<usize>>,
    pinned: RwSignal<Option<usize>>,
) -> impl IntoView {
    let data = {
        let name = name.clone();
        Signal::derive(move || {
            series.with(|all| {
                all.iter()
                    .find(|(n, _)| *n == name)
                    .map(|(_, b)| b.clone())
                    .unwrap_or_default()
            })
        })
    };
    view! {
        <section class="queue-metrics-card">
            <header class="queue-metrics-card-head">
                <h3>{ name }</h3>
            </header>
            <div class="queue-perqueue-charts">
                <div>
                    <div class="queue-metrics-label">"Processing latency"</div>
                    <AreaChart
                        data=data
                        x_label=|b: &MetricSeriesBucket| bucket_label(b.at, BUCKET_SECS)
                        y_values=|b: &MetricSeriesBucket| vec![
                            b.proc_p99_ms as f64,
                            b.proc_p95_ms as f64,
                            b.proc_p50_ms as f64,
                        ]
                        series=vec![
                            Series::area("p99", "queue-p99"),
                            Series::area("p95", "queue-p95"),
                            Series::area("p50", "queue-p50"),
                        ]
                        height=180
                        legend=false
                        class="queue-perqueue-chart"
                        y_format=latency_y_format()
                        tooltip=metric_tooltip(data, proc_rows)
                        crosshair=crosshair
                        pinned=pinned
                    />
                </div>
                <div>
                    <div class="queue-metrics-label">"Total latency"</div>
                    <AreaChart
                        data=data
                        x_label=|b: &MetricSeriesBucket| bucket_label(b.at, BUCKET_SECS)
                        y_values=|b: &MetricSeriesBucket| vec![
                            b.total_p99_ms as f64,
                            b.total_p95_ms as f64,
                            b.total_p50_ms as f64,
                        ]
                        series=vec![
                            Series::area("p99", "queue-p99"),
                            Series::area("p95", "queue-p95"),
                            Series::area("p50", "queue-p50"),
                        ]
                        height=180
                        legend=false
                        class="queue-perqueue-chart"
                        y_format=latency_y_format()
                        tooltip=metric_tooltip(data, total_rows)
                        crosshair=crosshair
                        pinned=pinned
                    />
                </div>
                <div>
                    <div class="queue-metrics-label">"Throughput"</div>
                    <AreaChart
                        data=data
                        x_label=|b: &MetricSeriesBucket| bucket_label(b.at, BUCKET_SECS)
                        y_values=|b: &MetricSeriesBucket| vec![
                            b.enqueued as f64,
                            b.completed as f64,
                            b.failed as f64,
                        ]
                        series=vec![
                            Series::area("Enqueued", "queue-enqueued"),
                            Series::area("Completed", "queue-completed"),
                            Series::area("Failed", "queue-failed"),
                        ]
                        height=180
                        legend=false
                        class="queue-perqueue-chart"
                        tooltip=metric_tooltip(data, throughput_rows)
                        crosshair=crosshair
                        pinned=pinned
                    />
                </div>
            </div>
        </section>
    }
}

fn metric_tooltip(
    data: Signal<Vec<MetricSeriesBucket>>,
    rows: TipRows<MetricSeriesBucket>,
) -> TooltipSlot {
    tooltip_for(data, |b| b.at, rows)
}

fn proc_rows(b: &MetricSeriesBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![
        ("p50", "p50", fmt_ms(b.proc_p50_ms)),
        ("p95", "p95", fmt_ms(b.proc_p95_ms)),
        ("p99", "p99", fmt_ms(b.proc_p99_ms)),
    ]
}

fn total_rows(b: &MetricSeriesBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![
        ("p50", "p50", fmt_ms(b.total_p50_ms)),
        ("p95", "p95", fmt_ms(b.total_p95_ms)),
        ("p99", "p99", fmt_ms(b.total_p99_ms)),
    ]
}

fn throughput_rows(b: &MetricSeriesBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![
        ("Enqueued", "enqueued", b.enqueued.to_string()),
        ("Completed", "completed", b.completed.to_string()),
        ("Failed", "failed", b.failed.to_string()),
    ]
}
