//! Collapsible "DB health" panel — storage-backend query latency,
//! throughput, and connection-pool saturation read from the
//! `metric_bucket` rollup.
//!
//! Answers the operator's spike-time question: "is the DB the
//! bottleneck right now — should I scale workers up or down?"
//! Diagnostic story:
//! - high throughput + low latency + moderate pool % → healthy,
//!   workers can scale up;
//! - high throughput + climbing p99 + pool % near 100 → saturated,
//!   workers should scale down;
//! - low throughput + lots of idle → headroom.
//!
//! Mirror of [`crate::resources::ResourcesPanel`]: same poll shape,
//! same crosshair model, same 4-card grid, just a different metric
//! set. CPU/RAM of the DB process aren't separable from the app
//! while SQLite is in-process; once Postgres ships, server-side
//! `pg_stat_*` will fill that gap (separate follow-up).

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;
use forge_charts::{AreaChart, Series};

use crate::chart_fmt::{BUCKET_SECS, TipRows, pct_y_format, tooltip_for};
use crate::ipc::{DbHealthBucket, DbHealthHostSeries, IpcCtx};
use crate::timeline::{bucket_label, fmt_ms, latency_y_format};

const WINDOW_SECS: i64 = 60 * 60;
const POLL_MS: u64 = 5_000;

#[component]
pub fn DbHealthPanel() -> impl IntoView {
    let expanded = RwSignal::new(false);
    let series = RwSignal::new(Vec::<DbHealthHostSeries>::new());
    let load_err = RwSignal::new(Option::<String>::None);
    let crosshair = RwSignal::new(Option::<usize>::None);
    let pinned = RwSignal::new(Option::<usize>::None);
    // The data layer keys per-pod by `host_id` (ULID, minted fresh
    // per process start). After a restart the rollup has rows under
    // both the old and new ULIDs; the aggregator's BTreeMap sorts
    // ascending so `first()` returns the *previous* run's data and
    // the chart looks dead post-restart. ULIDs are time-sortable, so
    // `last()` is "the most recent restart" — the right pick for a
    // single-process desktop app. Cluster UI will render `For` over
    // all hosts.
    let db_data = Signal::derive(move || {
        series.with(|r| r.last().map(|s| s.buckets.clone()).unwrap_or_default())
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
                match ipc.queue_db_series(from, to, BUCKET_SECS).await {
                    Ok(rows) => {
                        series.set(rows);
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
                <h3>"DB health"</h3>
                <span class="queue-metrics-sub">
                    "storage backend · query latency + pool saturation · click to expand"
                </span>
            </header>

            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "DB health: " }{ e }</div>
            }) }

            <Show when=move || expanded.get() fallback=|| ()>
                <div class="queue-perqueue-body queue-section-grid queue-section-grid-4">
                    <section class="queue-metrics-card">
                        <div class="queue-metrics-label">"Write latency"</div>
                        <AreaChart
                            data=db_data
                            x_label=|b: &DbHealthBucket| bucket_label(b.at, BUCKET_SECS)
                            y_values=|b: &DbHealthBucket| vec![
                                b.write_p99_ms as f64,
                                b.write_p95_ms as f64,
                                b.write_p50_ms as f64,
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
                            tooltip=db_tooltip(db_data, write_latency_rows)
                            crosshair=crosshair
                            pinned=pinned
                        />
                    </section>
                    <section class="queue-metrics-card">
                        <div class="queue-metrics-label">"Throughput (ops/min)"</div>
                        <AreaChart
                            data=db_data
                            x_label=|b: &DbHealthBucket| bucket_label(b.at, BUCKET_SECS)
                            y_values=|b: &DbHealthBucket| vec![
                                b.writes_per_min as f64,
                                b.reads_per_min as f64,
                            ]
                            series=vec![
                                Series::area("writes", "queue-failed"),
                                Series::area("reads", "queue-enqueued"),
                            ]
                            height=180
                            class="queue-perqueue-chart"
                            tooltip=db_tooltip(db_data, throughput_rows)
                            crosshair=crosshair
                            pinned=pinned
                        />
                    </section>
                    // Pool sub-charts are essential on Postgres (server-
                    // side pool model is real). On SQLite the lines stay
                    // flat at zero — `SQLite` has no server-side pool
                    // to read; that's the honest answer for an embedded
                    // backend, not a missing chart.
                    <section class="queue-metrics-card">
                        <div class="queue-metrics-label">"Pool saturation"</div>
                        <AreaChart
                            data=db_data
                            x_label=|b: &DbHealthBucket| bucket_label(b.at, BUCKET_SECS)
                            y_values=|b: &DbHealthBucket| vec![b.pool_used_pct]
                            series=vec![Series::area("Used %", "queue-failed")]
                            height=180
                            legend=false
                            class="queue-perqueue-chart"
                            y_format=pct_y_format()
                            tooltip=db_tooltip(db_data, pool_used_rows)
                            crosshair=crosshair
                            pinned=pinned
                        />
                    </section>
                    <section class="queue-metrics-card">
                        <div class="queue-metrics-label">"Pool connections"</div>
                        <AreaChart
                            data=db_data
                            x_label=|b: &DbHealthBucket| bucket_label(b.at, BUCKET_SECS)
                            y_values=|b: &DbHealthBucket| vec![
                                b.pool_active as f64,
                                b.pool_idle as f64,
                            ]
                            series=vec![
                                Series::area("In use", "queue-failed"),
                                Series::area("Idle", "queue-completed"),
                            ]
                            height=180
                            class="queue-perqueue-chart"
                            tooltip=db_tooltip(db_data, pool_conn_rows)
                            crosshair=crosshair
                            pinned=pinned
                        />
                    </section>
                </div>
            </Show>
        </section>
    }
}

fn db_tooltip(
    data: Signal<Vec<DbHealthBucket>>,
    rows: TipRows<DbHealthBucket>,
) -> forge_charts::TooltipSlot {
    tooltip_for(data, |b| b.at, rows)
}

fn write_latency_rows(b: &DbHealthBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![
        ("p50", "p50", fmt_ms(b.write_p50_ms)),
        ("p95", "p95", fmt_ms(b.write_p95_ms)),
        ("p99", "p99", fmt_ms(b.write_p99_ms)),
    ]
}

fn throughput_rows(b: &DbHealthBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![
        ("reads", "enqueued", b.reads_per_min.to_string()),
        ("writes", "failed", b.writes_per_min.to_string()),
    ]
}

fn pool_used_rows(b: &DbHealthBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![("Used", "failed", format!("{:.1}%", b.pool_used_pct))]
}

fn pool_conn_rows(b: &DbHealthBucket) -> Vec<(&'static str, &'static str, String)> {
    vec![
        ("In use", "failed", b.pool_active.to_string()),
        ("Idle", "completed", b.pool_idle.to_string()),
        ("Max", "p50", b.pool_max.to_string()),
    ]
}
