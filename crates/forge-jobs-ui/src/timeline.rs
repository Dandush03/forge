//! Sidekiq-style timeline at the top of the panel.
//!
//! Two interaction surfaces drive the view:
//!
//! 1. **Preset buttons** (5m / 30m / 1h / 3h / 6h / 24h / 7d) pick a
//!    rolling time span. Polling re-fetches the same span every
//!    [`POLL_INTERVAL_MS`] so the right edge tracks `now`.
//! 2. **Drag-to-zoom** on the chart pins the view to a sub-range of
//!    the currently displayed buckets. The narrower span gets a finer
//!    bucket size from [`pick_bucket_secs`] — so as you drill in,
//!    x-axis ticks adapt all the way down to per-second.
//!
//! State split:
//!
//! - [`ViewSpec`] is the user's *intent* (which preset, or which
//!   pinned zoom). Only the Effect reads this, and only user input
//!   writes to it — that's the loop-free contract.
//! - `displayed_bucket_secs` + `buckets` are the *result* of the
//!   last fetch. Polling writes them; the chart reads them. The
//!   Effect never reads them, so the 5-second poll cannot retrigger
//!   itself.
//!
//! Polling and zoom compose: while pinned, the same poll cadence
//! re-fetches the (frozen) zoom window so any new buckets that land
//! inside it keep stacking in.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Local, Utc};
use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::tachys::view::any_view::IntoAny;
use leptos::task::spawn_local;
use forge_charts::{AreaChart, Series, TooltipSlot, YFormat, ZoomCommit};

use crate::ipc::{IpcCtx, TimelineBucket};

const POLL_INTERVAL_MS: u64 = 5_000;

/// User-pickable time spans for the timeline (left → right in the
/// header). Ordered shortest-first so the natural reading flow matches
/// the zoom-in direction.
const PRESETS: &[Preset] = &[
    Preset::new("5m", 5 * 60),
    Preset::new("30m", 30 * 60),
    Preset::new("1h", 60 * 60),
    Preset::new("3h", 3 * 60 * 60),
    Preset::new("6h", 6 * 60 * 60),
    Preset::new("24h", 24 * 60 * 60),
    Preset::new("7d", 7 * 24 * 60 * 60),
];

/// Default preset on first mount. 1h gives enough recency for the
/// "what's happening right now" use-case without being so short that
/// a quiet queue looks empty.
const DEFAULT_PRESET_IDX: usize = 2;

#[derive(Clone, Copy)]
struct Preset {
    label: &'static str,
    secs: u32,
}

impl Preset {
    const fn new(label: &'static str, secs: u32) -> Self {
        Self { label, secs }
    }
}

/// User intent that drives the next fetch. Kept deliberately small
/// so the Effect's dependency set is small — see the module docs for
/// why this is split out from the fetched data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ViewSpec {
    /// Rolling window of the given preset. `to = now` is computed at
    /// fetch time, not stored.
    Preset(usize),
    /// Pinned wall-clock window from a drag-to-zoom commit. Fetch
    /// uses these endpoints verbatim — polling still re-fetches at
    /// the same cadence so live activity inside the window updates.
    Pinned {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        bucket_secs: u32,
    },
}

#[component]
pub fn Timeline() -> impl IntoView {
    let spec = RwSignal::new(ViewSpec::Preset(DEFAULT_PRESET_IDX));
    let buckets = RwSignal::new(Vec::<TimelineBucket>::new());
    let displayed_bucket_secs = RwSignal::new({
        let p = PRESETS[DEFAULT_PRESET_IDX];
        pick_bucket_secs(ChronoDuration::seconds(i64::from(p.secs)))
    });
    let load_err = RwSignal::new(Option::<String>::None);
    // Shared crosshair across all three charts: hovering any one draws
    // the line on all (same bucket index → same instant), and a click
    // pins it so you can read the same moment across the count + both
    // latency charts. All three share `buckets`, so indices line up.
    let crosshair = RwSignal::new(Option::<usize>::None);
    let pinned = RwSignal::new(Option::<usize>::None);
    let ipc_ctx = expect_context::<IpcCtx>();

    let refresh = {
        let ipc_ctx = ipc_ctx.clone();
        move || {
            // Resolve the spec into a concrete (from, to, bucket_secs)
            // tuple. Crucially, this never writes back to `spec` —
            // that's what keeps the polling Effect from looping.
            let (from, to, bucket_secs) = match spec.get_untracked() {
                ViewSpec::Preset(idx) => {
                    let p = PRESETS[idx];
                    let to = Utc::now();
                    let from = to - ChronoDuration::seconds(i64::from(p.secs));
                    let bs = pick_bucket_secs(to - from);
                    (from, to, bs)
                }
                ViewSpec::Pinned {
                    from,
                    to,
                    bucket_secs,
                } => (from, to, bucket_secs),
            };
            displayed_bucket_secs.set(bucket_secs);
            let ipc = ipc_ctx.clone();
            spawn_local(async move {
                match ipc.queue_timeline_range(from, to, bucket_secs).await {
                    Ok(rows) => {
                        buckets.set(rows);
                        load_err.set(None);
                    }
                    Err(e) => load_err.set(Some(e.to_string())),
                }
            });
        }
    };

    // Effect fires immediately on mount AND every time the *spec*
    // changes (preset pick, zoom commit, zoom reset). It does **not**
    // depend on `displayed_bucket_secs` or `buckets`, so the fetch's
    // own writes cannot retrigger it.
    let refresh_for_effect = refresh.clone();
    Effect::new(move |_| {
        let _ = spec.get();
        refresh_for_effect();
    });

    let handle = set_interval_with_handle(refresh, Duration::from_millis(POLL_INTERVAL_MS)).ok();
    on_cleanup(move || {
        if let Some(h) = handle {
            h.clear();
        }
    });

    let totals = Memo::new(move |_| {
        buckets.with(|bs| {
            let enq: u64 = bs.iter().map(|b| b.enqueued).sum();
            let ret: u64 = bs.iter().map(|b| b.retried).sum();
            let don: u64 = bs.iter().map(|b| b.completed).sum();
            let fld: u64 = bs.iter().map(|b| b.failed).sum();
            (enq, ret, don, fld)
        })
    });
    let bucket_size_label = Memo::new(move |_| fmt_bucket_size(displayed_bucket_secs.get()));

    let active_preset_idx = Memo::new(move |_| match spec.get() {
        ViewSpec::Preset(idx) => Some(idx),
        ViewSpec::Pinned { .. } => None,
    });
    let zoomed = Memo::new(move |_| matches!(spec.get(), ViewSpec::Pinned { .. }));

    // Changing the window remaps every bucket index, so a parked pin (or
    // a stale hover) would point at the wrong instant — clear both.
    let clear_markers = move || {
        crosshair.set(None);
        pinned.set(None);
    };
    let pick_preset = move |idx: usize| {
        clear_markers();
        spec.set(ViewSpec::Preset(idx));
    };

    let on_reset_zoom = move |_| {
        // Falling back to the default preset when the user clears a
        // zoom is a deliberate UX choice: any preset they had picked
        // before the zoom is no longer obviously the "right" one.
        clear_markers();
        spec.set(ViewSpec::Preset(DEFAULT_PRESET_IDX));
    };

    let tooltip = Arc::new(move |idx: usize| {
        let (bucket, bs) = buckets.with(|bs| (bs.get(idx).cloned(), displayed_bucket_secs.get()));
        let Some(b) = bucket else {
            return view! { <div></div> }.into_any();
        };
        view! {
            <div class="charts-tooltip-card">
                <div class="charts-tooltip-date">{ bucket_label(b.at, bs) }</div>
                <div class="charts-tooltip-row">
                    <span class="charts-tooltip-dot queue-series-enqueued"></span>
                    <span class="charts-tooltip-label">"Enqueued"</span>
                    <span class="charts-tooltip-value">{ b.enqueued }</span>
                </div>
                <div class="charts-tooltip-row">
                    <span class="charts-tooltip-dot queue-series-retried"></span>
                    <span class="charts-tooltip-label">"Retried"</span>
                    <span class="charts-tooltip-value">{ b.retried }</span>
                </div>
                <div class="charts-tooltip-row">
                    <span class="charts-tooltip-dot queue-series-completed"></span>
                    <span class="charts-tooltip-label">"Completed"</span>
                    <span class="charts-tooltip-value">{ b.completed }</span>
                </div>
                <div class="charts-tooltip-row">
                    <span class="charts-tooltip-dot queue-series-failed"></span>
                    <span class="charts-tooltip-label">"Failed"</span>
                    <span class="charts-tooltip-value">{ b.failed }</span>
                </div>
            </div>
        }
        .into_any()
    });

    // Latency tooltips reuse the same bucket lookup; they read the
    // percentile fields instead of the event counts.
    let processing_tooltip: TooltipSlot = Arc::new(move |idx: usize| {
        let (bucket, bs) = buckets.with(|v| (v.get(idx).cloned(), displayed_bucket_secs.get()));
        bucket.map_or_else(
            || view! { <div></div> }.into_any(),
            |b| {
                latency_tooltip_card(
                    b.at,
                    bs,
                    b.processing_p50_ms,
                    b.processing_p95_ms,
                    b.processing_p99_ms,
                )
            },
        )
    });
    let total_tooltip: TooltipSlot = Arc::new(move |idx: usize| {
        let (bucket, bs) = buckets.with(|v| (v.get(idx).cloned(), displayed_bucket_secs.get()));
        bucket.map_or_else(
            || view! { <div></div> }.into_any(),
            |b| latency_tooltip_card(b.at, bs, b.total_p50_ms, b.total_p95_ms, b.total_p99_ms),
        )
    });

    let on_zoom: ZoomCommit = Arc::new(move |from_idx: usize, to_idx: usize| {
        let bs = buckets.get_untracked();
        let cur_bucket_secs = displayed_bucket_secs.get_untracked();
        let Some(start) = bs.get(from_idx) else {
            return;
        };
        let Some(end) = bs.get(to_idx) else {
            return;
        };
        // The selected end-bucket spans `bucket_secs` after its `at`
        // anchor — include the full bucket so the user gets the
        // count they saw rather than a half-open window that drops
        // the last data point.
        let new_from = start.at;
        let new_to = end.at + ChronoDuration::seconds(i64::from(cur_bucket_secs));
        let new_bucket = pick_bucket_secs(new_to - new_from);
        spec.set(ViewSpec::Pinned {
            from: new_from,
            to: new_to,
            bucket_secs: new_bucket,
        });
    });

    view! {
        <section class="queue-timeline">
            <header class="queue-timeline-head">
                <h3>"Timeline"</h3>
                <div class="queue-timeline-totals">
                    { move || {
                        let (enq, retried, done, failed) = totals.get();
                        let bs = bucket_size_label.get();
                        format!("· {enq} enqueued · {retried} retried · {done} completed · {failed} failed · {bs}/bucket")
                    } }
                </div>
                <div class="queue-timeline-window">
                    { PRESETS.iter().enumerate().map(|(idx, p)| {
                        let label = p.label;
                        view! {
                            <button
                                class="queue-window-btn"
                                class:active=move || active_preset_idx.get() == Some(idx)
                                on:click=move |_| pick_preset(idx)
                            >{ label }</button>
                        }
                    }).collect_view() }
                    { move || zoomed.get().then(|| view! {
                        <button
                            class="queue-window-btn"
                            on:click=on_reset_zoom
                            title="Clear zoom and return to the default preset"
                        >"Reset"</button>
                    }) }
                </div>
            </header>

            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "Timeline load failed: " }{ e }</div>
            }) }

            <div class="queue-timeline-chart">
                <AreaChart
                    data=Signal::derive(move || buckets.get())
                    x_label=move |b: &TimelineBucket| bucket_label(b.at, displayed_bucket_secs.get())
                    y_values=|b: &TimelineBucket| vec![
                        b.enqueued as f64,
                        b.retried as f64,
                        b.completed as f64,
                        b.failed as f64,
                    ]
                    series=vec![
                        Series::area("Enqueued", "queue-enqueued"),
                        Series::area("Retried", "queue-retried"),
                        Series::area("Completed", "queue-completed"),
                        Series::area("Failed", "queue-failed"),
                    ]
                    height=240
                    tooltip=tooltip
                    on_zoom=on_zoom.clone()
                    crosshair=crosshair
                    pinned=pinned
                />
            </div>
        </section>

        <div class="queue-metrics-row">
            <section class="queue-metrics-card">
                <header class="queue-metrics-card-head">
                    <h3>"Processing latency"</h3>
                    <span class="queue-metrics-sub">"claim → finalize · p50 / p95 / p99"</span>
                </header>
                <AreaChart
                    data=Signal::derive(move || buckets.get())
                    x_label=move |b: &TimelineBucket| bucket_label(b.at, displayed_bucket_secs.get())
                    y_values=|b: &TimelineBucket| vec![
                        b.processing_p99_ms as f64,
                        b.processing_p95_ms as f64,
                        b.processing_p50_ms as f64,
                    ]
                    series=vec![
                        Series::area("p99", "queue-p99"),
                        Series::area("p95", "queue-p95"),
                        Series::area("p50", "queue-p50"),
                    ]
                    height=220
                    class="queue-latency-chart"
                    tooltip=processing_tooltip
                    on_zoom=on_zoom.clone()
                    y_format=latency_y_format()
                    crosshair=crosshair
                    pinned=pinned
                />
            </section>
            <section class="queue-metrics-card">
                <header class="queue-metrics-card-head">
                    <h3>"Total latency"</h3>
                    <span class="queue-metrics-sub">"enqueue → finalize · p50 / p95 / p99"</span>
                </header>
                <AreaChart
                    data=Signal::derive(move || buckets.get())
                    x_label=move |b: &TimelineBucket| bucket_label(b.at, displayed_bucket_secs.get())
                    y_values=|b: &TimelineBucket| vec![
                        b.total_p99_ms as f64,
                        b.total_p95_ms as f64,
                        b.total_p50_ms as f64,
                    ]
                    series=vec![
                        Series::area("p99", "queue-p99"),
                        Series::area("p95", "queue-p95"),
                        Series::area("p50", "queue-p50"),
                    ]
                    height=220
                    class="queue-latency-chart"
                    tooltip=total_tooltip
                    on_zoom=on_zoom
                    y_format=latency_y_format()
                    crosshair=crosshair
                    pinned=pinned
                />
            </section>
        </div>
    }
}

/// Tooltip card body for a latency chart: bucket time plus the three
/// percentile rows, each colored to match its series. Shared by the
/// processing and total charts — only the values differ.
fn latency_tooltip_card(
    at: DateTime<Utc>,
    bucket_secs: u32,
    p50: u64,
    p95: u64,
    p99: u64,
) -> leptos::prelude::AnyView {
    view! {
        <div class="charts-tooltip-card">
            <div class="charts-tooltip-date">{ bucket_label(at, bucket_secs) }</div>
            <div class="charts-tooltip-row">
                <span class="charts-tooltip-dot queue-series-p50"></span>
                <span class="charts-tooltip-label">"p50"</span>
                <span class="charts-tooltip-value">{ fmt_ms(p50) }</span>
            </div>
            <div class="charts-tooltip-row">
                <span class="charts-tooltip-dot queue-series-p95"></span>
                <span class="charts-tooltip-label">"p95"</span>
                <span class="charts-tooltip-value">{ fmt_ms(p95) }</span>
            </div>
            <div class="charts-tooltip-row">
                <span class="charts-tooltip-dot queue-series-p99"></span>
                <span class="charts-tooltip-label">"p99"</span>
                <span class="charts-tooltip-value">{ fmt_ms(p99) }</span>
            </div>
        </div>
    }
    .into_any()
}

/// Y-axis formatter for the latency charts: same humanized duration as
/// the tooltip, so a raw `6000000` ms tick reads as `1h 40m`. Built
/// fresh per chart (the chart wants an owned `Arc`).
pub(crate) fn latency_y_format() -> YFormat {
    Arc::new(fmt_latency_axis)
}

/// Format a y-axis tick (milliseconds, as `f64`) as a duration. Tick
/// values from `nice_y_ticks` are non-negative and small enough to fit
/// `u64` exactly, so the rounding cast is safe for display.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "y ticks are non-negative ms well within u64; rounded only for the label"
)]
fn fmt_latency_axis(v: f64) -> String {
    fmt_ms(v.max(0.0).round() as u64)
}

/// Human-readable latency for the hover tooltip: the two most-
/// significant non-zero units, largest first (e.g. `2h 46m`, `1s
/// 500ms`, `850ms`). A flat `10000s` is unreadable; this caps at two
/// units so the magnitude reads at a glance. Integer-only math, so no
/// float-cast lints and the value stays exact. Months are the 30-day
/// approximation — fine for a latency readout, not for calendars.
pub(crate) fn fmt_ms(ms: u64) -> String {
    const UNITS: &[(u64, &str)] = &[
        (30 * 24 * 60 * 60 * 1000, "mo"),
        (7 * 24 * 60 * 60 * 1000, "w"),
        (24 * 60 * 60 * 1000, "d"),
        (60 * 60 * 1000, "h"),
        (60 * 1000, "m"),
        (1000, "s"),
        (1, "ms"),
    ];
    if ms == 0 {
        return "0ms".to_owned();
    }
    let mut rem = ms;
    let mut parts: Vec<String> = Vec::with_capacity(2);
    for &(size, suffix) in UNITS {
        if rem >= size {
            parts.push(format!("{}{suffix}", rem / size));
            rem %= size;
            if parts.len() == 2 {
                break;
            }
        }
    }
    parts.join(" ")
}

/// Choose a bucket size that yields ~30–100 buckets across `span`.
/// Picks the smallest step from a fixed ladder that keeps the bucket
/// count at or below 100, so the chart stays readable even on a
/// 1280px-wide display where every bucket needs ~10px to be hoverable.
///
/// The ladder is deliberately sparse: human-friendly intervals
/// (1s/5s/15s/30s/1m/...). Falling back to 1d on absurdly long spans
/// caps the worst case.
fn pick_bucket_secs(span: ChronoDuration) -> u32 {
    const LADDER: &[u32] = &[
        1,
        5,
        15,
        30,
        60,
        5 * 60,
        15 * 60,
        30 * 60,
        60 * 60,
        2 * 60 * 60,
        6 * 60 * 60,
        12 * 60 * 60,
        24 * 60 * 60,
    ];
    let target = 100_u64;
    let total = u64::try_from(span.num_seconds().max(0)).unwrap_or(0);
    LADDER
        .iter()
        .copied()
        .find(|&b| total / u64::from(b) <= target)
        .unwrap_or_else(|| LADDER.last().copied().unwrap_or(3600))
}

/// Human-readable bucket size, shown in the header so the user knows
/// the resolution the chart is rendering at. `60` → `"1m"`, `3600` →
/// `"1h"`, etc. Falls back to raw seconds for odd values.
fn fmt_bucket_size(secs: u32) -> String {
    const DAY: u32 = 24 * 60 * 60;
    if secs >= DAY && secs.is_multiple_of(DAY) {
        format!("{}d", secs / DAY)
    } else if secs >= 3600 && secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs >= 60 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// X-axis (and tooltip) label for one bucket. The format steps up
/// from full date to wall-clock seconds based on the current bucket
/// granularity — so a 24h view shows `HH:MM` while a per-second zoom
/// shows `HH:MM:SS`.
pub(crate) fn bucket_label(at: DateTime<Utc>, bucket_secs: u32) -> String {
    let local = at.with_timezone(&Local);
    if bucket_secs >= 24 * 60 * 60 {
        local.format("%a %-d").to_string()
    } else if bucket_secs >= 60 {
        local.format("%H:%M").to_string()
    } else {
        local.format("%H:%M:%S").to_string()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        reason = "panicking helpers are fine in unit tests; failure is what we want"
    )]
    use super::*;

    #[test]
    fn pick_bucket_secs_for_5min_keeps_count_under_target() {
        let secs = pick_bucket_secs(ChronoDuration::minutes(5));
        let count = 5 * 60 / secs;
        assert!(count <= 100, "5-min span at {secs}s → {count} buckets");
        // 5 minutes / 5-second buckets = 60 — should land on 5s.
        assert_eq!(secs, 5, "5-min span should pick 5-second buckets");
    }

    #[test]
    fn pick_bucket_secs_for_1h_picks_minute_buckets() {
        let secs = pick_bucket_secs(ChronoDuration::hours(1));
        // 3600 / 60 = 60 buckets — minute granularity is the sweet spot.
        assert_eq!(secs, 60);
    }

    #[test]
    fn pick_bucket_secs_for_24h_picks_under_100_buckets() {
        let secs = pick_bucket_secs(ChronoDuration::hours(24));
        let count = 24 * 60 * 60 / secs;
        assert!(count <= 100, "24h at {secs}s → {count} buckets");
    }

    #[test]
    fn pick_bucket_secs_for_7d_picks_hour_or_bigger() {
        let secs = pick_bucket_secs(ChronoDuration::days(7));
        assert!(secs >= 60 * 60, "7d should land on hour-or-bigger buckets");
    }

    #[test]
    fn pick_bucket_secs_for_tiny_span_picks_one_second() {
        // 30-second span: ladder's first rung (1s) gives 30 buckets,
        // well under the 100 target.
        let secs = pick_bucket_secs(ChronoDuration::seconds(30));
        assert_eq!(secs, 1);
    }

    #[test]
    fn fmt_bucket_size_picks_largest_round_unit() {
        assert_eq!(fmt_bucket_size(1), "1s");
        assert_eq!(fmt_bucket_size(45), "45s");
        assert_eq!(fmt_bucket_size(60), "1m");
        assert_eq!(fmt_bucket_size(5 * 60), "5m");
        assert_eq!(fmt_bucket_size(60 * 60), "1h");
        assert_eq!(fmt_bucket_size(2 * 60 * 60), "2h");
        assert_eq!(fmt_bucket_size(24 * 60 * 60), "1d");
        assert_eq!(fmt_bucket_size(3 * 24 * 60 * 60), "3d");
        // Non-round-minute count falls through to seconds.
        assert_eq!(fmt_bucket_size(90), "90s");
    }

    #[test]
    fn fmt_ms_breaks_into_two_significant_units() {
        assert_eq!(fmt_ms(0), "0ms");
        assert_eq!(fmt_ms(42), "42ms");
        assert_eq!(fmt_ms(999), "999ms");
        assert_eq!(fmt_ms(1000), "1s");
        assert_eq!(fmt_ms(1234), "1s 234ms");
        assert_eq!(fmt_ms(30_500), "30s 500ms");
        assert_eq!(fmt_ms(90_000), "1m 30s");
        // 10,000s — the case that motivated this: 2h 46m, not "10000s".
        assert_eq!(fmt_ms(10_000_000), "2h 46m");
        // Non-adjacent units: 1h 0m 5s keeps the two non-zero ones.
        assert_eq!(fmt_ms(3_605_000), "1h 5s");
        // Days and weeks ladder up correctly.
        assert_eq!(
            fmt_ms(2 * 24 * 60 * 60 * 1000 + 3 * 60 * 60 * 1000),
            "2d 3h"
        );
        assert_eq!(fmt_ms(8 * 24 * 60 * 60 * 1000), "1w 1d");
    }

    #[test]
    fn bucket_label_format_steps_up_with_granularity() {
        let at = DateTime::parse_from_rfc3339("2024-01-15T14:32:07Z")
            .unwrap()
            .with_timezone(&Utc);
        // Second-granularity should include seconds.
        let s = bucket_label(at, 5);
        assert!(s.matches(':').count() >= 2, "got {s}");
        // Minute-granularity drops the seconds.
        let m = bucket_label(at, 60);
        assert_eq!(m.matches(':').count(), 1, "got {m}");
        // Day-granularity is a weekday + day-of-month.
        let d = bucket_label(at, 24 * 60 * 60);
        assert!(!d.contains(':'), "got {d}");
    }
}
