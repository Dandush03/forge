//! Standalone usage example.
//!
//! Reads as a complete consumer of `forge_charts`. Doesn't run
//! as-is — Leptos CSR needs a `trunk`/`tauri`-style entry point
//! (`mount_to_body` + `index.html` + bundler). Copy the body of
//! `App` into your own component.
//!
//! Build sanity-check (won't actually start a runtime):
//!
//! ```bash
//! cargo build -p forge-charts --example basic
//! ```

use std::sync::Arc;

use forge_charts::{AreaChart, Series};
use leptos::prelude::*;
use leptos::tachys::view::any_view::IntoAny;

/// One row of the consumer's domain data. The crate's API takes
/// accessor closures into `T`, so you keep your own types.
#[derive(Clone)]
struct Datum {
    date: String,
    opened: u32,
    closed: u32,
    avg_response_secs: u64,
}

#[component]
fn App() -> impl IntoView {
    // In a real app the data would come from a fetch / signal /
    // resource. Static here keeps the example self-contained.
    let data = RwSignal::new(vec![
        Datum {
            date: "May 17".into(),
            opened: 12,
            closed: 8,
            avg_response_secs: 420,
        },
        Datum {
            date: "May 18".into(),
            opened: 18,
            closed: 11,
            avg_response_secs: 360,
        },
        Datum {
            date: "May 19".into(),
            opened: 9,
            closed: 14,
            avg_response_secs: 600,
        },
        Datum {
            date: "May 20".into(),
            opened: 22,
            closed: 17,
            avg_response_secs: 280,
        },
        Datum {
            date: "May 21".into(),
            opened: 16,
            closed: 21,
            avg_response_secs: 410,
        },
        Datum {
            date: "May 22".into(),
            opened: 25,
            closed: 19,
            avg_response_secs: 300,
        },
        Datum {
            date: "May 23".into(),
            opened: 14,
            closed: 23,
            avg_response_secs: 540,
        },
    ]);

    // Custom tooltip — the chart hands us the hovered index; we
    // render whatever markup we want inside its translucent card.
    let tooltip = Arc::new(move |idx: usize| {
        let row = data.with(|d| d.get(idx).cloned());
        let Some(r) = row else {
            return view! { <div /> }.into_any();
        };
        view! {
            <div class="charts-tooltip-card">
                <div class="charts-tooltip-date">{ r.date.clone() }</div>
                <div class="charts-tooltip-row">
                    <span class="charts-tooltip-dot charts-series-opened"></span>
                    <span class="charts-tooltip-label">"Opened"</span>
                    <span class="charts-tooltip-value">{ r.opened }</span>
                </div>
                <div class="charts-tooltip-row">
                    <span class="charts-tooltip-dot charts-series-closed"></span>
                    <span class="charts-tooltip-label">"Closed"</span>
                    <span class="charts-tooltip-value">{ r.closed }</span>
                </div>
                <div class="charts-tooltip-row sub">
                    <span class="charts-tooltip-label">"avg response"</span>
                    <span class="charts-tooltip-value">
                        { format!("{}s", r.avg_response_secs) }
                    </span>
                </div>
            </div>
        }
        .into_any()
    });

    view! {
        <div style="max-width: 800px; margin: 32px auto;">
            <h2>"Daily ticket flow"</h2>
            <AreaChart
                data=Signal::derive(move || data.get())
                x_label=|d: &Datum| d.date.clone()
                y_values=|d: &Datum| vec![f64::from(d.opened), f64::from(d.closed)]
                series=vec![
                    Series::area("Opened", "opened"),
                    Series::area("Closed", "closed"),
                ]
                height=320
                tooltip=tooltip
            />
        </div>
    }
}

fn main() {
    // In a real Leptos CSR entry point you'd call:
    //     leptos::mount::mount_to_body(App);
    // and inject the bundled stylesheet via
    //     <link rel="stylesheet" href="/charts.css">
    // (the contents of `forge_charts::CHART_CSS`).
    //
    // For build-time validation we just instantiate the type to
    // prove the API works.
    let _: fn() -> _ = App;
}
