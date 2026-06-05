# forge-charts

[![crates.io](https://img.shields.io/crates/v/forge-charts.svg)](https://crates.io/crates/forge-charts)
[![docs.rs](https://img.shields.io/docsrs/forge-charts)](https://docs.rs/forge-charts)
[![license](https://img.shields.io/crates/l/forge-charts.svg)](https://github.com/dandush03/forge#license)

Pure-Rust + SVG interactive charts for [Leptos](https://leptos.dev)
(CSR). No JS, no canvas, no Tailwind — just `leptos`, `chrono`, and
`web-sys`. The public API is project-agnostic: consumers pass
accessor closures over their own data type.

## What you get

- `AreaChart` — multi-series filled area with axes, legend, hover
  crosshair, per-series hover dots, and a consumer-rendered tooltip
  card.
- Modern visuals out of the box: gradient fills, soft axes,
  translucent rounded tooltip, light + dark mode via CSS variables.
- Clickable legend dots — native `<input type="color">` lets the
  user re-theme any series at runtime.
- Animations on mount (`scaleY` rise from baseline, staggered per
  series). Respects `prefers-reduced-motion`.

## Install

```toml
[dependencies]
forge-charts = "0.1"
leptos       = { version = "0.8", features = ["csr"] }
```

Or as a git dep during pre-publish:

```toml
[dependencies]
forge-charts = { git = "https://github.com/dandush03/forge" }
```

You also need the bundled stylesheet served alongside your bundle.
With [Trunk](https://trunkrs.dev/), add to `index.html`:

```html
<link data-trunk rel="css" href="vendor/forge-charts/charts.css" />
```

…where `vendor/forge-charts/charts.css` is a copy (or symlink)
of `crates/charts/src/charts.css`. Alternatively, the CSS is exposed
as a `&'static str` via the `CHART_CSS` constant for runtime
injection through `leptos_meta::Stylesheet`:

```rust,ignore
use leptos_meta::Stylesheet;
use forge_charts::CHART_CSS;

view! { <Stylesheet text=CHART_CSS /> }
```

## Quick start

```rust,ignore
use leptos::prelude::*;
use forge_charts::{AreaChart, Series};

#[derive(Clone)]
struct Datum { date: String, opened: u32, closed: u32 }

#[component]
fn MyChart(data: Signal<Vec<Datum>>) -> impl IntoView {
    view! {
        <AreaChart
            data=data
            x_label=|d: &Datum| d.date.clone()
            y_values=|d: &Datum| vec![f64::from(d.opened), f64::from(d.closed)]
            series=vec![
                Series::area("Opened", "opened"),
                Series::area("Closed", "closed"),
            ]
            height=320
        />
    }
}
```

### Custom tooltip

The chart hands the hovered data-point index back to a closure you
provide; render whatever you want inside the tooltip card.

```rust,ignore
use std::sync::Arc;
use leptos::prelude::*;
use leptos::tachys::view::any_view::IntoAny;
use forge_charts::{AreaChart, Series};

#[component]
fn MyChartWithTooltip(data: Signal<Vec<Datum>>) -> impl IntoView {
    let tooltip = Arc::new(move |idx: usize| {
        let row = data.with(|d| d.get(idx).cloned());
        let Some(r) = row else { return view! { <div /> }.into_any() };
        view! {
            <div class="charts-tooltip-card">
                <div class="charts-tooltip-date">{ r.date }</div>
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
            </div>
        }.into_any()
    });

    view! {
        <AreaChart
            data=data
            x_label=|d: &Datum| d.date.clone()
            y_values=|d: &Datum| vec![f64::from(d.opened), f64::from(d.closed)]
            series=vec![
                Series::area("Opened", "opened"),
                Series::area("Closed", "closed"),
            ]
            tooltip=tooltip
        />
    }
}
```

## API surface

| Prop          | Type                                          | Description                                                                                                                  |
|---------------|-----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `data`        | `Signal<Vec<T>>`                              | Data points. Order = X-axis order; no internal sorting.                                                                      |
| `x_label`     | `Fn(&T) -> String`                            | Per-point X-axis label.                                                                                                      |
| `y_values`    | `Fn(&T) -> Vec<f64>`                          | Y values per point, one per declared series (positional).                                                                    |
| `series`      | `Vec<Series>`                                 | Declares each plotted series + its CSS color hook.                                                                           |
| `height`      | `u32` (default `320`)                         | Outer container height in CSS pixels.                                                                                        |
| `legend`      | `bool` (default `true`)                       | Show the legend chip strip above the chart.                                                                                  |
| `tooltip`     | `Option<TooltipSlot>`                         | `Arc<dyn Fn(usize) -> AnyView + Send + Sync>`. When `None`, the crosshair still tracks the cursor but no tooltip card draws. |
| `class`       | `String`                                      | Extra classes on the outer `.charts-root` container.                                                                         |

`Series::area(name, color_class)` builds an area series. `name` is
the legend + tooltip label. `color_class` is the CSS-class suffix
the chart uses for its color hooks (`.charts-series-<color_class>`).

## Theming

The bundled stylesheet exposes a small set of CSS variables you can
override per consumer. Set them on `:root`, on any parent of
`.charts-root`, or via inline `style=` on the chart itself.

| Variable                                | Default (light)                | Purpose                                              |
|-----------------------------------------|--------------------------------|------------------------------------------------------|
| `--charts-fg`                           | `rgb(17 24 39)`                | Default text color inside the chart.                 |
| `--charts-fg-muted`                     | `rgb(107 114 128)`             | Axis labels.                                         |
| `--charts-fg-faint`                     | `rgb(156 163 175)`             | Crosshair color.                                     |
| `--charts-grid-color`                   | `rgba(229, 231, 235, 0.7)`     | Gridline color.                                      |
| `--charts-series-<color_class>`         | (none — set per series)        | Solid color for series stroke + tooltip dot.         |
| `--charts-series-<color_class>-soft`    | (none — set per series)        | Soft variant (≤ 0.5 alpha) for the gradient fill.    |

Default palette ships with `--charts-series-opened` (blue) and
`--charts-series-closed` (green). Add more pairs for every
`color_class` you use:

```css
:root {
  --charts-series-amber: hsl(38 92% 50%);
  --charts-series-amber-soft: hsla(38 92% 50% / 0.45);
}
```

Dark mode is automatic via `@media (prefers-color-scheme: dark)`.

### Runtime color override

Each legend dot is a clickable color picker (`<input type="color">`)
backed by an internal `RwSignal<HashMap<String, String>>`. Picking a
new color writes inline CSS variables on `.charts-root` so the chart
re-themes immediately. Choices are per-instance and **not** persisted
to disk — wire your own persistence by reading the override map from
your app's state if needed.

## License

MIT OR Apache-2.0.
