//! The `AreaChart` Leptos component.
//!
//! Renders an SVG with one or more filled-area series, axes, legend,
//! a crosshair that tracks the cursor, per-series hover dots, a
//! consumer-provided tooltip card, and drag-to-zoom with a reset pill.
//!
//! v0.3 ships drag-to-zoom (Phase C) on top of the v0.2 hover stack.

use leptos::ev::MouseEvent;
use leptos::html::Div;
use leptos::leptos_dom::helpers::window_event_listener;
use leptos::prelude::*;
use leptos::tachys::view::any_view::AnyView;
use std::sync::Arc;

use wasm_bindgen::JsCast;

use crate::axis::{nice_y_ticks_capped, pad_y_max, select_x_indices};
use crate::hover::{HoverState, pixel_to_index};
use crate::path::{area_path, line_path};
use crate::scale::{index_to_x, value_to_y};
use crate::series::Series;
use crate::zoom::{ZoomRange, commit_drag};

/// Internal SVG dimensions (viewBox). The actual rendered size is
/// driven by the CSS `.charts-svg { width: 100%; height: …px; }`
/// rule; SVG stretches via `preserveAspectRatio`.
const VIEW_W: f64 = 800.0;
const VIEW_H: f64 = 280.0;
/// Max number of X-axis labels rendered before [`select_x_indices`]
/// thins them.
const MAX_X_LABELS: usize = 7;
/// At which fraction of the chart width the tooltip flips to the
/// left of the crosshair instead of the right.
const TOOLTIP_FLIP_FRACTION: f64 = 0.65;

/// Boxed tooltip slot. Receives the hovered data-point index and
/// returns whatever Leptos view the consumer wants to render.
pub type TooltipSlot = Arc<dyn Fn(usize) -> AnyView + Send + Sync>;

/// Callback fired when the user commits a drag-to-zoom. Receives the
/// inclusive **original-index** bounds of the selected range (already
/// composed with any prior internal slice, if the chart is uncontrolled).
///
/// When this callback is set, the chart switches to **controlled-zoom**
/// mode: it does *not* apply the slice internally. The consumer is
/// expected to react — typically by refetching a narrower window of
/// data and pushing it into the chart's `data` signal — and the chart
/// just redraws with whatever shape the consumer hands back.
pub type ZoomCommit = Arc<dyn Fn(usize, usize) + Send + Sync>;

/// Formats a y-axis tick value into its display label. When unset the
/// chart renders a plain number. Provide one to label the axis in
/// real units — durations, bytes, percentages — so a raw `6000000`
/// reads as `1h 40m` instead.
pub type YFormat = Arc<dyn Fn(f64) -> String + Send + Sync>;

/// Interactive area chart component.
#[component]
pub fn AreaChart<T, FxLabel, FyValues>(
    /// The data series. Each element becomes one X tick. Order is
    /// preserved (no internal sorting).
    #[prop(into)]
    data: Signal<Vec<T>>,
    /// X-axis label for one data point.
    x_label: FxLabel,
    /// Y values for one data point. Must return one value per
    /// declared series, in the same order as the `series` prop.
    y_values: FyValues,
    /// Series metadata. Length determines the number of plotted
    /// areas; CSS hooks come from each `Series::color_class`.
    series: Vec<Series>,
    /// Outer container height in CSS pixels. Defaults to 320.
    #[prop(default = 320)]
    height: u32,
    /// Show a legend chip strip above the chart. Defaults to true.
    #[prop(default = true)]
    legend: bool,
    /// Custom tooltip slot — receives the hovered point index and
    /// returns the markup to render inside the tooltip card. When
    /// `None`, no tooltip appears (the crosshair still draws).
    #[prop(optional)]
    tooltip: Option<TooltipSlot>,
    /// Zoom-commit callback. When set, drag-to-zoom switches to
    /// controlled mode: the chart fires the callback with the committed
    /// inclusive index range and does *not* slice its data internally
    /// — the consumer reacts by updating the `data` signal.
    #[prop(optional)]
    on_zoom: Option<ZoomCommit>,
    /// Extra classes applied to the outer `.charts-root` container.
    #[prop(default = String::new(), into)]
    class: String,
    /// Custom y-axis tick formatter. When `None`, ticks render as plain
    /// numbers. Provide one to label the axis in real units (see
    /// [`YFormat`]).
    #[prop(optional)]
    y_format: Option<YFormat>,
    /// Shared live-hover index. When several charts are handed the *same*
    /// signal, hovering one moves the crosshair on all of them — they
    /// must share an identical x-domain (same point count + window).
    /// `None` keeps a private hover, matching standalone behavior.
    #[prop(optional)]
    crosshair: Option<RwSignal<Option<usize>>>,
    /// Shared pinned index. When a signal is provided, a plain click in
    /// the plot toggles a crosshair that persists after the cursor
    /// leaves, so sibling charts keep showing the same point. `None`
    /// disables click-to-pin (the standalone default).
    #[prop(optional)]
    pinned: Option<RwSignal<Option<usize>>>,
) -> impl IntoView
where
    T: Clone + Send + Sync + 'static,
    FxLabel: Fn(&T) -> String + Send + Sync + 'static + Copy,
    FyValues: Fn(&T) -> Vec<f64> + Send + Sync + 'static + Copy,
{
    let series_for_model = series.clone();
    let render = Memo::new(move |_| {
        data.with(|rows| build_model(rows, x_label, y_values, &series_for_model))
    });

    let hover = RwSignal::new(Option::<HoverState>::None);
    // Live-hover index drives the crosshair + dots. Shared across
    // sibling charts when the consumer passes a `crosshair` signal;
    // otherwise private, so a standalone chart behaves exactly as before.
    let live_idx = crosshair.unwrap_or_else(|| RwSignal::new(None));
    let pinned_idx = pinned;
    // Click outside any chart's plot area clears the pinned crosshair.
    // Every chart sharing the pin installs this; they all test "inside
    // *any* chart", so clicking one chart keeps the pin while a click on
    // empty space dismisses it.
    if let Some(p) = pinned_idx {
        let click_handle =
            window_event_listener(leptos::ev::mousedown, move |ev: web_sys::MouseEvent| {
                if !event_in_plot_area(&ev) {
                    p.set(None);
                }
            });
        // Esc also dismisses the pin — keyboard parity with click-outside.
        let key_handle =
            window_event_listener(leptos::ev::keydown, move |ev: web_sys::KeyboardEvent| {
                if ev.key() == "Escape" {
                    p.set(None);
                }
            });
        on_cleanup(move || {
            click_handle.remove();
            key_handle.remove();
        });
    }
    let plot_ref: NodeRef<Div> = NodeRef::new();

    // Phase C — drag-to-zoom state. `zoom` is a committed range in
    // *original* (unsliced) index space; `drag_start`/`drag_end` are
    // display-space indices captured during an in-progress drag. We
    // reset all three together when the user clears the zoom.
    let zoom: RwSignal<Option<ZoomRange>> = RwSignal::new(None);
    let drag_start: RwSignal<Option<usize>> = RwSignal::new(None);
    let drag_end: RwSignal<Option<usize>> = RwSignal::new(None);

    // Color overrides: color_class -> hex. Driven by the legend's
    // hidden <input type="color"> per dot. When set, we emit inline
    // CSS variables on .charts-root that override the bundled
    // stylesheet's per-series colors.
    let overrides: RwSignal<std::collections::HashMap<String, String>> =
        RwSignal::new(std::collections::HashMap::new());

    // Series hidden via a legend-name click (keyed by color_class).
    // Filtered out of the render below; the y-axis rescales to whatever
    // stays visible.
    let hidden: RwSignal<std::collections::HashSet<String>> =
        RwSignal::new(std::collections::HashSet::new());

    let outer_class = format!("charts-root {class}");
    // The inline style carries both the height var and any color
    // overrides set via the legend picker. Reactive — recomputes on
    // every `overrides` change.
    let style_attr = move || {
        let mut s = format!("--charts-height: {height}px;");
        overrides.with(|map| {
            for (color_class, hex) in map {
                // Defense-in-depth: write-site validation in the
                // legend picker rejects non-hex input, but
                // re-validate here so a future bug bypassing the
                // write path can't inject CSS via the read path.
                if !is_hex_color(hex) {
                    continue;
                }
                // Set both the solid stroke color and a soft variant
                // (50% alpha) for the gradient fill. Hex stays the
                // truthful source; CSS color-mix isn't broadly enough
                // supported in older browsers so we hand-derive.
                let soft = soften_hex(hex);
                s.push_str(&format!(" --charts-series-{color_class}: {hex};"));
                s.push_str(&format!(" --charts-series-{color_class}-soft: {soft};"));
            }
        });
        s
    };

    view! {
        <div class=outer_class style=style_attr>
            { (legend).then(|| view! { <Legend series=series.clone() overrides=overrides hidden=hidden /> }) }
            { move || {
                let m = filter_hidden(render.get(), &hidden.get());
                if m.empty {
                    return view! {
                        <div class="charts-empty">"No data in window."</div>
                    }.into_any();
                }
                // Apply any committed zoom by slicing the model in
                // original-index space. The display body then operates
                // on a tighter window with its own y_max so the zoom
                // rescales both axes (Apex behaviour).
                let z = zoom.get();
                let displayed = slice_model(&m, z);
                let from_offset = z.map_or(0, |r| r.from_index);
                view_chart_body(
                    displayed,
                    from_offset,
                    hover,
                    live_idx,
                    pinned_idx,
                    drag_start,
                    drag_end,
                    zoom,
                    plot_ref,
                    tooltip.clone(),
                    on_zoom.clone(),
                    y_format.clone(),
                    height,
                ).into_any()
            } }
        </div>
    }
}

#[component]
#[allow(
    clippy::implicit_hasher,
    reason = "we own the signals end-to-end; consumers never construct the collections themselves"
)]
fn Legend(
    series: Vec<Series>,
    overrides: RwSignal<std::collections::HashMap<String, String>>,
    hidden: RwSignal<std::collections::HashSet<String>>,
) -> impl IntoView {
    view! {
        <div class="charts-legend">
            { series.into_iter().map(|s| {
                let color_class = s.color_class;
                let dot_class = format!("charts-legend-dot charts-series-{color_class}");
                let label_for = format!("charts-color-{color_class}");
                let input_id = label_for.clone();
                // <input type="color"> default value must be a 7-char
                // hex per the HTML spec; the live color may come from
                // the CSS variable, but reading computed style isn't
                // reactive. Initial value is a neutral fallback — the
                // user picks from there.
                let initial_value = "#4262ff".to_owned();
                let cc_input = color_class.clone();
                let on_input = move |ev: leptos::ev::Event| {
                    let target = event_target_value(&ev);
                    // Defense-in-depth: native `<input type="color">`
                    // always emits `#rrggbb`, but a malicious extension
                    // or synthetic event can dispatch arbitrary text.
                    // Reject anything that isn't a hex color so the
                    // value can't break out of the CSS context when
                    // it's later interpolated into `style="…"`.
                    if !is_hex_color(&target) {
                        return;
                    }
                    overrides.update(|map| {
                        map.insert(cc_input.clone(), target);
                    });
                };
                // Clicking the *name* (not the swatch) toggles the series
                // in/out of the chart — standard legend behavior.
                let cc_toggle = color_class.clone();
                let on_toggle = move |_| {
                    hidden.update(|h| {
                        if !h.remove(&cc_toggle) {
                            h.insert(cc_toggle.clone());
                        }
                    });
                };
                let cc_class = color_class;
                let is_hidden = move || hidden.with(|h| h.contains(&cc_class));
                view! {
                    <div class="charts-legend-item" class:is-hidden=is_hidden>
                        <label class="charts-legend-swatch" for=label_for title="Click to change color">
                            <span class=dot_class></span>
                            <input
                                class="charts-legend-color-input"
                                id=input_id
                                type="color"
                                value=initial_value
                                on:input=on_input
                            />
                        </label>
                        <span
                            class="charts-legend-name"
                            title="Click to show / hide this series"
                            on:click=on_toggle
                        >{ s.name }</span>
                    </div>
                }
            }).collect_view() }
        </div>
    }
}

/// True for `#?[0-9a-fA-F]{3}` or `#?[0-9a-fA-F]{6}` — exactly the
/// shape native `<input type="color">` emits. Used to gate writes
/// to the overrides map so a synthetic input event with arbitrary
/// text can't land CSS-injection payload in inline `style="…"`.
fn is_hex_color(s: &str) -> bool {
    let h = s.trim().trim_start_matches('#');
    matches!(h.len(), 3 | 6) && h.chars().all(|c| c.is_ascii_hexdigit())
}

/// Produce a "soft" rgba string from a `#rrggbb` hex by holding the
/// RGB and dropping alpha to 0.45. Used when the user overrides a
/// series color via the legend picker — we need both the solid
/// stroke + the soft fill stop in the gradient.
fn soften_hex(hex: &str) -> String {
    // Accept `#rgb`, `#rrggbb`, or anything else (passthrough).
    let h = hex.trim().trim_start_matches('#');
    let (r, g, b) = match h.len() {
        3 => (
            u8::from_str_radix(&h[0..1].repeat(2), 16).ok(),
            u8::from_str_radix(&h[1..2].repeat(2), 16).ok(),
            u8::from_str_radix(&h[2..3].repeat(2), 16).ok(),
        ),
        6 => (
            u8::from_str_radix(&h[0..2], 16).ok(),
            u8::from_str_radix(&h[2..4], 16).ok(),
            u8::from_str_radix(&h[4..6], 16).ok(),
        ),
        _ => (None, None, None),
    };
    match (r, g, b) {
        (Some(r), Some(g), Some(b)) => format!("rgba({r}, {g}, {b}, 0.45)"),
        _ => hex.to_owned(),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "all signals/refs are co-owned by AreaChart; threading through a struct adds noise without changing the surface"
)]
#[allow(
    clippy::too_many_lines,
    reason = "deliberately co-located: signals + handlers + sub-view bindings + the final view! flow naturally as one unit; further extraction hurts readability more than it helps"
)]
fn view_chart_body(
    model: RenderModel,
    from_offset: usize,
    hover: RwSignal<Option<HoverState>>,
    live_idx: RwSignal<Option<usize>>,
    pinned_idx: Option<RwSignal<Option<usize>>>,
    drag_start: RwSignal<Option<usize>>,
    drag_end: RwSignal<Option<usize>>,
    zoom: RwSignal<Option<ZoomRange>>,
    plot_ref: NodeRef<Div>,
    tooltip: Option<TooltipSlot>,
    on_zoom: Option<ZoomCommit>,
    y_format: Option<YFormat>,
    chart_height: u32,
) -> impl IntoView {
    let BodyLayout {
        y_max,
        y_grid,
        x_axis_ticks,
        series_paths,
        view_box,
        n_points,
        series_for_dots,
    } = BodyLayout::from_model(model, y_format.as_ref(), chart_height);

    let resolve_idx = make_resolve_idx(plot_ref, n_points);
    let on_mousemove = move |ev: MouseEvent| {
        let Some((idx, cx, cy)) = resolve_idx(&ev) else {
            return;
        };
        hover.set(Some(HoverState {
            index: idx,
            client_x: cx,
            client_y: cy,
        }));
        live_idx.set(Some(idx));
        if drag_start.with(Option::is_some) {
            drag_end.set(Some(idx));
        }
    };
    let on_mousedown = move |ev: MouseEvent| {
        if ev.button() != 0 {
            return;
        }
        let Some((idx, _, _)) = resolve_idx(&ev) else {
            return;
        };
        drag_start.set(Some(idx));
        drag_end.set(Some(idx));
    };
    let on_zoom_for_mouseup = on_zoom;
    let on_mouseup = move |ev: MouseEvent| {
        let Some(start) = drag_start.get() else {
            return;
        };
        let end = resolve_idx(&ev).map_or(start, |(i, _, _)| i);
        drag_start.set(None);
        drag_end.set(None);
        // A click (no drag movement) toggles the shared pin instead of
        // zooming — only when the consumer wired a `pinned` signal.
        if start == end {
            if let Some(p) = pinned_idx {
                p.update(|cur| {
                    *cur = if *cur == Some(start) {
                        None
                    } else {
                        Some(start)
                    }
                });
            }
            return;
        }
        live_idx.set(None);
        // A zoom changes what each index *means*, so a pin parked in the
        // old window would point at the wrong instant — drop it.
        if let Some(p) = pinned_idx {
            p.set(None);
        }
        commit_zoom_drag(
            start,
            end,
            n_points,
            from_offset,
            hover,
            zoom,
            on_zoom_for_mouseup.as_ref(),
        );
    };
    let on_mouseleave = move |_: MouseEvent| {
        hover.set(None);
        live_idx.set(None);
        drag_start.set(None);
        drag_end.set(None);
    };
    let on_reset = move |_: MouseEvent| {
        zoom.set(None);
        drag_start.set(None);
        drag_end.set(None);
        hover.set(None);
        live_idx.set(None);
        if let Some(p) = pinned_idx {
            p.set(None);
        }
    };

    // Two independent crosshairs: the *live* one follows the cursor
    // (and syncs across sibling charts via `live_idx`); the *pinned*
    // one is parked by a click and persists so you can compare a fixed
    // instant against wherever the cursor currently is. Both render on
    // every synced chart.
    let live_x = move || live_idx.get().map(|i| index_to_x(i, n_points, VIEW_W));
    let pinned_x = move || {
        pinned_idx
            .and_then(|p| p.get())
            .map(|i| index_to_x(i, n_points, VIEW_W))
    };
    let live_dots = make_dots_closure(
        move || live_idx.get(),
        series_for_dots.clone(),
        y_max,
        n_points,
    );
    let pinned_dots = make_dots_closure(
        move || pinned_idx.and_then(|p| p.get()),
        series_for_dots,
        y_max,
        n_points,
    );
    let pinned_tooltip_view =
        make_pinned_tooltip_closure(pinned_idx, tooltip.clone(), n_points, from_offset);
    let tooltip_view = make_tooltip_closure(hover, tooltip, plot_ref, from_offset);
    let zoom_band = make_zoom_band_closure(drag_start, drag_end, n_points);
    let reset_visible = move || zoom.with(Option::is_some);

    let color_classes: Vec<String> = series_paths
        .iter()
        .map(|sp| sp.color_class.clone())
        .collect();

    view! {
        <div class="charts-plot">
            <div class="charts-y-axis">
                // Inner scale matches the SVG's height (flex:1); the
                // sibling spacer below it stands in for the x-axis strip
                // so tick `top:%` maps to the plot's top..baseline, not
                // the whole column (which would drop "0ms" past the
                // baseline and push the top tick into the legend).
                <div class="charts-y-axis-scale">
                    { y_axis_view(y_grid.clone()) }
                </div>
            </div>
            <div
                class="charts-plot-area"
                node_ref=plot_ref
                on:mousemove=on_mousemove
                on:mousedown=on_mousedown
                on:mouseup=on_mouseup
                on:mouseleave=on_mouseleave
            >
                <svg
                    class="charts-svg"
                    viewBox=view_box
                    preserveAspectRatio="none"
                >
                    <defs>{ svg_defs_view(color_classes) }</defs>
                    <g class="charts-grid">{ svg_grid_view(y_grid) }</g>
                    { svg_series_view(series_paths) }
                    { move || zoom_band().map(zoom_band_rect) }
                </svg>
                // Crosshairs render as HTML in an overlay above the SVG
                // rather than as SVG <line>s. The series <g> carries a
                // CSS transform (the rise animation) which, in WebKit,
                // forms a stacking context that paints over sibling SVG
                // lines — so an in-SVG crosshair vanishes behind a tall
                // spike. An HTML overlay sits cleanly above the SVG (same
                // trick the dots use), below the dots overlay.
                <div class="charts-crosshair-overlay">
                    { move || pinned_x().map(crosshair_line_pinned) }
                    { move || live_x().map(crosshair_line) }
                </div>
                { dots_overlay_view(pinned_dots) }
                { dots_overlay_view(live_dots) }
                { x_axis_view(x_axis_ticks) }
                { move || pinned_tooltip_view().map(tooltip_card_pinned) }
                { move || tooltip_view().map(tooltip_card) }
                { move || reset_visible().then(|| reset_button(on_reset)) }
            </div>
        </div>
    }
}

fn y_axis_view(y_grid: Vec<(f64, String)>) -> impl IntoView {
    // The default `.charts-y-tick` rule centers each label on its
    // gridline (`translateY(-50%)`). For the topmost tick that pushes
    // half the label above the plot, where it collides with the legend;
    // for the bottom tick it dips below into the x-axis. Anchor those
    // two edge labels inward so they stay inside the plot box.
    let top_y = y_grid.iter().map(|(y, _)| *y).fold(f64::INFINITY, f64::min);
    let bottom_y = y_grid
        .iter()
        .map(|(y, _)| *y)
        .fold(f64::NEG_INFINITY, f64::max);
    y_grid
        .into_iter()
        .map(|(y, label)| {
            let shift = if (y - top_y).abs() < 0.5 {
                "translateY(0)"
            } else if (y - bottom_y).abs() < 0.5 {
                "translateY(-100%)"
            } else {
                "translateY(-50%)"
            };
            let style = format!("top: {:.2}%; transform: {shift};", (y / VIEW_H) * 100.0);
            view! { <div class="charts-y-tick" style=style>{ label }</div> }
        })
        .collect_view()
}

fn svg_defs_view(color_classes: Vec<String>) -> impl IntoView {
    color_classes
        .into_iter()
        .map(|color_class| {
            let id = format!("charts-grad-{color_class}");
            let cls = format!("charts-gradient charts-series-{color_class}");
            view! {
                <linearGradient id=id class=cls x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" class="charts-gradient-top"></stop>
                    <stop offset="100%" class="charts-gradient-bottom"></stop>
                </linearGradient>
            }
        })
        .collect_view()
}

fn svg_grid_view(y_grid: Vec<(f64, String)>) -> impl IntoView {
    y_grid
        .into_iter()
        .map(|(y, _)| {
            view! { <line class="charts-grid-line" x1="0" x2=VIEW_W y1=y y2=y /> }
        })
        .collect_view()
}

fn svg_series_view(series_paths: Vec<SeriesPaths>) -> impl IntoView {
    let paths = series_paths
        .into_iter()
        .map(|sp| {
            let area_class = format!("charts-area charts-series-{}", sp.color_class);
            let line_class = format!("charts-line charts-series-{}", sp.color_class);
            let fill = format!("url(#charts-grad-{})", sp.color_class);
            view! {
                <g class="charts-series-paths">
                    <path class=area_class d=sp.area_d fill=fill></path>
                    <path class=line_class d=sp.line_d fill="none"></path>
                </g>
            }
        })
        .collect_view();
    view! { <g class="charts-series-group">{ paths }</g> }
}

fn x_axis_view(x_axis_ticks: Vec<(f64, String)>) -> impl IntoView {
    let ticks = x_axis_ticks
        .into_iter()
        .map(|(x, label)| {
            let style = x_tick_style((x / VIEW_W) * 100.0);
            view! { <div class="charts-x-tick" style=style>{ label }</div> }
        })
        .collect_view();
    view! { <div class="charts-x-axis">{ ticks }</div> }
}

/// Position style for one X-axis label. Extreme ticks anchor to their
/// edge instead of centering, otherwise `translateX(-50%)` would let
/// half of the leftmost/rightmost label bleed past the chart edge.
fn x_tick_style(pct: f64) -> String {
    if pct >= 99.5 {
        "right: 0; left: auto; transform: none;".to_owned()
    } else if pct <= 0.5 {
        "left: 0; transform: none;".to_owned()
    } else {
        format!("left: {pct:.2}%;")
    }
}

fn dots_overlay_view(
    dots: impl Fn() -> Option<Vec<DotPos>> + Send + Sync + 'static,
) -> impl IntoView {
    // Per-series hover dots — rendered as HTML divs in pixel space
    // rather than SVG circles, otherwise `preserveAspectRatio="none"`
    // stretches them into ovals. The overlay covers the SVG region
    // only so absolute percent positioning maps 1:1 to viewBox coords.
    view! {
        <div class="charts-dots-overlay">
            { move || dots().map(|ds| ds.into_iter().map(dot_view).collect_view()) }
        </div>
    }
}

fn dot_view(d: DotPos) -> impl IntoView {
    let cls = format!("charts-dot charts-series-{}", d.color_class);
    let style = format!(
        "left: {:.2}%; top: {:.2}%;",
        (d.x / VIEW_W) * 100.0,
        (d.y / VIEW_H) * 100.0,
    );
    view! { <div class=cls style=style></div> }
}

fn zoom_band_rect(band: ZoomBand) -> impl IntoView {
    view! {
        <rect class="charts-zoom-band" x=band.x y=0 width=band.width height=VIEW_H />
    }
}

/// True when a mouse event happened inside some chart's plot area
/// (`.charts-plot-area`). Used to decide whether an outside click should
/// dismiss the pinned crosshair.
fn event_in_plot_area(ev: &web_sys::MouseEvent) -> bool {
    ev.target()
        .and_then(|t| t.dyn_into::<web_sys::Element>().ok())
        .and_then(|el| el.closest(".charts-plot-area").ok().flatten())
        .is_some()
}

fn crosshair_line(x: f64) -> impl IntoView {
    let style = format!("left: {:.3}%;", (x / VIEW_W) * 100.0);
    view! { <div class="charts-crosshair" style=style></div> }
}

fn crosshair_line_pinned(x: f64) -> impl IntoView {
    let style = format!("left: {:.3}%;", (x / VIEW_W) * 100.0);
    view! { <div class="charts-crosshair charts-crosshair-pinned" style=style></div> }
}

fn tooltip_card(tip: TooltipView) -> impl IntoView {
    view! { <div class="charts-tooltip" style=tip.style>{ tip.inner }</div> }
}

fn tooltip_card_pinned(tip: TooltipView) -> impl IntoView {
    view! { <div class="charts-tooltip charts-tooltip-pinned" style=tip.style>{ tip.inner }</div> }
}

fn reset_button(on_reset: impl Fn(MouseEvent) + 'static) -> impl IntoView {
    view! {
        <button
            class="charts-zoom-reset"
            type="button"
            title="Reset zoom"
            on:click=on_reset
        >
            "Reset zoom"
        </button>
    }
}

/// Materialized render model. Computed once per data change.
#[derive(Clone, Debug, PartialEq)]
struct RenderModel {
    labels: Vec<String>,
    series: Vec<SeriesValues>,
    y_max: f64,
    empty: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct SeriesValues {
    name: String,
    color_class: String,
    values: Vec<f64>,
}

#[derive(Clone)]
struct SeriesPaths {
    color_class: String,
    area_d: String,
    line_d: String,
}

#[derive(Clone)]
struct DotPos {
    x: f64,
    y: f64,
    color_class: String,
}

fn build_model<T, FxLabel, FyValues>(
    data: &[T],
    x_label: FxLabel,
    y_values: FyValues,
    series_meta: &[Series],
) -> RenderModel
where
    FxLabel: Fn(&T) -> String,
    FyValues: Fn(&T) -> Vec<f64>,
{
    if data.is_empty() || series_meta.is_empty() {
        return RenderModel {
            labels: Vec::new(),
            series: Vec::new(),
            y_max: 0.0,
            empty: true,
        };
    }

    let labels: Vec<String> = data.iter().map(&x_label).collect();
    let mut series_values: Vec<SeriesValues> = series_meta
        .iter()
        .map(|s| SeriesValues {
            name: s.name.clone(),
            color_class: s.color_class.clone(),
            values: Vec::with_capacity(data.len()),
        })
        .collect();

    for row in data {
        let ys = y_values(row);
        for (i, slot) in series_values.iter_mut().enumerate() {
            slot.values.push(ys.get(i).copied().unwrap_or(0.0));
        }
    }

    let y_max = series_values
        .iter()
        .flat_map(|s| s.values.iter().copied())
        .fold(0.0_f64, f64::max);

    RenderModel {
        labels,
        series: series_values,
        y_max,
        empty: false,
    }
}

/// Drop series the user hid via the legend, then rescale `y_max` to the
/// survivors so the axis fits what's actually drawn. Keeps `empty` keyed
/// on the data (labels), not visibility — hiding every series shows an
/// empty plot with axes, not the "no data" placeholder.
fn filter_hidden(
    mut model: RenderModel,
    hidden: &std::collections::HashSet<String>,
) -> RenderModel {
    if hidden.is_empty() {
        return model;
    }
    model.series.retain(|s| !hidden.contains(&s.color_class));
    model.y_max = model
        .series
        .iter()
        .flat_map(|s| s.values.iter().copied())
        .fold(0.0_f64, f64::max);
    model
}

/// Trim a fully-built [`RenderModel`] down to a [`ZoomRange`] in
/// original-index space. Returns a fresh model with its own `y_max`
/// recomputed within the window (so a zoom rescales both axes), or
/// the input unchanged when there's no active zoom.
fn slice_model(model: &RenderModel, zoom: Option<ZoomRange>) -> RenderModel {
    let Some(z) = zoom else {
        return model.clone();
    };
    let n = model.labels.len();
    if n == 0 || z.from_index >= n {
        return model.clone();
    }
    let to = z.to_index.min(n - 1);
    let from = z.from_index.min(to);
    let labels = model.labels[from..=to].to_vec();
    let series: Vec<SeriesValues> = model
        .series
        .iter()
        .map(|s| SeriesValues {
            name: s.name.clone(),
            color_class: s.color_class.clone(),
            values: s.values[from..=to].to_vec(),
        })
        .collect();
    let y_max = series
        .iter()
        .flat_map(|s| s.values.iter().copied())
        .fold(0.0_f64, f64::max);
    RenderModel {
        labels,
        series,
        y_max,
        empty: false,
    }
}

fn format_y_tick(v: f64) -> String {
    if v.fract().abs() < 1e-9 {
        format!("{:.0}", v.round())
    } else {
        format!("{v:.1}")
    }
}

/// Pre-computed layout: anything derivable from the [`RenderModel`]
/// alone, with no signals or DOM access. Computed once per
/// `view_chart_body` call so the body just wires signals into a
/// pre-built grid.
struct BodyLayout {
    y_max: f64,
    y_grid: Vec<(f64, String)>,
    x_axis_ticks: Vec<(f64, String)>,
    series_paths: Vec<SeriesPaths>,
    view_box: String,
    n_points: usize,
    series_for_dots: Vec<SeriesValues>,
}

impl BodyLayout {
    fn from_model(model: RenderModel, y_format: Option<&YFormat>, chart_height: u32) -> Self {
        // Inflate the raw max by 10% so the topmost data point doesn't
        // touch the upper edge — both the tick generator and every
        // value-to-pixel call below see the same padded value.
        let padded_max = pad_y_max(model.y_max);
        // Cap tick count by chart height (~40px per label minimum) so
        // mini-charts don't pile up overlapping labels.
        let max_ticks = ((chart_height / 40) as usize).clamp(3, 6);
        let y_ticks = nice_y_ticks_capped(padded_max, max_ticks);
        // `nice_y_ticks_capped` climbs the 1/2/5 ladder when the natural
        // step count exceeds the cap, and its top tick is "the first
        // round number at-or-above max" — so it can sit significantly
        // *above* the padded data max (e.g. padded=220 → cap=4 → top
        // tick=300). If we keep the smaller `padded_max` as y_max, every
        // y-pixel — including the top tick *label*'s position — gets
        // scaled by `data_value / padded_max > 1`, mapping the top tick
        // to a *negative* SVG y. The label then lands above the chart's
        // container (negative `top:%`), overlapping the title row above.
        // Pin y_max to the topmost tick so the axis ends where the top
        // tick is drawn and labels never escape the plot rectangle.
        let y_max = y_ticks
            .last()
            .copied()
            .map_or(padded_max, |top| top.max(padded_max));
        let n_points = model.labels.len();
        let y_grid = y_ticks
            .iter()
            .copied()
            .map(|t| {
                let label = y_format.map_or_else(|| format_y_tick(t), |f| f(t));
                (value_to_y(t, y_max, VIEW_H), label)
            })
            .collect();
        let x_axis_ticks = select_x_indices(n_points, MAX_X_LABELS)
            .into_iter()
            .map(|i| {
                (
                    index_to_x(i, n_points, VIEW_W),
                    model.labels.get(i).cloned().unwrap_or_default(),
                )
            })
            .collect();
        let series_paths = model
            .series
            .iter()
            .map(|s| SeriesPaths {
                color_class: s.color_class.clone(),
                area_d: area_path(&s.values, y_max, VIEW_W, VIEW_H),
                line_d: line_path(&s.values, y_max, VIEW_W, VIEW_H),
            })
            .collect();
        Self {
            y_max,
            y_grid,
            x_axis_ticks,
            series_paths,
            view_box: format!("0 0 {VIEW_W} {VIEW_H}"),
            n_points,
            series_for_dots: model.series,
        }
    }
}

/// Build a `Copy` closure that maps a `MouseEvent` to
/// `(display-index, client_x, client_y)`. Returns `None` when the plot
/// element is detached (transient during mount/unmount).
fn make_resolve_idx(
    plot_ref: NodeRef<Div>,
    n_points: usize,
) -> impl Fn(&MouseEvent) -> Option<(usize, f64, f64)> + Copy {
    move |ev: &MouseEvent| {
        let div_el = plot_ref.get()?;
        let target_el: web_sys::Element = (*div_el).clone().unchecked_into();
        let rect = target_el.get_bounding_client_rect();
        let cx = f64::from(ev.client_x());
        let cy = f64::from(ev.client_y());
        let idx = pixel_to_index(cx, rect.left(), rect.width(), n_points);
        Some((idx, cx, cy))
    }
}

/// Apply (or surface) a committed drag-to-zoom selection. `start` /
/// `end` are display-space indices; we compose with `from_offset` to
/// land back in original-data space. In controlled mode (`on_zoom`
/// set) we fire the callback and leave the internal `zoom` signal
/// alone — the consumer is expected to react. In uncontrolled mode we
/// write the new range straight onto `zoom`.
fn commit_zoom_drag(
    start: usize,
    end: usize,
    n_points: usize,
    from_offset: usize,
    hover: RwSignal<Option<HoverState>>,
    zoom: RwSignal<Option<ZoomRange>>,
    on_zoom: Option<&ZoomCommit>,
) {
    let Some(range) = commit_drag(start, end, n_points) else {
        return;
    };
    let composed = ZoomRange {
        from_index: from_offset + range.from_index,
        to_index: from_offset + range.to_index,
    };
    if let Some(cb) = on_zoom {
        cb(composed.from_index, composed.to_index);
    } else {
        zoom.set(Some(composed));
    }
    // Clear hover so the dot doesn't linger on a now-stale display
    // index while the new slice renders.
    hover.set(None);
}

/// Build the per-series hover-dot closure. Returns `None` when the
/// pointer isn't over the chart.
fn make_dots_closure<F>(
    get_idx: F,
    series_for_dots: Vec<SeriesValues>,
    y_max: f64,
    n_points: usize,
) -> impl Fn() -> Option<Vec<DotPos>> + Send + Sync + 'static
where
    F: Fn() -> Option<usize> + Send + Sync + 'static,
{
    move || {
        let idx = get_idx()?;
        let dots = series_for_dots
            .iter()
            .map(|sv| {
                let v = sv.values.get(idx).copied().unwrap_or(0.0);
                DotPos {
                    x: index_to_x(idx, n_points, VIEW_W),
                    y: value_to_y(v, y_max, VIEW_H),
                    color_class: sv.color_class.clone(),
                }
            })
            .collect();
        Some(dots)
    }
}

/// Build the tooltip closure: positions the floating card relative to
/// the plot's bounding rect, flips it to the cursor's left when the
/// pointer crosses [`TOOLTIP_FLIP_FRACTION`], and forwards the
/// original-space index to the consumer slot.
fn make_tooltip_closure(
    hover: RwSignal<Option<HoverState>>,
    tooltip: Option<TooltipSlot>,
    plot_ref: NodeRef<Div>,
    from_offset: usize,
) -> impl Fn() -> Option<TooltipView> + Send + Sync + 'static {
    move || {
        let h = hover.get()?;
        let cb = tooltip.as_ref()?;
        let plot_el = plot_ref.get()?;
        let plot_rect = (*plot_el)
            .clone()
            .unchecked_into::<web_sys::Element>()
            .get_bounding_client_rect();
        let style = tooltip_inline_style(&plot_rect, h.client_x, h.client_y);
        let original_idx = from_offset + h.index;
        let inner = (cb)(original_idx);
        Some(TooltipView { style, inner })
    }
}

/// Build the *pinned* tooltip closure. Unlike the live one, there's no
/// cursor, so it positions itself from the pinned data index in the
/// chart's own percent space — which is why it can render on sibling
/// charts the cursor never touched. Returns `None` when nothing is
/// pinned or no tooltip slot was provided.
fn make_pinned_tooltip_closure(
    pinned_idx: Option<RwSignal<Option<usize>>>,
    tooltip: Option<TooltipSlot>,
    n_points: usize,
    from_offset: usize,
) -> impl Fn() -> Option<TooltipView> + Send + Sync + 'static {
    move || {
        let idx = pinned_idx.and_then(|p| p.get())?;
        let cb = tooltip.as_ref()?;
        let frac = if n_points > 1 {
            index_to_x(idx, n_points, VIEW_W) / VIEW_W
        } else {
            0.0
        };
        // Anchor on whichever side keeps the card inside the plot, and
        // push it 14px off the line so the pinned point stays visible
        // underneath rather than hidden behind the card.
        let style = if frac > TOOLTIP_FLIP_FRACTION {
            format!(
                "right: calc({:.2}% + 14px); top: 6px;",
                (1.0 - frac) * 100.0
            )
        } else {
            format!("left: calc({:.2}% + 14px); top: 6px;", frac * 100.0)
        };
        let inner = (cb)(from_offset + idx);
        Some(TooltipView { style, inner })
    }
}

/// Container for the tooltip closure's two outputs — the inline style
/// string and the consumer-supplied inner view. Splits cleanly so the
/// view! macro at the call site can read both fields without rebuilding
/// the bounding-rect math.
struct TooltipView {
    style: String,
    inner: AnyView,
}

/// Compute the tooltip's inline `style="..."` value. Pulled out as a
/// pure function so it's testable without standing up a DOM.
fn tooltip_inline_style(plot_rect: &web_sys::DomRect, client_x: f64, client_y: f64) -> String {
    let plot_left = plot_rect.left();
    let plot_width = plot_rect.width();
    let frac_x = if plot_width > 0.0 {
        (client_x - plot_left) / plot_width
    } else {
        0.0
    };
    let flip = frac_x > TOOLTIP_FLIP_FRACTION;
    let left_px = client_x - plot_left;
    let top_px = (client_y - plot_rect.top()).clamp(8.0, plot_rect.height() - 8.0);
    if flip {
        format!(
            "right: {:.2}px; top: {:.2}px;",
            plot_width - left_px + 14.0,
            top_px
        )
    } else {
        format!("left: {:.2}px; top: {:.2}px;", left_px + 14.0, top_px)
    }
}

/// Build the zoom-band closure: the translucent rectangle painted
/// while a drag is in progress. Suppressed for zero-width drags so a
/// stray click doesn't flash a 0-width sliver.
fn make_zoom_band_closure(
    drag_start: RwSignal<Option<usize>>,
    drag_end: RwSignal<Option<usize>>,
    n_points: usize,
) -> impl Fn() -> Option<ZoomBand> + Send + Sync + 'static {
    move || {
        let s = drag_start.get()?;
        let e = drag_end.get()?;
        if s == e {
            return None;
        }
        let lo = s.min(e);
        let hi = s.max(e);
        let x1 = index_to_x(lo, n_points, VIEW_W);
        let x2 = index_to_x(hi, n_points, VIEW_W);
        Some(ZoomBand {
            x: x1,
            width: (x2 - x1).max(0.0),
        })
    }
}

/// Drag-band geometry in viewBox units. Tiny so the view! macro can
/// destructure cleanly.
#[derive(Clone, Copy)]
struct ZoomBand {
    x: f64,
    width: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_model_aligns_series_with_y_values() {
        let data = vec![(1.0, 4.0), (2.0, 5.0), (3.0, 6.0)];
        let m = build_model(
            &data,
            |d: &(f64, f64)| format!("{}", d.0),
            |d: &(f64, f64)| vec![d.0, d.1],
            &[Series::area("A", "a"), Series::area("B", "b")],
        );
        assert_eq!(m.labels, vec!["1", "2", "3"]);
        assert_eq!(m.series[0].values, vec![1.0, 2.0, 3.0]);
        assert_eq!(m.series[1].values, vec![4.0, 5.0, 6.0]);
        assert!((m.y_max - 6.0).abs() < 1e-9);
    }

    #[test]
    fn build_model_empty_data_marks_empty() {
        let data: Vec<(f64, f64)> = vec![];
        let m = build_model(
            &data,
            |_d: &(f64, f64)| String::new(),
            |_d: &(f64, f64)| vec![0.0],
            &[Series::area("A", "a")],
        );
        assert!(m.empty);
        assert_eq!(m.y_max, 0.0);
    }

    #[test]
    fn build_model_fills_missing_series_with_zero() {
        let data = vec![(1.0,)];
        let m = build_model(
            &data,
            |_d| "x".to_owned(),
            |_d| vec![1.0],
            &[Series::area("A", "a"), Series::area("B", "b")],
        );
        assert_eq!(m.series[0].values, vec![1.0]);
        assert_eq!(
            m.series[1].values,
            vec![0.0],
            "missing series should be zero-filled"
        );
    }

    #[test]
    fn format_y_tick_drops_fraction_for_integers() {
        assert_eq!(format_y_tick(0.0), "0");
        assert_eq!(format_y_tick(20.0), "20");
        assert_eq!(format_y_tick(100.0), "100");
    }

    #[test]
    fn format_y_tick_shows_one_decimal_for_fractional() {
        assert_eq!(format_y_tick(0.5), "0.5");
    }

    #[test]
    fn soften_hex_parses_long_hex() {
        assert_eq!(soften_hex("#4262ff"), "rgba(66, 98, 255, 0.45)");
        assert_eq!(soften_hex("4262ff"), "rgba(66, 98, 255, 0.45)");
    }

    #[test]
    fn soften_hex_parses_short_hex() {
        assert_eq!(soften_hex("#f0a"), "rgba(255, 0, 170, 0.45)");
    }

    #[test]
    fn soften_hex_passes_through_garbage() {
        assert_eq!(soften_hex("not a color"), "not a color");
    }

    #[test]
    fn is_hex_color_accepts_short_and_long_forms() {
        assert!(is_hex_color("#fff"));
        assert!(is_hex_color("#FFF"));
        assert!(is_hex_color("#4262ff"));
        assert!(is_hex_color("#4262FF"));
        // Allow the `#`-less form because some pickers emit that
        // shape; soften_hex already strips the leading `#`.
        assert!(is_hex_color("fff"));
        assert!(is_hex_color("4262ff"));
    }

    #[test]
    fn is_hex_color_rejects_css_injection_payloads() {
        // The realistic synthetic-event injection: arbitrary CSS
        // that would otherwise land inside inline `style="…"` and
        // execute as additional declarations.
        assert!(!is_hex_color("red; background-image: url(http://evil)"));
        assert!(!is_hex_color("#fff; color: red"));
        assert!(!is_hex_color(""));
        assert!(!is_hex_color("#"));
        assert!(!is_hex_color("#ggg"));
        // Reject the 4 / 5 / 7 / 8-char in-between sizes that don't
        // match either of HTML5's accepted hex shapes.
        assert!(!is_hex_color("#4262f"));
        assert!(!is_hex_color("#4262fff"));
        assert!(!is_hex_color("rgb(255, 0, 0)"));
    }

    #[test]
    fn x_tick_style_anchors_left_for_first_label() {
        assert_eq!(
            x_tick_style(0.0),
            "left: 0; transform: none;",
            "leftmost tick must anchor to the edge, not center-translate past it"
        );
    }

    #[test]
    fn x_tick_style_anchors_right_for_last_label() {
        assert_eq!(x_tick_style(99.9), "right: 0; left: auto; transform: none;");
    }

    #[test]
    fn x_tick_style_centers_middle_label() {
        assert_eq!(x_tick_style(50.0), "left: 50.00%;");
    }

    #[test]
    fn body_layout_includes_all_derived_data() {
        let model = five_point_model();
        let n_points_expected = model.labels.len();
        let layout = BodyLayout::from_model(model, None, 320);
        assert_eq!(layout.n_points, n_points_expected);
        assert!(!layout.y_grid.is_empty(), "y_grid must have ticks");
        assert!(
            !layout.x_axis_ticks.is_empty(),
            "x_axis_ticks must include at least the endpoints"
        );
        assert_eq!(layout.series_paths.len(), 2, "two series in the fixture");
        assert!(
            layout.y_max > 0.0,
            "y_max should reflect the padded fixture max"
        );
        assert_eq!(layout.view_box, format!("0 0 {VIEW_W} {VIEW_H}"));
    }

    #[test]
    fn body_layout_y_max_covers_topmost_tick_on_mini_chart() {
        // Regression: with a small chart height the tick generator climbs
        // the 1/2/5 ladder and emits a top tick well above the padded
        // data max (e.g. data≈200 → padded≈220 → cap=4 → top tick=300).
        // If `y_max` stayed at the padded value, `value_to_y(300, 220)`
        // would be negative — the top label would render *above* the
        // plot, overlapping the chart title. The fix pins `y_max` to the
        // topmost tick so the label lands at SVG y=0.
        let data: Vec<f64> = vec![10.0, 50.0, 120.0, 180.0, 200.0];
        let model = build_model(
            &data,
            |_: &f64| String::new(),
            |v: &f64| vec![*v],
            &[Series::area("A", "a")],
        );
        // height=180 → max_ticks = clamp(180/40, 3, 6) = 4 → forces climb.
        let layout = BodyLayout::from_model(model, None, 180);
        let top_tick = layout
            .y_grid
            .iter()
            .map(|(_, label)| label.parse::<f64>().unwrap_or(0.0))
            .fold(0.0_f64, f64::max);
        assert!(
            layout.y_max + 1e-9 >= top_tick,
            "y_max ({}) must be ≥ the topmost tick ({}) so labels never \
             render at negative SVG y",
            layout.y_max,
            top_tick,
        );
        // And every gridline's pixel position must be ≥ 0 (i.e. inside
        // the plot rect). A negative `y` here is what positions the
        // label above the chart container.
        assert!(
            layout.y_grid.iter().all(|(y, _)| *y >= -1e-9),
            "every gridline must sit inside the plot: got {:?}",
            layout.y_grid,
        );
    }

    #[test]
    fn body_layout_applies_custom_y_format() {
        let model = five_point_model();
        let fmt: YFormat = Arc::new(|v: f64| format!("{v:.0}ms"));
        let layout = BodyLayout::from_model(model, Some(&fmt), 320);
        assert!(
            layout.y_grid.iter().all(|(_, label)| label.ends_with("ms")),
            "every y tick should use the custom formatter, got {:?}",
            layout.y_grid
        );
    }

    fn five_point_model() -> RenderModel {
        let data: Vec<(f64, f64)> = vec![
            (10.0, 1.0),
            (20.0, 2.0),
            (30.0, 5.0),
            (40.0, 8.0),
            (50.0, 3.0),
        ];
        build_model(
            &data,
            |d: &(f64, f64)| format!("{}", d.0),
            |d: &(f64, f64)| vec![d.0, d.1],
            &[Series::area("A", "a"), Series::area("B", "b")],
        )
    }

    #[test]
    fn slice_model_returns_unchanged_when_no_zoom() {
        let m = five_point_model();
        let s = slice_model(&m, None);
        assert_eq!(s.labels, m.labels);
        assert_eq!(s.series[0].values, m.series[0].values);
        assert!((s.y_max - m.y_max).abs() < 1e-9);
    }

    #[test]
    fn slice_model_trims_to_zoom_range_inclusive() {
        let m = five_point_model();
        let z = ZoomRange {
            from_index: 1,
            to_index: 3,
        };
        let s = slice_model(&m, Some(z));
        assert_eq!(s.labels, vec!["20", "30", "40"]);
        assert_eq!(s.series[0].values, vec![20.0, 30.0, 40.0]);
        assert_eq!(s.series[1].values, vec![2.0, 5.0, 8.0]);
    }

    #[test]
    fn slice_model_rescales_y_max_within_window() {
        let m = five_point_model();
        assert!((m.y_max - 50.0).abs() < 1e-9);
        let z = ZoomRange {
            from_index: 1,
            to_index: 2,
        };
        let s = slice_model(&m, Some(z));
        // Window covers (20, 2) and (30, 5) — max is 30, not the full
        // data's 50. Confirms the zoom rescales the Y axis.
        assert!((s.y_max - 30.0).abs() < 1e-9);
    }

    #[test]
    fn slice_model_clamps_out_of_range_zoom() {
        let m = five_point_model();
        let z = ZoomRange {
            from_index: 3,
            to_index: 999,
        };
        let s = slice_model(&m, Some(z));
        assert_eq!(s.labels, vec!["40", "50"]);
    }
}
