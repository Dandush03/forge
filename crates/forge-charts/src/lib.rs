#![forbid(unsafe_code)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    // Chart math: cast bounds are well below 2^53 for f64 mantissa;
    // float comparisons in loop bounds are guarded by an epsilon; doc
    // paragraphs in math code are intentionally long. Pedantic noise.
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::while_float,
    clippy::float_cmp,
    clippy::suboptimal_flops,
    clippy::format_push_string,
    clippy::doc_markdown,
    clippy::too_long_first_doc_paragraph,
    clippy::needless_pass_by_value,
    clippy::module_name_repetitions
)]

//! Pure-Rust + SVG interactive charts for Leptos CSR.
//!
//! v0.1 ships one chart type — [`AreaChart`] — with the visual style
//! of ApexCharts (gradient fills, soft axes, modern legend) but
//! implemented entirely in Rust + SVG. No JS, no canvas, no Tailwind.
//! Designed to be project-agnostic: no consumer types leak into the
//! public API.
//!
//! ## Quick start
//!
//! ```ignore
//! use leptos::prelude::*;
//! use forge_charts::{AreaChart, Series, CHART_CSS};
//!
//! #[derive(Clone)]
//! struct Datum { date: String, opened: u32, closed: u32 }
//!
//! #[component]
//! fn MyChart(data: Signal<Vec<Datum>>) -> impl IntoView {
//!     view! {
//!         <Stylesheet text=CHART_CSS />
//!         <AreaChart
//!             data=data
//!             x_label=|d: &Datum| d.date.clone()
//!             y_values=|d: &Datum| vec![f64::from(d.opened), f64::from(d.closed)]
//!             series=vec![
//!                 Series::area("Opened", "opened"),
//!                 Series::area("Closed", "closed"),
//!             ]
//!             height=320
//!         />
//!     }
//! }
//! ```
//!
//! ## Styling
//!
//! The crate ships a default stylesheet exposed as the [`CHART_CSS`]
//! constant. Consumers inject it once via Leptos `<Stylesheet />` at
//! the app root. CSS variables (`--charts-fg`, `--charts-grid-color`,
//! `--charts-series-opened`, `--charts-series-closed`, …) let
//! consumers theme without forking the stylesheet.
//!
//! ## Roadmap
//!
//! - **Phase B** (shipped): hover crosshair + consumer-provided
//!   tooltip slot.
//! - **Phase C** (shipped): drag-to-zoom + reset.
//! - **Future**: Bar / Line variants, smooth Bezier curves, stacked
//!   areas. Extract shared math when the second chart needs it.

pub mod axis;
pub mod chart;
pub mod hover;
pub mod path;
pub mod scale;
pub mod series;
pub mod zoom;

pub use chart::{AreaChart, TooltipSlot, YFormat, ZoomCommit};
pub use hover::HoverState;
pub use series::{Series, SeriesKind};
pub use zoom::{MIN_ZOOM_SPAN, ZoomRange};

/// Default stylesheet bundled with the crate. Inject once at the app
/// root via `<Stylesheet text=CHART_CSS />` (Leptos meta).
pub const CHART_CSS: &str = include_str!("charts.css");
