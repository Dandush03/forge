//! Shared chart formatters + tooltip helpers used by the metrics panels.
//!
//! Lives in its own module so the per-queue panel ([`crate::per_queue`])
//! and the resources panel ([`crate::resources`]) (plus the future DB
//! health panel) can reach for the same `pct_y_format`, `bytes_y_format`,
//! and `tooltip_for` without cross-importing.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use forge_charts::{TooltipSlot, YFormat};
use leptos::prelude::*;
use leptos::tachys::view::any_view::IntoAny;

use crate::timeline::bucket_label;

/// Bucket granularity (seconds) the metrics panels fetch + label by.
/// Matches the rollup's base granularity in `crates/jobs/src/runtime/metrics.rs`.
pub const BUCKET_SECS: u32 = 60;

/// Y-axis formatter for percent charts (CPU%, disk-used %, pool %).
/// Whole-percent for ≥1, one decimal below — without this a near-idle
/// CPU axis prints `0%` on every tick.
#[must_use]
pub fn pct_y_format() -> YFormat {
    Arc::new(|v: f64| {
        if v >= 1.0 {
            format!("{v:.0}%")
        } else if v == 0.0 {
            "0%".to_owned()
        } else {
            format!("{v:.1}%")
        }
    })
}

/// Y-axis formatter for the byte charts (RAM, disk I/O).
#[must_use]
pub fn bytes_y_format() -> YFormat {
    Arc::new(|v: f64| fmt_bytes(v))
}

/// Format a byte count with a binary unit (`1.5 GB`). Sub-KB stays whole.
#[must_use]
pub fn fmt_bytes(v: f64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let mut x = v.max(0.0);
    let mut i = 0;
    while x >= 1024.0 && i < UNITS.len() - 1 {
        x /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{x:.0} {}", UNITS[i])
    } else {
        format!("{x:.1} {}", UNITS[i])
    }
}

/// Logical CPU count visible to the browser/WebView — matches what the
/// host sampler used for normalization (`available_parallelism`). 1 is
/// the fallback if the runtime can't report it.
#[must_use]
pub fn cpu_cores() -> u32 {
    leptos::web_sys::window()
        .map(|w| w.navigator().hardware_concurrency() as u32)
        .filter(|&n| n > 0)
        .unwrap_or(1)
}

/// Rows of `(label, series-color-class, value)` a tooltip renders for a
/// hovered bucket of type `T`. A plain `fn` pointer so it's trivially
/// `Send + Sync + Copy` for the tooltip `Arc`.
pub type TipRows<T> = fn(&T) -> Vec<(&'static str, &'static str, String)>;

/// Build a tooltip slot that reads the hovered bucket from `data` and
/// renders one row per metric via `rows`. Generic over the bucket type
/// so every metric panel shares one tooltip card.
pub fn tooltip_for<T>(
    data: Signal<Vec<T>>,
    at: fn(&T) -> DateTime<Utc>,
    rows: TipRows<T>,
) -> TooltipSlot
where
    T: Clone + Send + Sync + 'static,
{
    Arc::new(move |idx: usize| {
        let Some(b) = data.with(|v| v.get(idx).cloned()) else {
            return view! { <div></div> }.into_any();
        };
        let when = bucket_label(at(&b), BUCKET_SECS);
        view! {
            <div class="charts-tooltip-card">
                <div class="charts-tooltip-date">{ when }</div>
                { rows(&b).into_iter().map(|(label, cls, val)| {
                    let dot = format!("charts-tooltip-dot queue-series-{cls}");
                    view! {
                        <div class="charts-tooltip-row">
                            <span class=dot></span>
                            <span class="charts-tooltip-label">{ label }</span>
                            <span class="charts-tooltip-value">{ val }</span>
                        </div>
                    }
                }).collect_view() }
            </div>
        }
        .into_any()
    })
}

#[cfg(test)]
mod tests {
    use super::fmt_bytes;

    #[test]
    fn fmt_bytes_scales_units() {
        assert_eq!(fmt_bytes(512.0), "512 B");
        assert_eq!(fmt_bytes(1024.0), "1.0 KB");
        assert_eq!(fmt_bytes(1536.0), "1.5 KB");
        assert_eq!(fmt_bytes(5.0 * 1024.0 * 1024.0), "5.0 MB");
        assert_eq!(fmt_bytes(2.0 * 1024.0 * 1024.0 * 1024.0), "2.0 GB");
        assert_eq!(fmt_bytes(0.0), "0 B");
    }
}
