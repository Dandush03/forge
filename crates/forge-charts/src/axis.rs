//! Axis tick selection. Picks "nice" round numbers for the Y axis
//! and a sparse-but-meaningful subset of X labels when the data is
//! denser than the available label space.

/// Pick round Y-axis ticks covering `[0, max]`. Always starts at 0;
/// spacing follows the classic 1/2/5×10ⁿ progression so labels are
/// human-readable. Targets ~5 steps (≤ 6 ticks) — the legacy default
/// suitable for charts ≥ ~240px tall.
///
/// `max == 0` returns `[0, 1]` so the chart still draws a baseline +
/// a single line above (otherwise the Y axis collapses).
///
/// For small mini-charts (≤ 180px tall) use [`nice_y_ticks_capped`]
/// with a lower `max_ticks` so labels don't stack vertically.
#[must_use]
#[allow(
    dead_code,
    reason = "default-ticks shorthand wrapping `nice_y_ticks_capped(_, 6)`; kept for callers that want the legacy default and for the doc cross-link from the `capped` variant"
)]
pub fn nice_y_ticks(max: f64) -> Vec<f64> {
    nice_y_ticks_capped(max, 6)
}

/// Variant that caps the rendered tick count at `max_ticks`. Used by
/// mini-charts whose vertical space can't fit the default ~6 labels —
/// without this, a near-idle CPU axis at `max ≈ 0.4` emits 9 ticks that
/// stack on top of each other and (after a sub-1% formatter) collapse
/// into visual duplicates like `"0.1%, 0.1%, 0.1%"`.
///
/// The algorithm picks an initial `nice` step from the 1/2/5×10ⁿ ladder
/// targeting `max_ticks` ticks, then **climbs the ladder** if the
/// "include the tick at-or-above max" convention pushed the count over
/// `max_ticks`. Guaranteed to return at most `max_ticks` entries
/// (clamped to ≥ 2 — need at least `[0, top]`).
#[must_use]
pub fn nice_y_ticks_capped(max: f64, max_ticks: usize) -> Vec<f64> {
    if max <= 0.0 {
        return vec![0.0, 1.0];
    }
    let max_ticks = max_ticks.max(2);
    // Target one fewer step than ticks (ticks include 0 and the
    // at-or-above-max boundary).
    #[allow(
        clippy::cast_precision_loss,
        reason = "max_ticks is single-digit; exact as f64"
    )]
    let target_steps = (max_ticks - 1).max(1) as f64;
    let mut nice = pick_nice_step(max, target_steps);
    let mut ticks = build_ticks(max, nice);
    // The "include the tick at-or-above max" convention can put us one
    // over `max_ticks`. Climb the 1/2/5 ladder until we fit.
    while ticks.len() > max_ticks {
        nice = next_step_on_ladder(nice);
        ticks = build_ticks(max, nice);
        // Safety: even with a huge step we'll have at most 2 ticks (0
        // and the at-or-above-max boundary) — guaranteed to terminate.
        if ticks.len() <= 2 {
            break;
        }
    }
    ticks
}

/// Choose a step on the 1/2/5×10ⁿ ladder targeting `target_steps`
/// steps across `[0, max]`.
fn pick_nice_step(max: f64, target_steps: f64) -> f64 {
    let raw_step = max / target_steps;
    let magnitude = 10f64.powi(raw_step.log10().floor() as i32);
    let normalized = raw_step / magnitude;
    let nice = if normalized < 1.5 {
        1.0
    } else if normalized < 3.5 {
        2.0
    } else if normalized < 7.5 {
        5.0
    } else {
        10.0
    };
    nice * magnitude
}

/// Next coarser step on the 1/2/5×10ⁿ ladder: 1→2→5→10→20→50→100…
fn next_step_on_ladder(step: f64) -> f64 {
    let magnitude = 10f64.powi(step.log10().floor() as i32);
    let normalized = step / magnitude;
    // Round to handle f64 fuzziness from prior multiplies.
    if normalized < 1.5 {
        2.0 * magnitude
    } else if normalized < 3.5 {
        5.0 * magnitude
    } else if normalized < 7.5 {
        10.0 * magnitude
    } else {
        20.0 * magnitude
    }
}

/// Build `[0, step, 2*step, …]` up to the first tick at-or-above `max`.
fn build_ticks(max: f64, step: f64) -> Vec<f64> {
    let magnitude = 10f64.powi(step.log10().floor() as i32);
    let mut ticks = Vec::new();
    let mut v = 0.0;
    loop {
        ticks.push(round_to(v, magnitude / 10.0));
        if v >= max {
            break;
        }
        v += step;
    }
    ticks
}

fn round_to(v: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return v;
    }
    (v / step).round() * step
}

/// Headroom factor applied to a series' raw max before tick + scale
/// generation. Keeps the topmost data point from kissing the chart's
/// upper edge — a value of 1.10 means the rendered Y axis covers at
/// least 110% of the data range.
pub const Y_HEADROOM: f64 = 1.10;

/// Inflate a raw Y maximum so the rendered chart leaves visual
/// headroom above the data. Used by both the tick generator and the
/// value-to-pixel scale so the two stay in sync — without padding,
/// a data point equal to its own top tick would touch the top of the
/// plot area.
///
/// `raw_max <= 0` is left unchanged so [`nice_y_ticks`] still emits
/// its `[0, 1]` fallback for all-zero data.
#[must_use]
pub fn pad_y_max(raw_max: f64) -> f64 {
    if raw_max <= 0.0 {
        raw_max
    } else {
        raw_max * Y_HEADROOM
    }
}

/// Pick a subset of X-axis label indices to render. Keeps the
/// endpoints. Targets at most `max_shown` labels so the axis doesn't
/// crowd. Stride-based: every Nth index, where N is chosen so the
/// output count lands at or just below `max_shown`.
///
/// Examples:
/// - 7 labels, max_shown=7 → `[0, 1, 2, 3, 4, 5, 6]`
/// - 30 labels, max_shown=6 → `[0, 6, 12, 18, 24, 29]`
/// - 1 label, max_shown=6 → `[0]`
#[must_use]
pub fn select_x_indices(n_labels: usize, max_shown: usize) -> Vec<usize> {
    if n_labels == 0 {
        return Vec::new();
    }
    if n_labels == 1 {
        return vec![0];
    }
    if max_shown <= 1 {
        return vec![0, n_labels - 1];
    }
    if n_labels <= max_shown {
        return (0..n_labels).collect();
    }

    // Stride to get approximately `max_shown` evenly-spaced labels,
    // always including 0 and n_labels-1.
    let stride = ((n_labels - 1) as f64 / (max_shown - 1) as f64).ceil() as usize;
    let stride = stride.max(1);

    let mut out: Vec<usize> = (0..n_labels).step_by(stride).collect();
    // Ensure the last index is included even if the stride misses it.
    if *out.last().unwrap_or(&0) != n_labels - 1 {
        out.push(n_labels - 1);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nice_y_ticks_picks_round_numbers_for_small_max() {
        let ticks = nice_y_ticks(10.0);
        assert_eq!(ticks, vec![0.0, 2.0, 4.0, 6.0, 8.0, 10.0]);
    }

    #[test]
    fn nice_y_ticks_picks_round_numbers_for_medium_max() {
        let ticks = nice_y_ticks(87.0);
        // Expected: stride of 20 → [0, 20, 40, 60, 80, 100]
        assert_eq!(ticks, vec![0.0, 20.0, 40.0, 60.0, 80.0, 100.0]);
    }

    #[test]
    fn nice_y_ticks_handles_zero_max() {
        assert_eq!(nice_y_ticks(0.0), vec![0.0, 1.0]);
    }

    #[test]
    fn nice_y_ticks_handles_fractional_max() {
        let ticks = nice_y_ticks(0.45);
        // step ≈ 0.09 → rounded to 0.1 → [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
        assert!(ticks.first().is_some_and(|&t| t == 0.0));
        assert!(ticks.last().is_some_and(|&t| t >= 0.45));
    }

    #[test]
    fn nice_y_ticks_capped_respects_max_ticks() {
        // Small max + low cap: 0.4 → at the default ~5 steps you'd get
        // 5 ticks (0/0.1/0.2/0.3/0.4); capped to 3 should climb the
        // ladder to step=0.2 → 0/0.2/0.4 (≤ 3 entries).
        let ticks = nice_y_ticks_capped(0.4, 3);
        assert!(ticks.len() <= 3, "got {ticks:?}");
        assert_eq!(ticks.first(), Some(&0.0));
        assert!(ticks.last().is_some_and(|&t| t >= 0.4));
    }

    #[test]
    fn nice_y_ticks_capped_no_regression_on_big_chart() {
        // Cap 6 (legacy default behaviour) should match the un-capped
        // output for a typical big-number axis.
        let big = nice_y_ticks_capped(100.0, 6);
        assert_eq!(big, vec![0.0, 20.0, 40.0, 60.0, 80.0, 100.0]);
    }

    #[test]
    fn nice_y_ticks_capped_tiny_max_doesnt_overflow_cap() {
        // The pathological small-CPU case: max ≈ 0.15 used to emit
        // 8-9 ticks at step=0.02; capped to 4 must climb to 0.05.
        let ticks = nice_y_ticks_capped(0.15, 4);
        assert!(
            ticks.len() <= 4,
            "expected at most 4 ticks, got {} ({ticks:?})",
            ticks.len()
        );
        assert!(ticks.last().is_some_and(|&t| t >= 0.15));
    }

    #[test]
    fn select_x_indices_keeps_endpoints() {
        let out = select_x_indices(30, 6);
        assert_eq!(out.first(), Some(&0));
        assert_eq!(out.last(), Some(&29));
    }

    #[test]
    fn select_x_indices_caps_count() {
        let out = select_x_indices(30, 6);
        assert!(out.len() <= 7, "got {} indices for max_shown=6", out.len());
    }

    #[test]
    fn select_x_indices_returns_all_when_under_cap() {
        assert_eq!(select_x_indices(5, 10), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn select_x_indices_handles_single_label() {
        assert_eq!(select_x_indices(1, 6), vec![0]);
    }

    #[test]
    fn select_x_indices_handles_empty() {
        assert_eq!(select_x_indices(0, 6), Vec::<usize>::new());
    }

    #[test]
    fn pad_y_max_adds_headroom_above_data() {
        // 140 * 1.10 = 154 → nice_y_ticks rounds up to 160, giving
        // visible padding between the topmost data point and the
        // upper edge of the plot.
        let padded = pad_y_max(140.0);
        assert!(padded >= 140.0 * 1.10 - 1e-9);
        let ticks = nice_y_ticks(padded);
        assert!(
            ticks.last().is_some_and(|&t| t > 140.0),
            "top tick {:?} should exceed raw max of 140",
            ticks.last()
        );
    }

    #[test]
    fn pad_y_max_preserves_zero() {
        // Zero stays zero so the nice_y_ticks fallback ([0, 1]) still
        // kicks in for all-zero data.
        assert_eq!(pad_y_max(0.0), 0.0);
        assert_eq!(pad_y_max(-3.0), -3.0);
    }
}
