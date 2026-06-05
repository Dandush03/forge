//! Value ↔ pixel mapping. Pure math; trivially testable.

/// Maps a data value in `[0, max]` to an SVG Y coordinate in
/// `[0, height]`. Y=0 is the top of the chart, Y=height is the
/// baseline — so larger values map to **smaller** Y.
///
/// `max == 0` is treated as 1.0 to avoid division by zero (a chart
/// with no data still draws a flat baseline). Negative values get
/// clamped to 0 (the API is unsigned-only for v0.1).
#[must_use]
pub fn value_to_y(value: f64, max: f64, height: f64) -> f64 {
    let safe_max = if max > 0.0 { max } else { 1.0 };
    let clamped = value.max(0.0);
    height - (clamped / safe_max) * height
}

/// Inverse of [`value_to_y`]. Used by hover / zoom to convert
/// crosshair Y back to a data-space value. Provided for completeness
/// — v0.1 doesn't use it yet but Phases B/C will.
#[must_use]
#[allow(
    dead_code,
    reason = "inverse of value_to_y; kept as the named pair for Phases B/C consumers that will project crosshair-Y back to data space"
)]
pub fn y_to_value(y: f64, max: f64, height: f64) -> f64 {
    let safe_max = if max > 0.0 { max } else { 1.0 };
    let safe_height = if height > 0.0 { height } else { 1.0 };
    (1.0 - (y / safe_height)) * safe_max
}

/// Maps a data-point index in `0..n_points` to an SVG X coordinate
/// in `[0, width]`. Evenly spaced; the first point sits at x=0 and
/// the last at x=width. With one point the result is always 0
/// (the single point sits at the left edge).
#[must_use]
pub fn index_to_x(index: usize, n_points: usize, width: f64) -> f64 {
    if n_points <= 1 {
        return 0.0;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "indices and point counts are bounded well below 2^53"
    )]
    {
        (index as f64 / (n_points - 1) as f64) * width
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp, reason = "exact-value tests with simple inputs")]
mod tests {
    use super::*;

    #[test]
    fn value_to_y_inverts_at_endpoints() {
        assert_eq!(value_to_y(0.0, 10.0, 200.0), 200.0);
        assert_eq!(value_to_y(10.0, 10.0, 200.0), 0.0);
        assert_eq!(value_to_y(5.0, 10.0, 200.0), 100.0);
    }

    #[test]
    fn value_to_y_handles_zero_max() {
        // No data → flat baseline; everything maps to bottom.
        assert_eq!(value_to_y(0.0, 0.0, 200.0), 200.0);
    }

    #[test]
    fn value_to_y_clamps_negative_to_zero() {
        assert_eq!(value_to_y(-5.0, 10.0, 200.0), 200.0);
    }

    #[test]
    fn y_to_value_roundtrips() {
        let y = value_to_y(7.5, 10.0, 200.0);
        let back = y_to_value(y, 10.0, 200.0);
        assert!((back - 7.5).abs() < 1e-9);
    }

    #[test]
    fn index_to_x_spans_zero_to_width() {
        assert_eq!(index_to_x(0, 5, 100.0), 0.0);
        assert_eq!(index_to_x(4, 5, 100.0), 100.0);
        assert_eq!(index_to_x(2, 5, 100.0), 50.0);
    }

    #[test]
    fn index_to_x_handles_single_point() {
        assert_eq!(index_to_x(0, 1, 100.0), 0.0);
    }
}
