//! Hover state + pure pixel-to-index math.

/// Snapshot of where the cursor is in the chart.
///
/// `client_x` / `client_y` are viewport-relative CSS pixels (what
/// `MouseEvent.client_x()` reports). The tooltip div uses them to
/// position itself relative to its containing block via inline style.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HoverState {
    /// Index into the chart's data array.
    pub index: usize,
    /// Viewport X in CSS pixels (for tooltip positioning).
    pub client_x: f64,
    /// Viewport Y in CSS pixels.
    pub client_y: f64,
}

/// Snap a client-X pixel coordinate to the nearest data-point index.
///
/// `plot_left` and `plot_width` come from the SVG element's
/// `getBoundingClientRect()`. With one point, always returns 0.
/// Out-of-range pixels clamp to the nearest endpoint.
#[must_use]
pub fn pixel_to_index(client_x: f64, plot_left: f64, plot_width: f64, n_points: usize) -> usize {
    if n_points <= 1 || plot_width <= 0.0 {
        return 0;
    }
    let last = n_points - 1;
    let fraction = ((client_x - plot_left) / plot_width).clamp(0.0, 1.0);
    let raw = fraction * last as f64;
    let snapped = raw.round() as usize;
    snapped.min(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pixel_to_index_snaps_to_nearest() {
        // 10 points (indices 0..=9), plot 0..1000 px wide.
        assert_eq!(pixel_to_index(0.0, 0.0, 1000.0, 10), 0);
        assert_eq!(pixel_to_index(1000.0, 0.0, 1000.0, 10), 9);
        // Middle of the plot → middle index.
        assert_eq!(pixel_to_index(500.0, 0.0, 1000.0, 10), 5);
        // Just past midpoint between indices snaps to the higher one.
        assert_eq!(pixel_to_index(56.0, 0.0, 1000.0, 10), 1);
        assert_eq!(pixel_to_index(54.0, 0.0, 1000.0, 10), 0);
    }

    #[test]
    fn pixel_to_index_clamps_at_bounds() {
        assert_eq!(pixel_to_index(-100.0, 0.0, 1000.0, 5), 0);
        assert_eq!(pixel_to_index(2000.0, 0.0, 1000.0, 5), 4);
    }

    #[test]
    fn pixel_to_index_handles_single_point() {
        assert_eq!(pixel_to_index(500.0, 0.0, 1000.0, 1), 0);
    }

    #[test]
    fn pixel_to_index_respects_plot_left_offset() {
        // Plot starts at x=200 in viewport, 800 wide.
        // Click at viewport x=600 → 50% into the plot → middle index.
        assert_eq!(pixel_to_index(600.0, 200.0, 800.0, 11), 5);
    }

    #[test]
    fn pixel_to_index_handles_zero_width() {
        assert_eq!(pixel_to_index(500.0, 0.0, 0.0, 5), 0);
    }
}
