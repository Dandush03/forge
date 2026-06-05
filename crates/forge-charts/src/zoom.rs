//! Drag-to-zoom range selection.
//!
//! Pure data + math; the chart wires it to mouse events. The chart
//! component owns three signals — the committed [`ZoomRange`], the
//! drag-start pixel X (None when no drag is in progress), and the
//! drag-end pixel X — and uses the helpers here to commit a zoom on
//! mouseup, clamp inverted drags, and reject accidentally-tiny ones.

/// A committed zoom range in **data index space**, inclusive at both
/// ends. `from_index` and `to_index` index into the chart's original
/// unsliced data array; the chart slices the rendered view by this
/// range and offsets the hovered index back to the original space
/// when handing it to the tooltip slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoomRange {
    pub from_index: usize,
    pub to_index: usize,
}

impl ZoomRange {
    /// Number of points in this range, inclusive.
    #[must_use]
    pub const fn len(self) -> usize {
        // `to_index >= from_index` after `clamp`; the +1 stays in bounds
        // for usize since we never construct out-of-range values.
        self.to_index - self.from_index + 1
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.from_index > self.to_index
    }

    /// Slice indices to display from the unsliced source. Use to
    /// translate "display index i" → "original index `from + i`".
    #[must_use]
    pub const fn offset_to_original(self, display_index: usize) -> usize {
        self.from_index + display_index
    }
}

/// Minimum span (in data points) that a drag must cover before we
/// commit it as a zoom. Drags shorter than this are treated as a
/// click and ignored. Keeps accidental tiny drags from snapping the
/// chart to a single point.
pub const MIN_ZOOM_SPAN: usize = 2;

/// Build a [`ZoomRange`] from two indices that came out of a drag.
/// Auto-orients (handles right-to-left drags) and clamps to the
/// available range. Returns `None` when the drag is too short to
/// be useful (see [`MIN_ZOOM_SPAN`]).
#[must_use]
pub fn commit_drag(a: usize, b: usize, n_points: usize) -> Option<ZoomRange> {
    if n_points == 0 {
        return None;
    }
    let last = n_points - 1;
    let lo = a.min(b).min(last);
    let hi = a.max(b).min(last);
    if hi - lo + 1 < MIN_ZOOM_SPAN {
        return None;
    }
    Some(ZoomRange {
        from_index: lo,
        to_index: hi,
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "panicking helpers are fine in unit tests; failure is what we want"
    )]
    use super::*;

    #[test]
    fn commit_drag_oriented_lr() {
        let z = commit_drag(2, 5, 10).expect("ok");
        assert_eq!(z.from_index, 2);
        assert_eq!(z.to_index, 5);
        assert_eq!(z.len(), 4);
    }

    #[test]
    fn commit_drag_oriented_rl_normalizes() {
        let z = commit_drag(5, 2, 10).expect("ok");
        assert_eq!(z.from_index, 2);
        assert_eq!(z.to_index, 5);
    }

    #[test]
    fn commit_drag_clamps_to_last_index() {
        let z = commit_drag(0, 999, 10).expect("ok");
        assert_eq!(z.from_index, 0);
        assert_eq!(z.to_index, 9);
    }

    #[test]
    fn commit_drag_rejects_tiny_spans() {
        // Single-point drag (effectively a click) → no zoom.
        assert!(commit_drag(4, 4, 10).is_none());
    }

    #[test]
    fn commit_drag_accepts_min_span() {
        // 2-point span is the smallest allowed.
        let z = commit_drag(3, 4, 10).expect("min span ok");
        assert_eq!(z.len(), 2);
    }

    #[test]
    fn commit_drag_handles_empty_input() {
        assert!(commit_drag(0, 0, 0).is_none());
    }

    #[test]
    fn offset_to_original_shifts_by_from_index() {
        let z = ZoomRange {
            from_index: 7,
            to_index: 12,
        };
        assert_eq!(z.offset_to_original(0), 7);
        assert_eq!(z.offset_to_original(5), 12);
    }
}
