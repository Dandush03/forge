//! SVG path generation for area + line series.
//!
//! Straight-line connections only in v0.1; smooth Bezier curves
//! (Catmull-Rom → cubic Bezier conversion) is a planned follow-up.
//! Both APIs already produce SVG-valid path strings — switching to
//! smooth later won't change the public surface.

use crate::scale::{index_to_x, value_to_y};

/// Build the `d` attribute for a closed area path that fills the
/// region under the value curve down to the baseline.
///
/// Output shape: `M x0,y0 L x1,y1 … L xN,yN L xN,height L x0,height Z`.
/// `width × height` is the plot's viewBox in SVG units.
#[must_use]
pub fn area_path(values: &[f64], max: f64, width: f64, height: f64) -> String {
    if values.is_empty() {
        return String::new();
    }
    let n = values.len();
    let mut s = String::with_capacity(values.len() * 16);
    for (i, &v) in values.iter().enumerate() {
        let x = index_to_x(i, n, width);
        let y = value_to_y(v, max, height);
        if i == 0 {
            s.push_str(&format!("M {x:.2},{y:.2}"));
        } else {
            s.push_str(&format!(" L {x:.2},{y:.2}"));
        }
    }
    // Close the area: drop to baseline at the rightmost x, then to
    // baseline at the leftmost x, then close.
    let last_x = index_to_x(n - 1, n, width);
    s.push_str(&format!(" L {last_x:.2},{height:.2}"));
    s.push_str(&format!(" L 0.00,{height:.2}"));
    s.push_str(" Z");
    s
}

/// Build the `d` attribute for the stroke-only line that draws the
/// curve's top edge. Same points as the area path but no closure.
///
/// Output shape: `M x0,y0 L x1,y1 … L xN,yN`.
#[must_use]
pub fn line_path(values: &[f64], max: f64, width: f64, height: f64) -> String {
    if values.is_empty() {
        return String::new();
    }
    let n = values.len();
    let mut s = String::with_capacity(values.len() * 16);
    for (i, &v) in values.iter().enumerate() {
        let x = index_to_x(i, n, width);
        let y = value_to_y(v, max, height);
        if i == 0 {
            s.push_str(&format!("M {x:.2},{y:.2}"));
        } else {
            s.push_str(&format!(" L {x:.2},{y:.2}"));
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn area_path_closes_back_to_baseline() {
        let p = area_path(&[1.0, 2.0, 1.0], 2.0, 100.0, 50.0);
        // Should end with the baseline-down moves and a Z closure.
        assert!(p.ends_with(" L 100.00,50.00 L 0.00,50.00 Z"), "got: {p}");
        // Starts with a Move command at x=0.
        assert!(p.starts_with("M 0.00,"), "got: {p}");
    }

    #[test]
    fn area_path_empty_returns_empty() {
        assert_eq!(area_path(&[], 1.0, 100.0, 50.0), "");
    }

    #[test]
    fn line_path_visits_all_points_without_closing() {
        let p = line_path(&[0.0, 5.0, 10.0], 10.0, 200.0, 100.0);
        // No Z closure.
        assert!(!p.contains('Z'));
        // Starts at top-left baseline (value=0 → y=height).
        assert!(p.starts_with("M 0.00,100.00"), "got: {p}");
        // Ends at top-right (value=max → y=0).
        assert!(p.ends_with("L 200.00,0.00"), "got: {p}");
    }

    #[test]
    fn line_path_single_point_renders_a_move_only() {
        let p = line_path(&[5.0], 10.0, 200.0, 100.0);
        assert!(p.starts_with('M'));
        assert!(!p.contains(" L"));
    }
}
