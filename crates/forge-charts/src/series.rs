//! Series metadata. Drives the legend + per-series CSS color hooks.

/// What kind of geometry a [`Series`] renders.
///
/// v0.1 only supports `Area`; `Line` and `Bar` are placeholder
/// variants the API will need when those chart types ship. Pattern-
/// matching `non_exhaustive` so adding variants later isn't a
/// breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SeriesKind {
    /// Filled area under the curve. Used by [`crate::AreaChart`].
    Area,
}

/// One series in a chart. Carries display name + a CSS-class
/// fragment consumers use to theme color.
///
/// The `color_class` becomes part of the CSS class on the rendered
/// SVG group: `charts-series charts-series-<color_class>`. Consumers
/// hook into that with their own CSS (or set `--charts-series-<color>`
/// variables that the bundled stylesheet reads).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Series {
    pub name: String,
    pub color_class: String,
    pub kind: SeriesKind,
}

impl Series {
    /// Filled area series. The most common factory.
    #[must_use]
    pub fn area(name: impl Into<String>, color_class: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            color_class: color_class.into(),
            kind: SeriesKind::Area,
        }
    }
}
