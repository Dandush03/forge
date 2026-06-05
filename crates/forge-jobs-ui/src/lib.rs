#![forbid(unsafe_code)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::doc_markdown,
    clippy::too_long_first_doc_paragraph,
    // Framework mismatch — kept after a Task 9 audit (refactor pass
    // 2026-05-25). Every fire of this lint in this crate is on a
    // `#[component]` fn whose `impl IntoView` flows directly into the
    // parent `view!` macro; dropping the return value is not a code
    // smell here (the macro is what consumes it). The Leptos macro
    // doesn't expand to anything we could annotate `#[must_use]` on
    // ourselves, and adding it on the consumer-facing component fns
    // would be wrong (the discardable thing is the IntoView, not the
    // component handle). Revisit if Leptos ships its own `#[must_use]`
    // story upstream.
    clippy::must_use_candidate,
    // Selection / bulk-action structs wrap Leptos signals that don't
    // surface useful Debug output. Derive would just print opaque IDs.
    missing_debug_implementations
)]

//! Reusable Jobs panel for the
//! [`tech-admin-jobs`](https://github.com/dandush03/tech-admin) queue.
//!
//! Drop-in Leptos CSR panel that ships its own stylesheet and stays
//! framework-agnostic via the [`QueueIpc`] trait. Consumers (Tauri,
//! web, in-process mock) implement the trait once and the panel runs.
//!
//! ## Wiring
//!
//! - Provide an [`IpcCtx`] via Leptos context before rendering the
//!   panel: `provide_context::<IpcCtx>(Rc::new(MyIpc))`.
//! - Inject the bundled stylesheet once at the app root:
//!   `<Stylesheet text=PANEL_CSS />`.
//! - Render `<QueueRoot/>` wherever the panel should live.

// `ipc` stays `pub` — consumers implement `QueueIpc` against the types
// in this module. Everything else is internal UI machinery exposed
// only via the re-exports below; `pub(crate)` so the SemVer surface
// is the named items, not the module layout. The `unreachable_pub`
// allow keeps inner `pub fn` / `pub struct` markers as module-local
// API documentation rather than forcing every item to be `pub(crate)`.
pub mod ipc;

#[allow(unreachable_pub)]
pub(crate) mod bulk_actions;
#[allow(unreachable_pub)]
pub(crate) mod chart_fmt;
#[allow(unreachable_pub)]
pub(crate) mod cron;
#[allow(unreachable_pub)]
pub(crate) mod db_health;
#[allow(unreachable_pub)]
pub(crate) mod failed;
#[allow(unreachable_pub)]
pub(crate) mod inspector;
#[allow(unreachable_pub)]
pub(crate) mod job_table;
#[allow(unreachable_pub)]
pub(crate) mod overview;
#[allow(unreachable_pub)]
pub(crate) mod per_queue;
#[allow(unreachable_pub)]
pub(crate) mod queue_root;
#[allow(unreachable_pub)]
pub(crate) mod resources;
#[allow(unreachable_pub)]
pub(crate) mod scheduled;
#[allow(unreachable_pub)]
pub(crate) mod timeline;

pub use ipc::{
    CleanupReport, CronSchedule, DbHealthBucket, DbHealthHostSeries, IpcCtx, IpcError,
    JOB_STATUSES, JobInspect, JobRow, JobsEnqueueReq, JobsFilter, JobsPage, MetricSeriesBucket,
    QueueIpc, QueueOverview, QueueProcess, ResourceBucket, ResourceHostSeries, StatusCounts,
    TimelineBucket,
};
pub use queue_root::QueueRoot;

/// Default stylesheet bundled with the crate. Inject once at the app
/// root via Leptos `<Stylesheet text=PANEL_CSS />`.
pub const PANEL_CSS: &str = include_str!("panel.css");
