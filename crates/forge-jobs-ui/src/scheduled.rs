//! Scheduled tab — pending jobs queued for a *future* `scheduled_at`.
//!
//! Distinct from the Cron tab: cron rows are recurring *schedules*
//! that synthesize queue jobs on a cadence; this tab shows one-off
//! `sync_queue` rows where `status='pending'` and
//! `scheduled_at > now`. The Rails analog is
//! `MyJob.set(wait_until: future_time).perform_later(...)` — the
//! job is in the queue, just not eligible to claim yet.
//!
//! Surfaces:
//! - When each scheduled row will fire (relative + absolute).
//! - A per-row "Run now" button that advances `scheduled_at = now()`
//!   so the next worker claim picks it up.

use std::time::Duration;

use chrono::{DateTime, Utc};
use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::ipc::{IpcCtx, JobRow};
use crate::queue_root::RefreshTick;

/// Poll cadence. Scheduled rows change infrequently relative to
/// active jobs; 5s is plenty.
const POLL_INTERVAL_MS: u64 = 5_000;

#[component]
pub fn ScheduledTab() -> impl IntoView {
    let rows = RwSignal::new(Vec::<JobRow>::new());
    let load_err = RwSignal::new(Option::<String>::None);
    let ipc_ctx = expect_context::<IpcCtx>();

    let refresh = {
        let ipc_ctx = ipc_ctx.clone();
        move || {
            let ipc = ipc_ctx.clone();
            spawn_local(async move {
                match ipc.jobs_scheduled(None).await {
                    Ok(list) => {
                        rows.set(list);
                        load_err.set(None);
                    }
                    Err(e) => load_err.set(Some(e.to_string())),
                }
            });
        }
    };

    refresh();
    let refresh_for_tick = refresh.clone();
    let refresh_for_change = refresh.clone();
    let handle = set_interval_with_handle(refresh, Duration::from_millis(POLL_INTERVAL_MS)).ok();
    on_cleanup(move || {
        if let Some(h) = handle {
            h.clear();
        }
    });

    if let Some(RefreshTick(tick)) = use_context::<RefreshTick>() {
        Effect::new(move |_| {
            let _ = tick.get();
            refresh_for_tick();
        });
    }

    let on_change = Callback::new(move |()| {
        refresh_for_change();
        if let Some(RefreshTick(tick)) = use_context::<RefreshTick>() {
            tick.update(|n| *n = n.wrapping_add(1));
        }
    });

    view! {
        <div class="queue-scheduled">
            <header class="queue-scheduled-head">
                <h3>"Scheduled jobs"</h3>
                <p class="queue-scheduled-sub">
                    "Pending rows queued for a future run time \
                     (the Rails `wait_until:` cohort). Distinct from \
                     Cron schedules — these are one-off, not recurring. \
                     Hit \"Run now\" to advance `scheduled_at` to now \
                     so the next worker claim picks it up."
                </p>
            </header>

            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "Scheduled load: " }{ e }</div>
            }) }

            <Show when=move || rows.with(Vec::is_empty) && load_err.with(Option::is_none)>
                <div class="queue-empty">
                    "Nothing scheduled. Enqueue with a future `run_at` \
                     to land here."
                </div>
            </Show>

            <table class="queue-scheduled-table">
                <thead>
                    <tr>
                        <th>"Kind"</th>
                        <th>"Queue"</th>
                        <th>"Scheduled for"</th>
                        <th>"Fires in"</th>
                        <th>"Actions"</th>
                    </tr>
                </thead>
                <tbody>
                    <For
                        each=move || rows.get()
                        key=|r| r.id.clone()
                        children=move |row| view! {
                            <ScheduledRow row=row on_change=on_change />
                        }
                    />
                </tbody>
            </table>
        </div>
    }
}

#[component]
fn ScheduledRow(row: JobRow, #[prop(into)] on_change: Callback<()>) -> impl IntoView {
    let id_for_run = row.id.clone();
    let kind = row.kind.clone();
    let queue = row.queue_name.clone();
    let scheduled_at = row.scheduled_at;

    let on_run_now = move |_| {
        let cb = on_change;
        let id = id_for_run.clone();
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            match ipc.jobs_run_now(&id).await {
                Ok(true) => {
                    leptos::web_sys::console::log_1(&format!("jobs_run_now OK: id={id}").into());
                    cb.run(());
                }
                Ok(false) => {
                    leptos::web_sys::console::warn_1(
                        &format!("jobs_run_now: id={id} no longer pending; ignored").into(),
                    );
                    cb.run(());
                }
                Err(e) => {
                    leptos::web_sys::console::warn_1(
                        &format!("jobs_run_now failed: id={id} err={e}").into(),
                    );
                }
            }
        });
    };

    view! {
        <tr class="queue-scheduled-row">
            <td class="queue-scheduled-kind">{ kind }</td>
            <td class="queue-scheduled-queue">{ queue }</td>
            <td class="queue-scheduled-at" title=scheduled_at.to_rfc3339()>
                { fmt_absolute(scheduled_at) }
            </td>
            <td class="queue-scheduled-relative">{ fmt_relative(scheduled_at) }</td>
            <td class="queue-scheduled-actions">
                <button
                    class="queue-scheduled-run-now"
                    on:click=on_run_now
                    title="Advance scheduled_at to now so the next worker claim picks this up immediately"
                >
                    "Run now"
                </button>
            </td>
        </tr>
    }
}

fn fmt_absolute(t: DateTime<Utc>) -> String {
    // Local-tz YYYY-MM-DD HH:MM. The full RFC3339 is in `title`.
    t.with_timezone(&chrono::Local)
        .format("%Y-%m-%d %H:%M")
        .to_string()
}

fn fmt_relative(t: DateTime<Utc>) -> String {
    let now = Utc::now();
    let delta = (t - now).num_seconds();
    if delta <= 0 {
        return "any moment now".to_owned();
    }
    if delta < 60 {
        return format!("in {delta}s");
    }
    if delta < 3_600 {
        return format!("in {}m", delta / 60);
    }
    if delta < 86_400 {
        return format!("in {}h", delta / 3_600);
    }
    let days = delta / 86_400;
    let hours = (delta % 86_400) / 3_600;
    if hours == 0 {
        format!("in {days}d")
    } else {
        format!("in {days}d {hours}h")
    }
}
