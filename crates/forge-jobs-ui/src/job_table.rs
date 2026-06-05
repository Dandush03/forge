//! Filterable, paged job table.

use std::time::Duration;

use chrono::Local;
use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::bulk_actions::{BulkAction, BulkActionsBar, SelectionState};
use crate::ipc::{IpcCtx, JOB_STATUSES, JobsFilter, JobsPage};
use crate::queue_root::{PollIntervalMs, RefreshTick};

/// Fallback cadence when the panel-wide `PollIntervalMs` context
/// isn't provided (only happens if the panel is mounted without
/// `QueueRoot` — e.g. embedded directly in a test harness).
const FALLBACK_POLL_INTERVAL_MS: u64 = 5_000;
const PAGE_SIZE: u32 = 50;

#[component]
pub fn JobTable(
    #[prop(into)] on_inspect: Callback<String>,
    #[prop(into)] on_change: Callback<()>,
) -> impl IntoView {
    let queue_filter = RwSignal::new(String::new());
    let kind_filter = RwSignal::new(String::new());
    let status_filter = RwSignal::new(String::new());
    let search = RwSignal::new(String::new());

    let offset = RwSignal::new(0_u32);
    let page = RwSignal::new(Option::<JobsPage>::None);
    let load_err = RwSignal::new(Option::<String>::None);
    let kinds = RwSignal::new(Vec::<String>::new());
    let queues = RwSignal::new(Vec::<String>::new());
    let last_refresh_at = RwSignal::new(Option::<chrono::DateTime<chrono::Utc>>::None);
    let selection = SelectionState::default();

    // Untracked reads: subscription to filter changes lives in the
    // dedicated Effect below. `build_filter` is called from the
    // periodic timer too (no reactive context) — tracked reads there
    // emit Leptos warnings and have no useful subscriber.
    let build_filter = move || {
        let mut f = JobsFilter::default();
        if !queue_filter.with_untracked(String::is_empty) {
            f.queues = vec![queue_filter.get_untracked()];
        }
        if !kind_filter.with_untracked(String::is_empty) {
            f.kinds = vec![kind_filter.get_untracked()];
        }
        if !status_filter.with_untracked(String::is_empty) {
            f.statuses = vec![status_filter.get_untracked()];
        }
        let s = search.get_untracked();
        if !s.trim().is_empty() {
            f.payload_search = Some(s);
        }
        f
    };

    // Capture IPC handle at component mount — setInterval callbacks
    // don't preserve the Leptos owner scope, so use_context inside the
    // refresh closure would panic.
    let ipc_ctx = expect_context::<IpcCtx>();

    let refresh = {
        let ipc_ctx = ipc_ctx.clone();
        move || {
            leptos::web_sys::console::log_1(
                &format!(
                    "JobTable.refresh fired @ {}",
                    chrono::Utc::now().format("%H:%M:%S%.3f")
                )
                .into(),
            );
            let f = build_filter();
            let off = offset.get_untracked();
            let ipc = ipc_ctx.clone();
            spawn_local(async move {
                match ipc.jobs_list(f, PAGE_SIZE, off).await {
                    Ok(p) => {
                        let row_count = p.rows.len();
                        let total = p.total;
                        page.set(Some(p));
                        last_refresh_at.set(Some(chrono::Utc::now()));
                        load_err.set(None);
                        leptos::web_sys::console::log_1(
                            &format!("JobTable.refresh OK: rows={row_count} total={total}").into(),
                        );
                    }
                    Err(e) => {
                        leptos::web_sys::console::warn_1(
                            &format!("JobTable.refresh FAILED: {e}").into(),
                        );
                        load_err.set(Some(e.to_string()));
                    }
                }
            });
        }
    };

    let refresh_for_filters = refresh.clone();
    Effect::new(move |_| {
        let _ = queue_filter.get();
        let _ = kind_filter.get();
        let _ = status_filter.get();
        let _ = search.get();
        offset.set(0);
        refresh_for_filters();
    });

    // Subscribe to the panel-wide refresh tick so mutations elsewhere
    // (Enqueue demo, bulk actions, cron edits) trigger an immediate
    // refresh here without waiting on the 5s poll.
    if let Some(RefreshTick(tick)) = use_context::<RefreshTick>() {
        let refresh_for_tick = refresh.clone();
        Effect::new(move |_| {
            let _ = tick.get();
            refresh_for_tick();
        });
    }

    refresh();

    // Simple periodic poll. Reads the panel's `PollIntervalMs` once
    // at mount via `get_untracked` — the previous Effect-driven
    // approach was meant to re-create the timer on dropdown change
    // but broke the interval entirely (no refreshes fired at all).
    // The dynamic-rebind belongs in a follow-up; for now picking a
    // new cadence in the header requires re-opening the tab.
    let poll_ms_initial =
        use_context::<PollIntervalMs>().map_or(FALLBACK_POLL_INTERVAL_MS, |p| p.0.get_untracked());
    let refresh_for_bar = refresh.clone();
    let refresh_for_prev = refresh.clone();
    let refresh_for_next = refresh.clone();
    let poll = if poll_ms_initial > 0 {
        set_interval_with_handle(refresh, Duration::from_millis(poll_ms_initial)).ok()
    } else {
        None
    };
    on_cleanup(move || {
        if let Some(h) = poll {
            h.clear();
        }
    });

    // Populate filter dropdowns once.
    {
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            if let Ok(ks) = ipc.jobs_kinds().await {
                kinds.set(ks);
            }
            if let Ok(qs) = ipc.queue_overview().await {
                queues.set(qs.into_iter().map(|q| q.name).collect());
            }
        });
    }
    let bar = BulkActionsBar {
        selection,
        on_done: Callback::new(move |()| {
            refresh_for_bar();
            on_change.run(());
        }),
    };

    let on_clear_filters = move |_| {
        queue_filter.set(String::new());
        kind_filter.set(String::new());
        status_filter.set(String::new());
        search.set(String::new());
    };

    let on_prev = move |_| {
        offset.update(|o| *o = o.saturating_sub(PAGE_SIZE));
        refresh_for_prev();
    };
    let on_next = move |_| {
        if let Some(p) = page.get() {
            let next = offset.get_untracked() + PAGE_SIZE;
            if u64::from(next) < p.total {
                offset.set(next);
                refresh_for_next();
            }
        }
    };

    // Bulk-purge button: label + behavior track the active status
    // sub-tab. "All" falls back to the original retention-bypass purge
    // of `done` rows (the safety-rail default); any specific status
    // routes through `jobs_delete_by_status` so e.g. picking the
    // "Pending" sub-tab gives a "Purge pending" button that nukes
    // every pending row.
    let on_purge = move |_| {
        let status = status_filter.get_untracked();
        let (label, message) = if status.is_empty() {
            (
                "done".to_owned(),
                "Delete every job with status `done`?\n\n\
                 This bypasses the per-queue retention window.\n\
                 `failed` and `dead` rows are not touched."
                    .to_owned(),
            )
        } else {
            let pretty = pretty_status(&status).to_lowercase();
            (
                pretty.clone(),
                format!(
                    "Delete every job with status `{status}`?\n\n\
                     This affects ALL `{pretty}` rows across every \
                     queue — not just the rows visible on screen."
                ),
            )
        };
        let confirmed = leptos::web_sys::window()
            .and_then(|w| w.confirm_with_message(&message).ok())
            .unwrap_or(false);
        if !confirmed {
            return;
        }
        let ipc = expect_context::<IpcCtx>();
        let tick = use_context::<RefreshTick>();
        spawn_local(async move {
            let result = if status.is_empty() {
                ipc.jobs_delete_done_older_than(0).await
            } else {
                ipc.jobs_delete_by_status(&status).await
            };
            match result {
                Ok(_) => {
                    // Local refresh will happen via the periodic poll
                    // or by bumping the tick (which our refresh-on-tick
                    // Effect subscribes to).
                    if let Some(RefreshTick(t)) = tick {
                        t.update(|n| *n = n.wrapping_add(1));
                    }
                }
                Err(e) => {
                    leptos::web_sys::console::warn_1(
                        &format!("purge {label} failed: {e}").into(),
                    );
                }
            }
        });
    };

    view! {
        <section class="queue-jobs">
            <header class="queue-jobs-head">
                <h3>"All jobs"</h3>
                { move || page.with(|p| p.as_ref().map(|p| view! {
                    <span class="queue-jobs-total">{ format!("{} matching", p.total) }</span>
                })) }
                <span class="queue-jobs-refresh" title="When the table last successfully re-fetched from the host">
                    { move || last_refresh_at.get().map_or_else(
                        || "refreshing…".to_owned(),
                        |t| format!("refreshed {}",
                            t.with_timezone(&Local).format("%H:%M:%S")),
                    ) }
                </span>
                <button class="queue-jobs-clear" on:click=on_clear_filters>"Clear filters"</button>
                <button
                    class="queue-jobs-purge"
                    on:click=on_purge
                    title="Delete every row matching the active status sub-tab (or done rows when All is selected)"
                >
                    { move || {
                        let s = status_filter.get();
                        if s.is_empty() {
                            "Purge done".to_owned()
                        } else {
                            format!("Purge {}", pretty_status(&s).to_lowercase())
                        }
                    } }
                </button>
            </header>

            <nav class="queue-jobs-subtabs">
                <button
                    class="queue-subtab"
                    class:active=move || status_filter.with(String::is_empty)
                    on:click=move |_| status_filter.set(String::new())
                >"All"</button>
                { JOB_STATUSES.iter().map(|s| {
                    let s_value = (*s).to_owned();
                    let s_for_sel = (*s).to_owned();
                    let label = pretty_status(s);
                    view! {
                        <button
                            class="queue-subtab"
                            class:active=move || status_filter.with(|cur| cur == &s_for_sel)
                            on:click=move |_| status_filter.set(s_value.clone())
                        >{ label }</button>
                    }
                }).collect_view() }
            </nav>

            <div class="queue-jobs-filters">
                <select
                    class="queue-filter"
                    on:change=move |ev| queue_filter.set(event_target_value(&ev))
                >
                    <option value="" selected=move || queue_filter.with(String::is_empty)>"All queues"</option>
                    <For
                        each=move || queues.get()
                        key=|q| q.clone()
                        children=move |q| {
                            let q_value = q.clone();
                            let q_label = q.clone();
                            let q_for_sel = q;
                            view! {
                                <option
                                    value=q_value
                                    selected=move || queue_filter.with(|cur| cur == &q_for_sel)
                                >{ q_label }</option>
                            }
                        }
                    />
                </select>

                <select
                    class="queue-filter"
                    on:change=move |ev| kind_filter.set(event_target_value(&ev))
                >
                    <option value="" selected=move || kind_filter.with(String::is_empty)>"All kinds"</option>
                    <For
                        each=move || kinds.get()
                        key=|k| k.clone()
                        children=move |k| {
                            let k_value = k.clone();
                            let k_label = k.clone();
                            let k_for_sel = k;
                            view! {
                                <option
                                    value=k_value
                                    selected=move || kind_filter.with(|cur| cur == &k_for_sel)
                                >{ k_label }</option>
                            }
                        }
                    />
                </select>

                <input
                    class="queue-filter-search"
                    type="search"
                    placeholder="Payload search…"
                    prop:value=move || search.get()
                    on:input=move |ev| search.set(event_target_value(&ev))
                />
            </div>

            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "Jobs load: " }{ e }</div>
            }) }

            <table class="queue-jobs-table">
                <thead>
                    <tr>
                        <th class="queue-jobs-checkcol">
                            <input
                                type="checkbox"
                                title="Toggle all on this page"
                                prop:checked=move || {
                                    page.with(|p| p.as_ref().is_some_and(|p|
                                        !p.rows.is_empty()
                                            && p.rows.iter().all(|r| selection.contains(&r.id))))
                                }
                                on:change=move |ev| {
                                    let on = event_target_checked(&ev);
                                    if on {
                                        if let Some(p) = page.get() {
                                            selection.set_all(p.rows.iter().map(|r| r.id.clone()));
                                        }
                                    } else {
                                        selection.clear();
                                    }
                                }
                            />
                        </th>
                        <th>"Queue"</th>
                        <th>"Kind"</th>
                        <th>"Status"</th>
                        <th>"Attempts"</th>
                        <th>"Heartbeat"</th>
                        <th>"Enqueued"</th>
                    </tr>
                </thead>
                <tbody>
                    { move || page.with(|p| {
                        let rows = p.as_ref().map(|p| p.rows.clone()).unwrap_or_default();
                        rows.into_iter().map(|row| {
                            let id_for_check = row.id.clone();
                            let id_for_check2 = row.id.clone();
                            let id_for_toggle = row.id.clone();
                            let id_for_click = row.id.clone();
                            let status_class = format!("queue-badge is-{}", row.status);
                            let attempts_label = format!("{}/{}", row.attempts, row.max_attempts);
                            let enqueued_label = row
                                .enqueued_at
                                .with_timezone(&Local)
                                .format("%H:%M:%S")
                                .to_string();
                            let (hb_label, hb_class) = heartbeat_cell(&row);
                            let kind_text = row.kind.clone();
                            let status_text = row.status.clone();
                            let queue_text = row.queue_name.clone();
                            view! {
                                <tr
                                    class="queue-jobs-row"
                                    class:selected=move || selection.contains(&id_for_check)
                                    on:click=move |_| on_inspect.run(id_for_click.clone())
                                >
                                    <td
                                        class="queue-jobs-checkcol"
                                        on:click=|ev: leptos::ev::MouseEvent| ev.stop_propagation()
                                    >
                                        <input
                                            type="checkbox"
                                            prop:checked=move || selection.contains(&id_for_check2)
                                            on:change=move |_| selection.toggle(&id_for_toggle)
                                        />
                                    </td>
                                    <td>{ queue_text }</td>
                                    <td><code>{ kind_text }</code></td>
                                    <td><span class=status_class>{ status_text }</span></td>
                                    <td>{ attempts_label }</td>
                                    <td><span class=hb_class>{ hb_label }</span></td>
                                    <td>{ enqueued_label }</td>
                                </tr>
                            }
                        }).collect_view()
                    }) }
                </tbody>
            </table>

            <footer class="queue-jobs-foot">
                { paging_view(page, offset, on_prev, on_next) }
                { bulk_view(selection, bar) }
            </footer>
        </section>
    }
}

fn paging_view(
    page: RwSignal<Option<JobsPage>>,
    offset: RwSignal<u32>,
    on_prev: impl Fn(leptos::ev::MouseEvent) + 'static,
    on_next: impl Fn(leptos::ev::MouseEvent) + 'static,
) -> impl IntoView {
    view! {
        <div class="queue-jobs-paging">
            <button class="queue-page-btn" on:click=on_prev disabled=move || offset.get() == 0>"← Prev"</button>
            <span class="queue-page-label">
                { move || page.with(|p| p.as_ref().map(|p| {
                    let from = p.offset + 1;
                    let to = (p.offset + p.rows.len() as u32).min(p.total as u32);
                    format!("{from}–{to} of {}", p.total)
                }).unwrap_or_default()) }
            </span>
            <button class="queue-page-btn" on:click=on_next disabled=move || page.with(|p| {
                p.as_ref().is_none_or(|p|
                    u64::from(offset.get() + PAGE_SIZE) >= p.total)
            })>"Next →"</button>
        </div>
    }
}

fn bulk_view(selection: SelectionState, bar: BulkActionsBar) -> impl IntoView {
    let active = leptos::prelude::Signal::derive(move || selection.count() > 0);
    view! {
        <div class="queue-jobs-bulk" class:active=active>
            <span class="queue-jobs-selected">
                { move || format!("Selected {}", selection.count()) }
            </span>
            <button
                class="queue-bulk-btn"
                on:click=move |_| bar.run(BulkAction::Retry)
                disabled=move || selection.count() == 0
            >"Retry"</button>
            <button
                class="queue-bulk-btn"
                on:click=move |_| bar.run(BulkAction::Requeue)
                disabled=move || selection.count() == 0
            >"Requeue"</button>
            <button
                class="queue-bulk-btn is-danger"
                on:click=move |_| bar.run(BulkAction::Delete)
                disabled=move || selection.count() == 0
            >"Delete"</button>
        </div>
    }
}

/// `(label, css_class)` for the per-row heartbeat cell.
///
/// `in_progress` rows show the time since the last heartbeat, in
/// red if the worker has gone stale (≥ STALE_HEARTBEAT_SECS).
/// Terminal statuses (done/failed/dead) show "—". This is what makes
/// a stuck job visible: a row that's been "in_progress · ♥ 5m" is
/// the orphan signal the reaper hasn't gotten to yet.
fn heartbeat_cell(row: &crate::ipc::JobRow) -> (String, &'static str) {
    const STALE_HEARTBEAT_SECS: i64 = 60;
    if row.status != "in_progress" {
        return ("—".to_owned(), "queue-hb is-idle");
    }
    let Some(hb) = row.heartbeat_at else {
        return ("♥ —".to_owned(), "queue-hb is-warn");
    };
    let age = (chrono::Utc::now() - hb).num_seconds().max(0);
    let label = if age < 60 {
        format!("♥ {age}s")
    } else if age < 3_600 {
        format!("♥ {}m", age / 60)
    } else {
        format!("♥ {}h", age / 3_600)
    };
    let class = if age >= STALE_HEARTBEAT_SECS {
        "queue-hb is-stale"
    } else {
        "queue-hb is-ok"
    };
    (label, class)
}

/// Display labels for the status sub-tabs. Maps the underscore-cased
/// schema values to title case.
fn pretty_status(s: &str) -> &'static str {
    match s {
        "pending" => "Pending",
        "in_progress" => "In progress",
        "done" => "Done",
        "failed" => "Failed",
        "dead" => "Dead",
        _ => "?",
    }
}
