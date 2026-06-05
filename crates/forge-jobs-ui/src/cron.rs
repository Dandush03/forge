//! Cron tab — list, pause/resume, edit expression, and force-fire
//! recurring schedules.
//!
//! Each row is a `cron_schedule` table entry. The cron service in the
//! `tech-admin-jobs` crate evaluates these every CRON_TICK and
//! enqueues matching `sync_queue` rows; this UI is purely the control
//! surface.

use std::time::Duration;

use chrono::{DateTime, Utc};
use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::ipc::{CronSchedule, IpcCtx};
use crate::queue_root::RefreshTick;

const POLL_INTERVAL_MS: u64 = 5_000;

#[component]
pub fn CronTab() -> impl IntoView {
    let rows = RwSignal::new(Vec::<CronSchedule>::new());
    let load_err = RwSignal::new(Option::<String>::None);
    let ipc_ctx = expect_context::<IpcCtx>();

    let refresh = {
        let ipc_ctx = ipc_ctx.clone();
        move || {
            let ipc = ipc_ctx.clone();
            spawn_local(async move {
                match ipc.cron_list().await {
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

    // Refresh-on-tick: any panel mutation refetches here too.
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
        <div class="queue-cron">
            <header class="queue-cron-head">
                <h3>"Recurring schedules"</h3>
                <p class="queue-cron-sub">
                    "Each schedule fires a queue job on its cron cadence. \
                     Pause to stop firing; edit the expression to change \
                     cadence; Run now to enqueue a one-off without \
                     waiting for the next tick."
                </p>
            </header>

            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "Cron load: " }{ e }</div>
            }) }

            <Show when=move || rows.with(Vec::is_empty) && load_err.with(Option::is_none)>
                <div class="queue-empty">
                    "No cron schedules registered yet."
                </div>
            </Show>

            <ul class="queue-cron-list">
                <For
                    each=move || rows.get()
                    key=|r| r.name.clone()
                    children=move |row| view! {
                        <CronRow seed=row rows=rows on_change=on_change />
                    }
                />
            </ul>
        </div>
    }
}

#[component]
fn CronRow(
    /// Snapshot of the row when the `<For>` element was first created.
    /// Used for fields that genuinely don't change (name, kind, queue).
    seed: CronSchedule,
    /// Reactive source of all rows — we look up our row by name on
    /// every read so the UI reflects the latest poll instead of the
    /// stale value captured at component-construction time. Without
    /// this, the `<For>` keeps the same DOM after `cron_set_enabled`
    /// and the Pause/Resume label never flips.
    rows: RwSignal<Vec<CronSchedule>>,
    #[prop(into)] on_change: Callback<()>,
) -> impl IntoView {
    let name = seed.name.clone();
    let name_for_toggle = name.clone();
    let name_for_save = name.clone();
    let name_for_trigger = name.clone();
    let name_for_live = name.clone();
    let name_label = name;
    let kind_label = seed.kind.clone();
    let queue_label = seed
        .queue_name
        .clone()
        .unwrap_or_else(|| "(router)".to_owned());

    // Live view of this specific row. Re-derives whenever `rows`
    // updates, so every reactive read below sees the latest server
    // state without us forcing a new key on the `<For>`.
    let live =
        Memo::new(move |_| rows.with(|all| all.iter().find(|r| r.name == name_for_live).cloned()));
    let enabled = Memo::new(move |_| live.with(|l| l.as_ref().is_some_and(|r| r.enabled)));
    let live_expr = Memo::new(move |_| {
        live.with(|l| l.as_ref().map_or(String::new(), |r| r.cron_expr.clone()))
    });
    let next_fire_label = Memo::new(move |_| {
        relative_label_opt(
            live.with(|l| l.as_ref().and_then(|r| r.next_fire_at)),
            "overdue",
            "in",
        )
    });
    let last_fire_label = Memo::new(move |_| {
        relative_label_opt(
            live.with(|l| l.as_ref().and_then(|r| r.last_fired_at)),
            "ago",
            "in",
        )
    });
    let live_last_error =
        Memo::new(move |_| live.with(|l| l.as_ref().and_then(|r| r.last_error.clone())));

    let expr_input = RwSignal::new(seed.cron_expr);
    let save_err = RwSignal::new(Option::<String>::None);
    let pending_save = RwSignal::new(false);
    // Two transient signals so each button can show its own
    // "Pausing…/Resuming…/Running…" label independently of the others.
    let pending_toggle = RwSignal::new(false);
    let pending_run = RwSignal::new(false);
    // Flashes briefly after a successful Run-now so the user has a
    // visible "yes, it fired" beat before the row state refreshes.
    let run_flash = RwSignal::new(false);

    let on_toggle = move |_| {
        if pending_toggle.get_untracked() {
            return;
        }
        let target = !enabled.get_untracked();
        let name = name_for_toggle.clone();
        let ipc = expect_context::<IpcCtx>();
        pending_toggle.set(true);
        spawn_local(async move {
            if let Err(e) = ipc.cron_set_enabled(&name, target).await {
                leptos::web_sys::console::warn_1(&format!("cron_set_enabled failed: {e}").into());
            } else {
                on_change.run(());
            }
            pending_toggle.set(false);
        });
    };

    let on_save = move |_| {
        let new_expr = expr_input.get();
        // Skip the round-trip when the input still matches whatever the
        // server has — compares against the live value, not a stale
        // snapshot taken at row construction.
        if new_expr.trim() == live_expr.get_untracked().trim() {
            return;
        }
        let name = name_for_save.clone();
        let ipc = expect_context::<IpcCtx>();
        pending_save.set(true);
        save_err.set(None);
        spawn_local(async move {
            match ipc.cron_set_expr(&name, &new_expr).await {
                Ok(()) => {
                    save_err.set(None);
                    on_change.run(());
                }
                Err(e) => save_err.set(Some(e.to_string())),
            }
            pending_save.set(false);
        });
    };

    let on_run_now = move |_| {
        if pending_run.get_untracked() {
            return;
        }
        let name = name_for_trigger.clone();
        let ipc = expect_context::<IpcCtx>();
        pending_run.set(true);
        spawn_local(async move {
            if let Err(e) = ipc.cron_trigger_now(&name).await {
                leptos::web_sys::console::warn_1(&format!("cron_trigger_now failed: {e}").into());
            } else {
                run_flash.set(true);
                on_change.run(());
                // Clear the "Queued ✓" label after a brief beat so
                // the button returns to its idle state.
                leptos::leptos_dom::helpers::set_timeout(
                    move || run_flash.set(false),
                    std::time::Duration::from_millis(1500),
                );
            }
            pending_run.set(false);
        });
    };

    let dot_class = move || {
        if enabled.get() {
            "queue-cron-dot is-on"
        } else {
            "queue-cron-dot is-off"
        }
    };

    view! {
        <li class="queue-cron-row" class:is-paused=move || !enabled.get()>
            <header class="queue-cron-row-head">
                <span class=dot_class></span>
                <span class="queue-cron-name">{ name_label }</span>
                <span class="queue-cron-meta">
                    { format!("kind: {kind_label} · queue: {queue_label}") }
                </span>
                <button
                    class="queue-cron-toggle"
                    class:is-paused=move || !enabled.get()
                    on:click=on_toggle
                    disabled=move || pending_toggle.get()
                    title=move || if enabled.get() {
                        "Pause this schedule"
                    } else {
                        "Resume this schedule"
                    }
                >
                    { move || {
                        let busy = pending_toggle.get();
                        let on = enabled.get();
                        match (busy, on) {
                            (true, true) => "Pausing…",
                            (true, false) => "Resuming…",
                            (false, true) => "Pause",
                            (false, false) => "Resume",
                        }
                    } }
                </button>
                <button
                    class="queue-cron-run"
                    on:click=on_run_now
                    disabled=move || pending_run.get()
                    title="Fire one job now and advance next_fire_at"
                >
                    { move || if run_flash.get() {
                        "Queued ✓"
                    } else if pending_run.get() {
                        "Running…"
                    } else {
                        "Run now"
                    } }
                </button>
            </header>

            <div class="queue-cron-expr">
                <label class="queue-cron-expr-label">"cron expr"</label>
                <input
                    class="queue-cron-expr-input"
                    type="text"
                    spellcheck="false"
                    prop:value=move || expr_input.get()
                    on:input=move |ev| expr_input.set(event_target_value(&ev))
                />
                <button
                    class="queue-cron-save"
                    on:click=on_save
                    disabled=move || pending_save.get()
                >
                    { move || if pending_save.get() { "Saving…" } else { "Save" } }
                </button>
            </div>

            { move || save_err.get().map(|e| view! {
                <div class="queue-cron-save-err">{ "Save failed: " }{ e }</div>
            }) }

            <footer class="queue-cron-row-foot">
                <span class="queue-cron-fire">
                    { move || format!("next: {} · last: {}", next_fire_label.get(), last_fire_label.get()) }
                </span>
                { move || live_last_error.get().map(|msg| {
                    let title = msg.clone();
                    view! {
                        <span class="queue-cron-err" title=title>
                            { format!("⚠ {}", truncate(&msg, 80)) }
                        </span>
                    }
                }) }
            </footer>
        </li>
    }
}

fn relative_label_opt(
    when: Option<DateTime<Utc>>,
    past_suffix: &str,
    future_prefix: &str,
) -> String {
    when.map_or_else(
        || "—".to_owned(),
        |t| {
            let secs = (t - Utc::now()).num_seconds();
            if secs.abs() < 5 {
                "just now".to_owned()
            } else if secs > 0 {
                format!("{future_prefix} {}", short_duration(secs))
            } else {
                format!("{} {past_suffix}", short_duration(-secs))
            }
        },
    )
}

fn short_duration(secs: i64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3_600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3_600)
    } else {
        format!("{}d", secs / 86_400)
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_owned()
    } else {
        let mut out: String = s.chars().take(max - 1).collect();
        out.push('…');
        out
    }
}
