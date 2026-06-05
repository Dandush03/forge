//! `QueueRoot` — top-level Jobs panel with an internal tab nav.
//!
//! Six tabs map to distinct surfaces so users never see all the queue
//! state at once:
//!
//! - **Overview** — 24h timeline + cross-queue summary counts + live
//!   worker processes ("what's running right now?").
//! - **Jobs** — full filterable/searchable job table with bulk actions.
//! - **Retries** — first-class view of jobs mid-retry (`status =
//!   "failed"`).
//! - **Dead** — terminal failures (`status = "dead"`) with a retry-all.
//! - **Cron** — recurring schedules. Placeholder until the cron-to-queue
//!   migration lands.
//! - **Queues** — per-queue control surface (workers, pause, retention).
//!
//! Consumers render `<QueueRoot/>` inside their own container. Before
//! rendering, provide an [`IpcCtx`] via Leptos context:
//!
//! ```ignore
//! provide_context::<forge_jobs_ui::IpcCtx>(
//!     Rc::new(MyTauriQueueIpc) as Rc<dyn forge_jobs_ui::QueueIpc>,
//! );
//! ```
//!
//! And inject the stylesheet once at the app root:
//!
//! ```ignore
//! view! { <Stylesheet text=forge_jobs_ui::PANEL_CSS /> }
//! ```

use std::time::Duration;

use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::cron::CronTab;
use crate::db_health::DbHealthPanel;
use crate::failed::{FailedMode, FailedPanel};
use crate::inspector::Inspector;
use crate::ipc::{IpcCtx, QueueOverview, StatusCounts};
use crate::job_table::JobTable;
use crate::overview::QueueCard;
use crate::per_queue::PerQueueMetrics;
use crate::resources::ResourcesPanel;
use crate::scheduled::ScheduledTab;
use crate::timeline::Timeline;

/// Initial poll cadence for the Overview tab's `queue_overview` fetch.
/// Users override this via the header's refresh-interval selector;
/// other tabs run on their own pollers (see Cron tab @ 5s, JobTable
/// @ 5s, etc.).
const DEFAULT_POLL_INTERVAL_MS: u64 = 2_000;

/// Refresh-interval choices in the header dropdown. `0` = manual
/// (no auto-poll).
const INTERVAL_CHOICES: &[(u64, &str)] = &[
    (0, "Off"),
    (1_000, "1s"),
    (2_000, "2s"),
    (5_000, "5s"),
    (10_000, "10s"),
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Overview,
    Jobs,
    Scheduled,
    Retries,
    Dead,
    Cron,
    Queues,
}

impl Tab {
    const ALL: [Self; 7] = [
        Self::Overview,
        Self::Jobs,
        Self::Scheduled,
        Self::Retries,
        Self::Dead,
        Self::Cron,
        Self::Queues,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Jobs => "Jobs",
            Self::Scheduled => "Scheduled",
            Self::Retries => "Retries",
            Self::Dead => "Dead",
            Self::Cron => "Cron",
            Self::Queues => "Queues",
        }
    }
}

/// Bumped after any mutation (demo enqueue, bulk action, cron edit).
/// Tabs subscribe to it via context to refresh immediately rather
/// than waiting for the next periodic poll.
#[derive(Clone, Copy, Debug)]
pub struct RefreshTick(pub RwSignal<u32>);

/// Panel-wide polling cadence in milliseconds. Tabs read this via
/// context so the header's refresh dropdown controls all live views,
/// not just the Overview's queue card list. `0` = manual (no
/// periodic poll; mutations still trigger refresh via RefreshTick).
#[derive(Clone, Copy, Debug)]
pub struct PollIntervalMs(pub RwSignal<u64>);

#[component]
pub fn QueueRoot() -> impl IntoView {
    let queues = RwSignal::new(Vec::<QueueOverview>::new());
    let load_err = RwSignal::new(Option::<String>::None);
    // True while a card input/select is focused. The poll skips
    // applying fresh rows during edits so a 2s tick doesn't rebuild the
    // cards and wipe a value the operator is half-way through typing.
    let editing = RwSignal::new(false);
    let tab = RwSignal::new(Tab::Overview);
    let poll_ms = RwSignal::new(DEFAULT_POLL_INTERVAL_MS);
    let refresh_tick = RwSignal::new(0_u32);
    provide_context(RefreshTick(refresh_tick));
    provide_context(PollIntervalMs(poll_ms));

    // Cache the IPC handle at component mount, where the Leptos owner
    // scope is present. `use_context` inside a setInterval callback
    // returns None — the timer fires outside any Leptos reactive
    // scope — which is what caused the `expect("QueueIpc context")`
    // panic every poll tick.
    let ipc = expect_context::<IpcCtx>();

    let refresh = {
        let ipc = ipc.clone();
        move || {
            let ipc = ipc.clone();
            spawn_local(async move {
                match ipc.queue_overview().await {
                    Ok(rows) => {
                        // Don't clobber inputs the operator is editing —
                        // applying fresh rows rebuilds the cards and
                        // resets every `prop:value`. Counts freeze for
                        // the brief edit window, then catch up on blur.
                        if !editing.get_untracked() {
                            queues.set(rows);
                        }
                        load_err.set(None);
                    }
                    Err(e) => load_err.set(Some(e.to_string())),
                }
            });
        }
    };

    refresh();

    // Reactive timer: re-creates the interval whenever the user picks
    // a new cadence. The previous handle is cleared first so the two
    // never overlap. `0` disables the auto-poll entirely.
    let interval_slot =
        StoredValue::new(Option::<leptos::leptos_dom::helpers::IntervalHandle>::None);
    let refresh_for_timer = refresh.clone();
    Effect::new(move |_| {
        let ms = poll_ms.get();
        let refresh = refresh_for_timer.clone();
        interval_slot.update_value(|slot| {
            if let Some(prev) = slot.take() {
                prev.clear();
            }
            if ms == 0 {
                return;
            }
            if let Ok(h) = set_interval_with_handle(refresh, Duration::from_millis(ms)) {
                *slot = Some(h);
            }
        });
    });
    on_cleanup(move || {
        interval_slot.update_value(|slot| {
            if let Some(h) = slot.take() {
                h.clear();
            }
        });
    });

    let on_enqueue_demo = {
        let ipc = ipc.clone();
        let refresh = refresh.clone();
        move |_| {
            let ipc = ipc.clone();
            let refresh = refresh.clone();
            spawn_local(async move {
                match ipc.queue_enqueue_demo(serde_json::json!({})).await {
                    Ok(_) => {
                        refresh_tick.update(|n| *n = n.wrapping_add(1));
                        refresh();
                    }
                    Err(e) => {
                        leptos::web_sys::console::warn_1(
                            &format!("queue_enqueue_demo failed: {e}").into(),
                        );
                    }
                }
            });
        }
    };

    let selected = RwSignal::new(Option::<String>::None);
    let on_inspect = Callback::new(move |id: String| selected.set(Some(id)));
    // Bump the global RefreshTick so every panel that subscribes
    // refreshes immediately, then re-fetch the queue overview locally.
    // Without the tick, mutations in (say) FailedPanel would only show
    // up here after the FailedPanel's own 5 s timer fires.
    let on_change = Callback::new(move |()| {
        refresh_tick.update(|n| *n = n.wrapping_add(1));
        refresh();
    });

    view! {
        <div class="queue-panel">
            <header class="queue-panel-head">
                <h2>"Jobs"</h2>
                <p class="queue-panel-sub">
                    "Queue worker pool, per-queue control, retry / dead surfaces, \
                     cron schedules, and a filterable job table. Click any row to inspect."
                </p>
                <div class="queue-panel-actions">
                    <label class="queue-poll-control" title="How often to re-fetch live state from the host">
                        "Refresh:"
                        <select
                            class="queue-poll-select"
                            on:change=move |ev| {
                                if let Ok(n) = event_target_value(&ev).parse::<u64>() {
                                    poll_ms.set(n);
                                }
                            }
                        >
                            { INTERVAL_CHOICES.iter().map(|(ms, label)| {
                                let ms_val = *ms;
                                let label = *label;
                                view! {
                                    <option
                                        value=ms_val.to_string()
                                        selected=move || poll_ms.get() == ms_val
                                    >{ label }</option>
                                }
                            }).collect_view() }
                        </select>
                    </label>
                    <button
                        class="queue-demo-btn"
                        title="Enqueue a no-op demo job onto the `default` queue"
                        on:click=on_enqueue_demo
                    >
                        "Enqueue demo job"
                    </button>
                </div>
            </header>

            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "Could not load queue overview: " }{ e }</div>
            }) }

            <nav class="queue-tabs">
                { Tab::ALL.iter().copied().map(|t| {
                    let active = move || tab.get() == t;
                    let label = t.label();
                    view! {
                        <button
                            class="queue-tab"
                            class:active=active
                            on:click=move |_| tab.set(t)
                        >{ label }</button>
                    }
                }).collect_view() }
            </nav>

            <div class="queue-tab-body">
                { move || match tab.get() {
                    Tab::Overview  => overview_tab(queues).into_any(),
                    Tab::Jobs      => view! { <JobTable on_inspect=on_inspect on_change=on_change /> }.into_any(),
                    Tab::Scheduled => view! { <ScheduledTab /> }.into_any(),
                    Tab::Retries   => view! {
                        <FailedPanel mode=FailedMode::Retries on_inspect=on_inspect on_change=on_change />
                    }.into_any(),
                    Tab::Dead      => view! {
                        <FailedPanel mode=FailedMode::Dead on_inspect=on_inspect on_change=on_change />
                    }.into_any(),
                    Tab::Cron      => view! { <CronTab /> }.into_any(),
                    Tab::Queues    => queues_tab(queues, editing, on_change).into_any(),
                }}
            </div>

            <Inspector selected=selected on_change=on_change />
        </div>
    }
}

fn overview_tab(queues: RwSignal<Vec<QueueOverview>>) -> impl IntoView {
    view! {
        <div class="queue-overview">
            <Timeline />
            <ResourcesPanel />
            <DbHealthPanel />
            <PerQueueMetrics queues=queues />
            <div class="queue-overview-summary">
                { move || summary_counts_view(&queues.get()) }
            </div>
            <section class="queue-overview-procs">
                <h3>"Live processes"</h3>
                { move || live_procs_view(&queues.get()) }
            </section>
        </div>
    }
}

fn summary_counts_view(queues: &[QueueOverview]) -> impl IntoView + use<> {
    let totals = queues.iter().fold(StatusCounts::default(), |mut acc, q| {
        acc.pending += q.counts.pending;
        acc.scheduled += q.counts.scheduled;
        acc.in_progress += q.counts.in_progress;
        acc.done += q.counts.done;
        acc.failed += q.counts.failed;
        acc.dead += q.counts.dead;
        acc
    });
    view! {
        <ul class="queue-summary">
            <li class="queue-chip is-pending"><span class="queue-chip-n">{ totals.pending }</span><span class="queue-chip-label">"pending"</span></li>
            <li class="queue-chip is-scheduled"><span class="queue-chip-n">{ totals.scheduled }</span><span class="queue-chip-label">"scheduled"</span></li>
            <li class="queue-chip is-in_progress"><span class="queue-chip-n">{ totals.in_progress }</span><span class="queue-chip-label">"in-flight"</span></li>
            <li class="queue-chip is-done"><span class="queue-chip-n">{ totals.done }</span><span class="queue-chip-label">"done"</span></li>
            <li class="queue-chip is-failed"><span class="queue-chip-n">{ totals.failed }</span><span class="queue-chip-label">"retrying"</span></li>
            <li class="queue-chip is-dead"><span class="queue-chip-n">{ totals.dead }</span><span class="queue-chip-label">"dead"</span></li>
        </ul>
    }
}

fn live_procs_view(queues: &[QueueOverview]) -> impl IntoView + use<> {
    let mut rows = Vec::new();
    for q in queues {
        for p in &q.processes {
            rows.push((q.name.clone(), p.clone()));
        }
    }
    if rows.is_empty() {
        return view! {
            <div class="queue-empty">"No worker processes are running."</div>
        }
        .into_any();
    }
    view! {
        <table class="queue-procs-table">
            <thead>
                <tr>
                    <th>"Queue"</th>
                    <th>"Process"</th>
                    <th>"Current job"</th>
                    <th>"Heartbeat"</th>
                </tr>
            </thead>
            <tbody>
                { rows.into_iter().map(|(qn, p)| {
                    let age = (chrono::Utc::now() - p.heartbeat_at).num_seconds().max(0);
                    let current = p.current_job_id.unwrap_or_else(|| "idle".to_owned());
                    view! {
                        <tr>
                            <td>{ qn }</td>
                            <td><code>{ p.process_id }</code></td>
                            <td>{ current }</td>
                            <td>{ format!("{age}s ago") }</td>
                        </tr>
                    }
                }).collect_view() }
            </tbody>
        </table>
    }
    .into_any()
}

fn queues_tab(
    queues: RwSignal<Vec<QueueOverview>>,
    editing: RwSignal<bool>,
    on_change: Callback<()>,
) -> impl IntoView {
    // Rebuild cards from scratch on every refresh — Leptos's <For
    // key=name> would memoise the prop snapshot at first mount and
    // never reflect `paused` / `max_workers` changes coming back from
    // the host. Per-render rebuild is fine here: cards are cheap,
    // count is small (3 max in practice), and the user-tunable
    // inputs are rerouted to the freshly-fetched values each tick.
    //
    // `focusin`/`focusout` (which bubble, unlike focus/blur) flip the
    // `editing` flag so the poll pauses card rebuilds while an input is
    // focused — otherwise a 2s tick wipes a half-typed value.
    view! {
        <div
            class="queue-cards"
            on:focusin=move |_| editing.set(true)
            on:focusout=move |_| editing.set(false)
        >
            { move || {
                let rows = queues.get();
                if rows.is_empty() {
                    return view! {
                        <div class="queue-empty">
                            "No queues registered yet. The host ensures a `default` queue on boot."
                        </div>
                    }.into_any();
                }
                rows.into_iter()
                    .map(|q| view! { <QueueCard queue=q on_change=on_change /> })
                    .collect_view()
                    .into_any()
            }}
        </div>
    }
}
