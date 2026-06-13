//! `WorkersTab` — worker-centric health view.
//!
//! Where the Overview/Queues tabs are *queue*-centric (one card per
//! queue, listing the workers running it), this tab is *worker*-centric:
//! one card per live worker process (pod), showing the queues it declared
//! responsibility for, the rebalancer-assigned slots per queue, its
//! live/in-flight worker counts, and a heartbeat-health dot. A red banner
//! warns when a configured queue has no live worker covering it.
//!
//! Polls `queue_workers` on the panel-wide cadence (see [`PollIntervalMs`]),
//! refreshing immediately on any [`RefreshTick`].

use std::time::Duration;

use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::ipc::{IpcCtx, Worker, WorkersOverview};
use crate::queue_root::{PollIntervalMs, RefreshTick};

// Same thresholds the Overview process rows use, kept local so the two
// surfaces can diverge without coupling.
const HEARTBEAT_OK_SECS: u64 = 30;
const HEARTBEAT_WARN_SECS: u64 = 90;

#[component]
pub fn WorkersTab() -> impl IntoView {
    let data = RwSignal::new(WorkersOverview::default());
    let load_err = RwSignal::new(Option::<String>::None);
    let ipc = expect_context::<IpcCtx>();

    let refresh = {
        let ipc = ipc.clone();
        move || {
            let ipc = ipc.clone();
            spawn_local(async move {
                match ipc.queue_workers().await {
                    Ok(rows) => {
                        data.set(rows);
                        load_err.set(None);
                    }
                    Err(e) => load_err.set(Some(e.to_string())),
                }
            });
        }
    };

    refresh();

    // Refresh immediately whenever a mutation elsewhere bumps the tick.
    let refresh_for_tick = refresh.clone();
    if let Some(RefreshTick(tick)) = use_context::<RefreshTick>() {
        Effect::new(move |_| {
            tick.get();
            refresh_for_tick();
        });
    }

    // Poll on the panel-wide cadence.
    let poll_ms = use_context::<PollIntervalMs>().map_or(2_000, |p| p.0.get_untracked());
    let refresh_for_timer = refresh.clone();
    let handle = (poll_ms > 0)
        .then(|| {
            set_interval_with_handle(refresh_for_timer, Duration::from_millis(poll_ms)).ok()
        })
        .flatten();
    on_cleanup(move || {
        if let Some(h) = handle {
            h.clear();
        }
    });

    view! {
        <div class="worker-view">
            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "Could not load workers: " }{ e }</div>
            }) }

            { move || {
                let unassigned = data.get().unassigned_queues;
                (!unassigned.is_empty()).then(|| view! {
                    <div class="worker-unassigned" role="alert">
                        <strong>"⚠ Unassigned queues: "</strong>
                        { unassigned.join(", ") }
                        <span class="worker-unassigned-hint">
                            " — no live worker is consuming these; their jobs won't run until one declares them."
                        </span>
                    </div>
                })
            } }

            { move || {
                let workers = data.get().workers;
                if workers.is_empty() {
                    return view! {
                        <div class="queue-empty">"No worker processes are running."</div>
                    }.into_any();
                }
                view! {
                    <div class="worker-cards">
                        { workers.into_iter().map(|w| view! { <WorkerCard worker=w /> }).collect_view() }
                    </div>
                }.into_any()
            } }
        </div>
    }
}

#[component]
fn WorkerCard(worker: Worker) -> impl IntoView {
    let name = worker.display_name().to_owned();
    let host_id = worker.host_id.clone();
    let has_name = worker.worker_name.is_some();
    let age = worker.heartbeat_age_seconds;
    let dot_class = if age <= HEARTBEAT_OK_SECS {
        "worker-dot is-ok"
    } else if age <= HEARTBEAT_WARN_SECS {
        "worker-dot is-warn"
    } else {
        "worker-dot is-down"
    };
    let workers_live = worker.workers_live;
    let in_flight = worker.in_flight;
    let queues = worker.queues.clone();
    // Slots keyed by queue so each chip can show "queue ×N" when assigned.
    let slot_for = move |q: &str| worker.slots.iter().find(|s| s.queue_name == q).map(|s| s.slots);

    view! {
        <article class="worker-card">
            <header class="worker-card-head">
                <span class=dot_class title=format!("heartbeat {age}s ago")></span>
                <h3 class="worker-card-name">{ name }</h3>
                { has_name.then(|| view! { <code class="worker-card-host">{ host_id }</code> }) }
                <span class="worker-card-meta">
                    { format!("workers {workers_live} · in-flight {in_flight} · hb {age}s") }
                </span>
            </header>

            <div class="worker-card-queues">
                { if queues.is_empty() {
                    view! { <span class="worker-queue-none">"no queues declared"</span> }.into_any()
                } else {
                    queues.into_iter().map(|q| {
                        let label = match slot_for(&q) {
                            Some(n) if n > 0 => format!("{q} ×{n}"),
                            _ => q.clone(),
                        };
                        view! { <span class="worker-queue-chip">{ label }</span> }
                    }).collect_view().into_any()
                } }
            </div>
        </article>
    }
}
