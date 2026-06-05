//! `QueueCard` — one card per registered queue.
//!
//! Renders status counts, the live worker list, a pause/resume toggle,
//! and a worker-count slider.

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::ipc::{IpcCtx, QueueOverview, QueueProcess};

const HEARTBEAT_OK_SECS: i64 = 30;
const HEARTBEAT_WARN_SECS: i64 = 90;

#[component]
pub fn QueueCard(queue: QueueOverview, #[prop(into)] on_change: Callback<()>) -> impl IntoView {
    let name_for_pause = queue.name.clone();
    let name_for_workers = queue.name.clone();
    let name_for_retention_done = queue.name.clone();
    let name_for_retention_dead = queue.name.clone();
    let name_for_backoff_enabled = queue.name.clone();
    let name_for_backoff_base = queue.name.clone();
    let name_for_backoff_max = queue.name.clone();
    let name_header = queue.name.clone();
    let paused = queue.paused;
    let max_workers = queue.max_workers;
    let counts = queue.counts.clone();
    let processes = queue.processes.clone();
    let retain_done = queue.retain_done_days;
    let retain_dead = queue.retain_dead_days;
    let backoff_enabled = queue.backoff_enabled;
    let backoff_base = queue.backoff_base_seconds;
    let backoff_max = queue.backoff_max_seconds;
    let throttled_until = queue.throttled_until;
    let inflight_now = processes
        .iter()
        .filter(|p| p.current_job_id.is_some())
        .count();
    let workers_live = processes.len();

    // 1s tick driving the backoff cool-down countdown. Cheap (≤3 cards);
    // cleared on unmount / card rebuild.
    let now = RwSignal::new(Utc::now());
    let tick = set_interval_with_handle(move || now.set(Utc::now()), Duration::from_secs(1)).ok();
    on_cleanup(move || {
        if let Some(handle) = tick {
            handle.clear();
        }
    });

    let on_toggle_pause = move |_| {
        let target = !paused;
        let name = name_for_pause.clone();
        let cb = on_change;
        let ipc = expect_context::<IpcCtx>();
        leptos::web_sys::console::log_1(
            &format!("queue_set_paused: name={name} target={target}").into(),
        );
        spawn_local(async move {
            match ipc.queue_set_paused(&name, target).await {
                Ok(()) => {
                    leptos::web_sys::console::log_1(
                        &format!("queue_set_paused OK: name={name} target={target}").into(),
                    );
                    cb.run(());
                }
                Err(e) => {
                    leptos::web_sys::console::warn_1(
                        &format!("queue_set_paused FAILED: name={name} target={target} err={e}")
                            .into(),
                    );
                }
            }
        });
    };

    let on_workers_change = move |ev: leptos::ev::Event| {
        let raw = event_target_value(&ev);
        let Ok(n) = raw.parse::<i32>() else {
            return;
        };
        let clamped = n.clamp(0, 64);
        let name = name_for_workers.clone();
        let cb = on_change;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            if let Err(e) = ipc.queue_set_max_workers(&name, clamped).await {
                leptos::web_sys::console::warn_1(
                    &format!("queue_set_max_workers failed: {e}").into(),
                );
                return;
            }
            cb.run(());
        });
    };

    let on_retain_done_change = move |ev: leptos::ev::Event| {
        let raw = event_target_value(&ev);
        let Ok(n) = raw.parse::<i32>() else {
            return;
        };
        let clamped = n.clamp(0, 365);
        let name = name_for_retention_done.clone();
        let cb = on_change;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            if let Err(e) = ipc.queue_set_retention(&name, clamped, retain_dead).await {
                leptos::web_sys::console::warn_1(
                    &format!("queue_set_retention(done) failed: {e}").into(),
                );
                return;
            }
            cb.run(());
        });
    };

    let on_retain_dead_change = move |ev: leptos::ev::Event| {
        let raw = event_target_value(&ev);
        let Ok(n) = raw.parse::<i32>() else {
            return;
        };
        let clamped = n.clamp(0, 3650);
        let name = name_for_retention_dead.clone();
        let cb = on_change;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            if let Err(e) = ipc.queue_set_retention(&name, retain_done, clamped).await {
                leptos::web_sys::console::warn_1(
                    &format!("queue_set_retention(dead) failed: {e}").into(),
                );
                return;
            }
            cb.run(());
        });
    };

    let on_backoff_enabled_change = move |ev: leptos::ev::Event| {
        let target = event_target_checked(&ev);
        let name = name_for_backoff_enabled.clone();
        let cb = on_change;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            if let Err(e) = ipc
                .queue_set_backoff(&name, target, backoff_base, backoff_max)
                .await
            {
                leptos::web_sys::console::warn_1(
                    &format!("queue_set_backoff(enabled) failed: {e}").into(),
                );
                return;
            }
            cb.run(());
        });
    };

    let on_backoff_base_change = move |ev: leptos::ev::Event| {
        let raw = event_target_value(&ev);
        let Ok(n) = raw.parse::<i32>() else {
            return;
        };
        let clamped = n.clamp(1, 86_400);
        let name = name_for_backoff_base.clone();
        let cb = on_change;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            if let Err(e) = ipc
                .queue_set_backoff(&name, backoff_enabled, clamped, backoff_max)
                .await
            {
                leptos::web_sys::console::warn_1(
                    &format!("queue_set_backoff(base) failed: {e}").into(),
                );
                return;
            }
            cb.run(());
        });
    };

    let on_backoff_max_change = move |ev: leptos::ev::Event| {
        let raw = event_target_value(&ev);
        let Ok(n) = raw.parse::<i32>() else {
            return;
        };
        let clamped = n.clamp(1, 86_400);
        let name = name_for_backoff_max.clone();
        let cb = on_change;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            if let Err(e) = ipc
                .queue_set_backoff(&name, backoff_enabled, backoff_base, clamped)
                .await
            {
                leptos::web_sys::console::warn_1(
                    &format!("queue_set_backoff(max) failed: {e}").into(),
                );
                return;
            }
            cb.run(());
        });
    };

    let on_cleanup_now = move |_| {
        let cb = on_change;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            match ipc.queue_cleanup_now().await {
                Ok(report) => {
                    leptos::web_sys::console::log_1(
                        &format!(
                            "queue_cleanup_now OK: done_deleted={} dead_deleted={}",
                            report.done_deleted, report.dead_deleted,
                        )
                        .into(),
                    );
                    cb.run(());
                }
                Err(e) => {
                    leptos::web_sys::console::warn_1(
                        &format!("queue_cleanup_now failed: {e}").into(),
                    );
                }
            }
        });
    };

    let pulse_class = if paused {
        "queue-card-dot is-paused"
    } else if workers_live == 0 {
        "queue-card-dot is-down"
    } else {
        "queue-card-dot is-live"
    };

    view! {
        <article class="queue-card">
            <header class="queue-card-head">
                <span class=pulse_class></span>
                <h3 class="queue-card-name">{ name_header }</h3>
                <span class="queue-card-meta">
                    { format!("workers {workers_live}/{max_workers} · in-flight {inflight_now}") }
                </span>
                { move || throttled_until.and_then(|until| {
                    let secs = (until - now.get()).num_seconds();
                    (secs > 0).then(|| view! {
                        <span
                            class="queue-card-throttle"
                            title="Queue is in a backoff cool-down — not claiming new jobs until this elapses"
                        >
                            { format!("throttled · resuming in {secs}s") }
                        </span>
                    })
                }) }
                <div class="queue-card-controls">
                    <label class="queue-card-workers">
                        "Workers"
                        <input
                            type="number"
                            min="0"
                            max="64"
                            prop:value=max_workers
                            on:change=on_workers_change
                        />
                    </label>
                    <button
                        class="queue-card-pause"
                        class:is-paused=paused
                        on:click=on_toggle_pause
                        title=if paused { "Resume picking new jobs" } else { "Pause: workers finish current job then idle" }
                    >
                        { if paused { "Resume" } else { "Pause" } }
                    </button>
                </div>
            </header>

            <ul class="queue-card-counts">
                <CountChip label="pending"     n=counts.pending     status="pending" />
                <CountChip label="scheduled"   n=counts.scheduled   status="scheduled" />
                <CountChip label="in-flight"   n=counts.in_progress status="in_progress" />
                <CountChip label="done"        n=counts.done        status="done" />
                <CountChip label="failed"      n=counts.failed      status="failed" />
                <CountChip label="dead"        n=counts.dead        status="dead" />
            </ul>

            <details class="queue-card-procs">
                <summary>
                    { format!("Processes ({workers_live})") }
                </summary>
                <ul class="queue-card-proc-list">
                    { processes.into_iter().map(|p| view! { <ProcessRow proc=p /> }).collect_view() }
                </ul>
            </details>

            <footer class="queue-card-foot">
                <label class="queue-card-retain">
                    "retain done (d)"
                    <input
                        type="number"
                        min="0"
                        max="365"
                        prop:value=retain_done
                        on:change=on_retain_done_change
                    />
                </label>
                <label class="queue-card-retain">
                    "retain dead (d)"
                    <input
                        type="number"
                        min="0"
                        max="3650"
                        prop:value=retain_dead
                        on:change=on_retain_dead_change
                    />
                </label>
                <label
                    class="queue-card-backoff-toggle"
                    title="When on, throttle re-queues use the per-queue exponential curve. Off keeps the legacy flat 60s delay."
                >
                    <input
                        type="checkbox"
                        prop:checked=backoff_enabled
                        on:change=on_backoff_enabled_change
                    />
                    "backoff"
                </label>
                <label class="queue-card-retain" title="Initial throttle delay; doubles on each consecutive throttle.">
                    "base (s)"
                    <input
                        type="number"
                        min="1"
                        max="86400"
                        prop:value=backoff_base
                        prop:disabled=!backoff_enabled
                        on:change=on_backoff_base_change
                    />
                </label>
                <label class="queue-card-retain" title="Ceiling on the exponential curve.">
                    "max (s)"
                    <input
                        type="number"
                        min="1"
                        max="86400"
                        prop:value=backoff_max
                        prop:disabled=!backoff_enabled
                        on:change=on_backoff_max_change
                    />
                </label>
                <button
                    class="queue-card-cleanup"
                    on:click=on_cleanup_now
                    title="Apply retention now across all queues (next auto-tick runs every 5 min)"
                >
                    "Cleanup now"
                </button>
            </footer>
        </article>
    }
}

#[component]
fn CountChip(#[prop(into)] label: String, n: u64, #[prop(into)] status: String) -> impl IntoView {
    let class = format!("queue-chip is-{status}");
    let zero = n == 0;
    view! {
        <li class=class class:is-zero=zero>
            <span class="queue-chip-n">{ n }</span>
            <span class="queue-chip-label">{ label }</span>
        </li>
    }
}

#[component]
fn ProcessRow(proc: QueueProcess) -> impl IntoView {
    let age = (Utc::now() - proc.heartbeat_at).num_seconds().max(0);
    let hb_class = if age <= HEARTBEAT_OK_SECS {
        "hb is-ok"
    } else if age <= HEARTBEAT_WARN_SECS {
        "hb is-warn"
    } else {
        "hb is-down"
    };
    let current = proc
        .current_job_id
        .clone()
        .unwrap_or_else(|| "idle".to_owned());
    let started_label = relative_label(Utc::now() - proc.started_at);
    let hb_label = format!("{age}s ago");
    view! {
        <li class="queue-proc">
            <span class=hb_class title=hb_label></span>
            <span class="queue-proc-id">{ proc.process_id }</span>
            <span class="queue-proc-job">{ current }</span>
            <span class="queue-proc-uptime">{ format!("up {started_label}") }</span>
        </li>
    }
}

fn relative_label(d: ChronoDuration) -> String {
    let secs = d.num_seconds().max(0);
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
