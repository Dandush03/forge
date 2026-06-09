//! First-class "Failed" surfaces — backs the Retries and Dead tabs.
//!
//! `FailedMode::Retries` shows jobs that are mid-retry (`status = "failed"`),
//! sorted by next-attempt time. `FailedMode::Dead` shows terminal failures
//! (`status = "dead"`) sorted by last completion. Both surfaces share the
//! same table layout, with per-row Retry and a section-level Retry-all
//! that's only meaningful for Dead (re-arms terminal jobs).

use std::time::Duration;

use chrono::Local;
use leptos::leptos_dom::helpers::set_interval_with_handle;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::ipc::{IpcCtx, JobRow, JobsFilter};
use crate::queue_root::RefreshTick;

const POLL_INTERVAL_MS: u64 = 5_000;
const ROW_LIMIT: u32 = 100;

/// Which failure surface to render. Retries are mid-flight (still
/// retrying); Dead are terminal.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FailedMode {
    Retries,
    Dead,
}

impl FailedMode {
    const fn status(self) -> &'static str {
        match self {
            Self::Retries => "failed",
            Self::Dead => "dead",
        }
    }

    const fn title(self) -> &'static str {
        match self {
            Self::Retries => "Retries",
            Self::Dead => "Dead",
        }
    }

    const fn icon(self) -> &'static str {
        match self {
            Self::Retries => "↻",
            Self::Dead => "☠",
        }
    }

    const fn css_modifier(self) -> &'static str {
        match self {
            Self::Retries => "queue-failed is-retries",
            Self::Dead => "queue-failed is-dead",
        }
    }
}

#[component]
pub fn FailedPanel(
    mode: FailedMode,
    #[prop(into)] on_inspect: Callback<String>,
    #[prop(into)] on_change: Callback<()>,
) -> impl IntoView {
    let rows = RwSignal::new(Vec::<JobRow>::new());
    let load_err = RwSignal::new(Option::<String>::None);
    // Last bulk-retry result, e.g. "Requeued 3; skipped 9 …". Surfaced so a
    // 0-requeued outcome (everything dedupe-skipped) reads as informative
    // rather than a dead button.
    let retry_note = RwSignal::new(Option::<String>::None);
    let ipc_ctx = expect_context::<IpcCtx>();

    let refresh = {
        let ipc_ctx = ipc_ctx.clone();
        move || {
            let ipc = ipc_ctx.clone();
            spawn_local(async move {
                let filter = JobsFilter {
                    statuses: vec![mode.status().to_owned()],
                    ..JobsFilter::default()
                };
                match ipc.jobs_list(filter, ROW_LIMIT, 0).await {
                    Ok(page) => {
                        rows.set(page.rows);
                        load_err.set(None);
                    }
                    Err(e) => load_err.set(Some(e.to_string())),
                }
            });
        }
    };

    refresh();

    let refresh_for_tick = refresh.clone();
    let handle = set_interval_with_handle(refresh, Duration::from_millis(POLL_INTERVAL_MS)).ok();
    on_cleanup(move || {
        if let Some(h) = handle {
            h.clear();
        }
    });

    // Refresh-on-tick — see RefreshTick in queue_root.
    if let Some(RefreshTick(tick)) = use_context::<RefreshTick>() {
        Effect::new(move |_| {
            let _ = tick.get();
            refresh_for_tick();
        });
    }

    // Click handlers run inside Leptos's event-dispatch scope which
    // DOES preserve the owner — so `use_context` works directly here
    // and we don't need the captured-ipc clone dance that the timer
    // callback needs.
    let on_retry_all = move |_| {
        let ipc = expect_context::<IpcCtx>();
        let status = mode.status();
        let shown = rows.with(Vec::len);
        spawn_local(async move {
            match ipc.jobs_retry_all_by_status(status).await {
                Ok(requeued) => {
                    let skipped = shown.saturating_sub(usize::try_from(requeued).unwrap_or(shown));
                    retry_note.set(Some(if skipped == 0 {
                        format!("Requeued {requeued}.")
                    } else {
                        format!(
                            "Requeued {requeued}; skipped {skipped} \
                             (a job with the same dedupe key is already queued)."
                        )
                    }));
                    load_err.set(None);
                    on_change.run(());
                }
                Err(e) => load_err.set(Some(e.to_string())),
            }
        });
    };

    let on_retry_one = move |id: String| {
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            let ids = vec![id];
            if let Err(e) = ipc.jobs_retry(&ids).await {
                load_err.set(Some(e.to_string()));
                return;
            }
            on_change.run(());
        });
    };

    // Bulk-delete every job in this status. Mirrors the "Purge done"
    // button on the Jobs tab, but scoped to whichever status this
    // panel is rendering (`failed` for Retries, `dead` for Dead).
    let on_purge = move |_| {
        let confirmed = leptos::web_sys::window()
            .and_then(|w| {
                w.confirm_with_message(&format!(
                    "Delete every job in `{}` status?\n\n\
                     This cannot be undone.",
                    mode.status(),
                ))
                .ok()
            })
            .unwrap_or(false);
        if !confirmed {
            return;
        }
        let ipc = expect_context::<IpcCtx>();
        let status = mode.status();
        spawn_local(async move {
            // Retries / Dead panels are queue-agnostic — purge across all.
            match ipc.jobs_delete_by_status(status, None).await {
                Ok(_) => on_change.run(()),
                Err(e) => load_err.set(Some(e.to_string())),
            }
        });
    };

    let section_class = mode.css_modifier();
    let title = mode.title();
    let icon = mode.icon();
    let empty_label = match mode {
        FailedMode::Retries => "No jobs are currently retrying.",
        FailedMode::Dead => "No dead jobs — nothing has exhausted its retries.",
    };

    view! {
        <section class=section_class>
            <header class="queue-failed-head">
                <span class="queue-failed-title">
                    { format!("{icon} {title} (") }
                    { move || rows.with(Vec::len) }
                    { ")" }
                </span>
                <Show when=move || !rows.with(Vec::is_empty)>
                    <button class="queue-failed-retry-all" on:click=on_retry_all>
                        { match mode {
                            FailedMode::Retries => "Retry all",
                            FailedMode::Dead => "Retry all dead",
                        } }
                    </button>
                </Show>
                <Show when=move || !rows.with(Vec::is_empty)>
                    <button
                        class="queue-jobs-purge"
                        on:click=on_purge
                        title="Delete every job in this status"
                    >{ format!("Purge {}", title.to_lowercase()) }</button>
                </Show>
            </header>

            { move || load_err.get().map(|e| view! {
                <div class="queue-panel-err">{ "Load failed: " }{ e }</div>
            }) }

            { move || retry_note.get().map(|note| view! {
                <div class="queue-panel-note">{ note }</div>
            }) }

            <Show when=move || rows.with(Vec::is_empty) && load_err.with(Option::is_none)>
                <div class="queue-empty">{ empty_label }</div>
            </Show>

            <Show when=move || !rows.with(Vec::is_empty)>

                <table class="queue-failed-table">
                    <thead>
                        <tr>
                            <th>"Queue"</th>
                            <th>"Kind"</th>
                            <th>"Attempts"</th>
                            <th>"Last error"</th>
                            <th>"When"</th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody>
                        <For
                            each=move || rows.get()
                            key=|r| r.id.clone()
                            children=move |row| {
                                let id_for_inspect = row.id.clone();
                                let id_for_retry = row.id.clone();
                                let attempts_label = format!("{}/{}", row.attempts, row.max_attempts);
                                let status_class = format!("queue-badge is-{}", row.status);
                                let when_label = row
                                    .completed_at
                                    .or(row.started_at)
                                    .unwrap_or(row.enqueued_at)
                                    .with_timezone(&Local)
                                    .format("%H:%M:%S")
                                    .to_string();
                                let error_label = row
                                    .last_error
                                    .clone()
                                    .unwrap_or_else(|| "—".to_owned());
                                let truncated = truncate(&error_label, 80);
                                view! {
                                    <tr
                                        class="queue-failed-row"
                                        on:click=move |_| on_inspect.run(id_for_inspect.clone())
                                    >
                                        <td>{ row.queue_name }</td>
                                        <td><code>{ row.kind }</code></td>
                                        <td>
                                            <span class=status_class>{ row.status }</span>
                                            <span class="queue-failed-attempts">{ attempts_label }</span>
                                        </td>
                                        <td class="queue-failed-err" title=error_label>{ truncated }</td>
                                        <td>{ when_label }</td>
                                        <td>
                                            <button
                                                class="queue-failed-retry"
                                                on:click=move |ev: leptos::ev::MouseEvent| {
                                                    ev.stop_propagation();
                                                    on_retry_one(id_for_retry.clone());
                                                }
                                            >"Retry"</button>
                                        </td>
                                    </tr>
                                }
                            }
                        />
                    </tbody>
                </table>
            </Show>
        </section>
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
