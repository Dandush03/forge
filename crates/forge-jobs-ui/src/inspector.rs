//! Per-job inspector drawer.

use chrono::Local;
use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::ipc::{IpcCtx, JobInspect};

#[component]
pub fn Inspector(
    selected: RwSignal<Option<String>>,
    #[prop(into)] on_change: Callback<()>,
) -> impl IntoView {
    let detail = RwSignal::new(Option::<JobInspect>::None);
    let load_err = RwSignal::new(Option::<String>::None);
    let loading = RwSignal::new(false);

    Effect::new(move |_| {
        let id = selected.get();
        detail.set(None);
        load_err.set(None);
        if let Some(id) = id {
            loading.set(true);
            let ipc = expect_context::<IpcCtx>();
            spawn_local(async move {
                match ipc.job_inspect(&id).await {
                    Ok(d) => {
                        detail.set(Some(d));
                        load_err.set(None);
                    }
                    Err(e) => load_err.set(Some(e.to_string())),
                }
                loading.set(false);
            });
        }
    });

    let close = move |_| selected.set(None);

    let on_retry = move |_| {
        let Some(d) = detail.get() else { return };
        let id = d.row.id;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            let ids = vec![id];
            if let Err(e) = ipc.jobs_retry(&ids).await {
                load_err.set(Some(e.to_string()));
                return;
            }
            selected.set(None);
            on_change.run(());
        });
    };

    let on_delete = move |_| {
        let Some(d) = detail.get() else { return };
        let id = d.row.id;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            let ids = vec![id];
            if let Err(e) = ipc.jobs_delete(&ids).await {
                load_err.set(Some(e.to_string()));
                return;
            }
            selected.set(None);
            on_change.run(());
        });
    };

    let on_requeue = move |_| {
        let Some(d) = detail.get() else { return };
        let id = d.row.id;
        let ipc = expect_context::<IpcCtx>();
        spawn_local(async move {
            let ids = vec![id];
            if let Err(e) = ipc.jobs_requeue(&ids).await {
                load_err.set(Some(e.to_string()));
                return;
            }
            selected.set(None);
            on_change.run(());
        });
    };

    view! {
        <Show when=move || selected.with(Option::is_some)>
            <div class="queue-drawer-scrim" on:click=close></div>
            <aside class="queue-drawer">
                <header class="queue-drawer-head">
                    <h3>"Job"</h3>
                    <button class="queue-drawer-close" on:click=close title="Close">"×"</button>
                </header>

                { move || load_err.get().map(|e| view! {
                    <div class="queue-panel-err">{ e }</div>
                }) }

                { move || if loading.get() && detail.with(Option::is_none) {
                    view! { <div class="queue-drawer-loading">"Loading…"</div> }.into_any()
                } else if let Some(d) = detail.get() {
                    let row = d.row.clone();
                    let payload_json = serde_json::to_string_pretty(&d.payload)
                        .unwrap_or_else(|_| "<unserializable>".into());
                    let history = d.error_history;
                    let attempts_label = format!("{}/{}", row.attempts, row.max_attempts);
                    let status_class = format!("queue-badge is-{}", row.status);

                    view! {
                        <div class="queue-drawer-body">
                            <dl class="queue-drawer-meta">
                                <dt>"Id"</dt>           <dd class="queue-drawer-id">{ row.id.clone() }</dd>
                                <dt>"Queue"</dt>        <dd>{ row.queue_name.clone() }</dd>
                                <dt>"Kind"</dt>         <dd><code>{ row.kind.clone() }</code></dd>
                                <dt>"Status"</dt>       <dd><span class=status_class>{ row.status.clone() }</span></dd>
                                <dt>"Attempts"</dt>     <dd>{ attempts_label }</dd>
                                <dt>"Priority"</dt>     <dd>{ row.priority }</dd>
                                <dt>"Enqueued"</dt>     <dd>{ fmt_dt_opt(Some(row.enqueued_at)) }</dd>
                                <dt>"Scheduled"</dt>    <dd>{ fmt_dt_opt(Some(row.scheduled_at)) }</dd>
                                <dt>"Started"</dt>      <dd>{ fmt_dt_opt(row.started_at) }</dd>
                                <dt>"Completed"</dt>    <dd>{ fmt_dt_opt(row.completed_at) }</dd>
                                <dt>"Process"</dt>      <dd>{ row.process_id.clone().unwrap_or_else(|| "—".into()) }</dd>
                                <dt>"Dedupe key"</dt>   <dd>{ row.dedupe_key.clone().unwrap_or_else(|| "—".into()) }</dd>
                                <dt>"Last error"</dt>   <dd class="queue-drawer-err">{ row.last_error.unwrap_or_else(|| "—".into()) }</dd>
                            </dl>

                            <section class="queue-drawer-section">
                                <h4>"Payload"</h4>
                                <pre class="queue-drawer-payload">{ payload_json }</pre>
                            </section>

                            <section class="queue-drawer-section">
                                <h4>{ format!("Error history ({})", history.len()) }</h4>
                                { if history.is_empty() {
                                    view! { <p class="queue-drawer-empty">"No errors yet."</p> }.into_any()
                                } else {
                                    view! {
                                        <ol class="queue-drawer-history">
                                            { history.into_iter().enumerate().map(|(i, entry)| {
                                                let attempt = entry.get("attempt").and_then(serde_json::Value::as_i64).unwrap_or(0);
                                                let at = entry.get("at").and_then(serde_json::Value::as_str).unwrap_or("");
                                                let msg = entry.get("message").and_then(serde_json::Value::as_str).unwrap_or("");
                                                view! {
                                                    <li class="queue-drawer-history-row">
                                                        <span class="queue-drawer-history-attempt">{ format!("#{i} · attempt {attempt}") }</span>
                                                        <span class="queue-drawer-history-at">{ at.to_owned() }</span>
                                                        <span class="queue-drawer-history-msg">{ msg.to_owned() }</span>
                                                    </li>
                                                }
                                            }).collect_view() }
                                        </ol>
                                    }.into_any()
                                } }
                            </section>
                        </div>

                        <footer class="queue-drawer-foot">
                            <button class="queue-bulk-btn" on:click=on_retry>"Retry"</button>
                            <button class="queue-bulk-btn" on:click=on_requeue>"Requeue"</button>
                            <button class="queue-bulk-btn is-danger" on:click=on_delete>"Delete"</button>
                        </footer>
                    }.into_any()
                } else {
                    view! { <div class="queue-drawer-empty">"No detail."</div> }.into_any()
                } }
            </aside>
        </Show>
    }
}

fn fmt_dt_opt(dt: Option<chrono::DateTime<chrono::Utc>>) -> String {
    dt.map_or_else(
        || "—".to_owned(),
        |d| {
            d.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        },
    )
}
