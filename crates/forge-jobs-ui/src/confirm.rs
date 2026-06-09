//! In-DOM confirmation modal.
//!
//! Destructive actions (purge, large bulk deletes) used to gate on the
//! browser's native `window.confirm()` / `window.prompt()`. Those return
//! `false` *silently* inside Tauri's WKWebView host — no dialog, no
//! console output — so on the desktop app every guarded action became a
//! no-op. WKWebView renders ordinary DOM fine; it only blocks the native
//! modal primitives. So we raise our own modal instead, which works in
//! both the browser and the webview.
//!
//! Usage: provide a [`Confirmer`] once near the panel root and render a
//! [`ConfirmModal`] there; any descendant then calls
//! [`Confirmer::ask`] from a click handler to queue a confirmation.

use leptos::prelude::*;

/// A pending confirmation: the prompt text (newlines are honoured), the
/// affirmative-button label, and the action to run if confirmed.
#[derive(Clone)]
pub struct ConfirmRequest {
    pub message: String,
    pub confirm_label: String,
    pub on_confirm: Callback<()>,
}

/// Context handle for raising confirmation modals. Cheap to copy.
#[derive(Clone, Copy)]
pub struct Confirmer(pub(crate) RwSignal<Option<ConfirmRequest>>);

impl Confirmer {
    /// Create a confirmer backed by a fresh signal. Provide it via
    /// context and hand the same value to a [`ConfirmModal`].
    pub fn new() -> Self {
        Self(RwSignal::new(None))
    }

    /// Queue a confirmation. `on_confirm` runs only if the operator
    /// clicks the affirmative button; cancelling (button or scrim)
    /// dismisses without running it.
    pub fn ask(
        self,
        message: impl Into<String>,
        confirm_label: impl Into<String>,
        on_confirm: Callback<()>,
    ) {
        self.0.set(Some(ConfirmRequest {
            message: message.into(),
            confirm_label: confirm_label.into(),
            on_confirm,
        }));
    }
}

impl Default for Confirmer {
    fn default() -> Self {
        Self::new()
    }
}

/// Renders the active confirmation request (if any). Reads the
/// [`Confirmer`] from context, so it must be mounted under a provider.
#[component]
pub fn ConfirmModal() -> impl IntoView {
    let pending = expect_context::<Confirmer>().0;

    let cancel = move |_| pending.set(None);
    let confirm = move |_| {
        // Take-then-run: clear first so a fast double-click can't fire
        // the action twice.
        if let Some(req) = pending.get() {
            pending.set(None);
            req.on_confirm.run(());
        }
    };

    move || {
        pending.get().map(|req| {
            view! {
                <div class="queue-confirm-scrim" on:click=cancel></div>
                <div class="queue-confirm" role="dialog" aria-modal="true">
                    <p class="queue-confirm-msg">{ req.message }</p>
                    <div class="queue-confirm-actions">
                        <button class="queue-confirm-cancel" on:click=cancel>"Cancel"</button>
                        <button class="queue-confirm-go" on:click=confirm>
                            { req.confirm_label }
                        </button>
                    </div>
                </div>
            }
        })
    }
}
