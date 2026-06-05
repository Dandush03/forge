//! Selection state + bulk-action helpers shared by the job table.

use std::collections::HashSet;

use leptos::prelude::*;
use leptos::task::spawn_local;

use crate::ipc::IpcCtx;
use crate::queue_root::RefreshTick;

/// Threshold beyond which bulk actions require a typed confirmation.
const CONFIRM_THRESHOLD: usize = 500;

#[derive(Clone, Copy)]
pub struct SelectionState {
    pub selected: RwSignal<HashSet<String>>,
}

impl Default for SelectionState {
    fn default() -> Self {
        Self {
            selected: RwSignal::new(HashSet::new()),
        }
    }
}

impl SelectionState {
    pub fn toggle(self, id: &str) {
        self.selected.update(|s| {
            if !s.remove(id) {
                s.insert(id.to_owned());
            }
        });
    }

    pub fn clear(self) {
        self.selected.set(HashSet::new());
    }

    pub fn set_all(self, ids: impl IntoIterator<Item = String>) {
        let new: HashSet<String> = ids.into_iter().collect();
        self.selected.set(new);
    }

    pub fn contains(self, id: &str) -> bool {
        self.selected.with(|s| s.contains(id))
    }

    pub fn count(self) -> usize {
        self.selected.with(HashSet::len)
    }

    pub fn ids(self) -> Vec<String> {
        self.selected.with(|s| s.iter().cloned().collect())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkAction {
    Retry,
    Delete,
    Requeue,
}

impl BulkAction {
    pub const fn verb(self) -> &'static str {
        match self {
            Self::Retry => "Retry",
            Self::Delete => "Delete",
            Self::Requeue => "Requeue",
        }
    }

    pub const fn confirm_word(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::Delete => "delete",
            Self::Requeue => "requeue",
        }
    }
}

#[derive(Clone, Copy)]
pub struct BulkActionsBar {
    pub selection: SelectionState,
    pub on_done: Callback<()>,
}

impl BulkActionsBar {
    pub fn run(self, action: BulkAction) {
        let ids = self.selection.ids();
        if ids.is_empty() {
            return;
        }

        if ids.len() > CONFIRM_THRESHOLD {
            let prompt = format!(
                "About to {verb} {n} jobs. Type '{word}' to confirm:",
                verb = action.verb().to_lowercase(),
                n = ids.len(),
                word = action.confirm_word(),
            );
            let typed = leptos::web_sys::window()
                .and_then(|w| w.prompt_with_message(&prompt).ok())
                .flatten()
                .unwrap_or_default();
            if typed.trim() != action.confirm_word() {
                return;
            }
        }

        let selection = self.selection;
        let on_done = self.on_done;
        let ipc = expect_context::<IpcCtx>();
        let tick = use_context::<RefreshTick>();
        spawn_local(async move {
            let result = match action {
                BulkAction::Retry => ipc.jobs_retry(&ids).await,
                BulkAction::Delete => ipc.jobs_delete(&ids).await,
                BulkAction::Requeue => ipc.jobs_requeue(&ids).await,
            };
            if let Err(e) = result {
                leptos::web_sys::console::warn_1(
                    &format!("bulk {verb} failed: {e}", verb = action.verb()).into(),
                );
                return;
            }
            selection.clear();
            if let Some(RefreshTick(tick)) = tick {
                tick.update(|n| *n = n.wrapping_add(1));
            }
            on_done.run(());
        });
    }
}
