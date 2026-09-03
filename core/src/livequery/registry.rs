//! The node-held registry of live queries swept by a system reset.

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use super::WeakEntityLiveQuery;

#[derive(Clone)]
pub(crate) struct LiveQueryRegistry {
    inner: Arc<RegistryInner>,
}

pub(super) struct RegistryInner {
    state: std::sync::Mutex<RegistryState>,
}

#[derive(Default)]
struct RegistryState {
    entries: HashMap<usize, WeakEntityLiveQuery>,
    resetting: bool,
}

impl RegistryInner {
    pub(super) fn unregister(&self, key: usize) { self.state.lock().unwrap_or_else(|error| error.into_inner()).entries.remove(&key); }
}

pub(crate) struct ResetGuard(Arc<RegistryInner>);

impl Drop for ResetGuard {
    fn drop(&mut self) { self.0.state.lock().unwrap_or_else(|error| error.into_inner()).resetting = false; }
}

impl LiveQueryRegistry {
    pub(crate) fn new() -> Self { Self { inner: Arc::new(RegistryInner { state: std::sync::Mutex::new(RegistryState::default()) }) } }

    pub(super) fn downgrade(&self) -> Weak<RegistryInner> { Arc::downgrade(&self.inner) }

    /// Register a query and report whether it was born during a reset.
    pub(super) fn insert(&self, key: usize, query: WeakEntityLiveQuery) -> bool {
        let mut state = self.inner.state.lock().unwrap_or_else(|error| error.into_inner());
        state.entries.insert(key, query);
        state.resetting
    }

    pub(crate) fn begin_system_reset(&self) -> ResetGuard {
        let queries = {
            let mut state = self.inner.state.lock().unwrap_or_else(|error| error.into_inner());
            debug_assert!(!state.resetting, "live-query resets are serialized");
            state.resetting = true;
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        for query in queries {
            if let Some(query) = query.upgrade() {
                query.system_reset();
            }
        }
        ResetGuard(self.inner.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dead_query() -> WeakEntityLiveQuery { WeakEntityLiveQuery(Weak::new()) }

    #[test]
    fn insert_reports_an_active_reset() {
        let registry = LiveQueryRegistry::new();
        assert!(!registry.insert(1, dead_query()));
        let reset = registry.begin_system_reset();
        assert!(registry.insert(2, dead_query()));
        drop(reset);
        assert!(!registry.insert(3, dead_query()));
    }
}
