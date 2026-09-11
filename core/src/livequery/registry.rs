//! Live-query bookkeeping without keeping queries alive.

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use super::{inner::LiveQueryInner, EntityLiveQuery, WeakEntityLiveQuery};

#[derive(Clone)]
pub(crate) struct LiveQueryRegistry {
    inner: Arc<RegistryInner>,
}

pub(super) struct RegistryInner {
    // Each weak value keeps its allocation address reserved.
    entries: std::sync::Mutex<HashMap<usize, WeakEntityLiveQuery>>,
}

impl RegistryInner {
    pub(super) fn unregister(&self, query: &LiveQueryInner) {
        let address = query as *const LiveQueryInner as usize;
        self.entries.lock().unwrap_or_else(|error| error.into_inner()).remove(&address);
    }
}

impl LiveQueryRegistry {
    pub(crate) fn new() -> Self { Self { inner: Arc::new(RegistryInner { entries: std::sync::Mutex::new(HashMap::new()) }) } }

    pub(super) fn downgrade(&self) -> Weak<RegistryInner> { Arc::downgrade(&self.inner) }

    pub(super) fn insert(&self, query: &EntityLiveQuery) {
        let address = Arc::as_ptr(&query.0) as usize;
        self.inner.entries.lock().unwrap_or_else(|error| error.into_inner()).insert(address, query.weak());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        context::Context,
        node::Node,
        policy::{PermissiveAgent, DEFAULT_CONTEXT},
        test_utils::TestStorage,
    };
    use ankql::ast::{Parsed, Predicate};

    #[tokio::test]
    async fn clones_share_one_registration_until_the_last_handle_drops() {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let context = Context::new(node.clone(), DEFAULT_CONTEXT);
        let query = EntityLiveQuery::new_with_context(&node, context.0, None, "test".into(), Predicate::<Parsed>::True.into()).unwrap();
        let address = Arc::as_ptr(&query.0) as usize;
        let clone = query.clone();
        node.live_queries.insert(&clone);
        let registered = || node.live_queries.inner.entries.lock().unwrap().contains_key(&address);
        assert!(registered());
        drop(query);
        assert!(registered());
        drop(clone);
        assert!(!registered());
    }
}
