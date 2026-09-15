//! Live-query bookkeeping without keeping queries alive.

use std::{
    collections::HashMap,
    sync::Arc,
};

use super::{inner::LiveQueryInner, EntityLiveQuery, WeakEntityLiveQuery};

pub(crate) struct LiveQueryRegistry {
    // Each weak value keeps its allocation address reserved.
    entries: std::sync::Mutex<HashMap<usize, WeakEntityLiveQuery>>,
}

impl LiveQueryRegistry {
    pub(crate) fn new() -> Self { Self { entries: std::sync::Mutex::new(HashMap::new()) } }

    pub(super) fn insert(&self, query: &EntityLiveQuery) {
        let address = Arc::as_ptr(&query.0) as usize;
        self.entries.lock().unwrap_or_else(|error| error.into_inner()).insert(address, query.weak());
    }

    pub(super) fn unregister(&self, query: &LiveQueryInner) {
        let address = query as *const LiveQueryInner as usize;
        self.entries.lock().unwrap_or_else(|error| error.into_inner()).remove(&address);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        context::Context,
        node::{CachePolicy, Node},
        policy::{PermissiveAgent, DEFAULT_CONTEXT},
        test_utils::TestStorage,
    };

    #[tokio::test]
    async fn clones_share_one_registration_until_the_last_handle_drops() {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let context = Context::new(node.clone(), DEFAULT_CONTEXT);
        let query =
            EntityLiveQuery::new(context.0, CachePolicy::Durable, super::super::QueryResolution::Pending(Box::pin(futures::future::pending()))).unwrap();
        let address = Arc::as_ptr(&query.0) as usize;
        let clone = query.clone();
        node.live_queries.insert(&clone);
        let registered = || node.live_queries.entries.lock().unwrap().contains_key(&address);
        assert!(registered());
        drop(query);
        assert!(registered());
        drop(clone);
        assert!(!registered());
    }

    #[tokio::test]
    async fn expired_weak_context_cannot_register_a_query() {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let context = Context::new_weak(&node, DEFAULT_CONTEXT);
        let weak = node.weak();
        let access = context.0.node().unwrap();
        drop(node);
        assert!(weak.upgrade().is_some(), "the erased handle retains the node only while held");
        drop(access);
        assert!(weak.upgrade().is_none());

        let result = EntityLiveQuery::new(
            context.0,
            CachePolicy::Durable,
            super::super::QueryResolution::Pending(Box::pin(futures::future::pending())),
        );
        assert!(matches!(result, Err(crate::error::RetrievalError::NodeDropped(_))));
    }
}
