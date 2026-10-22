//! Live-query bookkeeping without keeping queries alive.

use std::collections::HashMap;

use ankurah_proto::QueryId;

use super::{inner::LiveQueryInner, EntityLiveQuery, WeakEntityLiveQuery};

pub(crate) struct LiveQueryRegistry {
    entries: std::sync::Mutex<HashMap<QueryId, WeakEntityLiveQuery>>,
}

impl LiveQueryRegistry {
    pub(crate) fn new() -> Self { Self { entries: std::sync::Mutex::new(HashMap::new()) } }

    pub(crate) fn get(&self, id: QueryId) -> Option<EntityLiveQuery> {
        let entry = self.entries.lock().unwrap_or_else(|error| error.into_inner()).get(&id).cloned();
        entry.and_then(|query| query.upgrade())
    }

    /// A failed event-history proof invalidates the query, not the write already committed.
    pub(crate) fn fail_notifications(&self, failures: &[crate::reactor::NotificationFailure]) {
        for failure in failures {
            if let Some(query) = failure.query.and_then(|id| self.get(id)) {
                query.fail_resolution(failure.version, failure.error.clone());
            } else {
                tracing::warn!(error = %failure.error, "reactor notification could not establish event coverage");
            }
        }
    }

    pub(super) fn insert(&self, query: &EntityLiveQuery) {
        self.entries.lock().unwrap_or_else(|error| error.into_inner()).insert(query.query_id(), query.weak());
    }

    pub(super) fn unregister(&self, query: &LiveQueryInner) {
        self.entries.lock().unwrap_or_else(|error| error.into_inner()).remove(&query.query_id);
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
    use std::sync::Arc;

    #[tokio::test]
    async fn clones_share_one_registration_until_the_last_handle_drops() {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let context = Context::new(node.clone(), DEFAULT_CONTEXT);
        let query = EntityLiveQuery::new(
            context.0,
            CachePolicy::Durable,
            super::super::QueryResolution::Pending(Box::pin(futures::future::pending())),
        )
        .unwrap();
        let id = query.query_id();
        let clone = query.clone();
        node.live_queries.insert(&clone);
        let registered = || node.live_queries.get(id).is_some();
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
