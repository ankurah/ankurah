use super::registry;
use crate::context::{Context, DynContextInner};
use crate::internal::prelude::*;
use crate::reactor::fetch_gap::{GapFetcher, QueryGapFetcher};
use crate::reactor::{PreNotifyHook, ReactorSubscription};
use ankql::ast::Resolved;
use ankurah_signals::Mut;
use futures::future::RemoteHandle;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, Weak,
};
use tracing::{debug, warn};

/// Shared query state; versions identify selection attempts, not system epochs.
pub(super) struct LiveQueryInner {
    pub(super) query_id: proto::QueryId,
    // Must drop before context; cleanup uses the context's reactor.
    pub(super) subscription: ReactorSubscription,
    pub(super) context: Arc<dyn DynContextInner>,
    pub(super) resultset: EntityResultSet,
    pub(super) error: Mut<Option<Arc<RetrievalError>>>,
    pub(super) version_lock: std::sync::Mutex<()>,
    initialized_notify: tokio::sync::Notify,
    initialized_version: AtomicU32,
    /// Newest version answered by local durable storage or a durable peer.
    durable_version: AtomicU32,
    durable_notify: tokio::sync::Notify,
    pub(super) current_version: AtomicU32,
    /// Resolved, policy-scoped intent; absent while initial resolution waits.
    pub(super) selection: Mut<Option<(ankql::ast::Selection<Resolved>, u32)>>,
    pub(super) collection_id: CollectionId,
    gap_fetcher: Arc<dyn GapFetcher<Entity>>,
    pub(super) schema: Option<&'static crate::schema::ModelStructDescriptor>,
    pub(super) cached: bool,
    registry: Weak<registry::RegistryInner>,
    pub(super) resolution_task: std::sync::Mutex<Option<RemoteHandle<()>>>,
    #[cfg(test)]
    before_wait: std::sync::Mutex<Option<Box<dyn FnOnce(&Self) + Send>>>,
}

impl LiveQueryInner {
    /// Create the shared inner before its resolved selection is installed.
    pub(super) fn new<SE, PA>(
        node: &Node<SE, PA>,
        context: Arc<dyn DynContextInner>,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        cached: bool,
        collection_id: CollectionId,
    ) -> Self
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let subscription = node.reactor.subscribe();

        let query_id = proto::QueryId::new();
        let gap_fetcher: Arc<dyn GapFetcher<Entity>> = Arc::new(QueryGapFetcher::new(Context(context.clone())));

        Self {
            query_id,
            context,
            subscription,
            resultset: EntityResultSet::empty(),
            error: Mut::new(None),
            version_lock: std::sync::Mutex::new(()),
            initialized_notify: tokio::sync::Notify::new(),
            initialized_version: AtomicU32::new(0),
            durable_version: AtomicU32::new(0),
            durable_notify: tokio::sync::Notify::new(),
            current_version: AtomicU32::new(1),
            selection: Mut::new(None),
            collection_id,
            gap_fetcher,
            schema,
            cached,
            registry: node.live_queries.downgrade(),
            resolution_task: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_wait: std::sync::Mutex::new(None),
        }
    }

    /// Wait for local results or an initialization error, following the current query version.
    pub(super) async fn wait_initialized(&self) -> Result<(), RetrievalError> {
        loop {
            let notified = self.initialized_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(error) = self.initialization_error() {
                return Err(error);
            }

            if self.initialized_version.load(Ordering::Acquire) >= self.current_version.load(Ordering::Acquire) {
                return Ok(());
            }

            #[cfg(test)]
            self.before_wait();
            notified.await;
        }
    }

    /// Wait for initialization and a durable answer; cached local results alone are insufficient.
    pub(super) async fn wait_durable_answered(&self) -> Result<(), RetrievalError> {
        self.wait_initialized().await?;
        loop {
            let notified = self.durable_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(error) = self.initialization_error() {
                return Err(error);
            }

            if self.durable_version.load(Ordering::Acquire) >= self.current_version.load(Ordering::Acquire) {
                return Ok(());
            }
            #[cfg(test)]
            self.before_wait();
            notified.await;
        }
    }

    #[cfg(test)]
    fn before_wait(&self) {
        let hook = self.before_wait.lock().unwrap().take();
        if let Some(hook) = hook {
            hook(self);
        }
    }

    /// Record a durable answer without letting stale confirmations regress it.
    pub(super) fn mark_durable_answered(&self, version: u32) {
        self.durable_version.fetch_max(version, Ordering::AcqRel);
        self.durable_notify.notify_waiters();
    }

    /// Return the current version's initialization failure, if any.
    fn initialization_error(&self) -> Option<RetrievalError> {
        let _state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        self.error.with(|error| error.as_deref().cloned())
    }

    /// Publish an initialization failure unless this query version has been superseded.
    pub(super) fn fail_initialization(&self, version: u32, error: RetrievalError) {
        let state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        if !self.is_current(version) {
            return;
        }
        self.error.set_before_notify(Some(Arc::new(error)), || drop(state));
        self.initialized_notify.notify_waiters();
        self.durable_notify.notify_waiters();
    }

    /// Start a new version and clear its error, retaining the installed selection until replacement.
    pub(super) fn advance_version(&self) -> u32 {
        let state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        let previous = self
            .current_version
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |version| version.checked_add(1))
            .expect("live-query version exhausted");
        let version = previous + 1;
        self.resolution_task.lock().unwrap().take();
        self.error.set_before_notify(None, || drop(state));
        version
    }

    /// Install or refresh the resolved query in the reactor using local storage.
    /// Used for local initialization and after a peer's initial rows are applied; ignores superseded versions.
    pub(super) async fn activate(&self, version: u32) -> Result<(), RetrievalError> {
        let Some((selection, stored_version)) = self.selection.value() else {
            return Err(RetrievalError::Other("live query activated before its selection was resolved".into()));
        };

        if version != stored_version || !self.is_current(version) {
            warn!("LiveQuery - Dropped stale activation request for version {} (current version is {})", version, stored_version);
            return Ok(());
        }

        debug!("LiveQuery.activate() for predicate {} (version {})", self.query_id, version);

        let reactor = self.context.reactor().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))?;

        reactor
            .upsert_query_and_notify(
                self.subscription.id(),
                self.query_id,
                self.collection_id.clone(),
                selection,
                self.context.as_ref(),
                self.resultset.clone(),
                self.gap_fetcher.clone(),
                version,
                self,
            )
            .await?;

        Ok(())
    }

    /// Mark the current version initialized before the reactor notifies result observers.
    fn mark_initialized(&self, version: u32) {
        let state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        if !self.is_current(version) {
            return;
        }
        if self.error.with(Option::is_some) {
            self.error.set_before_notify(None, || {
                self.initialized_version.fetch_max(version, Ordering::AcqRel);
                drop(state);
            });
        } else {
            self.initialized_version.fetch_max(version, Ordering::AcqRel);
            drop(state);
        }
        self.initialized_notify.notify_waiters();
    }
}

impl crate::reactor::PreNotifyHook for &LiveQueryInner {
    fn is_current(&self, version: u32) -> bool { self.current_version.load(Ordering::Acquire) == version }

    fn pre_notify(&self, version: u32) { self.mark_initialized(version); }
}

impl Drop for LiveQueryInner {
    fn drop(&mut self) {
        self.context.unsubscribe_remote_query(self.query_id);
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister(self);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{PermissiveAgent, DEFAULT_CONTEXT},
        test_utils::TestStorage,
    };

    fn query() -> EntityLiveQuery {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let context = Context::new(node.clone(), DEFAULT_CONTEXT);
        crate::livequery::EntityLiveQuery(Arc::new(LiveQueryInner::new(&node, context.0, None, false, "test".into())))
    }

    #[tokio::test]
    async fn waiters_follow_the_current_version_after_stale_wakes() {
        let query = query();
        query.0.mark_initialized(1);
        let durable = query.wait_durable_answered();
        tokio::pin!(durable);
        assert!(futures::poll!(&mut durable).is_pending());
        let version = query.0.advance_version();
        let initialized = query.wait_initialized();
        tokio::pin!(initialized);
        assert!(futures::poll!(&mut initialized).is_pending());

        query.0.mark_initialized(1);
        query.0.initialized_notify.notify_waiters();
        query.0.mark_durable_answered(1);
        assert!(futures::poll!(&mut initialized).is_pending());
        assert!(futures::poll!(&mut durable).is_pending());
        query.0.mark_initialized(version);
        assert!(futures::poll!(&mut initialized).is_ready());
        assert!(futures::poll!(&mut durable).is_pending());
        query.0.mark_durable_answered(version);
        durable.await.unwrap();
    }

    #[tokio::test]
    async fn notification_between_check_and_suspend_is_not_lost() {
        for durable in [false, true] {
            for fail in [false, true] {
                let query = query();
                if durable {
                    query.0.mark_initialized(1);
                }
                *query.0.before_wait.lock().unwrap() = Some(Box::new(move |inner| {
                    if fail {
                        inner.fail_initialization(1, RetrievalError::NoDurablePeers);
                    } else if durable {
                        inner.mark_durable_answered(1);
                    } else {
                        inner.mark_initialized(1);
                    }
                }));
                let wait = async {
                    if durable {
                        query.wait_durable_answered().await
                    } else {
                        query.wait_initialized().await
                    }
                };
                tokio::pin!(wait);
                let std::task::Poll::Ready(result) = futures::poll!(wait) else {
                    panic!("the notification between checking and suspension was lost");
                };
                if fail {
                    assert!(matches!(result, Err(RetrievalError::NoDurablePeers)));
                } else {
                    result.unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn failure_wakes_waiters_and_preserves_the_error_variant() {
        let query = query();
        let initialized = query.wait_initialized();
        let durable = query.wait_durable_answered();
        tokio::pin!(initialized, durable);
        assert!(futures::poll!(&mut initialized).is_pending());
        assert!(futures::poll!(&mut durable).is_pending());
        query.fail_resolution(1, RetrievalError::AccessDenied(crate::policy::AccessDenied::ByPolicy("denied")));
        for result in [initialized.await, durable.await, query.wait_initialized().await] {
            assert!(matches!(result, Err(RetrievalError::AccessDenied(crate::policy::AccessDenied::ByPolicy("denied")))));
        }
        assert!(matches!(query.error().value().as_deref(), Some(RetrievalError::AccessDenied(_))));
        let version = query.0.advance_version();
        query.fail_resolution(1, RetrievalError::Other("stale failure".into()));
        assert!(query.error().value().is_none());
        query.0.mark_initialized(version);
        query.0.mark_durable_answered(version);
        query.wait_durable_answered().await.unwrap();
    }

    #[tokio::test]
    async fn selection_parse_errors_keep_their_variant() {
        let query = query();
        assert!(matches!(query.update_selection("("), Err(RetrievalError::ParseError(_))));
    }
}
