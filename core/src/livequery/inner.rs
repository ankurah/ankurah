use crate::context::{Context, DynContextInner};
use crate::internal::prelude::*;
use crate::reactor::fetch_gap::{GapFetcher, QueryGapFetcher};
use crate::reactor::{PreNotifyHook, ReactorSubscription};
use ankql::ast::Resolved;
use ankurah_signals::Mut;
use futures::future::RemoteHandle;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tracing::{debug, warn};

/// Shared query state; versions identify selection attempts, not system epochs.
pub(super) struct LiveQueryInner {
    pub(super) tree: Arc<crate::node::state_sync::QueryTree>,
    pub(super) remote_activated_version: AtomicU32,
    remote_stream: std::sync::Mutex<Option<RemoteStream>>,
    pub(super) query_id: proto::QueryId,
    // Must drop before context, which may hold the node's last strong reference.
    pub(super) subscription: ReactorSubscription,
    pub(super) context: Arc<dyn DynContextInner>,
    pub(super) resultset: EntityResultSet,
    pub(super) error: Mut<Option<Arc<RetrievalError>>>,
    /// Keep the version check and selection/error publication together: an old resolution
    /// must not overwrite a newer selection. Release before notifying signal listeners,
    /// which may update the query again.
    pub(super) version_lock: std::sync::Mutex<()>,
    initialized_notify: tokio::sync::Notify,
    initialized_version: AtomicU32,
    /// Newest version answered by local durable storage or a durable peer.
    durable_version: AtomicU32,
    durable_notify: tokio::sync::Notify,
    pub(super) current_version: AtomicU32,
    /// Resolved, policy-scoped intent; absent while initial resolution waits.
    pub(super) selection: Mut<Option<(ankql::ast::Selection<Resolved>, u32)>>,
    gap_fetcher: Arc<dyn GapFetcher<Entity>>,
    pub(super) cache_policy: CachePolicy,
    /// Dropping this handle cancels pending resolution on replacement or query drop.
    /// The task holds only a weak query reference, so it cannot keep its owner alive.
    pub(super) resolution_task: std::sync::Mutex<Option<RemoteHandle<()>>>,
    #[cfg(test)]
    before_wait: std::sync::Mutex<Option<Box<dyn FnOnce(&Self) + Send>>>,
}

struct RemoteStream {
    version: u32,
    origin: proto::RequestId,
    activated: bool,
    installed: bool,
    candidates: Arc<std::sync::RwLock<std::collections::HashSet<proto::EntityId>>>,
    refresh: Option<crate::node::state_sync::RefreshHandle>,
}

impl LiveQueryInner {
    fn is_current(&self, version: u32) -> bool { self.current_version.load(Ordering::Acquire) == version }

    /// Create the shared inner before its resolved selection is installed.
    pub(super) fn new(context: Arc<dyn DynContextInner>, subscription: ReactorSubscription, cache_policy: CachePolicy) -> Self {
        let query_id = proto::QueryId::new();
        let gap_fetcher: Arc<dyn GapFetcher<Entity>> = Arc::new(QueryGapFetcher::new(Context(context.clone())));

        Self {
            tree: Arc::new(crate::node::state_sync::QueryTree::default()),
            remote_activated_version: AtomicU32::new(0),
            remote_stream: std::sync::Mutex::new(None),
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
            gap_fetcher,
            cache_policy,
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
        let _state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        if !self.is_current(version) || self.remote_stream_pending(version) {
            return;
        }
        self.durable_version.fetch_max(version, Ordering::AcqRel);
        self.durable_notify.notify_waiters();
    }

    pub(super) fn bind_remote_stream(&self, version: u32, origin: proto::RequestId, canceled: &(dyn Fn() -> bool + Send + Sync)) {
        let mut results = self.resultset.write();
        let state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut stream = self.remote_stream.lock().unwrap();
        if self.is_current(version) && !canceled() {
            // A replacement can bind before the old loss callback runs.
            // Its pending state must already invalidate that earlier answer.
            if stream.as_ref().is_some_and(|stream| stream.version == version && stream.activated) {
                self.initialized_version.store(version.saturating_sub(1), Ordering::Release);
                self.remote_activated_version.store(0, Ordering::Release);
                results.set_loaded(false);
            }
            self.durable_version.store(version.saturating_sub(1), Ordering::Release);
            *stream = Some(RemoteStream {
                version,
                origin,
                activated: false,
                installed: false,
                candidates: Arc::new(std::sync::RwLock::new(results.iter_entities().map(|(id, _)| id).collect())),
                refresh: None,
            });
        }
        drop(stream);
        drop(state);
        drop(results);
    }

    pub(super) fn set_candidates(&self, version: u32, origin: &proto::RequestId, ids: impl IntoIterator<Item = proto::EntityId>) -> bool {
        let stream = self.remote_stream.lock().unwrap();
        let Some(stream) = stream.as_ref().filter(|stream| stream.version == version && &stream.origin == origin) else { return false };
        *stream.candidates.write().unwrap() = ids.into_iter().collect();
        true
    }

    pub(super) fn set_refresh(&self, version: u32, origin: &proto::RequestId, refresh: crate::node::state_sync::RefreshHandle) {
        let mut stream = self.remote_stream.lock().unwrap();
        if let Some(stream) = stream.as_mut().filter(|stream| stream.version == version && &stream.origin == origin) {
            stream.refresh = Some(refresh);
        }
    }

    pub(super) fn refresh_handle(&self) -> Option<crate::node::state_sync::RefreshHandle> {
        self.remote_stream.lock().unwrap().as_ref().filter(|stream| stream.activated).and_then(|stream| stream.refresh.clone())
    }

    pub(super) fn remote_stream_pending(&self, version: u32) -> bool {
        self.remote_stream.lock().unwrap().as_ref().is_some_and(|stream| stream.version == version && !stream.activated)
    }

    fn remote_stream_current(&self, version: u32, origin: &proto::RequestId) -> bool {
        self.is_current(version)
            && self.remote_stream.lock().unwrap().as_ref().is_some_and(|stream| stream.version == version && &stream.origin == origin)
    }

    pub(super) fn lose_remote_stream(&self, version: u32, origin: &proto::RequestId) {
        let mut results = self.resultset.write();
        let state = self.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut bound = self.remote_stream.lock().unwrap();
        let Some(stream) = bound.as_mut().filter(|stream| stream.version == version && &stream.origin == origin) else { return };
        if !self.is_current(version) {
            return;
        }
        stream.activated = false;
        self.remote_activated_version.store(0, Ordering::Release);
        self.initialized_version.store(version.saturating_sub(1), Ordering::Release);
        self.durable_version.store(version.saturating_sub(1), Ordering::Release);
        results.set_loaded(false);
        drop(bound);
        drop(state);
        drop(results);
        self.initialized_notify.notify_waiters();
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

        let reactor = self.context.node()?.reactor().clone();

        reactor
            .upsert_query_and_notify(
                self.subscription.id(),
                self.query_id,
                selection,
                self.context.clone(),
                self.resultset.clone(),
                self.gap_fetcher.clone(),
                version,
                LocalActivation(self),
            )
            .await?;

        Ok(())
    }

    pub(super) async fn install_remote(
        &self,
        version: u32,
        entities: Vec<Entity>,
        origin: &proto::RequestId,
        canceled: &(dyn Fn() -> bool + Send + Sync),
        complete: bool,
    ) -> Result<bool, RetrievalError> {
        let Some((selection, stored_version)) = self.selection.value() else { return Ok(false) };
        if version != stored_version || canceled() || !self.remote_stream_current(version, origin) {
            return Ok(false);
        }
        let source = {
            let stream = self.remote_stream.lock().unwrap();
            let Some(stream) = stream.as_ref().filter(|stream| stream.version == version && &stream.origin == origin) else {
                return Ok(false);
            };
            Arc::new(PeerQuerySource { context: self.context.clone(), candidates: stream.candidates.clone() })
        };
        self.context
            .node()?
            .reactor()
            .upsert_query_from_peer(
                self.subscription.id(),
                self.query_id,
                selection,
                source.clone(),
                self.resultset.clone(),
                source,
                version,
                RemoteActivation { query: self, origin, canceled, complete },
                entities,
            )
            .await?;
        if canceled() || !self.remote_stream_current(version, origin) {
            return Ok(false);
        }
        Ok(true)
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

/// The source owns the remote candidate window. Local merge/policy evaluation
/// may remove candidates, but a local cache scan cannot invent a source winner.
struct PeerQuerySource {
    context: Arc<dyn DynContextInner>,
    candidates: Arc<std::sync::RwLock<std::collections::HashSet<proto::EntityId>>>,
}
#[async_trait::async_trait]
impl crate::reactor::LocalEntitySource for PeerQuerySource {
    fn can_read(&self, entity: &Entity) -> bool { self.context.can_read(entity) }
    fn admits_query_entity(&self, entity: &Entity) -> bool { self.candidates.read().unwrap().contains(&entity.id()) }
    async fn fetch_entities_from_local(&self, selection: &ankql::ast::Selection<Resolved>) -> Result<Vec<Entity>, RetrievalError> {
        Ok(self.context.fetch_entities_from_local(selection).await?.into_iter().filter(|entity| self.admits_query_entity(entity)).collect())
    }
}
#[async_trait::async_trait]
impl GapFetcher<Entity> for PeerQuerySource {
    async fn fetch_gap(
        &self,
        _selection: &ankql::ast::Selection<Resolved>,
        _last: Option<&Entity>,
        _size: usize,
    ) -> Result<Vec<Entity>, RetrievalError> {
        // A remote LIMIT window is refilled by coherent source publications.
        // Local storage can contain predicate matches outside that window.
        Ok(Vec::new())
    }
}

struct RemoteActivation<'a> {
    query: &'a LiveQueryInner,
    origin: &'a proto::RequestId,
    canceled: &'a (dyn Fn() -> bool + Send + Sync),
    complete: bool,
}

impl crate::reactor::PreNotifyHook for RemoteActivation<'_> {
    fn is_current(&self, version: u32) -> bool { !(self.canceled)() && self.query.remote_stream_current(version, self.origin) }

    fn marks_loaded(&self) -> bool { self.complete }

    fn pre_notify(&self, version: u32) {
        let state = self.query.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        let mut stream = self.query.remote_stream.lock().unwrap();
        let Some(current) = stream.as_mut().filter(|stream| stream.version == version && &stream.origin == self.origin) else { return };
        if (self.canceled)() || !self.query.is_current(version) {
            return;
        }
        current.installed = true;
        if !self.complete {
            return;
        }
        current.activated = true;
        self.query.remote_activated_version.store(version, Ordering::Release);
        self.query.initialized_version.fetch_max(version, Ordering::AcqRel);
        drop(stream);
        if self.query.error.with(Option::is_some) {
            self.query.error.set_before_notify(None, || drop(state));
        } else {
            drop(state);
        }
        self.query.initialized_notify.notify_waiters();
    }
}

// A cached initialization may be in flight when the peer answers. It must
// not replace the peer's authoritative membership after that installation.
struct LocalActivation<'a>(&'a LiveQueryInner);
impl PreNotifyHook for LocalActivation<'_> {
    fn is_current(&self, version: u32) -> bool {
        self.0.is_current(version)
            && !self.0.remote_stream.lock().unwrap().as_ref().is_some_and(|stream| stream.version == version && stream.installed)
    }
    fn pre_notify(&self, version: u32) { self.0.mark_initialized(version); }
}

impl Drop for LiveQueryInner {
    fn drop(&mut self) {
        if let Ok(node) = self.context.node() {
            node.unsubscribe_remote_query(self.query_id);
            node.live_queries().unregister(self);
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
        crate::livequery::EntityLiveQuery(Arc::new(LiveQueryInner::new(context.0, node.reactor.subscribe(), CachePolicy::Durable)))
    }

    #[tokio::test]
    async fn a_failed_stream_clears_readiness_and_old_callbacks_cannot_restore_it() {
        let query = query();
        let original = proto::RequestId::new();
        let current = proto::RequestId::new();
        let canceled = || false;
        query.0.bind_remote_stream(1, original.clone(), &canceled);
        RemoteActivation { query: &query.0, origin: &original, canceled: &canceled, complete: true }.pre_notify(1);
        query.0.resultset.set_loaded(true);
        query.0.mark_durable_answered(1);
        query.wait_durable_answered().await.unwrap();

        let weak = query.weak();
        let guard = ankurah_signals::Signal::listen(
            &query.resultset(),
            Arc::new(move |_: ()| {
                let query = weak.upgrade().unwrap();
                query.0.mark_durable_answered(1);
                assert!(!query.resultset().is_loaded());
            }),
        );
        query.0.lose_remote_stream(1, &original);
        drop(guard);
        assert!(!query.resultset().is_loaded());
        let waiting = query.wait_durable_answered();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        query.0.mark_durable_answered(1);
        assert!(futures::poll!(&mut waiting).is_pending(), "late readiness from the failed stream must be ignored");

        query.0.bind_remote_stream(1, current.clone(), &canceled);
        query.0.lose_remote_stream(1, &original);
        RemoteActivation { query: &query.0, origin: &original, canceled: &canceled, complete: true }.pre_notify(1);
        query.0.mark_durable_answered(1);
        assert!(futures::poll!(&mut waiting).is_pending());
        RemoteActivation { query: &query.0, origin: &current, canceled: &canceled, complete: true }.pre_notify(1);
        query.0.mark_durable_answered(1);
        waiting.await.unwrap();
        query.0.lose_remote_stream(1, &original);
        query.wait_durable_answered().await.unwrap();
    }

    #[tokio::test]
    async fn a_canceled_activation_cannot_publish_readiness() {
        let query = query();
        let origin = proto::RequestId::new();
        query.0.bind_remote_stream(1, origin.clone(), &|| false);
        let canceled = || true;
        let activation = RemoteActivation { query: &query.0, origin: &origin, canceled: &canceled, complete: true };
        assert!(!activation.is_current(1));
        activation.pre_notify(1);
        query.0.mark_durable_answered(1);
        let waiting = query.wait_durable_answered();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
    }

    #[tokio::test]
    async fn replacement_binding_invalidates_readiness_before_the_old_loss_callback() {
        let query = query();
        let original = proto::RequestId::new();
        let replacement = proto::RequestId::new();
        let canceled = || false;
        query.0.bind_remote_stream(1, original.clone(), &canceled);
        RemoteActivation { query: &query.0, origin: &original, canceled: &canceled, complete: true }.pre_notify(1);
        query.0.mark_durable_answered(1);
        query.wait_durable_answered().await.unwrap();
        query.0.bind_remote_stream(1, replacement.clone(), &canceled);
        query.0.lose_remote_stream(1, &original);
        let waiting = query.wait_durable_answered();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        RemoteActivation { query: &query.0, origin: &replacement, canceled: &canceled, complete: true }.pre_notify(1);
        query.0.mark_durable_answered(1);
        waiting.await.unwrap();
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
        let query = query().map::<crate::schema::catalog::SysModelRowView>();
        assert!(matches!(query.update_selection("("), Err(RetrievalError::ParseError(_))));
    }
}
