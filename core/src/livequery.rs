use crate::context::DynContextInner;
use crate::internal::prelude::*;
use ankql::ast::{Resolved, Selection};
use ankurah_signals::Read;
use futures::FutureExt;
use std::{
    marker::PhantomData,
    sync::{atomic::Ordering, Arc, Weak},
};

mod inner;
mod registry;
mod resolve;
mod typed;

use inner::LiveQueryInner;
pub(crate) use registry::LiveQueryRegistry;
pub(crate) use resolve::QueryResolution;
pub use typed::LiveQuery;

/// A type-erased local query, including remote subscription cleanup.
#[derive(Clone)]
pub struct EntityLiveQuery(Arc<LiveQueryInner>);

impl EntityLiveQuery {
    pub(crate) fn new(
        context: Arc<dyn DynContextInner>,
        cache_policy: CachePolicy,
        resolution: QueryResolution,
    ) -> Result<Self, RetrievalError> {
        let node = context.node()?;
        node.check_not_halted()?;
        let me = Self(Arc::new(LiveQueryInner::new(context, node.reactor().subscribe(), cache_policy)));
        node.live_queries().insert(&me);
        me.apply_resolution(resolution, 1)?;
        Ok(me)
    }

    fn apply_resolution(&self, resolution: QueryResolution, version: u32) -> Result<(), RetrievalError> {
        match resolution {
            QueryResolution::Resolved(selection) => self.install_resolved(selection, version),
            QueryResolution::Pending(resolution) => {
                self.spawn_query_resolution(resolution, version);
                Ok(())
            }
        }
    }

    /// Query drop or a newer selection cancels pending resolution.
    fn spawn_query_resolution(
        &self,
        resolution: futures::future::BoxFuture<'static, Result<Selection<Resolved>, RetrievalError>>,
        version: u32,
    ) {
        let state = self.0.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        if self.0.current_version.load(Ordering::Acquire) != version {
            return;
        }
        let query = self.weak();
        let (task, handle) = async move {
            let resolved = resolution.await;
            if let Some(query) = query.upgrade() {
                if let Err(error) = resolved.and_then(|selection| query.install_resolved(selection, version)) {
                    query.fail_resolution(version, error);
                }
            }
        }
        .remote_handle();
        *self.0.resolution_task.lock().unwrap() = Some(handle);
        drop(state);
        crate::task::spawn(task);
    }

    pub(crate) fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    pub(crate) fn fail_resolution(&self, version: u32, error: RetrievalError) { self.0.fail_initialization(version, error); }

    /// Install a resolved selection and start its subscription; ignore superseded versions.
    pub(crate) fn install_resolved(&self, selection: ankql::ast::Selection<Resolved>, version: u32) -> Result<(), RetrievalError> {
        let state = self.0.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        if self.0.current_version.load(Ordering::Acquire) != version {
            return Ok(());
        }
        let cached = self.0.cache_policy == CachePolicy::Local && self.0.selection.with(Option::is_none);
        self.0.selection.set_before_notify(Some((selection.clone(), version)), || {
            let has_relay = self.0.context.subscribe_remote_query(self, selection, version)?;
            if cached || !has_relay {
                let inner = self.0.clone();
                crate::task::spawn(async move {
                    if inner.remote_activated_version.load(Ordering::Acquire) == version {
                        return;
                    }
                    match inner.activate(version).await {
                        Ok(()) if !has_relay => inner.mark_durable_answered(version),
                        Ok(()) => {}
                        Err(error) => inner.fail_initialization(version, error),
                    }
                });
            }
            drop(state);
            Ok(())
        })
    }

    /// Wait for the current selection version to initialize, returning any initialization error.
    pub async fn wait_initialized(&self) -> Result<(), RetrievalError> { self.0.wait_initialized().await }

    /// Wait for the current selection's durable answer and local initialization.
    /// A peer's answer includes applying its initial rows; without a relay, local storage answers.
    pub async fn wait_durable_answered(&self) -> Result<(), RetrievalError> { self.0.wait_durable_answered().await }

    /// Replace the selection: advance the version, cancel pending resolution, and restart initialization.
    fn update_resolution(&self, resolution: QueryResolution) -> Result<(), RetrievalError> {
        let version = self.0.advance_version();
        self.0.resultset.set_loaded(false);
        if let Err(error) = self.apply_resolution(resolution, version) {
            self.0.fail_initialization(version, error.clone());
            return Err(error);
        }
        Ok(())
    }

    /// The current version's initialization error, cleared when a new version starts.
    pub fn error(&self) -> Read<Option<Arc<RetrievalError>>> { self.0.error.read() }
    pub(crate) fn is_current(&self, version: u32) -> bool { self.0.current_version.load(Ordering::Acquire) == version }
    /// Recompare this query on its current standing stream. Completion covers the
    /// captured source round; later live publications do not move that goal.
    pub async fn refresh(&self) -> Result<(), RetrievalError> {
        self.0.refresh_handle().ok_or_else(|| RetrievalError::Other("query has no active remote stream".into()))?.refresh().await
    }
    pub(crate) fn set_remote_candidates(
        &self,
        version: u32,
        origin: &proto::RequestId,
        ids: impl IntoIterator<Item = proto::EntityId>,
    ) -> bool {
        self.0.set_candidates(version, origin, ids)
    }
    pub(crate) fn set_refresh(&self, version: u32, origin: &proto::RequestId, refresh: crate::node::state_sync::RefreshHandle) {
        self.0.set_refresh(version, origin, refresh);
    }
    pub(crate) fn resolved_selection(&self) -> Option<(ankql::ast::Selection<Resolved>, u32)> { self.0.selection.value() }
    pub(crate) fn digest_tree(&self) -> Arc<crate::node::state_sync::QueryTree> { self.0.tree.clone() }
    pub(crate) async fn activate_remote(
        &self,
        version: u32,
        entities: Vec<Entity>,
        membership: crate::reactor::PeerQueryMembership,
        changes: Vec<crate::changes::EntityChange>,
        origin: &proto::RequestId,
        canceled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<bool, RetrievalError> {
        self.0.install_remote(version, entities, membership, changes, origin, canceled, true).await
    }
    pub(crate) async fn install_remote_partial(
        &self,
        version: u32,
        entities: Vec<Entity>,
        membership: crate::reactor::PeerQueryMembership,
        changes: Vec<crate::changes::EntityChange>,
        origin: &proto::RequestId,
        canceled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<bool, RetrievalError> {
        self.0.install_remote(version, entities, membership, changes, origin, canceled, false).await
    }
    pub(crate) fn subscription_id(&self) -> crate::reactor::ReactorSubscriptionId { self.0.subscription.id() }
    pub fn query_id(&self) -> proto::QueryId { self.0.query_id }
    /// The latest installed selection and version; `None` until initial resolution.
    pub fn selection(&self) -> Read<Option<(ankql::ast::Selection<Resolved>, u32)>> { self.0.selection.read() }
    pub fn resultset(&self) -> EntityResultSet { self.0.resultset.clone() }

    /// Create a weak reference to this LiveQuery
    pub fn weak(&self) -> WeakEntityLiveQuery { WeakEntityLiveQuery(Arc::downgrade(&self.0)) }
}

/// Weak reference to an [`EntityLiveQuery`].
#[derive(Clone)]
pub struct WeakEntityLiveQuery(Weak<LiveQueryInner>);

impl WeakEntityLiveQuery {
    pub fn upgrade(&self) -> Option<EntityLiveQuery> { self.0.upgrade().map(EntityLiveQuery) }
}

#[async_trait::async_trait]
impl crate::peer_subscription::RemoteQuerySubscriber for WeakEntityLiveQuery {
    async fn subscription_established(&self, version: u32) {
        if let Some(inner) = self.0.upgrade() {
            tracing::debug!("Subscription established for query {}: {}", inner.query_id, version);
            if inner.remote_stream_pending(version) {
                return;
            }
            if inner.remote_activated_version.load(Ordering::Acquire) == version {
                inner.mark_durable_answered(version);
                return;
            }
            match inner.activate(version).await {
                Ok(()) => inner.mark_durable_answered(version),
                Err(e) => {
                    tracing::error!("Failed to activate subscription for query {}: {}", inner.query_id, e);
                    inner.fail_initialization(version, e);
                }
            }
        }
    }

    fn set_last_error(&self, version: u32, error: RetrievalError) {
        if let Some(inner) = self.0.upgrade() {
            tracing::info!("Setting last error for LiveQuery {}: {}", inner.query_id, error);
            inner.fail_initialization(version, error);
        }
    }

    fn stream_bound(&self, version: u32, origin: proto::RequestId, canceled: Arc<dyn Fn() -> bool + Send + Sync>) {
        if let Some(inner) = self.0.upgrade() {
            inner.bind_remote_stream(version, origin, canceled.as_ref());
        }
    }

    fn subscription_lost(&self, version: u32, origin: &proto::RequestId) {
        if let Some(inner) = self.0.upgrade() {
            inner.lose_remote_stream(version, origin);
        }
    }
}
