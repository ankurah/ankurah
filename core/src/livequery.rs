use crate::context::DynContextInner;
use crate::internal::prelude::*;
use crate::reactor::PreNotifyHook;
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

    pub(crate) fn map<R: crate::model::ComponentViews>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

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
                let query = self.weak();
                crate::task::spawn(async move {
                    query.activate(version, !has_relay).await;
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

    /// Snapshot conflicts leave initialization pending. Wait without retaining
    /// the query, so dropping or replacing it also ends its retries.
    async fn activate(&self, version: u32, durable: bool) {
        let mut delay = std::time::Duration::from_millis(50);
        loop {
            let Some(inner) = self.0.upgrade() else { return };
            if !inner.as_ref().is_current(version) {
                return;
            }
            match inner.activate(version).await {
                Ok(()) => {
                    if durable {
                        inner.mark_durable_answered(version);
                    }
                    return;
                }
                Err(RetrievalError::MutationError(error)) if matches!(*error, MutationError::WriteConflict) => {}
                Err(error) => {
                    inner.fail_initialization(version, error);
                    return;
                }
            }
            drop(inner);
            futures_timer::Delay::new(delay).await;
            delay = (delay * 2).min(std::time::Duration::from_secs(1));
        }
    }
}

#[async_trait::async_trait]
impl crate::peer_subscription::RemoteQuerySubscriber for WeakEntityLiveQuery {
    async fn subscription_established(&self, version: u32) { self.activate(version, true).await; }

    fn set_last_error(&self, version: u32, error: RetrievalError) {
        if let Some(inner) = self.0.upgrade() {
            tracing::info!("Setting last error for LiveQuery {}: {}", inner.query_id, error);
            inner.fail_initialization(version, error);
        }
    }
}
