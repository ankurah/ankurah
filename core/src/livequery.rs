use crate::context::{Context, DynContextInner};
use crate::internal::prelude::*;
use ankql::ast::{Parsed, Resolved, Selection, Stage};
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
use resolve::QueryResolutionError;
pub(crate) use resolve::ResolveQuery;
pub use typed::LiveQuery;

/// A type-erased local query, including remote subscription cleanup.
#[derive(Clone)]
pub struct EntityLiveQuery(Arc<LiveQueryInner>);

impl EntityLiveQuery {
    pub fn new<SE, PA>(
        node: &Node<SE, PA>,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: CollectionId,
        args: MatchArgs<Parsed>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        Context::new(node.clone(), sessions).0.query(schema, collection_id, args)
    }

    /// Create a node-owned query without forming a node/query reference cycle.
    pub fn new_with_weak_node<SE, PA>(
        node: &Node<SE, PA>,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: CollectionId,
        args: MatchArgs<Parsed>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        Context::new_weak(node, sessions).0.query(schema, collection_id, args)
    }

    /// Create a query, returning locally detectable resolution errors immediately.
    /// Missing readiness or declaration bindings resolve in the background; failures appear in `error()`.
    pub(crate) fn new_with_context<SE, PA, S: Stage>(
        node: &Node<SE, PA>,
        context: Arc<dyn DynContextInner>,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: CollectionId,
        args: MatchArgs<S>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        MatchArgs<S>: ResolveQuery,
    {
        node.system.check_not_halted()?;
        let cached = args.cached;
        let resolution = match args.resolve(context.schema_resolver(), schema, &collection_id) {
            Ok(args) => Ok(args.selection),
            Err(QueryResolutionError { error: RetrievalError::NodeNotReady, selection }) => Err(selection),
            Err(QueryResolutionError { error: RetrievalError::UnboundDeclaration { .. }, selection }) if schema.is_some() => Err(selection),
            Err(QueryResolutionError { error, .. }) => return Err(error),
        };
        let me = Self(Arc::new(LiveQueryInner::new(node, context, schema, cached, collection_id)));
        node.live_queries.insert(&me);
        match resolution {
            Ok(selection) => me.install_resolved(selection, 1)?,
            Err(selection) => me.spawn_query_resolution(selection, 1),
        }
        Ok(me)
    }

    /// Resolve in the background; query drop or a newer selection cancels the task.
    pub(crate) fn spawn_query_resolution(&self, selection: Selection<Parsed>, version: u32) {
        let state = self.0.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        if self.0.current_version.load(Ordering::Acquire) != version {
            return;
        }
        let resolution =
            self.0.context.schema_resolver().resolve_query_selection_when_ready(self.0.schema, self.0.collection_id.clone(), selection);
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

    pub fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    pub(crate) fn fail_resolution(&self, version: u32, error: RetrievalError) { self.0.fail_initialization(version, error); }

    /// Install a resolved selection and start its subscription; ignore superseded versions.
    pub(crate) fn install_resolved(&self, selection: ankql::ast::Selection<Resolved>, version: u32) -> Result<(), RetrievalError> {
        let state = self.0.version_lock.lock().unwrap_or_else(|error| error.into_inner());
        if self.0.current_version.load(Ordering::Acquire) != version {
            return Ok(());
        }
        let cached = self.0.cached && self.0.selection.with(Option::is_none);
        self.0.selection.set_before_notify(Some((selection.clone(), version)), || {
            let has_relay = self.0.context.subscribe_remote_query(self, selection, version)?;
            if cached || !has_relay {
                let inner = self.0.clone();
                crate::task::spawn(async move {
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

    /// Accept a new selection, returning errors detectable from local bindings immediately.
    /// Registration and initialization may finish later; failures appear in `error()`.
    pub fn update_selection(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        let new_selection = new_selection.try_into().map_err(|e| e.into())?;
        let resolved =
            self.0.context.schema_resolver().resolve_query_selection(self.0.schema, &self.0.collection_id, new_selection.clone());
        let resolved = match resolved {
            Ok(selection) => Some(selection),
            Err(RetrievalError::NodeNotReady) => None,
            Err(RetrievalError::UnboundDeclaration { .. }) if self.0.schema.is_some() => None,
            Err(error) => return Err(error),
        };
        let new_version = self.0.advance_version();
        self.0.resultset.set_loaded(false);
        match resolved {
            Some(resolved) => match self.install_resolved(resolved, new_version) {
                Ok(()) => Ok(()),
                Err(error) => {
                    self.0.fail_initialization(new_version, error.clone());
                    Err(error)
                }
            },
            None => {
                self.spawn_query_resolution(new_selection, new_version);
                Ok(())
            }
        }
    }

    /// Update the selection and wait for the current version to initialize, returning any failure.
    pub async fn update_selection_wait(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        self.update_selection(new_selection)?;
        self.0.wait_initialized().await
    }

    /// The current version's initialization error, cleared when a new version starts.
    pub fn error(&self) -> Read<Option<Arc<RetrievalError>>> { self.0.error.read() }
    pub fn query_id(&self) -> proto::QueryId { self.0.query_id }
    pub(crate) fn collection_id(&self) -> &CollectionId { &self.0.collection_id }
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
}
