use crate::internal::prelude::*;
use ankql::ast::{Parsed, Resolved};
use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Weak,
    },
};

use ankurah_signals::{
    broadcast::BroadcastId,
    porcelain::subscribe::{IntoSubscribeListener, SubscriptionGuard},
    signal::{Listener, ListenerGuard},
    Get, Mut, Peek, Read, Signal, Subscribe,
};
use tracing::{debug, warn};

use crate::reactor::fetch_gap::{GapFetcher, QueryGapFetcher};
use crate::reactor::{ReactorSubscription, ReactorUpdate};
use crate::resultset::ResultSet;

mod registry;

pub(crate) use registry::LiveQueryRegistry;

/// A type-erased local query, including remote subscription cleanup.
#[derive(Clone)]
pub struct EntityLiveQuery(Arc<Inner>);

#[derive(Clone, Copy)]
pub(crate) enum ResolutionCause {
    Initial { cached: bool },
    SelectionUpdate,
    SystemReset,
}

struct Inner {
    pub(crate) query_id: proto::QueryId,
    // Must drop before context; cleanup uses the context's reactor.
    pub(crate) subscription: ReactorSubscription,
    pub(crate) context: crate::context::Context,
    pub(crate) resultset: EntityResultSet,
    pub(crate) error: Mut<Option<Arc<RetrievalError>>>,
    version_state: std::sync::Mutex<()>,
    pub(crate) initialized: tokio::sync::Notify,
    pub(crate) initialized_version: AtomicU32,
    activation: tokio::sync::Mutex<()>,
    /// Newest version answered by local durable storage or a durable peer.
    pub(crate) durable_version: AtomicU32,
    pub(crate) durable_notify: tokio::sync::Notify,
    pub(crate) current_version: AtomicU32,
    /// Resolved, policy-scoped intent; absent while initial resolution waits.
    pub(crate) selection: Mut<Option<(ankql::ast::Selection<Resolved>, u32)>>,
    pub(crate) collection_id: CollectionId,
    pub(crate) gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>>,
    /// Epoch-independent input retained for resolution after a system reset.
    pub(crate) schema: Option<&'static crate::schema::ModelStructDescriptor>,
    pub(crate) parsed: Mut<ankql::ast::Selection<Parsed>>,
    pub(crate) registry: Weak<registry::RegistryInner>,
    drop_tx: tokio::sync::watch::Sender<()>,
}

/// Weak reference to an [`EntityLiveQuery`].
#[derive(Clone)]
pub struct WeakEntityLiveQuery(Weak<Inner>);

impl WeakEntityLiveQuery {
    pub fn upgrade(&self) -> Option<EntityLiveQuery> { self.0.upgrade().map(EntityLiveQuery) }
}

#[derive(Clone)]
pub struct LiveQuery<R: View>(EntityLiveQuery, PhantomData<R>);

impl<R: View> std::ops::Deref for LiveQuery<R> {
    type Target = EntityLiveQuery;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Inner {
    /// Wait for the current selection version to initialize; a failed
    /// initialization is this call's error.
    async fn wait_initialized(&self) -> Result<(), RetrievalError> {
        loop {
            let notified = self.initialized.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(error) = self.initialization_error() {
                return Err(error);
            }

            if self.initialized_version.load(Ordering::Acquire) >= self.current_version.load(Ordering::Acquire) {
                return Ok(());
            }

            notified.await;
        }
    }

    /// Wait for a durable authority to have answered the current selection
    /// version: the relay's confirmation that a peer holds the subscription
    /// and its rows are applied. With no relay, local storage is the authority.
    async fn wait_durable_answered(&self) -> Result<(), RetrievalError> {
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
            notified.await;
        }
    }

    /// Whether a durable authority has answered the current selection
    /// version and the query has initialized it.
    fn is_durable_answered(&self) -> bool {
        let current = self.current_version.load(Ordering::Acquire);
        self.durable_version.load(Ordering::Acquire) >= current && self.initialized_version.load(Ordering::Acquire) >= current
    }

    /// Record a durable answer without letting stale confirmations regress it.
    fn mark_durable_answered(&self, version: u32) {
        self.durable_version.fetch_max(version, Ordering::AcqRel);
        self.durable_notify.notify_waiters();
    }

    /// Return the current version's initialization failure, if any.
    fn initialization_error(&self) -> Option<RetrievalError> {
        let _state = self.version_state.lock().unwrap_or_else(|error| error.into_inner());
        self.error.with(|error| error.as_ref().map(|error| RetrievalError::Shared(error.clone())))
    }

    /// Record the current version's terminal initialization failure and wake
    /// local and durable waiters.
    fn fail_initialization(&self, version: u32, error: RetrievalError) { self.fail_initialization_shared(version, Arc::new(error)); }

    fn fail_initialization_shared(&self, version: u32, error: Arc<RetrievalError>) {
        let state = self.version_state.lock().unwrap_or_else(|error| error.into_inner());
        if self.current_version.load(Ordering::Acquire) != version {
            return;
        }
        self.error.set_before_notify(Some(error.clone()), || drop(state));
        self.initialized.notify_waiters();
        self.durable_notify.notify_waiters();
    }

    fn advance_version(&self, parsed: Option<ankql::ast::Selection<Parsed>>) -> u32 {
        let state = self.version_state.lock().unwrap_or_else(|error| error.into_inner());
        let previous = self
            .current_version
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |version| version.checked_add(1))
            .expect("live-query version exhausted");
        let version = previous + 1;
        let clear_error = || self.error.set_before_notify(None, || drop(state));
        match parsed {
            Some(parsed) => self.parsed.set_before_notify(parsed, clear_error),
            None => clear_error(),
        };
        version
    }

    /// Run a resolved version against local storage, ignoring stale callbacks.
    async fn activate(&self, version: u32) -> Result<(), RetrievalError> {
        let _activation = self.activation.lock().await;
        let Some((selection, stored_version)) = self.selection.value() else {
            return Err(RetrievalError::Other("live query activated before its selection was resolved".into()));
        };

        if version != stored_version || self.current_version.load(Ordering::Acquire) != version {
            warn!("LiveQuery - Dropped stale activation request for version {} (current version is {})", version, stored_version);
            return Ok(());
        }

        debug!("LiveQuery.activate() for predicate {} (version {})", self.query_id, version);

        let reactor = self.context.reactor().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))?;

        let hook = InnerPreNotifyHook(self);
        if !reactor.contains_query(self.subscription.id(), self.query_id) {
            reactor
                .add_query_and_notify(
                    self.subscription.id(),
                    self.query_id,
                    self.collection_id.clone(),
                    selection,
                    &self.context,
                    self.resultset.clone(),
                    self.gap_fetcher.clone(),
                    version,
                    &hook,
                )
                .await?
        } else {
            reactor
                .update_query_and_notify(
                    self.subscription.id(),
                    self.query_id,
                    self.collection_id.clone(),
                    selection,
                    &self.context,
                    version,
                    &hook,
                )
                .await?;
        };

        Ok(())
    }

    fn mark_initialized(&self, version: u32) {
        let state = self.version_state.lock().unwrap_or_else(|error| error.into_inner());
        if self.current_version.load(Ordering::Acquire) != version {
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
        self.initialized.notify_waiters();
    }
}

/// Adapts a borrowed query to the reactor's pre-notify version fence.
struct InnerPreNotifyHook<'a>(&'a Inner);
impl crate::reactor::PreNotifyHook for &InnerPreNotifyHook<'_> {
    fn is_current(&self, version: u32) -> bool { self.0.current_version.load(Ordering::Acquire) == version }

    fn pre_notify(&self, version: u32) { self.0.mark_initialized(version); }
}

/// Create the shared inner before its resolved selection is installed.
fn create_inner<SE, PA>(
    node: &Node<SE, PA>,
    context: crate::context::Context,
    schema: Option<&'static crate::schema::ModelStructDescriptor>,
    parsed: ankql::ast::Selection<Parsed>,
    collection_id: CollectionId,
) -> Arc<Inner>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let subscription = node.reactor.subscribe();
    let (drop_tx, _) = tokio::sync::watch::channel(());

    let query_id = proto::QueryId::new();
    let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(context.clone()));

    Arc::new(Inner {
        query_id,
        context,
        subscription,
        resultset: EntityResultSet::empty(),
        error: Mut::new(None),
        version_state: std::sync::Mutex::new(()),
        initialized: tokio::sync::Notify::new(),
        initialized_version: AtomicU32::new(0),
        activation: tokio::sync::Mutex::new(()),
        durable_version: AtomicU32::new(0),
        durable_notify: tokio::sync::Notify::new(),
        current_version: AtomicU32::new(1),
        selection: Mut::new(None),
        collection_id,
        gap_fetcher,
        schema,
        parsed: Mut::new(parsed),
        registry: node.live_queries.downgrade(),
        drop_tx,
    })
}

/// Install version 1 and start it locally, remotely, or both when cached.
fn start_resolved<SE, PA>(
    inner: &Arc<Inner>,
    node: &Node<SE, PA>,
    me: &EntityLiveQuery,
    selection: ankql::ast::Selection<Resolved>,
    cached: bool,
    sessions: SessionSet<PA::ContextData>,
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let state = inner.version_state.lock().unwrap_or_else(|error| error.into_inner());
    if inner.current_version.load(Ordering::Acquire) != 1 {
        return;
    }

    let query_id = inner.query_id;
    let has_relay = node.subscription_relay.is_some();
    inner.selection.set_before_notify(Some((selection.clone(), 1)), || {
        if cached || !has_relay {
            let inner = inner.clone();
            crate::task::spawn(async move {
                if let Err(error) = inner.activate(1).await {
                    debug!("LiveQuery initialization failed for predicate {}: {}", query_id, error);
                    inner.fail_initialization(1, error);
                }
            });
        }
        if has_relay {
            node.subscribe_remote_query(query_id, inner.collection_id.clone(), selection, sessions, 1, me.weak());
        } else {
            inner.mark_durable_answered(1);
        }
        drop(state);
    });
}

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
        crate::context::Context::new(node.clone(), sessions).query_entity(schema, collection_id, args)
    }

    /// Create a node-owned query without forming a node/query reference cycle.
    pub fn new_weak_node<SE, PA>(
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
        crate::context::Context::new_weak(node, sessions).query_entity(schema, collection_id, args)
    }

    pub(crate) fn new_with_context<SE, PA>(
        node: &Node<SE, PA>,
        context: crate::context::Context,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: CollectionId,
        args: MatchArgs<Parsed>,
        sessions: SessionSet<PA::ContextData>,
        resolved: Option<(ankql::ast::Selection<Resolved>, Option<crate::schema::SchemaEpoch>)>,
    ) -> Self
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let MatchArgs { selection, cached } = args;
        let inner = create_inner(node, context, schema, selection, collection_id);
        let me = Self(inner.clone());

        let resetting = node.live_queries.insert(Arc::as_ptr(&inner) as usize, me.weak());

        match (resetting, resolved) {
            (true, _) => me.0.context.schedule_query_resolution(&me, 1, ResolutionCause::SystemReset),
            (false, Some((selection, epoch))) if node.system.schema_epoch() == epoch => {
                start_resolved(&inner, node, &me, selection, cached, sessions)
            }
            (false, _) => me.0.context.schedule_query_resolution(&me, 1, ResolutionCause::Initial { cached }),
        }

        me
    }
    pub fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    pub(crate) fn system_reset(&self) {
        self.0.context.suspend_remote_query(self.0.query_id);
        let version = self.0.advance_version(None);
        self.0.resultset.set_loaded(false);
        self.0.context.schedule_query_resolution(self, version, ResolutionCause::SystemReset);
    }

    pub(crate) fn resolution_state(
        &self,
    ) -> (Option<&'static crate::schema::ModelStructDescriptor>, CollectionId, ankql::ast::Selection<Parsed>) {
        (self.0.schema, self.0.collection_id.clone(), self.0.parsed.value())
    }

    pub(crate) fn drop_receiver(&self) -> tokio::sync::watch::Receiver<()> { self.0.drop_tx.subscribe() }

    pub(crate) fn fail_resolution(&self, version: u32, error: RetrievalError) { self.0.fail_initialization(version, error); }

    pub(crate) fn install_resolved<SE, PA>(
        &self,
        node: &Node<SE, PA>,
        selection: ankql::ast::Selection<Resolved>,
        sessions: SessionSet<PA::ContextData>,
        version: u32,
        cause: ResolutionCause,
    ) where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        match cause {
            ResolutionCause::Initial { cached } => {
                start_resolved(&self.0, node, self, selection, cached, sessions);
            }
            ResolutionCause::SystemReset if node.subscription_relay.is_some() => {
                let state = self.0.version_state.lock().unwrap_or_else(|error| error.into_inner());
                if self.0.current_version.load(Ordering::Acquire) != version {
                    return;
                }
                self.0.selection.set_before_notify(Some((selection.clone(), version)), || {
                    node.subscribe_remote_query(self.0.query_id, self.0.collection_id.clone(), selection, sessions, version, self.weak());
                    drop(state);
                });
            }
            ResolutionCause::SelectionUpdate | ResolutionCause::SystemReset => {
                if let Err(error) = self.install_selection_update(selection, version) {
                    self.0.fail_initialization(version, error);
                }
            }
        }
    }

    /// Wait for the current selection version to initialize; a failed
    /// initialization is this call's error.
    pub async fn wait_initialized(&self) -> Result<(), RetrievalError> { self.0.wait_initialized().await }

    /// Wait for a durable authority to have answered the current selection
    /// version. See [`Inner::wait_durable_answered`].
    pub async fn wait_durable_answered(&self) -> Result<(), RetrievalError> { self.0.wait_durable_answered().await }

    /// Whether a durable authority has answered the current selection version.
    pub(crate) fn is_durable_answered(&self) -> bool { self.0.is_durable_answered() }

    pub fn update_selection(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        let new_selection = new_selection.try_into().map_err(|e| e.into())?;
        let epoch = self.0.context.schema_epoch();
        let resolved = self.0.context.resolve_query_selection(self.0.schema, &self.0.collection_id, new_selection.clone())?;
        let new_version = self.0.advance_version(Some(new_selection));
        self.0.resultset.set_loaded(false);
        if self.0.context.schema_epoch() != epoch {
            self.0.context.schedule_query_resolution(self, new_version, ResolutionCause::SelectionUpdate);
            return Ok(());
        }
        match resolved {
            Some(resolved) => match self.install_selection_update(resolved, new_version) {
                Ok(()) => Ok(()),
                Err(error) => {
                    let error = Arc::new(error);
                    self.0.fail_initialization_shared(new_version, error.clone());
                    Err(RetrievalError::Shared(error))
                }
            },
            None => {
                self.0.context.schedule_query_resolution(self, new_version, ResolutionCause::SelectionUpdate);
                Ok(())
            }
        }
    }

    /// Install a resolved replacement under its version and start it.
    fn install_selection_update(&self, new_selection: ankql::ast::Selection<Resolved>, new_version: u32) -> Result<(), RetrievalError> {
        let state = self.0.version_state.lock().unwrap_or_else(|error| error.into_inner());
        if self.0.current_version.load(Ordering::Acquire) != new_version {
            return Ok(());
        }
        self.0.selection.set_before_notify(Some((new_selection.clone(), new_version)), || {
            let result = if self.0.context.has_subscription_relay()? {
                self.0.context.update_remote_query(self.0.query_id, new_selection, new_version)
            } else {
                let inner = self.0.clone();
                let query_id = self.0.query_id;
                crate::task::spawn(async move {
                    match inner.activate(new_version).await {
                        Ok(()) => inner.mark_durable_answered(new_version),
                        Err(e) => {
                            tracing::error!("LiveQuery update failed for predicate {}: {}", query_id, e);
                            inner.fail_initialization(new_version, e);
                        }
                    }
                });
                Ok(())
            };
            drop(state);
            result
        })?;
        Ok(())
    }

    pub async fn update_selection_wait(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        self.update_selection(new_selection)?;
        self.0.wait_initialized().await
    }

    pub fn error(&self) -> Read<Option<Arc<RetrievalError>>> { self.0.error.read() }
    pub fn query_id(&self) -> proto::QueryId { self.0.query_id }
    /// The resolved selection and its version, or `None` while resolution is pending.
    pub fn selection(&self) -> Read<Option<(ankql::ast::Selection<Resolved>, u32)>> { self.0.selection.read() }
    pub fn resultset(&self) -> EntityResultSet { self.0.resultset.clone() }

    /// Create a weak reference to this LiveQuery
    pub fn weak(&self) -> WeakEntityLiveQuery { WeakEntityLiveQuery(Arc::downgrade(&self.0)) }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.context.unsubscribe_remote_predicate(self.query_id);
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister(self as *const Inner as usize);
        }
    }
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

impl<R: View> LiveQuery<R> {
    /// Wait for initialization or its terminal error.
    pub async fn wait_initialized(&self) -> Result<(), RetrievalError> { self.0.wait_initialized().await }

    pub fn resultset(&self) -> ResultSet<R> { self.0 .0.resultset.wrap::<R>() }

    pub fn loaded(&self) -> bool { self.0 .0.resultset.is_loaded() }

    pub fn ids(&self) -> Vec<proto::EntityId> { self.0 .0.resultset.keys().collect() }

    pub fn ids_sorted(&self) -> Vec<proto::EntityId> {
        use itertools::Itertools;
        self.0 .0.resultset.keys().sorted().collect()
    }
}

impl<R: View> Signal for LiveQuery<R> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0 .0.subscription.listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.0 .0.subscription.broadcast_id() }
}

impl<R: View + Clone + 'static> Get<Vec<R>> for LiveQuery<R> {
    fn get(&self) -> Vec<R> {
        use ankurah_signals::CurrentObserver;
        CurrentObserver::track(&self);
        self.0 .0.resultset.wrap::<R>().peek()
    }
}

impl<R: View + Clone + 'static> Peek<Vec<R>> for LiveQuery<R> {
    fn peek(&self) -> Vec<R> { self.0 .0.resultset.wrap().peek() }
}

impl<R: View> Subscribe<ChangeSet<R>> for LiveQuery<R>
where R: Clone + Send + Sync + 'static
{
    fn subscribe<L>(&self, listener: L) -> SubscriptionGuard
    where L: IntoSubscribeListener<ChangeSet<R>> {
        let listener = listener.into_subscribe_listener();

        let me = self.clone();
        self.0 .0.subscription.subscribe(move |reactor_update: ReactorUpdate| {
            let changeset: ChangeSet<R> = livequery_change_set_from(me.0 .0.resultset.wrap::<R>(), reactor_update);
            listener(changeset);
        })
    }
}

fn livequery_change_set_from<R: View>(resultset: ResultSet<R>, reactor_update: ReactorUpdate) -> ChangeSet<R>
where R: View {
    use crate::changes::{ChangeSet, ItemChange};

    let mut changes = Vec::new();

    for item in reactor_update.items {
        let view = R::from_entity(item.entity);

        if let Some((_, membership_change)) = item.predicate_relevance.first() {
            match membership_change {
                crate::reactor::MembershipChange::Initial => {
                    changes.push(ItemChange::Initial { item: view });
                }
                crate::reactor::MembershipChange::Add => {
                    changes.push(ItemChange::Add { item: view, events: item.events });
                }
                crate::reactor::MembershipChange::Remove => {
                    changes.push(ItemChange::Remove { item: view, events: item.events });
                }
            }
        } else {
            changes.push(ItemChange::Update { item: view, events: item.events });
        }
    }

    ChangeSet { changes, resultset }
}
