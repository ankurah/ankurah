use std::{
    marker::PhantomData,
    sync::{Arc, Weak},
};

use ankurah_proto::{self as proto, CollectionId};

use ankurah_signals::{
    broadcast::BroadcastId,
    porcelain::subscribe::{IntoSubscribeListener, SubscriptionGuard},
    signal::{Listener, ListenerGuard},
    Get, Mut, Peek, Read, Signal, Subscribe,
};
use tracing::{debug, warn};

use crate::{
    changes::ChangeSet,
    entity::Entity,
    error::RetrievalError,
    model::View,
    node::{MatchArgs, NodeInner, TNodeErased},
    policy::PolicyAgent,
    reactor::{
        fetch_gap::{GapFetcher, QueryGapFetcher},
        ReactorSubscription, ReactorUpdate,
    },
    resultset::{EntityResultSet, ResultSet},
    storage::StorageEngine,
    util::request_fence::RequestValidity,
    Node,
};

/// A local subscription that handles both reactor subscription and remote cleanup
/// This is a type-erased version that can be used in the TContext trait
///
/// Whether the query keeps its node alive is a construction-time choice:
/// [`EntityLiveQuery::new`] holds the node strongly, [`EntityLiveQuery::new_weak_node`] does not.
#[derive(Clone)]
pub struct EntityLiveQuery(Arc<Inner>);

/// Type-erased reference to a node. Strong variants keep the node alive; weak variants do not.
trait NodeRef: Send + Sync {
    fn upgrade(&self) -> Option<Box<dyn TNodeErased>>;
}

/// Strong node reference — keeps the node alive as long as Inner exists.
struct StrongNodeRef<SE, PA: PolicyAgent>(Arc<NodeInner<SE, PA>>);

impl<SE, PA> NodeRef for StrongNodeRef<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn upgrade(&self) -> Option<Box<dyn TNodeErased>> { Some(Box::new(Node(self.0.clone()))) }
}

/// Weak node reference — does NOT keep the node alive.
struct WeakNodeRefImpl<SE, PA: PolicyAgent>(Weak<NodeInner<SE, PA>>);

impl<SE, PA> NodeRef for WeakNodeRefImpl<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn upgrade(&self) -> Option<Box<dyn TNodeErased>> { self.0.upgrade().map(|inner| Box::new(Node(inner)) as Box<dyn TNodeErased>) }
}

struct Inner {
    pub(crate) query_id: proto::QueryId,
    // subscription must be declared before node so it drops first —
    // dropping node (StrongNodeRef) deallocates the reactor, and
    // subscription's Drop needs the reactor to unsubscribe.
    pub(crate) subscription: ReactorSubscription,
    pub(crate) node: Box<dyn NodeRef>,
    pub(crate) resultset: EntityResultSet,
    pub(crate) error: Mut<Option<RetrievalError>>,
    pub(crate) initialized: tokio::sync::Notify,
    pub(crate) initialized_version: std::sync::atomic::AtomicU32,
    // Selection updates wait for the async initial resolver. On ephemeral
    // nodes, "ready" also means the SubscriptionRelay entry exists. State:
    // 0 pending, 1 ready, 2 terminal initial-resolution failure.
    pub(crate) initial_query_state: tokio::sync::watch::Sender<u8>,
    // Version tracking for predicate updates
    pub(crate) current_version: std::sync::atomic::AtomicU32,
    // Catalog-resolved selection with its version. Async updates keep the
    // previous resolved value here until the next version resolves.
    // Using Mut for reactive updates that can be observed in WASM.
    pub(crate) selection: Mut<(ankql::ast::Selection, u32)>,
    // Store collection_id for selection updates
    pub(crate) collection_id: CollectionId,
    /// Exact compiled declaration whose predicate is being resolved. Internal
    /// catalog/system queries carry `None`; typed Context queries carry their
    /// model schema so registration cannot be inferred collection-globally.
    pub(crate) schema: Option<&'static crate::schema::ModelSchema>,
    // Gap fetcher for reactor.add_query (type-erased)
    pub(crate) gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>>,
}

/// Weak reference to EntityLiveQuery for breaking circular dependencies
pub struct WeakEntityLiveQuery(Weak<Inner>);

impl WeakEntityLiveQuery {
    pub fn upgrade(&self) -> Option<EntityLiveQuery> { self.0.upgrade().map(EntityLiveQuery) }
}

impl Clone for WeakEntityLiveQuery {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

#[derive(Clone)]
pub struct LiveQuery<R: View>(EntityLiveQuery, PhantomData<R>);

impl<R: View> std::ops::Deref for LiveQuery<R> {
    type Target = EntityLiveQuery;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Inner {
    fn node(&self) -> Option<Box<dyn TNodeErased>> { self.node.upgrade() }

    async fn wait_initialized(&self) {
        // Capture the version this caller is waiting for. A later concurrent
        // update has its own waiter; it must not silently lengthen this wait.
        let target_version = self.current_version.load(std::sync::atomic::Ordering::Acquire);
        loop {
            // `notify_waiters` stores no permit. Create the future before the
            // state check so an initialization completing between the check
            // and the await cannot strand this caller.
            let notified = self.initialized.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.initialized_version.load(std::sync::atomic::Ordering::Acquire) >= target_version {
                return;
            }
            notified.await;
        }
    }

    /// Activate the LiveQuery by fetching entities and calling reactor.add_query or reactor.update_query
    /// Called after deltas have been applied for both initial subscription and selection updates
    /// Gets all parameters from self (collection_id, query_id, selection)
    /// Marks initialization as complete regardless of success/failure
    /// Rejects activation if the version is older than the current selection to prevent regression
    async fn activate(&self, version: u32) -> Result<(), RetrievalError> {
        // Get the current selection and its version
        let (selection, stored_version) = self.selection.value();
        let current_version = self.current_version.load(std::sync::atomic::Ordering::Acquire);

        // Reject activation unless the resolved selection and the requested
        // activation are the current version. Resolution is asynchronous, so
        // an older task may finish after a newer update has begun.
        if version != stored_version || version != current_version {
            warn!(
                "LiveQuery - Dropped stale activation request for version {} (stored version {}, current version {})",
                version, stored_version, current_version
            );
            return Ok(());
        }

        debug!("LiveQuery.activate() for predicate {} (version {})", self.query_id, version);

        let node = self.node().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))?;
        let reactor = node.reactor();
        let initialized_version = self.initialized_version.load(std::sync::atomic::Ordering::Relaxed);

        let hook = InnerPreNotifyHook(self);
        // Determine if this is the first activation (query not yet in reactor)
        if initialized_version == 0 {
            // First activation ever: call reactor.add_query_and_notify which will populate the resultset
            // Pass the hook as pre_notify_hook to mark initialized before notification
            reactor
                .add_query_and_notify(
                    self.subscription.id(),
                    self.query_id,
                    self.collection_id.clone(),
                    selection,
                    &*node,
                    self.resultset.clone(),
                    self.gap_fetcher.clone(),
                    version,
                    &hook,
                )
                .await?
        } else {
            // Subsequent activation (including cached re-initialization or selection update): use update_query_and_notify
            // This handles both: (1) cached queries re-activating after remote deltas, and (2) selection updates
            reactor
                .update_query_and_notify(
                    self.subscription.id(),
                    self.query_id,
                    self.collection_id.clone(),
                    selection,
                    &*node,
                    version,
                    &hook,
                )
                .await?;
        };

        Ok(())
    }

    /// Mark initialization as complete for a given version
    fn mark_initialized(&self, version: u32) {
        // TASK: Serialize or coalesce concurrent activations to prevent version regression https://github.com/ankurah/ankurah/issues/146
        self.initialized_version.store(version, std::sync::atomic::Ordering::Relaxed);
        self.initialized.notify_waiters();
    }
}

/// Adapts a borrowed Inner to the reactor's PreNotifyHook (previously implemented on &EntityLiveQuery,
/// but activation now lives on Inner so both LiveQuery variants share it)
struct InnerPreNotifyHook<'a>(&'a Inner);
impl crate::reactor::PreNotifyHook for &InnerPreNotifyHook<'_> {
    fn pre_notify(&self, version: u32) {
        // Mark as initialized before notification is sent
        self.0.mark_initialized(version);
    }
}

/// Helper: create the Inner and set up initialization (shared by strong- and weak-node constructors)
fn create_inner<SE, PA>(
    node: &Node<SE, PA>,
    node_ref: Box<dyn NodeRef>,
    collection_id: CollectionId,
    schema: Option<&'static crate::schema::ModelSchema>,
    mut args: MatchArgs,
    cdata: PA::ContextData,
    request_validity: Option<RequestValidity>,
) -> Result<(Arc<Inner>, proto::QueryId), RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node.policy_agent.can_access_collection(&cdata, &collection_id)?;
    args.selection.predicate = node.policy_agent.filter_predicate(&cdata, &collection_id, args.selection.predicate)?;

    // NOTE: property RESOLUTION (PathExpr -> Identifier, RFC 5.5 in specs/model-property-metadata/rfc.md) cannot run
    // here: create_inner is SYNC (called from the LiveQuery constructors) and
    // the catalog-lag deferral must be able to await catalog readiness. It is
    // therefore deferred into the async paths that follow this function:
    //   - DURABLE node: resolved (with deferral) inside the spawned activation
    //     task below, which writes the resolved selection back into `inner`
    //     before the reactor sees it.
    //   - EPHEMERAL node: resolved on the forward path in
    //     `Node::subscribe_remote_query`, then stored back into `inner`
    //     before relay registration (see new_with_node_ref).
    // Resolution is idempotent and precedes type_resolver at each site.

    // Resolve types in the AST (converts literals for JSON path comparisons)
    args.selection = node.type_resolver.resolve_selection_types(args.selection);

    let subscription = node.reactor.subscribe();

    let resultset = EntityResultSet::empty();
    let query_id = proto::QueryId::new();
    let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(&node, cdata.clone()));

    let has_relay = node.subscription_relay.is_some();
    let inner = Arc::new(Inner {
        query_id,
        node: node_ref,
        subscription,
        resultset: resultset.clone(),
        error: Mut::new(None),
        initialized: tokio::sync::Notify::new(),
        initialized_version: std::sync::atomic::AtomicU32::new(0), // 0 means uninitialized
        initial_query_state: tokio::sync::watch::channel(0).0,
        current_version: std::sync::atomic::AtomicU32::new(1), // Start at version 1
        selection: Mut::new((args.selection.clone(), 1)),      // Start with version 1
        collection_id: collection_id.clone(),
        schema,
        gap_fetcher,
    });

    // Check if this is a durable node (no relay) or ephemeral node (has relay)
    if args.cached || !has_relay {
        // Durable node: spawn initialization task directly (no remote subscription needed)
        let inner2 = inner.clone();
        // Capture the catalog + collection (and the node + cdata the
        // deferral needs to kick an ephemeral catalog subscription) so the
        // async task can run property RESOLUTION that create_inner cannot,
        // being sync. The resolved selection is written back into `inner`
        // before activate reads it (see below).
        //
        // The node is captured WEAK and upgraded transiently around each
        // resolve attempt: a strong clone baked into the closure would keep
        // the node alive from construction until the task finishes --
        // resolution can span a subscription kick and a registration
        // round-trip -- and would break the new_weak_node contract outright.
        // The readiness wait itself needs only the catalog.
        let catalog = node.catalog.clone();
        let resolve_node = node.weak();
        let resolve_cdata = cdata.clone();
        let resolve_collection = collection_id.clone();
        let local_validity = request_validity.clone();

        debug!("LiveQuery::new() spawning initialization task for durable node predicate {}", query_id);
        crate::task::spawn(async move {
            debug!("LiveQuery initialization task starting for predicate {}", query_id);
            // Cached ephemeral catalog queries have both a local activation
            // and a remote relay request owned by the same reset-sensitive
            // generation. Hold the owner fence continuously through local
            // resolution and activation (including its storage fetch), so a
            // hard reset drains this work before deleting storage.
            let _local_lease = match local_validity {
                Some(validity) => match validity.try_acquire() {
                    Some(lease) => Some(lease),
                    None => {
                        let version = inner2.current_version.load(std::sync::atomic::Ordering::Acquire);
                        inner2.mark_initialized(version.max(1));
                        return;
                    }
                },
                None => None,
            };
            // Resolve PathExpr to Identifier before reactor activation. The
            // exact compiled model registers inside this pass; when that
            // cannot run (policy denial or no durable peer),
            // the query fails LOUD -- the catalog subscription is a cache, and
            // registration is the doubt-resolver, so an unresolvable
            // reference here is a real error, not something to idle on.
            let (current, current_version) = inner2.selection.value();
            // Transient strong ref for the resolve attempt only, so a slow
            // resolve cannot pin a node the caller has dropped.
            let resolved = match resolve_node.upgrade() {
                Some(node) => catalog.resolve_selection_deferred(&node, Some(&resolve_cdata), &resolve_collection, schema, &current).await,
                None => {
                    inner2.error.set(Some(RetrievalError::Other("Node has been dropped".into())));
                    if !has_relay {
                        inner2.initial_query_state.send_replace(2);
                    }
                    inner2.mark_initialized(current_version.max(1));
                    return;
                }
            };
            match resolved {
                Ok(resolved) => {
                    if inner2.current_version.load(std::sync::atomic::Ordering::Acquire) == current_version {
                        inner2.selection.set((resolved, current_version));
                    }
                    if !has_relay {
                        inner2.initial_query_state.send_replace(1);
                    }
                }
                Err(e) => {
                    debug!("LiveQuery resolution failed for predicate {}: {}", query_id, e);
                    inner2.error.set(Some(e.into()));
                    if !has_relay {
                        inner2.initial_query_state.send_replace(2);
                    }
                    // Still mark initialized so waiters do not hang on a query
                    // that fails closed; the error is surfaced via `error`.
                    inner2.mark_initialized(current_version.max(1));
                    return;
                }
            }
            if let Err(e) = inner2.activate(current_version).await {
                debug!("LiveQuery initialization failed for predicate {}: {}", query_id, e);
                inner2.error.set(Some(e));
                // Activation failed before the reactor's pre-notify hook could
                // mark this version. The error is terminal for this attempt,
                // so release initialization waiters just like resolution
                // failures do above.
                inner2.mark_initialized(current_version.max(1));
            } else {
                debug!("LiveQuery initialization completed for predicate {}", query_id);
            }
        });
    }

    Ok((inner, query_id))
}

impl EntityLiveQuery {
    pub fn new<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: CollectionId,
        args: MatchArgs,
        cdata: PA::ContextData,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(StrongNodeRef(Arc::clone(&node.0)));
        Self::new_with_node_ref(node, node_ref, collection_id, None, args, cdata, None)
    }

    /// Create a typed model query whose exact compiled schema is registered
    /// before property resolution.
    pub(crate) fn new_for_model<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: CollectionId,
        schema: &'static crate::schema::ModelSchema,
        args: MatchArgs,
        cdata: PA::ContextData,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(StrongNodeRef(Arc::clone(&node.0)));
        Self::new_with_node_ref(node, node_ref, collection_id, Some(schema), args, cdata, None)
    }

    /// Create a LiveQuery that does NOT keep the node alive.
    ///
    /// Used by PolicyAgent and other internal subscribers that should not create
    /// reference cycles (node → agent → livequery → node). Operations that need
    /// the node (activation, selection updates) fail with "Node has been dropped"
    /// once the node is gone.
    pub fn new_weak_node<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: CollectionId,
        args: MatchArgs,
        cdata: PA::ContextData,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(WeakNodeRefImpl(Arc::downgrade(&node.0)));
        Self::new_with_node_ref(node, node_ref, collection_id, None, args, cdata, None)
    }

    /// Create a weak-node query whose remote responses are accepted only
    /// while an owning lifecycle remains current.
    pub(crate) fn new_weak_node_with_request_validity<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: CollectionId,
        args: MatchArgs,
        cdata: PA::ContextData,
        request_validity: RequestValidity,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(WeakNodeRefImpl(Arc::downgrade(&node.0)));
        Self::new_with_node_ref(node, node_ref, collection_id, None, args, cdata, Some(request_validity))
    }

    fn new_with_node_ref<SE, PA>(
        node: &Node<SE, PA>,
        node_ref: Box<dyn NodeRef>,
        collection_id: CollectionId,
        schema: Option<&'static crate::schema::ModelSchema>,
        args: MatchArgs,
        cdata: PA::ContextData,
        request_validity: Option<RequestValidity>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let has_relay = node.subscription_relay.is_some();
        let (inner, query_id) = create_inner(node, node_ref, collection_id.clone(), schema, args, cdata.clone(), request_validity.clone())?;

        let me = Self(inner.clone());

        // Ephemeral node: register with relay for remote subscription
        // Remote will call activate() after applying deltas via subscription_established
        if has_relay {
            node.subscribe_remote_query(query_id, collection_id, schema, inner.selection.value().0, cdata, 1, request_validity, me.weak());
        }

        Ok(me)
    }
    pub fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    /// Wait for the LiveQuery to be fully initialized with initial states
    pub async fn wait_initialized(&self) { self.0.wait_initialized().await; }

    pub fn update_selection(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        let new_selection = new_selection.try_into().map_err(|e| e.into())?;
        let node = self.0.node().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))?;

        // Increment current_version atomically and get the new version number
        let new_version = self.0.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

        // Mark resultset as not loaded since we're changing the selection
        self.0.resultset.set_loaded(false);

        // Keep the previously RESOLVED selection while this version resolves.
        // The node stores the resolved form before either forwarding it to a
        // relay or activating it locally. This prevents the local reactor and
        // the durable peer from observing different AST/value types.
        node.update_query_selection(self.0.query_id, self.0.collection_id.clone(), new_selection, new_version, self.weak())?;

        Ok(())
    }

    pub async fn update_selection_wait(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        self.update_selection(new_selection)?;
        self.0.wait_initialized().await;
        // Initialization completing is not success: a failed resolution or
        // activation latches an error and releases the waiters (see
        // set_error_for_version). Surface it instead of returning Ok for a
        // dead query.
        match self.latched_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn error(&self) -> Read<Option<RetrievalError>> { self.0.error.read() }

    /// The latched error as an owned value, for waiters that must RETURN it
    /// (`update_selection_wait`, `Context::query_wait`). [`RetrievalError`]
    /// holds non-cloneable sources, so the returned value carries the latched
    /// error's rendered message while the original stays latched for
    /// [`Self::error`] readers.
    pub(crate) fn latched_error(&self) -> Option<RetrievalError> {
        self.0.error.with(|error| error.as_ref().map(|e| RetrievalError::Anyhow(anyhow::anyhow!("{e}"))))
    }

    /// Store the catalog-resolved selection only if `version` is still the
    /// current user intent. Async resolution tasks may complete out of order.
    pub(crate) fn set_resolved_selection(&self, selection: ankql::ast::Selection, version: u32) -> bool {
        if self.0.current_version.load(std::sync::atomic::Ordering::Acquire) != version {
            return false;
        }
        self.0.selection.set((selection, version));
        self.0.current_version.load(std::sync::atomic::Ordering::Acquire) == version
    }

    /// Activate a selection after the node has resolved and stored it.
    pub(crate) async fn activate(&self, version: u32) -> Result<(), RetrievalError> { self.0.activate(version).await }

    /// Fail a selection update closed without letting a stale resolver poison
    /// the current query's error/initialization state.
    pub(crate) fn set_error_for_version(&self, version: u32, error: RetrievalError) {
        if self.0.current_version.load(std::sync::atomic::Ordering::Acquire) == version {
            self.0.error.set(Some(error));
            self.0.mark_initialized(version);
        }
    }

    pub(crate) fn mark_initial_query_ready(&self) { self.0.initial_query_state.send_replace(1); }

    pub(crate) fn mark_initial_query_failed(&self) { self.0.initial_query_state.send_replace(2); }

    pub(crate) fn schema(&self) -> Option<&'static crate::schema::ModelSchema> { self.0.schema }

    /// Wait until the initial selection has resolved. For an ephemeral query,
    /// the initial SubscriptionRelay entry is also present before readiness.
    pub(crate) async fn wait_initial_query_ready(&self) -> bool {
        let mut state = self.0.initial_query_state.subscribe();
        loop {
            let current = *state.borrow_and_update();
            match current {
                1 => return true,
                2 => return false,
                _ => {
                    if state.changed().await.is_err() {
                        return false;
                    }
                }
            }
        }
    }
    pub fn query_id(&self) -> proto::QueryId { self.0.query_id }
    pub fn selection(&self) -> Read<(ankql::ast::Selection, u32)> { self.0.selection.read() }
    pub fn resultset(&self) -> EntityResultSet { self.0.resultset.clone() }

    /// The underlying reactor subscription, for internal subscribers that
    /// need the raw `ReactorUpdate` stream (entity-level, not `View`-mapped)
    /// -- e.g. the catalog map, which maintains itself over unmapped
    /// `Entity`s. External consumers use the `Subscribe<ChangeSet<R>>` impl
    /// on `LiveQuery<R>` instead.
    pub(crate) fn reactor_subscription(&self) -> &crate::reactor::ReactorSubscription { &self.0.subscription }

    /// Create a weak reference to this LiveQuery
    pub fn weak(&self) -> WeakEntityLiveQuery { WeakEntityLiveQuery(Arc::downgrade(&self.0)) }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(node) = self.node.upgrade() {
            node.unsubscribe_remote_predicate(self.query_id);
        }
    }
}

// Implement RemoteQuerySubscriber for WeakEntityLiveQuery to break circular dependencies
#[async_trait::async_trait]
impl crate::peer_subscription::RemoteQuerySubscriber for WeakEntityLiveQuery {
    async fn subscription_established(&self, version: u32) {
        // Try to upgrade the weak reference
        if let Some(inner) = self.0.upgrade() {
            // Activate the query (fetch entities, call reactor, and mark initialized)
            // Handle errors internally by setting last_error
            tracing::debug!("Subscription established for query {}: {}", inner.query_id, version);
            if let Err(e) = inner.activate(version).await {
                tracing::error!("Failed to activate subscription for query {}: {}", inner.query_id, e);
                inner.error.set(Some(e));
                // Remote activation owns initialization for non-cached
                // ephemeral queries. If it fails before the reactor's hook,
                // the error is terminal for this version and must release
                // waiters explicitly.
                inner.mark_initialized(version.max(1));
            }
        }
        // If upgrade fails, the LiveQuery was already dropped - nothing to do
    }

    fn set_last_error(&self, error: RetrievalError) {
        // Try to upgrade the weak reference
        if let Some(inner) = self.0.upgrade() {
            tracing::info!("Setting last error for LiveQuery {}: {}", inner.query_id, error);
            inner.error.set(Some(error));
        }
        // If upgrade fails, the LiveQuery was already dropped - nothing to do
    }
}

impl<R: View> LiveQuery<R> {
    /// Wait for the LiveQuery to be fully initialized with initial states
    pub async fn wait_initialized(&self) { self.0.wait_initialized().await; }

    pub fn resultset(&self) -> ResultSet<R> { self.0 .0.resultset.wrap::<R>() }

    pub fn loaded(&self) -> bool { self.0 .0.resultset.is_loaded() }

    pub fn ids(&self) -> Vec<proto::EntityId> { self.0 .0.resultset.keys().collect() }

    pub fn ids_sorted(&self) -> Vec<proto::EntityId> {
        use itertools::Itertools;
        self.0 .0.resultset.keys().sorted().collect()
    }
}

// Implement Signal trait - delegate to the subscription (not resultset)
// This ensures that LiveQuery tracking fires on ALL entity changes, not just membership changes
impl<R: View> Signal for LiveQuery<R> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0 .0.subscription.listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.0 .0.subscription.broadcast_id() }
}

// Implement Get trait - delegate to ResultSet<R>
impl<R: View + Clone + 'static> Get<Vec<R>> for LiveQuery<R> {
    fn get(&self) -> Vec<R> {
        use ankurah_signals::CurrentObserver;
        CurrentObserver::track(&self);
        self.0 .0.resultset.wrap::<R>().peek()
    }
}

// Implement Peek trait - delegate to ResultSet<R>
impl<R: View + Clone + 'static> Peek<Vec<R>> for LiveQuery<R> {
    fn peek(&self) -> Vec<R> { self.0 .0.resultset.wrap().peek() }
}

// Implement Subscribe trait - convert ReactorUpdate to ChangeSet<R>
impl<R: View> Subscribe<ChangeSet<R>> for LiveQuery<R>
where R: Clone + Send + Sync + 'static
{
    fn subscribe<L>(&self, listener: L) -> SubscriptionGuard
    where L: IntoSubscribeListener<ChangeSet<R>> {
        let listener = listener.into_subscribe_listener();

        let me = self.clone();
        // Subscribe to the underlying ReactorUpdate stream and convert to ChangeSet<R>
        self.0 .0.subscription.subscribe(move |reactor_update: ReactorUpdate| {
            let changeset: ChangeSet<R> = livequery_change_set_from(me.0 .0.resultset.wrap::<R>(), reactor_update);
            listener(changeset);
        })
    }
}

/// Notably, this function does not filter by query_id, because it should only be used by LiveQuery, which entails a single-predicate subscription
fn livequery_change_set_from<R: View>(resultset: ResultSet<R>, reactor_update: ReactorUpdate) -> ChangeSet<R>
where R: View {
    use crate::changes::{ChangeSet, ItemChange};

    let mut changes = Vec::new();

    for item in reactor_update.items {
        let view = R::from_entity(item.entity);

        // Determine the change type based on predicate relevance
        // ignore the query_id, because it should only be used by LiveQuery, which entails a single-predicate subscription
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
            // No membership change, just an update
            changes.push(ItemChange::Update { item: view, events: item.events });
        }
    }

    ChangeSet { changes, resultset }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_signals::With;

    use crate::{peer_subscription::RemoteQuerySubscriber, reactor::Reactor};

    #[derive(Clone)]
    struct FailingNodeRef(Reactor);

    impl NodeRef for FailingNodeRef {
        fn upgrade(&self) -> Option<Box<dyn TNodeErased>> { Some(Box::new(FailingNode(self.0.clone()))) }
    }

    struct FailingNode(Reactor);

    #[async_trait::async_trait]
    impl TNodeErased for FailingNode {
        fn unsubscribe_remote_predicate(&self, _query_id: proto::QueryId) {}

        fn update_query_selection(
            &self,
            _query_id: proto::QueryId,
            _collection_id: CollectionId,
            _selection: ankql::ast::Selection,
            version: u32,
            livequery: WeakEntityLiveQuery,
        ) -> Result<(), anyhow::Error> {
            // Mirror the node's failure path: a failed resolution latches the
            // error for this version, which also releases initialization
            // waiters (see Node::update_query_selection).
            if let Some(query) = livequery.upgrade() {
                query.set_error_for_version(version, RetrievalError::Other("forced selection update failure".to_string()));
            }
            Ok(())
        }

        async fn fetch_entities_from_local(
            &self,
            _collection_id: &CollectionId,
            _selection: &ankql::ast::Selection,
        ) -> Result<Vec<Entity>, RetrievalError> {
            Err(RetrievalError::Other("forced remote activation failure".to_string()))
        }

        fn reactor(&self) -> &Reactor { &self.0 }
    }

    struct EmptyGapFetcher;

    #[async_trait::async_trait]
    impl GapFetcher<Entity> for EmptyGapFetcher {
        async fn fetch_gap(
            &self,
            _collection_id: &CollectionId,
            _selection: &ankql::ast::Selection,
            _last_entity: Option<&Entity>,
            _gap_size: usize,
        ) -> Result<Vec<Entity>, RetrievalError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn remote_activation_error_releases_initialization_waiters() {
        let reactor = Reactor::new();
        let query = EntityLiveQuery(Arc::new(Inner {
            query_id: proto::QueryId::new(),
            subscription: reactor.subscribe(),
            node: Box::new(FailingNodeRef(reactor)),
            resultset: EntityResultSet::empty(),
            error: Mut::new(None),
            initialized: tokio::sync::Notify::new(),
            initialized_version: std::sync::atomic::AtomicU32::new(0),
            initial_query_state: tokio::sync::watch::channel(1).0,
            current_version: std::sync::atomic::AtomicU32::new(1),
            selection: Mut::new((ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }, 1)),
            collection_id: CollectionId::from("remote_failure"),
            schema: None,
            gap_fetcher: Arc::new(EmptyGapFetcher),
        }));

        query.weak().subscription_established(1).await;
        tokio::time::timeout(std::time::Duration::from_secs(1), query.wait_initialized())
            .await
            .expect("terminal remote activation error must release initialization waiters");
        query.error().with(|error| {
            let error = error.as_ref().expect("remote activation error must remain observable");
            assert!(error.to_string().contains("forced remote activation failure"), "unexpected error: {error}");
        });
    }

    /// Waiting out a selection update must surface the update's latched
    /// error, not report Ok for a dead query.
    #[tokio::test]
    async fn update_selection_wait_surfaces_the_latched_error() {
        let reactor = Reactor::new();
        let query = EntityLiveQuery(Arc::new(Inner {
            query_id: proto::QueryId::new(),
            subscription: reactor.subscribe(),
            node: Box::new(FailingNodeRef(reactor)),
            resultset: EntityResultSet::empty(),
            error: Mut::new(None),
            initialized: tokio::sync::Notify::new(),
            initialized_version: std::sync::atomic::AtomicU32::new(0),
            initial_query_state: tokio::sync::watch::channel(1).0,
            current_version: std::sync::atomic::AtomicU32::new(1),
            selection: Mut::new((ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None }, 1)),
            collection_id: CollectionId::from("selection_update_failure"),
            schema: None,
            gap_fetcher: Arc::new(EmptyGapFetcher),
        }));

        let error = tokio::time::timeout(std::time::Duration::from_secs(1), query.update_selection_wait("true"))
            .await
            .expect("a failed selection update must release its waiter")
            .expect_err("a latched selection failure must surface from update_selection_wait");
        assert!(error.to_string().contains("forced selection update failure"), "unexpected error: {error}");

        // The latch itself stays observable for error() readers.
        query.error().with(|error| {
            let error = error.as_ref().expect("the latched error must remain observable");
            assert!(error.to_string().contains("forced selection update failure"), "unexpected error: {error}");
        });
    }
}
