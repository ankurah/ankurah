use ankql::ast::{Parsed, Resolved};
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
    session::SessionSet,
    storage::StorageEngine,
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
    // Version tracking for predicate updates
    pub(crate) current_version: std::sync::atomic::AtomicU32,
    // The admitted selection with its version (starts with version 1, updated
    // on selection changes). This represents user intent (client-side state),
    // separate from reactor's QueryState.selection (reactor-side state).
    // Using Mut for reactive updates that can be observed in WASM.
    //
    // Optional because the inner is built before `start_admitted` installs
    // the selection; nothing runs against the query in that window, since it
    // is neither activated nor registered with the relay until one lands.
    pub(crate) selection: Mut<Option<(ankql::ast::Selection<Resolved>, u32)>>,
    // Store collection_id for selection updates
    pub(crate) collection_id: CollectionId,
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
        // If already initialized, return immediately
        if self.initialized_version.load(std::sync::atomic::Ordering::Relaxed)
            >= self.current_version.load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }

        // FIXME - this should be waiting for the correct version, not any version
        // Otherwise wait for the notification
        self.initialized.notified().await;
    }

    /// Activate the LiveQuery by fetching entities and calling reactor.add_query or reactor.update_query
    /// Called after deltas have been applied for both initial subscription and selection updates
    /// Gets all parameters from self (collection_id, query_id, selection)
    /// Marks initialization as complete regardless of success/failure
    /// Rejects activation if the version is older than the current selection to prevent regression
    async fn activate(&self, version: u32) -> Result<(), RetrievalError> {
        // Get the current selection and its version
        let (selection, stored_version) = self.selection.value();

        // Reject activation if this is an older version than what's currently stored
        // This prevents out-of-order activations from regressing the state
        if version < stored_version {
            warn!("LiveQuery - Dropped stale activation request for version {} (current version is {})", version, stored_version);
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
    args: MatchArgs<Parsed>,
    sessions: SessionSet<PA::ContextData>,
) -> Result<(Arc<Inner>, proto::QueryId), RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    // One credential snapshot for the whole derivation; re-derivation
    // on change arrives with https://github.com/ankurah/ankurah/pull/426.
    let cdata = sessions.current();
    node.policy_agent.can_access_collection(&cdata, &collection_id)?;
    // Bind every property name to its durable identity and canonicalize
    // comparison values, then let the policy narrow what came back: the
    // agent ANDs its own conditions in in the same resolved vocabulary the
    // reactor and the relay consume.
    let mut selection = node.catalog.resolve_selection(&collection_id, args.selection)?;
    selection.predicate = node.policy_agent.filter_predicate(&cdata, &collection_id, selection.predicate)?;

    let subscription = node.reactor.subscribe();

    let resultset = EntityResultSet::empty();
    let query_id = proto::QueryId::new();
    let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(&node, sessions));

    let inner = Arc::new(Inner {
        query_id,
        node: node_ref,
        subscription,
        resultset: resultset.clone(),
        error: Mut::new(None),
        initialized: tokio::sync::Notify::new(),
        initialized_version: std::sync::atomic::AtomicU32::new(0), // 0 means uninitialized
        current_version: std::sync::atomic::AtomicU32::new(1),     // Start at version 1
        selection: Mut::new((selection, 1)),                       // Start with version 1
        collection_id: collection_id.clone(),
        gap_fetcher,
    });

    // Check if this is a durable node (no relay) or ephemeral node (has relay)
    let has_relay = node.subscription_relay.is_some();

    if args.cached || !has_relay {
        // Durable node: spawn initialization task directly (no remote subscription needed)
        let inner2 = inner.clone();

        debug!("LiveQuery::new() spawning initialization task for durable node predicate {}", query_id);
        crate::task::spawn(async move {
            debug!("LiveQuery initialization task starting for predicate {}", query_id);
            if let Err(e) = inner2.activate(1).await {
                debug!("LiveQuery initialization failed for predicate {}: {}", query_id, e);
                inner2.error.set(Some(e));
                // Initialization is over, unsuccessfully: wake waiters so
                // wait_initialized returns instead of hanging on a query
                // that will never activate. The error slot carries why.
                inner2.initialized.notify_waiters();
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
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: CollectionId,
        args: MatchArgs<Parsed>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(StrongNodeRef(Arc::clone(&node.0)));
        Self::new_with_node_ref(node, node_ref, schema, collection_id, args, sessions.into(), RemoteSubscription::AtStart)
    }


    /// Create a LiveQuery that does NOT keep the node alive.
    ///
    /// Used by the PolicyAgent's own bootstrap query and the catalog
    /// projection: node-owned subscribers whose strong reference would cycle
    /// (node → agent or catalog → livequery → node). Operations that need the
    /// node (activation, selection updates) fail with "Node has been dropped"
    /// once the node is gone.
    pub fn new_weak_node<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: CollectionId,
        args: MatchArgs<Parsed>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(WeakNodeRefImpl(Arc::downgrade(&node.0)));
        Self::new_with_node_ref(node, node_ref, None, collection_id, args, sessions.into(), RemoteSubscription::AtStart)
    }

    fn new_with_node_ref<SE, PA>(
        node: &Node<SE, PA>,
        node_ref: Box<dyn NodeRef>,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: CollectionId,
        args: MatchArgs<Parsed>,
        sessions: SessionSet<PA::ContextData>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let has_relay = node.subscription_relay.is_some();
        let (inner, query_id) = create_inner(node, node_ref, collection_id.clone(), args, sessions.clone())?;

        let me = Self(inner.clone());

        // Ephemeral node: register with relay for remote subscription
        // Remote will call activate() after applying deltas via subscription_established
        if has_relay {
            node.subscribe_remote_query(query_id, collection_id, inner.selection.value().0, sessions, 1, me.weak());
        }

        Ok(me)
    }
    pub fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    /// The initialization/admission failure, if one occurred, rendered as a
    /// message. Waking from [`Self::wait_initialized`] with this set means
    /// the query will never populate.
    pub fn error_message(&self) -> Option<String> { self.0.error.with(|e| e.as_ref().map(|e| e.to_string())) }

    /// Wait for the LiveQuery to be fully initialized with initial states
    pub async fn wait_initialized(&self) { self.0.wait_initialized().await; }

    pub fn update_selection(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        let new_selection = new_selection.try_into().map_err(|e| e.into())?;
        let node = self.0.node().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))?;

        // A replacement selection arrives as a parsed string, below the
        // typed entries: bind its names through the catalog before anything
        // stores or forwards it -- the reactor and the relay consume
        // resolved selections only. (Policy re-injection on replacement is
        // a recorded gap, tracked with the update_selection admission
        // issue.)
        let new_selection = node.resolve_selection(&self.0.collection_id, new_selection)?;

        // Increment current_version atomically and get the new version number
        let new_version = self.0.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

        // Mark resultset as not loaded since we're changing the selection
        self.0.resultset.set_loaded(false);

        // Store new selection and version
        self.0.selection.set((new_selection.clone(), new_version));

        // Check if this node has a relay (ephemeral) or not (durable)
        let has_relay = node.has_subscription_relay();

        if has_relay {
            // Ephemeral node: delegate to relay, which will call update_selection_init after applying deltas
            node.update_remote_query(self.0.query_id, new_selection.clone(), new_version)?;
        } else {
            // Durable node: spawn task to call update_selection_init directly
            let inner = self.0.clone();
            let query_id = self.0.query_id;

            crate::task::spawn(async move {
                if let Err(e) = inner.activate(new_version).await {
                    tracing::error!("LiveQuery update failed for predicate {}: {}", query_id, e);
                    inner.error.set(Some(e));
                    // Wake update_selection_wait: the update is over,
                    // unsuccessfully, and the error slot carries why.
                    inner.initialized.notify_waiters();
                }
            });
        }

        Ok(())
    }

    pub async fn update_selection_wait(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        self.update_selection(new_selection)?;
        self.0.wait_initialized().await;
        Ok(())
    }

    pub fn error(&self) -> Read<Option<RetrievalError>> { self.0.error.read() }
    pub fn query_id(&self) -> proto::QueryId { self.0.query_id }
    pub fn selection(&self) -> Read<(ankql::ast::Selection<Resolved>, u32)> { self.0.selection.read() }
    pub fn resultset(&self) -> EntityResultSet { self.0.resultset.clone() }

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
