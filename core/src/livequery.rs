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
    // Serializes activate() calls. On relay-bearing nodes two same-version activations race
    // by design (the local initialization task and the relay's subscription_established);
    // without serialization both read initialized_version == 0 and both take the reactor's
    // add path, which refuses duplicates. https://github.com/ankurah/ankurah/issues/146
    pub(crate) activation_lock: tokio::sync::Mutex<()>,
    // Version tracking for predicate updates
    pub(crate) current_version: std::sync::atomic::AtomicU32,
    // Store selection with its version (starts with version 1, updated on selection changes)
    // This represents user intent (client-side state), separate from reactor's QueryState.selection (reactor-side state)
    // Using Mut for reactive updates that can be observed in WASM
    pub(crate) selection: Mut<(ankql::ast::Selection, u32)>,
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
        // Serialize activations so a racing same-version twin waits for the winner, observes
        // its mark_initialized, and takes the update path instead of a duplicate add. try_lock
        // first purely for observability: contention means an activation was coalesced, and a
        // future scheduling regression should be visible in traces rather than silently absorbed.
        let _activation = match self.activation_lock.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                debug!("LiveQuery.activate() for query {} (version {}) waiting for in-flight activation", self.query_id, version);
                self.activation_lock.lock().await
            }
        };

        // Both reads below must happen under the lock so this activation sees the effects
        // of whichever activation ran before it.
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
            // Pass the hook as pre_notify_hook to mark initialized before notification.
            // On failure the hook never runs, leaving initialized_version at zero so the next
            // activation retries the add rather than updating a query the reactor never registered.
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
    /// Runs inside activate()'s reactor call, so writes are ordered by the activation lock
    fn mark_initialized(&self, version: u32) {
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
    mut args: MatchArgs,
    cdata: PA::ContextData,
) -> Result<(Arc<Inner>, proto::QueryId), RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node.policy_agent.can_access_collection(&cdata, &collection_id)?;
    args.selection.predicate = node.policy_agent.filter_predicate(&cdata, &collection_id, args.selection.predicate)?;

    // Resolve types in the AST (converts literals for JSON path comparisons)
    args.selection = node.type_resolver.resolve_selection_types(args.selection);

    let subscription = node.reactor.subscribe();

    let resultset = EntityResultSet::empty();
    let query_id = proto::QueryId::new();
    let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(&node, cdata.clone()));

    let inner = Arc::new(Inner {
        query_id,
        node: node_ref,
        subscription,
        resultset: resultset.clone(),
        error: Mut::new(None),
        initialized: tokio::sync::Notify::new(),
        initialized_version: std::sync::atomic::AtomicU32::new(0), // 0 means uninitialized
        activation_lock: tokio::sync::Mutex::new(()),
        current_version: std::sync::atomic::AtomicU32::new(1), // Start at version 1
        selection: Mut::new((args.selection.clone(), 1)),      // Start with version 1
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
        Self::new_with_node_ref(node, node_ref, collection_id, args, cdata)
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
        Self::new_with_node_ref(node, node_ref, collection_id, args, cdata)
    }

    fn new_with_node_ref<SE, PA>(
        node: &Node<SE, PA>,
        node_ref: Box<dyn NodeRef>,
        collection_id: CollectionId,
        args: MatchArgs,
        cdata: PA::ContextData,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let has_relay = node.subscription_relay.is_some();
        let (inner, query_id) = create_inner(node, node_ref, collection_id.clone(), args, cdata.clone())?;

        let me = Self(inner.clone());

        // Ephemeral node: register with relay for remote subscription
        // Remote will call activate() after applying deltas via subscription_established
        if has_relay {
            node.subscribe_remote_query(query_id, collection_id, inner.selection.value().0, cdata, 1, me.weak());
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
                }
            });
        }

        Ok(())
    }

    pub async fn update_selection_wait(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        self.update_selection(new_selection)?;
        self.0.wait_initialized().await;
        Ok(())
    }

    pub fn error(&self) -> Read<Option<RetrievalError>> { self.0.error.read() }
    pub fn query_id(&self) -> proto::QueryId { self.0.query_id }
    pub fn selection(&self) -> Read<(ankql::ast::Selection, u32)> { self.0.selection.read() }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::MutationError,
        peer_subscription::RemoteQuerySubscriber,
        policy::{PermissiveAgent, DEFAULT_CONTEXT},
        storage::StorageCollection,
    };
    use ankurah_signals::With;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    /// Test gate shared between a [`GatedEngine`] and the test body. Fetches against the
    /// gated collection park until the test opens the gate, so an activation can be held
    /// open at its storage fetch while the test drives a competing activation.
    struct Gate {
        open_tx: tokio::sync::watch::Sender<bool>,
        open_rx: tokio::sync::watch::Receiver<bool>,
        fetches_started: AtomicUsize,
        states: Mutex<Vec<proto::Attested<proto::EntityState>>>,
    }

    impl Gate {
        fn new() -> Arc<Self> {
            let (open_tx, open_rx) = tokio::sync::watch::channel(false);
            Arc::new(Self { open_tx, open_rx, fetches_started: AtomicUsize::new(0), states: Mutex::new(Vec::new()) })
        }

        fn open(&self) { self.open_tx.send(true).expect("gate receiver dropped"); }

        fn fetches_started(&self) -> usize { self.fetches_started.load(Ordering::SeqCst) }

        /// Stand-in for remote deltas having been applied to local storage
        fn add_state(&self, state: proto::Attested<proto::EntityState>) { self.states.lock().unwrap().push(state); }

        async fn wait_open(&self) {
            let mut rx = self.open_rx.clone();
            while !*rx.borrow_and_update() {
                rx.changed().await.expect("gate sender dropped");
            }
        }
    }

    /// Storage stub whose fetch path parks on the gate for one target collection.
    /// Other collections (notably the system catalog, fetched at node construction)
    /// pass through so node setup is not entangled with the gate.
    struct GatedEngine {
        gated_collection: CollectionId,
        gate: Arc<Gate>,
    }

    impl GatedEngine {
        fn new(gated_collection: CollectionId) -> (Self, Arc<Gate>) {
            let gate = Gate::new();
            (Self { gated_collection, gate: gate.clone() }, gate)
        }
    }

    #[async_trait::async_trait]
    impl StorageEngine for GatedEngine {
        type Value = ();

        async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
            Ok(Arc::new(GatedCollection { gated: *id == self.gated_collection, gate: self.gate.clone() }))
        }

        async fn delete_all_collections(&self) -> Result<bool, MutationError> { Ok(false) }
    }

    struct GatedCollection {
        gated: bool,
        gate: Arc<Gate>,
    }

    #[async_trait::async_trait]
    impl StorageCollection for GatedCollection {
        async fn set_state(&self, _state: proto::Attested<proto::EntityState>) -> Result<bool, MutationError> { Ok(true) }

        async fn get_state(&self, id: proto::EntityId) -> Result<proto::Attested<proto::EntityState>, RetrievalError> {
            Err(RetrievalError::EntityNotFound(id))
        }

        async fn fetch_states(
            &self,
            _selection: &ankql::ast::Selection,
        ) -> Result<Vec<proto::Attested<proto::EntityState>>, RetrievalError> {
            if self.gated {
                self.gate.fetches_started.fetch_add(1, Ordering::SeqCst);
                self.gate.wait_open().await;
                Ok(self.gate.states.lock().unwrap().clone())
            } else {
                Ok(Vec::new())
            }
        }

        async fn add_event(&self, _entity_event: &proto::Attested<proto::Event>) -> Result<bool, MutationError> { Ok(true) }

        async fn get_events(&self, _event_ids: Vec<proto::EventId>) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
            Ok(Vec::new())
        }

        async fn dump_entity_events(&self, _id: proto::EntityId) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
            Ok(Vec::new())
        }
    }

    /// Tracing subscriber that records event messages so a test can assert on them.
    /// Thread-local via set_default; sufficient here because the current-thread test
    /// runtime polls every spawned task on this thread.
    #[derive(Clone)]
    struct RecordingSubscriber(Arc<Mutex<Vec<String>>>);

    impl tracing::Subscriber for RecordingSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool { true }
        fn new_span(&self, _attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id { tracing::span::Id::from_u64(1) }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            struct MessageVisitor<'a>(&'a mut String);
            impl tracing::field::Visit for MessageVisitor<'_> {
                fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                    if field.name() == "message" {
                        use std::fmt::Write;
                        let _ = write!(self.0, "{:?}", value);
                    }
                }
            }
            let mut message = String::new();
            event.record(&mut MessageVisitor(&mut message));
            self.0.lock().unwrap().push(message);
        }
    }

    /// Drive the current-thread runtime until the condition holds. Cooperative scheduling
    /// makes this deterministic: each yield lets every ready task run to its next await point.
    async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..10_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("wait_until timed out: {what}");
    }

    fn latched_error(query: &EntityLiveQuery) -> Option<String> { query.error().with(|e| e.as_ref().map(|e| e.to_string())) }

    /// On relay-bearing nodes two same-version activations fire by design: the local
    /// initialization task (serve-from-cache) and the relay's subscription_established
    /// (after initial deltas apply). Nothing but timing ordered them before the activation
    /// lock existed: both could read initialized_version == 0, both took the reactor's add
    /// path, and the reactor refused the duplicate with "Query ... already exists".
    /// This pins the required behavior, not just the absence of the error: the second
    /// activation must wait on the activation lock (no second add fetch while the first
    /// holds the gate), then run as an update after the winner's add.
    #[tokio::test]
    async fn racing_same_version_activations_coalesce_into_add_then_update() {
        let trace_messages = Arc::new(Mutex::new(Vec::new()));
        let _trace_guard = tracing::subscriber::set_default(RecordingSubscriber(trace_messages.clone()));

        let collection_id = CollectionId::fixed_name("album");
        let (engine, gate) = GatedEngine::new(collection_id.clone());
        let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());

        // The constructor spawns the local initialization task, which runs activate(1) and
        // parks inside the gated storage fetch while holding the activation lock
        let args: MatchArgs = "name = 'x'".try_into().unwrap();
        let query = EntityLiveQuery::new(&node, collection_id, args, DEFAULT_CONTEXT).unwrap();
        wait_until("local activation reaches the storage fetch", || gate.fetches_started() == 1).await;

        // Drive the remote half exactly as the relay does after initial deltas apply
        let remote = {
            let weak = query.weak();
            tokio::spawn(async move { weak.subscription_established(1).await })
        };

        // Let the remote activation run as far as it can get. Serialized, it parks on the
        // activation lock before any storage access; unserialized, it would reach the add
        // path's storage fetch and bump the counter to two while the gate is still held
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        let fetches_while_gated = gate.fetches_started();

        gate.open();
        remote.await.expect("remote activation task panicked");

        // A double add latches "Query ... already exists"; an update before the add would
        // latch "Query not found for update". A correctly coalesced run latches nothing.
        assert_eq!(latched_error(&query), None, "no activation error may latch");

        // The coalesced activation waited instead of starting a second add fetch
        assert_eq!(fetches_while_gated, 1, "second activation must wait on the activation lock, not start its own add fetch");
        // ... and still ran afterwards, as the update path's own fetch
        assert_eq!(gate.fetches_started(), 2, "coalesced activation must run as an update after the winner's add");

        // Coalescing must not be silent: the waiter announced itself in traces
        let query_id = query.query_id().to_string();
        let saw_contention =
            trace_messages.lock().unwrap().iter().any(|m| m.contains("waiting for in-flight activation") && m.contains(&query_id));
        assert!(saw_contention, "contended activation must emit the coalescing debug line");

        assert_eq!(query.0.initialized_version.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(query.resultset().is_loaded());
        query.wait_initialized().await;

        // The query must remain fully operational after the race: a selection update takes
        // the update path against the state the winning add registered
        tokio::time::timeout(std::time::Duration::from_secs(30), query.update_selection_wait("name = 'y'"))
            .await
            .expect("selection update timed out")
            .expect("selection update failed");
        assert_eq!(latched_error(&query), None, "selection update after the race may not latch an error");
        assert_eq!(query.0.initialized_version.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(gate.fetches_started(), 3);
    }

    /// Probe: documents current behavior on relay-bearing nodes, not an endorsement of it.
    /// The local serve-from-cache activation alone releases wait_initialized(), so a reader
    /// gating on it (Context::query_wait does exactly this) can observe a loaded but EMPTY
    /// resultset on a fresh query before the authoritative remote refresh arrives. The remote
    /// refresh later rewrites the resultset via the update path. If the maintainer decides
    /// fresh relay-backed queries should hold readers until the remote answer, this test is
    /// the fixture to flip.
    #[tokio::test]
    async fn relay_node_wait_initialized_releases_local_results_before_remote_refresh() {
        let collection_id = CollectionId::fixed_name("album");
        let (engine, gate) = GatedEngine::new(collection_id.clone());
        // Normal timing: the local fetch is unimpeded
        gate.open();

        // Ephemeral node: has a subscription relay. No durable peer connects, so the remote
        // half stays pending, exactly like the window before a connection is established
        let node = Node::new(Arc::new(engine), PermissiveAgent::new());

        // cached args, so the local serve-from-cache task runs alongside the pending remote
        let args = MatchArgs::from(ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None });
        let query = EntityLiveQuery::new(&node, collection_id.clone(), args, DEFAULT_CONTEXT).unwrap();

        // Released by the LOCAL activation alone: loaded, initialized, and empty
        tokio::time::timeout(std::time::Duration::from_secs(30), query.wait_initialized()).await.expect("wait_initialized timed out");
        assert!(query.resultset().is_loaded());
        assert_eq!(query.resultset().len(), 0, "reader observes the empty local answer as initialized");
        assert_eq!(query.0.initialized_version.load(std::sync::atomic::Ordering::Relaxed), 1);

        // The authoritative remote answer arrives afterwards: deltas land in storage, then
        // the relay activates. Only now does the resultset reflect the remote entity
        let entity_state =
            proto::EntityState { entity_id: proto::EntityId::new(), collection: collection_id, state: proto::State::default() };
        gate.add_state(entity_state.into());
        query.weak().subscription_established(1).await;

        assert_eq!(latched_error(&query), None);
        assert_eq!(query.resultset().len(), 1, "remote refresh rewrites the resultset the reader already saw as empty");
    }
}
