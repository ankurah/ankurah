use ankql::ast::{Parsed, Resolved};
use std::{
    marker::PhantomData,
    sync::{Arc, Weak},
};

use ankurah_proto::{self as proto, ModelId};

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
    // Whether a remote peer has answered this query: the relay reports its
    // subscription established once the peer's deltas are applied. A query
    // with no relay is answered by its own storage and is marked here as
    // soon as it starts, because no remote will ever answer it.
    pub(crate) remote_answered: std::sync::atomic::AtomicBool,
    pub(crate) remote_answered_notify: tokio::sync::Notify,
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
    // The model this query reads, which is also what it addresses storage
    // and the relay by. Absent for the same window as `selection` and filled
    // by the same `start_admitted`: a typed query written against a
    // declaration this system has never been told about has no model
    // identity until its healing registration returns one.
    pub(crate) collection_id: std::sync::RwLock<Option<ModelId>>,
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

    async fn wait_initialized(&self) -> Result<(), RetrievalError> {
        // `notify_waiters` wakes the waiters REGISTERED at that instant and
        // stores no permit, so this waiter registers (enable) BEFORE reading
        // what it is waiting for. Reading first would lose an initialization
        // -- or a failure -- that lands in between, and park for the
        // lifetime of a query whose news has already come and gone.
        let notified = self.initialized.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // A failure ends initialization: it is not something more waiting
        // will fix, so it is this call's answer whenever the slot holds one.
        if let Some(error) = self.initialization_error() {
            return Err(error);
        }

        // If already initialized, return immediately
        if self.initialized_version.load(std::sync::atomic::Ordering::Relaxed)
            >= self.current_version.load(std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(());
        }

        // FIXME - this should be waiting for the correct version, not any version
        // Otherwise wait for the notification
        notified.await;

        match self.initialization_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Wait for a remote peer to have answered this query at least once.
    ///
    /// Initialization says the query has run; this says whose data it ran
    /// over. A cached query on an ephemeral node initializes from local
    /// storage, which may hold nothing or hold yesterday's rows, and only the
    /// relay's confirmation tells a caller that a durable peer took the
    /// subscription and its rows are applied. A query with no relay has no
    /// remote to hear from -- its own storage is the authority -- so for one
    /// of those this is exactly initialization.
    ///
    /// A failure that ends initialization ends this wait too, for the same
    /// reason: nothing further is coming.
    async fn wait_remote_answered(&self) -> Result<(), RetrievalError> {
        self.wait_initialized().await?;
        loop {
            // Register before reading, like `wait_initialized`: a
            // confirmation landing between the read and the wait would
            // otherwise be lost and this waiter parked for good.
            let notified = self.remote_answered_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(error) = self.initialization_error() {
                return Err(error);
            }
            if self.remote_answered.load(std::sync::atomic::Ordering::Acquire) {
                return Ok(());
            }
            notified.await;
        }
    }

    /// The model this query reads, once admission has settled it.
    fn collection_id(&self) -> Option<ModelId> { *self.collection_id.read().unwrap() }

    /// Record that a remote has answered, and wake everyone waiting on it.
    fn mark_remote_answered(&self) {
        self.remote_answered.store(true, std::sync::atomic::Ordering::Release);
        self.remote_answered_notify.notify_waiters();
    }

    /// The failure that ended initialization, if one did. The slot keeps the
    /// original for [`EntityLiveQuery::error`] to hand out; a waiter gets its
    /// rendering, because the error itself is not clonable.
    fn initialization_error(&self) -> Option<RetrievalError> {
        self.error.with(|error| error.as_ref().map(|error| RetrievalError::Other(error.to_string())))
    }

    /// Record a failure that ends initialization, and wake everyone waiting
    /// on it: the query will never populate, and the slot says why. Waiters
    /// on a remote answer hear it too -- a failure is the end of that news
    /// as well, and one that lands after initialization succeeded would
    /// otherwise leave them parked forever.
    fn fail_initialization(&self, error: RetrievalError) {
        self.error.set(Some(error));
        self.initialized.notify_waiters();
        self.remote_answered_notify.notify_waiters();
    }

    /// Activate the LiveQuery by fetching entities and calling reactor.add_query or reactor.update_query
    /// Called after deltas have been applied for both initial subscription and selection updates
    /// Gets all parameters from self (collection_id, query_id, selection)
    /// Marks initialization as complete regardless of success/failure
    /// Rejects activation if the version is older than the current selection to prevent regression
    async fn activate(&self, version: u32) -> Result<(), RetrievalError> {
        // Get the model being read, the current selection, and its version
        let Some(collection_id) = self.collection_id() else {
            return Err(RetrievalError::Other("live query activated before its model was admitted".into()));
        };
        let Some((selection, stored_version)) = self.selection.value() else {
            // Unreachable by construction -- a query is started only once its
            // admitted selection is installed -- but a query with no
            // selection has nothing to run, and inventing one here would run
            // the wrong query.
            return Err(RetrievalError::Other("live query activated before its selection was admitted".into()));
        };

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
                    collection_id,
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
                .update_query_and_notify(self.subscription.id(), self.query_id, collection_id, selection, &*node, version, &hook)
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

/// Admit a selection for `collection_id`: bind every property name to its
/// durable identity and canonicalize comparison values, then let the policy
/// narrow what came back. The agent ANDs its own conditions in in the same
/// resolved vocabulary the reactor and the relay consume, so nothing past
/// this point is left to bind.
///
/// `schema` is the compiled declaration a typed query was written against,
/// and `None` for a raw one that names its collection by string: the typed
/// form binds field names through the descriptor's cells, the raw form
/// through the catalog's current display names.
///
/// A catalog collection skips the agent entirely, on both counts
/// ([`crate::schema::is_catalog_collection`]): the catalog projection runs
/// before this node has a credential to be judged under, and it is what makes
/// every other query resolvable.
fn admit<SE, PA>(
    node: &Node<SE, PA>,
    sessions: &SessionSet<PA::ContextData>,
    schema: Option<&'static crate::schema::ModelStructDescriptor>,
    collection_id: &ModelId,
    selection: ankql::ast::Selection<Parsed>,
) -> Result<ankql::ast::Selection<Resolved>, RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let exempt = crate::schema::is_catalog_collection(collection_id);
    // One credential snapshot for the whole derivation; re-derivation
    // on change arrives with https://github.com/ankurah/ankurah/pull/426.
    let cdata = sessions.current();
    if !exempt {
        node.policy_agent.can_access_collection(&cdata, collection_id)?;
    }
    let mut selection = match schema {
        Some(schema) => node.catalog.resolve_selection_with_descriptor(schema, selection)?,
        None => node.catalog.resolve_selection(collection_id, selection)?,
    };
    if !exempt {
        selection.predicate = node.policy_agent.filter_predicate(&cdata, collection_id, selection.predicate)?;
    }
    Ok(selection)
}

/// Helper: create the Inner shared by every constructor. `selection` is the
/// admitted selection, or `None` when [`start_admitted`] installs it a moment
/// later; nothing runs against the query until one is there.
fn create_inner<SE, PA>(
    node: &Node<SE, PA>,
    node_ref: Box<dyn NodeRef>,
    selection: Option<ankql::ast::Selection<Resolved>>,
    sessions: SessionSet<PA::ContextData>,
) -> (Arc<Inner>, proto::QueryId)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
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
        remote_answered: std::sync::atomic::AtomicBool::new(false),
        remote_answered_notify: tokio::sync::Notify::new(),
        current_version: std::sync::atomic::AtomicU32::new(1), // Start at version 1
        selection: Mut::new(selection.map(|selection| (selection, 1))), // Start with version 1
        collection_id: std::sync::RwLock::new(None),
        gap_fetcher,
    });

    (inner, query_id)
}

/// When a query registers with the subscription relay: as it starts, or when
/// its owner says so.
///
/// Only the catalog projection asks for the second. Its three queries all
/// start at once, and each subscribe request is sent when that query's own
/// storage read finishes, so subscribing them at start would put three
/// requests on the wire in whatever order those reads landed -- traffic that
/// is not a function of the node's inputs. The catalog therefore attaches
/// them one at a time, each after the last was answered.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteSubscription {
    AtStart,
    OnRequest,
}

/// What a live query reads. A typed query is written against a compiled
/// declaration and takes its model identity from that declaration's binding;
/// a raw one is handed the identity outright.
#[derive(Clone, Copy)]
enum QueryTarget {
    Raw(ModelId),
    Typed(&'static crate::schema::ModelStructDescriptor),
}

/// Install an admitted selection and set the query running for this node
/// kind: a durable node (and any cached query) activates against local
/// storage, and an ephemeral node registers the query with its relay, whose
/// established callback activates it once the remote's deltas are applied.
/// A cached ephemeral query does BOTH -- storage serves what it already holds
/// while the remote subscription refreshes it; these are not alternatives.
fn start_admitted<SE, PA>(
    inner: &Arc<Inner>,
    node: &Node<SE, PA>,
    me: &EntityLiveQuery,
    collection_id: ModelId,
    selection: ankql::ast::Selection<Resolved>,
    cached: bool,
    sessions: SessionSet<PA::ContextData>,
    remote: RemoteSubscription,
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let query_id = inner.query_id;
    // The model first: it is what the activation below addresses storage by,
    // and what the relay subscribes to.
    *inner.collection_id.write().unwrap() = Some(collection_id);
    inner.selection.set(Some((selection.clone(), 1)));

    let has_relay = node.subscription_relay.is_some();
    if cached || !has_relay {
        let inner = inner.clone();
        debug!("LiveQuery spawning initialization task for predicate {}", query_id);
        crate::task::spawn(async move {
            debug!("LiveQuery initialization task starting for predicate {}", query_id);
            if let Err(e) = inner.activate(1).await {
                debug!("LiveQuery initialization failed for predicate {}: {}", query_id, e);
                inner.fail_initialization(e);
            } else {
                debug!("LiveQuery initialization completed for predicate {}", query_id);
            }
        });
    }
    match (has_relay, remote) {
        (true, RemoteSubscription::AtStart) => node.subscribe_remote_query(query_id, collection_id, selection, sessions, 1, me.weak()),
        // Waiting for the owner to ask; it marks the query answered when the
        // relay confirms what it attached.
        (true, RemoteSubscription::OnRequest) => {}
        // No relay, no remote: this query's own storage is the authority, so
        // there is no confirmation to wait for and a waiter for one must not
        // wait for something that will never happen.
        (false, _) => inner.mark_remote_answered(),
    }
}

impl EntityLiveQuery {
    pub fn new<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: ModelId,
        args: MatchArgs<Parsed>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(StrongNodeRef(Arc::clone(&node.0)));
        Self::new_with_node_ref(node, node_ref, QueryTarget::Raw(collection_id), args, sessions.into(), RemoteSubscription::AtStart)
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
        collection_id: ModelId,
        args: MatchArgs<Parsed>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(WeakNodeRefImpl(Arc::downgrade(&node.0)));
        Self::new_with_node_ref(node, node_ref, QueryTarget::Raw(collection_id), args, sessions.into(), RemoteSubscription::AtStart)
    }

    /// [`Self::new_weak_node`] for a query whose owner attaches the remote
    /// subscription itself, with [`Self::subscribe_remote`], rather than
    /// having it go out as the query starts. See [`RemoteSubscription`] for
    /// why the catalog projection is built this way.
    pub(crate) fn new_weak_node_local<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: ModelId,
        args: MatchArgs<Parsed>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(WeakNodeRefImpl(Arc::downgrade(&node.0)));
        Self::new_with_node_ref(node, node_ref, QueryTarget::Raw(collection_id), args, sessions.into(), RemoteSubscription::OnRequest)
    }

    /// Create a typed LiveQuery: one written against a compiled declaration,
    /// whose field names bind through that declaration's descriptor cells.
    ///
    /// Admission is synchronous whenever those cells are bound: the
    /// resolver's answers are then definitive, so this either binds every
    /// name and returns a running query, or it returns the error now -- a
    /// name that does not resolve against a bound declaration is a mistake,
    /// not a race. A declaration this system has never been told about is
    /// the one exception, and it is healed inside the query's initialization
    /// rather than refused (the catalog's `bind_or_register`), because this
    /// entry cannot await. The node is held strongly, like
    /// [`EntityLiveQuery::new`].
    pub fn new_typed<SE, PA>(
        node: &Node<SE, PA>,
        schema: &'static crate::schema::ModelStructDescriptor,
        args: MatchArgs<Parsed>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(StrongNodeRef(Arc::clone(&node.0)));
        Self::new_with_node_ref(node, node_ref, QueryTarget::Typed(schema), args, sessions.into(), RemoteSubscription::AtStart)
    }

    /// The one construction path: settle what the query reads, admit its
    /// selection, build the inner, and set the query running. The target
    /// distinguishes a typed entry (the model comes from the declaration's
    /// binding, and names bind through its cells) from a raw one (the caller
    /// names the model, and names bind through the catalog's current display
    /// names); the node reference distinguishes a query that keeps its node
    /// alive from one that does not. An admission failure is this call's
    /// error, never a query that will never populate.
    ///
    /// The single exception is a typed entry whose declaration this system
    /// has never been told about. Healing it means registering it, which
    /// means awaiting the durable allocator, and this entry is synchronous:
    /// so that one case builds the query first, and finishes settling and
    /// admitting it in a spawned task, where a failure lands in the error
    /// slot instead.
    fn new_with_node_ref<SE, PA>(
        node: &Node<SE, PA>,
        node_ref: Box<dyn NodeRef>,
        target: QueryTarget,
        args: MatchArgs<Parsed>,
        sessions: SessionSet<PA::ContextData>,
        remote: RemoteSubscription,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // Binding is what the synchronous judgment rests on, and it is also
        // what a typed query takes its model identity from, so ask for it
        // here: a declaration that binds admits inline, and only one that
        // does not takes the deferred path. A raw query needs no binding --
        // the caller named the model.
        let settled = match target {
            QueryTarget::Raw(collection_id) => Some((None, collection_id)),
            QueryTarget::Typed(schema) => node.catalog.bind_descriptor(schema).ok().map(|(model, _epoch)| (Some(schema), model)),
        };
        if let Some((schema, collection_id)) = settled {
            let selection = admit(node, &sessions, schema, &collection_id, args.selection)?;
            let (inner, _query_id) = create_inner(node, node_ref, None, sessions.clone());

            let me = Self(inner.clone());
            start_admitted(&inner, node, &me, collection_id, selection, args.cached, sessions, remote);

            return Ok(me);
        }
        let QueryTarget::Typed(unbound) = target else {
            unreachable!("a raw query settles above; only a declaration can be unbound");
        };

        let (inner, _query_id) = create_inner(node, node_ref, None, sessions.clone());
        let me = Self(inner.clone());

        // The task holds neither the query nor the node: a caller who drops
        // the query before healing finishes has abandoned it, and this
        // background work must not be what keeps either alive.
        let (weak_inner, weak_node) = (Arc::downgrade(&inner), node.weak());
        let MatchArgs { selection, cached } = args;
        crate::task::spawn(async move {
            let Some(inner) = weak_inner.upgrade() else { return };
            let Some(node) = weak_node.upgrade() else {
                inner.fail_initialization(RetrievalError::Other("Node has been dropped".into()));
                return;
            };
            // Registration is also what supplies the model identity: until
            // this system has been told about the declaration, there is no
            // identity for the query to be addressed to.
            let admitted = match node.catalog.bind_or_register(&sessions, unbound).await {
                Ok((collection_id, _epoch)) => {
                    admit(&node, &sessions, Some(unbound), &collection_id, selection).map(|selection| (collection_id, selection))
                }
                Err(error) => Err(error),
            };
            match admitted {
                Ok((collection_id, selection)) => {
                    let me = Self(inner.clone());
                    start_admitted(&inner, &node, &me, collection_id, selection, cached, sessions, remote);
                }
                Err(error) => inner.fail_initialization(error),
            }
        });

        Ok(me)
    }
    pub fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    /// Wait for the LiveQuery to be fully initialized with initial states.
    /// An initialization that failed is this call's error: the query will
    /// never populate, and waiting longer would not change that.
    pub async fn wait_initialized(&self) -> Result<(), RetrievalError> { self.0.wait_initialized().await }

    /// Wait for a remote peer to have answered this query: the relay's
    /// confirmation that a peer took the subscription and its rows are
    /// applied. See [`Inner::wait_remote_answered`] for what that adds to
    /// initialization, and what it means on a query with no relay.
    pub async fn wait_remote_answered(&self) -> Result<(), RetrievalError> { self.0.wait_remote_answered().await }

    /// Put this query's subscribe request on the wire now.
    ///
    /// For a query built with [`Self::new_weak_node_local`], whose owner
    /// decides when that request goes out. A node with no relay has no remote
    /// to subscribe to, and answers for itself.
    pub(crate) fn subscribe_remote<SE, PA>(&self, node: &Node<SE, PA>, sessions: SessionSet<PA::ContextData>) -> Result<(), RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if node.subscription_relay.is_none() {
            self.0.mark_remote_answered();
            return Ok(());
        }
        let (Some(collection_id), Some((selection, version))) = (self.0.collection_id(), self.0.selection.value()) else {
            return Err(RetrievalError::Other("live query subscribed remotely before its selection was admitted".into()));
        };
        node.subscribe_remote_query(self.0.query_id, collection_id, selection, sessions, version, self.weak());
        Ok(())
    }

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
        let Some(collection_id) = self.0.collection_id() else {
            return Err(RetrievalError::Other("live query selection updated before its model was admitted".into()));
        };
        let new_selection = node.resolve_selection(&collection_id, new_selection)?;

        // Increment current_version atomically and get the new version number
        let new_version = self.0.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

        // Mark resultset as not loaded since we're changing the selection
        self.0.resultset.set_loaded(false);

        // Store new selection and version
        self.0.selection.set(Some((new_selection.clone(), new_version)));

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
                    // Wakes update_selection_wait: the update is over,
                    // unsuccessfully, and the error slot carries why.
                    inner.fail_initialization(e);
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
        self.0.wait_initialized().await
    }

    pub fn error(&self) -> Read<Option<RetrievalError>> { self.0.error.read() }
    pub fn query_id(&self) -> proto::QueryId { self.0.query_id }
    /// The admitted selection and its version, or `None` while a typed
    /// query's admission is still running.
    pub fn selection(&self) -> Read<Option<(ankql::ast::Selection<Resolved>, u32)>> { self.0.selection.read() }
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
            match inner.activate(version).await {
                // The peer's rows are applied and the query has run over
                // them: this is the remote answer, and what anyone waiting
                // for confirmed freshness is waiting for.
                Ok(()) => inner.mark_remote_answered(),
                Err(e) => {
                    tracing::error!("Failed to activate subscription for query {}: {}", inner.query_id, e);
                    // This was the query's initialization on an ephemeral node
                    // -- the relay established the subscription and handed it
                    // here -- so a failure ends it, and everyone waiting must
                    // hear that rather than wait out a query that will never
                    // populate.
                    inner.fail_initialization(e);
                }
            }
        }
        // If upgrade fails, the LiveQuery was already dropped - nothing to do
    }

    fn set_last_error(&self, error: RetrievalError) {
        // Try to upgrade the weak reference
        if let Some(inner) = self.0.upgrade() {
            tracing::info!("Setting last error for LiveQuery {}: {}", inner.query_id, error);
            // The relay reports a PERMANENT failure here (a retryable one goes
            // back to pending instead), so nothing further will initialize
            // this query and its waiters are owed the news.
            inner.fail_initialization(error);
        }
        // If upgrade fails, the LiveQuery was already dropped - nothing to do
    }
}

impl<R: View> LiveQuery<R> {
    /// Wait for the LiveQuery to be fully initialized with initial states.
    /// An initialization that failed is this call's error.
    pub async fn wait_initialized(&self) -> Result<(), RetrievalError> { self.0.wait_initialized().await }

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
