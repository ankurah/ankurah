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
    node::{erased::ErasedNodeRef, MatchArgs, NodeType, TNodeErased},
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

struct Inner {
    pub(crate) query_id: proto::QueryId,
    // subscription must be declared before node so it drops first —
    // dropping a strong node handle deallocates the reactor, and
    // subscription's Drop needs the reactor to unsubscribe.
    pub(crate) subscription: ReactorSubscription,
    pub(crate) node: Box<dyn ErasedNodeRef>,
    pub(crate) resultset: EntityResultSet,
    pub(crate) error: Mut<Option<RetrievalError>>,
    pub(crate) initialized: tokio::sync::Notify,
    pub(crate) initialized_version: std::sync::atomic::AtomicU32,
    // The newest selection version a durable authority has answered: the
    // relay reports its subscription established once the peer's deltas are
    // applied, and a node with no relay is its own authority and marks each
    // version as it starts. Compared against current_version, exactly like
    // initialized_version: a selection update or a system reset bumps
    // current_version, which downgrades both without any flag to clear.
    pub(crate) durable_version: std::sync::atomic::AtomicU32,
    pub(crate) durable_notify: tokio::sync::Notify,
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
    // The admission INPUT, retained so a system reset can re-admit under the
    // new epoch: the compiled declaration (typed entries) and the name-form
    // selection, kept current by update_selection. Resolved selections are
    // epoch-scoped; these are not.
    pub(crate) schema: Option<&'static crate::schema::ModelStructDescriptor>,
    pub(crate) parsed: Mut<ankql::ast::Selection<Parsed>>,
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

    /// Wait for a durable authority to have answered this query's CURRENT
    /// selection version.
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
    async fn wait_durable_answered(&self) -> Result<(), RetrievalError> {
        self.wait_initialized().await?;
        loop {
            // Register before reading, like `wait_initialized`: a
            // confirmation landing between the read and the wait would
            // otherwise be lost and this waiter parked for good.
            let notified = self.durable_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(error) = self.initialization_error() {
                return Err(error);
            }
            if self.durable_version.load(std::sync::atomic::Ordering::Acquire)
                >= self.current_version.load(std::sync::atomic::Ordering::Acquire)
            {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Whether a durable authority has answered the current selection
    /// version and the query has initialized it. Registering nothing: the
    /// synchronous-admission probe, not a wait.
    fn is_durable_answered(&self) -> bool {
        let current = self.current_version.load(std::sync::atomic::Ordering::Acquire);
        self.durable_version.load(std::sync::atomic::Ordering::Acquire) >= current
            && self.initialized_version.load(std::sync::atomic::Ordering::Relaxed) >= current
    }

    /// Record that a durable authority has answered `version`, and wake
    /// everyone waiting on it. `fetch_max`, so an out-of-order confirmation
    /// never regresses a newer one.
    fn mark_durable_answered(&self, version: u32) {
        self.durable_version.fetch_max(version, std::sync::atomic::Ordering::AcqRel);
        self.durable_notify.notify_waiters();
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
        self.durable_notify.notify_waiters();
    }

    /// Activate the LiveQuery by fetching entities and calling reactor.add_query or reactor.update_query
    /// Called after deltas have been applied for both initial subscription and selection updates
    /// Gets all parameters from self (collection_id, query_id, selection)
    /// Marks initialization as complete regardless of success/failure
    /// Rejects activation if the version is older than the current selection to prevent regression
    async fn activate(&self, version: u32) -> Result<(), RetrievalError> {
        // Get the current selection and its version
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
/// ([`crate::schema::reads_bypass_policy`]): the catalog projection runs
/// before this node has a credential to be judged under, and it is what makes
/// every other query resolvable.
fn admit<SE, PA>(
    node: &Node<SE, PA>,
    sessions: &SessionSet<PA::ContextData>,
    schema: Option<&'static crate::schema::ModelStructDescriptor>,
    collection_id: &CollectionId,
    selection: ankql::ast::Selection<Parsed>,
) -> Result<ankql::ast::Selection<Resolved>, RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let exempt = crate::schema::reads_bypass_policy(collection_id);
    // One credential snapshot for the whole derivation; re-derivation
    // on change arrives with https://github.com/ankurah/ankurah/pull/426.
    let cdata = sessions.current();
    if !exempt {
        node.policy_agent.can_access_collection(&cdata, collection_id)?;
    }
    let mut selection = match schema {
        Some(schema) => node.catalog.resolve_selection_with_descriptor(node, schema, selection)?,
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
    node_ref: Box<dyn ErasedNodeRef>,
    schema: Option<&'static crate::schema::ModelStructDescriptor>,
    parsed: ankql::ast::Selection<Parsed>,
    collection_id: CollectionId,
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
        durable_version: std::sync::atomic::AtomicU32::new(0),
        durable_notify: tokio::sync::Notify::new(),
        current_version: std::sync::atomic::AtomicU32::new(1), // Start at version 1
        selection: Mut::new(selection.map(|selection| (selection, 1))), // Start with version 1
        collection_id: collection_id.clone(),
        gap_fetcher,
        schema,
        parsed: Mut::new(parsed),
    });

    (inner, query_id)
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
    selection: ankql::ast::Selection<Resolved>,
    cached: bool,
    sessions: SessionSet<PA::ContextData>,
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let query_id = inner.query_id;
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
    if has_relay {
        node.subscribe_remote_query(query_id, inner.collection_id.clone(), selection, sessions, 1, me.weak());
    } else {
        // No relay: this query's own storage is the authority, and it
        // answers each version as it starts.
        inner.mark_durable_answered(1);
    }
}

/// An admission that could not be judged synchronously, finished in a
/// spawned task with failures landing in the query's error slot.
enum DeferredAdmission {
    /// A typed entry whose declaration this system has never been told
    /// about: healing it means registering, which awaits the allocator.
    Typed(&'static crate::schema::ModelStructDescriptor, ankql::ast::Selection<Parsed>),
    /// A raw entry that failed to admit before the catalog synced: only a
    /// synced catalog makes a raw miss authoritative, so it re-admits once
    /// the durable's catalog rows are applied.
    Raw(ankql::ast::Selection<Parsed>),
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
        let node_ref: Box<dyn ErasedNodeRef> = Box::new(NodeType::Strong(node.clone()));
        Self::new_with_node_ref(node, node_ref, schema, collection_id, args, sessions.into())
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
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: CollectionId,
        args: MatchArgs<Parsed>,
        sessions: impl Into<SessionSet<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn ErasedNodeRef> = Box::new(NodeType::Weak(node.weak()));
        Self::new_with_node_ref(node, node_ref, schema, collection_id, args, sessions.into())
    }

    /// The one construction path: admit the selection, build the inner, and
    /// set the query running. `schema` distinguishes a typed entry (names
    /// bind through the compiled declaration's cells) from a raw one (names
    /// bind through the catalog's current display names); the node reference
    /// distinguishes a query that keeps its node alive from one that does
    /// not. An admission failure is this call's error, never a query that
    /// will never populate.
    ///
    /// Two cases cannot be judged synchronously and finish admitting in a
    /// spawned task instead ([`DeferredAdmission`]), failures landing in the
    /// error slot.
    fn new_with_node_ref<SE, PA>(
        node: &Node<SE, PA>,
        node_ref: Box<dyn ErasedNodeRef>,
        schema: Option<&'static crate::schema::ModelStructDescriptor>,
        collection_id: CollectionId,
        args: MatchArgs<Parsed>,
        sessions: SessionSet<PA::ContextData>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let MatchArgs { selection, cached } = args;
        let parsed_input = selection.clone();
        // Binding is what the synchronous judgment rests on: a typed entry
        // whose declaration binds admits inline, and a raw entry admits
        // inline whenever it can (a name-free selection needs no catalog at
        // all). What cannot be judged now defers; an admission failure on a
        // synced catalog is the caller's error, here and now.
        let admission: Result<ankql::ast::Selection<Resolved>, DeferredAdmission> = match schema {
            Some(schema) => match admit(node, &sessions, Some(schema), &collection_id, selection.clone()) {
                Err(RetrievalError::UnboundDeclaration { .. }) => Err(DeferredAdmission::Typed(schema, selection)),
                result => Ok(result?),
            },
            None => match admit(node, &sessions, None, &collection_id, selection.clone()) {
                Err(_) if !node.catalog.is_synced() => Err(DeferredAdmission::Raw(selection)),
                result => Ok(result?),
            },
        };

        let (inner, _query_id) = create_inner(node, node_ref, schema, parsed_input, collection_id, None, sessions.clone());
        let me = Self(inner.clone());

        // A system reset voids the resolved selection (its identities are
        // epoch-scoped). The hook bumps the version so both waits go stale
        // immediately, then re-admits the retained name-form input under the
        // new system through the same lanes as construction.
        {
            let weak_inner = Arc::downgrade(&inner);
            let weak_node = node.weak();
            let sessions = sessions.clone();
            inner.subscription.set_reset_hook(move || {
                let Some(inner) = weak_inner.upgrade() else { return };
                let new_version = inner.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                inner.resultset.set_loaded(false);
                let schema = inner.schema;
                let parsed = inner.parsed.value();
                let collection_id = inner.collection_id.clone();
                drop(inner);
                let (weak_inner, weak_node, sessions) = (weak_inner.clone(), weak_node.clone(), sessions.clone());
                crate::task::spawn(async move {
                    // Wait out the join that follows the reset without
                    // keeping the node alive through the wait.
                    let system = {
                        let Some(node) = weak_node.upgrade() else { return };
                        node.system.clone()
                    };
                    system.wait_system_ready().await;
                    let Some(node) = weak_node.upgrade() else { return };
                    let admitted = match schema {
                        Some(schema) => match crate::context::register_for_read(&node, &sessions, schema).await {
                            Ok(()) => admit(&node, &sessions, Some(schema), &collection_id, parsed),
                            Err(error) => Err(error),
                        },
                        None => match node.catalog.wait_synced().await {
                            Ok(()) => admit(&node, &sessions, None, &collection_id, parsed),
                            Err(error) => Err(error),
                        },
                    };
                    let Some(inner) = weak_inner.upgrade() else { return };
                    match admitted {
                        Ok(resolved) => {
                            let me = Self(inner.clone());
                            if let Err(error) = me.install_selection_update(Box::new(node.clone()), resolved, new_version) {
                                inner.fail_initialization(error);
                            }
                        }
                        Err(error) => inner.fail_initialization(error),
                    }
                });
            });
        }

        let deferred = match admission {
            Ok(selection) => {
                start_admitted(&inner, node, &me, selection, cached, sessions);
                return Ok(me);
            }
            Err(deferred) => deferred,
        };

        // The task holds neither the query nor the node: a caller who drops
        // the query before admission finishes has abandoned it, and this
        // background work must not be what keeps either alive.
        let (weak_inner, weak_node) = (Arc::downgrade(&inner), node.weak());
        crate::task::spawn(async move {
            let Some(inner) = weak_inner.upgrade() else { return };
            let Some(node) = weak_node.upgrade() else {
                inner.fail_initialization(RetrievalError::Other("Node has been dropped".into()));
                return;
            };
            let admitted = match deferred {
                DeferredAdmission::Typed(unbound, selection) => match crate::context::register_for_read(&node, &sessions, unbound).await {
                    Ok(()) => admit(&node, &sessions, Some(unbound), &inner.collection_id, selection),
                    Err(error) => Err(error),
                },
                DeferredAdmission::Raw(selection) => match node.catalog.wait_synced().await {
                    Ok(()) => admit(&node, &sessions, None, &inner.collection_id, selection),
                    Err(error) => Err(error),
                },
            };
            match admitted {
                Ok(selection) => {
                    let me = Self(inner.clone());
                    start_admitted(&inner, &node, &me, selection, cached, sessions);
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

    /// Wait for a durable authority to have answered this query's current
    /// selection version: the relay's confirmation that a peer took the
    /// subscription and its rows are applied. See
    /// [`Inner::wait_durable_answered`] for what that adds to
    /// initialization, and what it means on a query with no relay.
    pub async fn wait_durable_answered(&self) -> Result<(), RetrievalError> { self.0.wait_durable_answered().await }

    /// Whether a durable authority has answered the current selection
    /// version. The synchronous probe behind [`Self::wait_durable_answered`].
    pub(crate) fn is_durable_answered(&self) -> bool { self.0.is_durable_answered() }

    pub fn update_selection(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection<Parsed>, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        let new_selection = new_selection.try_into().map_err(|e| e.into())?;
        let node = self.0.node().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))?;

        // A replacement selection arrives as a parsed string, below the
        // typed entries: bind its names through the catalog before anything
        // stores or forwards it -- the reactor and the relay consume
        // resolved selections only. A resolution failure on a synced catalog
        // is the caller's error, here and now; one before the catalog has
        // synced is not yet authoritative, so the update defers behind the
        // sync and a real failure reports through the error slot. (Policy
        // re-injection on replacement is a recorded gap, tracked with the
        // update_selection admission issue.)
        match node.resolve_selection(&self.0.collection_id, new_selection.clone()) {
            Ok(resolved) => {
                let new_version = self.0.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                self.0.resultset.set_loaded(false);
                self.0.parsed.set(new_selection);
                self.install_selection_update(node, resolved, new_version)
            }
            Err(error) if node.is_catalog_synced() => Err(error),
            Err(_) => {
                // The version is assigned NOW, so update_selection_wait
                // waits for this update rather than being satisfied by the
                // previous version's initialization.
                let new_version = self.0.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                self.0.resultset.set_loaded(false);
                self.0.parsed.set(new_selection.clone());
                let weak = self.weak();
                crate::task::spawn(async move {
                    let Some(me) = weak.upgrade() else { return };
                    let Some(node) = me.0.node() else {
                        me.0.fail_initialization(RetrievalError::Other("Node has been dropped".into()));
                        return;
                    };
                    let installed = match node.wait_catalog_synced().await {
                        Ok(()) => node
                            .resolve_selection(&me.0.collection_id, new_selection)
                            .and_then(|resolved| me.install_selection_update(node, resolved, new_version)),
                        Err(error) => Err(error),
                    };
                    if let Err(error) = installed {
                        me.0.fail_initialization(error);
                    }
                });
                Ok(())
            }
        }
    }

    /// Install an already-resolved replacement selection under its assigned
    /// version and set it running for this node kind.
    fn install_selection_update(
        &self,
        node: Box<dyn TNodeErased>,
        new_selection: ankql::ast::Selection<Resolved>,
        new_version: u32,
    ) -> Result<(), RetrievalError> {
        self.0.selection.set(Some((new_selection.clone(), new_version)));

        if node.has_subscription_relay() {
            // Ephemeral node: delegate to relay; its established callback
            // activates this version and marks its durable answer.
            node.update_remote_query(self.0.query_id, new_selection, new_version)?;
        } else {
            // Durable node: activate directly. Its own storage is the
            // authority, so success answers this version durably too.
            let inner = self.0.clone();
            let query_id = self.0.query_id;
            crate::task::spawn(async move {
                match inner.activate(new_version).await {
                    Ok(()) => inner.mark_durable_answered(new_version),
                    Err(e) => {
                        tracing::error!("LiveQuery update failed for predicate {}: {}", query_id, e);
                        // Wakes update_selection_wait: the update is over,
                        // unsuccessfully, and the error slot carries why.
                        inner.fail_initialization(e);
                    }
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
                // them: this is the durable answer, and what anyone waiting
                // for confirmed freshness is waiting for.
                Ok(()) => inner.mark_durable_answered(version),
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
