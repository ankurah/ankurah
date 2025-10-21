use std::{
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc, Weak},
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
    node::{MatchArgs, TNodeErased},
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
#[derive(Clone)]
pub struct EntityLiveQuery(Arc<Inner>);
struct Inner {
    pub(crate) query_id: proto::QueryId,
    pub(crate) node: Box<dyn TNodeErased>,
    // Store the actual subscription - now non-generic!
    pub(crate) subscription: ReactorSubscription,
    pub(crate) resultset: EntityResultSet,
    pub(crate) error: Mut<Option<RetrievalError>>,
    pub(crate) initialized: tokio::sync::Notify,
    pub(crate) initialized_version: std::sync::atomic::AtomicU32,
    // Version tracking for predicate updates
    pub(crate) current_version: std::sync::atomic::AtomicU32,
    // Store selection with its version (starts with version 1, updated on selection changes)
    // This represents user intent (client-side state), separate from reactor's QueryState.selection (reactor-side state)
    pub(crate) selection: std::sync::Mutex<(ankql::ast::Selection, u32)>,
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

impl crate::reactor::PreNotifyHook for &EntityLiveQuery {
    fn pre_notify(&self, version: u32) {
        // Mark as initialized before notification is sent
        self.mark_initialized(version);
    }
}

impl EntityLiveQuery {
    pub fn new<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: CollectionId,
        mut args: MatchArgs,
        cdata: PA::ContextData,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        node.policy_agent.can_access_collection(&cdata, &collection_id)?;
        args.selection.predicate = node.policy_agent.filter_predicate(&cdata, &collection_id, args.selection.predicate)?;

        let subscription = node.reactor.subscribe();

        let resultset = EntityResultSet::empty();
        let query_id = proto::QueryId::new();
        let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(&node, cdata.clone()));

        let me = Self(Arc::new(Inner {
            query_id,
            node: Box::new(node.clone()),
            subscription,
            resultset: resultset.clone(),
            error: Mut::new(None),
            initialized: tokio::sync::Notify::new(),
            initialized_version: std::sync::atomic::AtomicU32::new(0), // 0 means uninitialized
            current_version: std::sync::atomic::AtomicU32::new(1),     // Start at version 1
            selection: std::sync::Mutex::new((args.selection.clone(), 1)), // Start with version 1
            collection_id: collection_id.clone(),
            gap_fetcher,
        }));

        // Check if this is a durable node (no relay) or ephemeral node (has relay)
        let has_relay = node.subscription_relay.is_some();

        if args.cached || !has_relay {
            // Durable node: spawn initialization task directly (no remote subscription needed)
            let me2 = me.clone();

            debug!("LiveQuery::new() spawning initialization task for durable node predicate {}", query_id);
            crate::task::spawn(async move {
                debug!("LiveQuery initialization task starting for predicate {}", query_id);
                if let Err(e) = me2.activate(1).await {
                    debug!("LiveQuery initialization failed for predicate {}: {}", query_id, e);

                    me2.0.error.set(Some(e));
                } else {
                    debug!("LiveQuery initialization completed for predicate {}", query_id);
                }
            });
        }

        // Ephemeral node: register with relay for remote subscription
        // Remote will call activate() after applying deltas via subscription_established
        if has_relay {
            node.subscribe_remote_query(query_id, collection_id.clone(), args.selection.clone(), cdata.clone(), 1, me.weak());
        }

        Ok(me)
    }
    pub fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    /// Wait for the LiveQuery to be fully initialized with initial states
    pub async fn wait_initialized(&self) {
        // If already initialized, return immediately
        if self.0.initialized_version.load(std::sync::atomic::Ordering::Relaxed)
            >= self.0.current_version.load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }

        // Otherwise wait for the notification
        self.0.initialized.notified().await;
    }

    pub fn update_selection(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        let new_selection = new_selection.try_into().map_err(|e| e.into())?;

        // Increment current_version atomically and get the new version number
        let new_version = self.0.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

        // Store new selection and version in pending_selection mutex
        *self.0.selection.lock().unwrap() = (new_selection.clone(), new_version);

        // Check if this node has a relay (ephemeral) or not (durable)
        let has_relay = self.0.node.has_subscription_relay();

        if has_relay {
            // Ephemeral node: delegate to relay, which will call update_selection_init after applying deltas
            self.0.node.update_remote_query(self.0.query_id, new_selection.clone(), new_version)?;
        } else {
            // Durable node: spawn task to call update_selection_init directly
            let me2 = self.clone();
            let query_id = self.0.query_id;

            crate::task::spawn(async move {
                if let Err(e) = me2.activate(new_version).await {
                    tracing::error!("LiveQuery update failed for predicate {}: {}", query_id, e);
                    me2.0.error.set(Some(e));
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
        self.wait_initialized().await;
        Ok(())
    }

    /// Activate the LiveQuery by fetching entities and calling reactor.add_query or reactor.update_query
    /// Called after deltas have been applied for both initial subscription and selection updates
    /// Gets all parameters from self.0 (collection_id, query_id, selection)
    /// Marks initialization as complete regardless of success/failure
    /// Rejects activation if the version is older than the current selection to prevent regression
    async fn activate(&self, version: u32) -> Result<(), RetrievalError> {
        // Get the current selection and its version
        let (selection, stored_version) = self.0.selection.lock().unwrap().clone();

        // Reject activation if this is an older version than what's currently stored
        // This prevents out-of-order activations from regressing the state
        if version < stored_version {
            warn!("LiveQuery - Dropped stale activation request for version {} (current version is {})", version, stored_version);
            return Ok(());
        }

        debug!("LiveQuery.activate() for predicate {} (version {})", self.0.query_id, version);

        let reactor = self.0.node.reactor();
        let initialized_version = self.0.initialized_version.load(std::sync::atomic::Ordering::Relaxed);

        // Determine if this is the first activation (query not yet in reactor)
        if initialized_version == 0 {
            // First activation ever: call reactor.add_query_and_notify which will populate the resultset
            // Pass self as pre_notify_hook to mark initialized before notification
            reactor
                .add_query_and_notify(
                    self.0.subscription.id(),
                    self.0.query_id,
                    self.0.collection_id.clone(),
                    selection,
                    &*self.0.node,
                    self.0.resultset.clone(),
                    self.0.gap_fetcher.clone(),
                    self,
                )
                .await?
        } else {
            // Subsequent activation (including cached re-initialization or selection update): use update_query_and_notify
            // This handles both: (1) cached queries re-activating after remote deltas, and (2) selection updates
            reactor
                .update_query_and_notify(
                    self.0.subscription.id(),
                    self.0.query_id,
                    self.0.collection_id.clone(),
                    selection,
                    &*self.0.node,
                    version,
                    self,
                )
                .await?;
        };

        Ok(())
    }
    pub fn error(&self) -> Read<Option<RetrievalError>> { self.0.error.read() }
    pub fn query_id(&self) -> proto::QueryId { self.0.query_id }

    /// Create a weak reference to this LiveQuery
    pub fn weak(&self) -> WeakEntityLiveQuery { WeakEntityLiveQuery(Arc::downgrade(&self.0)) }

    /// Mark initialization as complete for a given version
    pub fn mark_initialized(&self, version: u32) {
        // TASK: Serialize or coalesce concurrent activations to prevent version regression https://github.com/ankurah/ankurah/issues/146
        self.0.initialized_version.store(version, std::sync::atomic::Ordering::Relaxed);
        self.0.initialized.notify_waiters();
    }
}

impl Drop for Inner {
    fn drop(&mut self) { self.node.unsubscribe_remote_predicate(self.query_id); }
}

// Implement RemoteQuerySubscriber for WeakEntityLiveQuery to break circular dependencies
#[async_trait::async_trait]
impl crate::peer_subscription::RemoteQuerySubscriber for WeakEntityLiveQuery {
    async fn subscription_established(&self, version: u32) {
        // Try to upgrade the weak reference
        if let Some(livequery) = self.upgrade() {
            // Activate the query (fetch entities, call reactor, and mark initialized)
            // Handle errors internally by setting last_error
            if let Err(e) = livequery.activate(version).await {
                tracing::error!("Failed to activate subscription for query {}: {}", livequery.0.query_id, e);
                livequery.0.error.set(Some(e));
            }
        }
        // If upgrade fails, the LiveQuery was already dropped - nothing to do
    }

    fn set_last_error(&self, error: RetrievalError) {
        // Try to upgrade the weak reference
        if let Some(livequery) = self.upgrade() {
            tracing::info!("Setting last error for LiveQuery {}: {}", livequery.0.query_id, error);
            livequery.0.error.set(Some(error));
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
            // Convert ReactorUpdate to ChangeSet<R>");
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
