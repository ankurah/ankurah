use std::{
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
};

use ankurah_proto::{self as proto, CollectionId};

use ankurah_signals::{
    broadcast::{BroadcastId, Listener, ListenerGuard},
    porcelain::subscribe::{IntoSubscribeListener, SubscriptionGuard},
    Get, Peek, Signal, Subscribe,
};
use tracing::{debug, warn};

use crate::{
    changes::ChangeSet,
    entity::Entity,
    error::RetrievalError,
    model::View,
    node::{MatchArgs, TNodeErased},
    policy::PolicyAgent,
    reactor::{ReactorSubscription, ReactorUpdate},
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
    pub(crate) has_error: AtomicBool,
    pub(crate) error: std::sync::Mutex<Option<RetrievalError>>,
    pub(crate) initialized: tokio::sync::Notify,
    pub(crate) is_initialized: std::sync::atomic::AtomicBool,
    // Version tracking for predicate updates
    pub(crate) current_version: std::sync::atomic::AtomicU32,
    // TODO: Is the selection pending in a version 0 case? I think it might be
    pub(crate) pending_selection: std::sync::Mutex<Option<(ankql::ast::Selection, u32)>>,
    // Store collection_id for selection updates
    pub(crate) collection_id: CollectionId,
}

#[derive(Clone)]
pub struct LiveQuery<R: View>(EntityLiveQuery, PhantomData<R>);

impl<R: View> std::ops::Deref for LiveQuery<R> {
    type Target = EntityLiveQuery;
    fn deref(&self) -> &Self::Target { &self.0 }
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

        let query_id = proto::QueryId::new();
        let rx = node.subscribe_remote_query(query_id, collection_id.clone(), args.selection.clone(), cdata, 0);

        let me = Self(Arc::new(Inner {
            query_id,
            node: Box::new(node.clone()),
            subscription,
            resultset: EntityResultSet::empty(),
            has_error: AtomicBool::new(false),
            error: std::sync::Mutex::new(None),
            initialized: tokio::sync::Notify::new(),
            is_initialized: std::sync::atomic::AtomicBool::new(false),
            current_version: std::sync::atomic::AtomicU32::new(0),
            // TODO: Even for version 0, predicate is "pending" until remote confirms (unless cached:true)
            pending_selection: std::sync::Mutex::new(None),
            collection_id: collection_id.clone(),
        }));

        let me2 = me.clone();
        let node = node.clone(); // initialize needs the typed node. Drop only needs the erased node.

        debug!("LiveQuery::new() spawning initialization task for predicate {}", query_id);
        crate::task::spawn(async move {
            debug!("LiveQuery initialization task starting for predicate {}", query_id);
            if let Err(e) = me2.initialize(node, collection_id, query_id, args, rx).await {
                debug!("LiveQuery initialization failed for predicate {}: {}", query_id, e);
                me2.0.has_error.store(true, std::sync::atomic::Ordering::Relaxed);
                *me2.0.error.lock().unwrap() = Some(e);
            } else {
                debug!("LiveQuery initialization completed for predicate {}", query_id);
            }

            // Signal that initialization is complete (success or failure)
            me2.0.is_initialized.store(true, std::sync::atomic::Ordering::Relaxed);
            me2.0.initialized.notify_waiters();
        });

        Ok(me)
    }
    pub fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    /// Wait for the LiveQuery to be fully initialized with initial states
    pub async fn wait_initialized(&self) {
        // If already initialized, return immediately
        if self.0.is_initialized.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }

        // Otherwise wait for the notification
        self.0.initialized.notified().await;
    }

    async fn initialize<SE, PA>(
        &self,
        node: Node<SE, PA>,
        collection_id: CollectionId,
        query_id: proto::QueryId,
        args: MatchArgs,
        rx: Option<tokio::sync::oneshot::Receiver<Vec<Entity>>>,
    ) -> Result<(), RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // Get initial entities from remote or local storage
        let initial_entities = match rx {
            Some(rx) if !args.cached => match rx.await {
                Ok(entities) => entities,
                Err(_) => {
                    warn!("Failed to receive first remote update for subscription {}", query_id);
                    node.fetch_entities_from_local(&collection_id, &args.selection).await?
                }
            },
            _ => node.fetch_entities_from_local(&collection_id, &args.selection).await?,
        };

        debug!(
            "LiveQuery.initialize() fetched {} initial entities for predicate {} with filter {:?}",
            initial_entities.len(),
            query_id,
            args.selection
        );

        debug!("LiveQuery.initialize() calling reactor.set_predicate with {} entities for predicate {}", initial_entities.len(), query_id);
        // Pre-populate the resultset with initial entities
        self.0.resultset.write().replace_all(initial_entities);
        node.reactor.add_query(
            self.0.subscription.id(),
            query_id,
            collection_id,
            args.selection,
            self.0.resultset.clone(),
            &crate::policy::DEFAULT_CONTEXT,
        )?;

        Ok(())
    }

    pub fn update_selection(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        let new_selection = new_selection.try_into().map_err(|e| e.into())?;

        // 1. Increment current_version atomically and get the new version number
        let new_version = self.0.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

        // 2. Set is_initialized to false (query results are stale until update completes)
        self.0.is_initialized.store(false, std::sync::atomic::Ordering::Relaxed);

        // 3. Store new selection and version in pending_predicate mutex
        *self.0.pending_selection.lock().unwrap() = Some((new_selection.clone(), new_version));

        // 4. Call node.update_remote_predicate (synchronous)
        let rx = self.0.node.update_remote_query(self.0.query_id, new_selection.clone(), new_version)?;

        // Spawn task to handle async update (similar to ::new pattern)
        let me2 = self.clone();
        let collection_id = self.0.collection_id.clone();
        let query_id = self.0.query_id;

        crate::task::spawn(async move {
            if let Err(e) = me2.update_selection_init(collection_id, new_selection, new_version, rx).await {
                tracing::error!("LiveQuery update failed for predicate {}: {}", query_id, e);
                me2.0.has_error.store(true, std::sync::atomic::Ordering::Relaxed);
                *me2.0.error.lock().unwrap() = Some(e);
            }

            // Signal that update is complete (success or failure)
            me2.0.is_initialized.store(true, std::sync::atomic::Ordering::Relaxed);
            me2.0.initialized.notify_waiters();
        });

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

    async fn update_selection_init(
        &self,
        collection_id: CollectionId,
        new_selection: ankql::ast::Selection,
        new_version: u32,
        rx: Option<tokio::sync::oneshot::Receiver<Vec<Entity>>>,
    ) -> Result<(), RetrievalError> {
        // Get entities from remote or local storage (follow initialize pattern)
        let included_entities = match rx {
            Some(rx) => match rx.await {
                Ok(entities) => entities,
                Err(_) => {
                    warn!("Failed to receive remote update for predicate {}, falling back to local", self.0.query_id);
                    self.0.node.fetch_entities_from_local(&collection_id, &new_selection).await?
                }
            },
            None => self.0.node.fetch_entities_from_local(&collection_id, &new_selection).await?,
        };

        // Call reactor.update_query via the trait
        self.0
            .node
            .reactor()
            .update_query(
                self.0.subscription.id(),
                self.0.query_id,
                collection_id,
                new_selection,
                included_entities,
                new_version,
                true, // emit_removes: true for local subscriptions
            )
            .map_err(|e| RetrievalError::storage(std::io::Error::other(e)))?;

        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) { self.node.unsubscribe_remote_predicate(self.query_id); }
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

// Implement Signal trait - delegate to the resultset
impl<R: View> Signal for LiveQuery<R> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0 .0.resultset.listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.0 .0.resultset.broadcast_id() }
}

// Implement Get trait - delegate to ResultSet<R>
impl<R: View + Clone + 'static> Get<Vec<R>> for LiveQuery<R> {
    fn get(&self) -> Vec<R> { self.0 .0.resultset.wrap::<R>().get() }
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
            tracing::info!("livequery_subscribe listener: got ReactorUpdate {reactor_update:?}");
            let changeset: ChangeSet<R> = livequery_change_set_from(me.0 .0.resultset.wrap::<R>(), reactor_update);
            tracing::info!("livequery_subscribe listener: converted to ChangeSet {changeset}");
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
