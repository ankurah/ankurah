use std::{
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
};

use ankql::ast::Predicate;
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
    retrieval::LocalRetriever,
    storage::StorageEngine,
    Node,
};

/// A local subscription that handles both reactor subscription and remote cleanup
/// This is a type-erased version that can be used in the TContext trait
#[derive(Clone)]
pub struct EntityLiveQuery(Arc<Inner>);
struct Inner {
    pub(crate) predicate_id: proto::PredicateId,
    pub(crate) node: Box<dyn TNodeErased>,
    pub(crate) peers: Vec<proto::EntityId>,
    // Store the actual subscription - now non-generic!
    pub(crate) subscription: ReactorSubscription,
    pub(crate) resultset: EntityResultSet,
    pub(crate) has_error: AtomicBool,
    pub(crate) error: std::sync::Mutex<Option<RetrievalError>>,
    pub(crate) initialized: tokio::sync::Notify,
    pub(crate) is_initialized: std::sync::atomic::AtomicBool,
    // Version tracking for predicate updates
    pub(crate) current_version: std::sync::atomic::AtomicU32,
    // TODO: Is the predicate pending in a version 0 case? I think it might be
    pub(crate) pending_predicate: std::sync::Mutex<Option<(ankql::ast::Predicate, u32)>>,
    // Store collection_id for predicate updates
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
        args.predicate = node.policy_agent.filter_predicate(&cdata, &collection_id, args.predicate)?;

        let subscription = node.reactor.subscribe();

        let predicate_id = proto::PredicateId::new();
        let rx = node.subscribe_remote_predicate(predicate_id, collection_id.clone(), args.predicate.clone(), cdata, 0);

        let me = Self(Arc::new(Inner {
            predicate_id,
            node: Box::new(node.clone()),
            peers: Vec::new(),
            subscription,
            resultset: EntityResultSet::empty(),
            has_error: AtomicBool::new(false),
            error: std::sync::Mutex::new(None),
            initialized: tokio::sync::Notify::new(),
            is_initialized: std::sync::atomic::AtomicBool::new(false),
            current_version: std::sync::atomic::AtomicU32::new(0),
            // TODO: Even for version 0, predicate is "pending" until remote confirms (unless cached:true)
            pending_predicate: std::sync::Mutex::new(None),
            collection_id: collection_id.clone(),
        }));

        let me2 = me.clone();
        let node = node.clone(); // initialize needs the typed node. Drop only needs the erased node.

        debug!("LiveQuery::new() spawning initialization task for predicate {}", predicate_id);
        crate::task::spawn(async move {
            debug!("LiveQuery initialization task starting for predicate {}", predicate_id);
            if let Err(e) = me2.initialize(node, collection_id, predicate_id, args, rx).await {
                debug!("LiveQuery initialization failed for predicate {}: {}", predicate_id, e);
                me2.0.has_error.store(true, std::sync::atomic::Ordering::Relaxed);
                *me2.0.error.lock().unwrap() = Some(e);
            } else {
                debug!("LiveQuery initialization completed for predicate {}", predicate_id);
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
        predicate_id: proto::PredicateId,
        args: MatchArgs,
        rx: Option<tokio::sync::oneshot::Receiver<Vec<Entity>>>,
    ) -> Result<(), RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let storage_collection = node.collections.get(&collection_id).await?;

        // Get initial entities from remote or local storage
        // TODO: This logic applies to both version 0 and updates:
        // - cached:false + rx present = wait for remote (predicate is pending)
        // - cached:true = use local immediately (predicate activates immediately)
        // - no rx (durable node) = use local (predicate activates immediately)
        let initial_entities = match rx {
            Some(rx) if !args.cached => match rx.await {
                Ok(entities) => entities,
                Err(_) => {
                    warn!("Failed to receive first remote update for subscription {}", predicate_id);
                    node.fetch_entities_from_local(&collection_id, &args.predicate).await?
                }
            },
            _ => node.fetch_entities_from_local(&collection_id, &args.predicate).await?,
        };

        debug!(
            "LiveQuery.initialize() fetched {} initial entities for predicate {} with filter {:?}",
            initial_entities.len(),
            predicate_id,
            args.predicate
        );

        debug!(
            "LiveQuery.initialize() calling reactor.set_predicate with {} entities for predicate {}",
            initial_entities.len(),
            predicate_id
        );
        // For initial subscription, use add_predicate (v0)
        node.reactor.add_predicate(
            self.0.subscription.id(),
            predicate_id,
            collection_id,
            args.predicate,
            self.0.resultset.clone(), // Pass the existing resultset
            initial_entities,
        )?;

        Ok(())
    }

    pub fn update_predicate(
        &self,
        new_predicate: impl TryInto<ankql::ast::Predicate, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        let new_predicate = new_predicate.try_into().map_err(|e| e.into())?;

        // 1. Increment current_version atomically and get the new version number
        let new_version = self.0.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

        // 2. Set is_initialized to false (query results are stale until update completes)
        self.0.is_initialized.store(false, std::sync::atomic::Ordering::Relaxed);

        // 3. Store new predicate and version in pending_predicate mutex
        *self.0.pending_predicate.lock().unwrap() = Some((new_predicate.clone(), new_version));

        // 4. Call node.update_remote_predicate (synchronous)
        tracing::info!("update_predicate calling node.update_remote_predicate for predicate {}", self.0.predicate_id);
        let rx = self.0.node.update_remote_predicate(self.0.predicate_id, self.0.collection_id.clone(), new_predicate.clone(), new_version);
        tracing::info!("update_predicate got rx: {:?}", rx.is_some());

        // Spawn task to handle async update (similar to ::new pattern)
        let me2 = self.clone();
        let collection_id = self.0.collection_id.clone();
        let predicate_id = self.0.predicate_id;

        tracing::info!("update_predicate spawning task for predicate {}", predicate_id);
        crate::task::spawn(async move {
            tracing::info!("LiveQuery update task starting for predicate {}", predicate_id);
            if let Err(e) = me2.update_predicate_init(collection_id, new_predicate, new_version, rx).await {
                tracing::info!("LiveQuery update failed for predicate {}: {}", predicate_id, e);
                me2.0.has_error.store(true, std::sync::atomic::Ordering::Relaxed);
                *me2.0.error.lock().unwrap() = Some(e);
            } else {
                tracing::info!("LiveQuery update completed for predicate {}", predicate_id);
            }

            // Signal that update is complete (success or failure)
            me2.0.is_initialized.store(true, std::sync::atomic::Ordering::Relaxed);
            me2.0.initialized.notify_waiters();
            tracing::info!("LiveQuery update task signaled completion for predicate {}", predicate_id);
        });

        tracing::info!("update_predicate returning for predicate {}", self.0.predicate_id);

        Ok(())
    }

    pub async fn update_predicate_wait(
        &self,
        new_predicate: impl TryInto<ankql::ast::Predicate, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        self.update_predicate(new_predicate)?;
        self.wait_initialized().await;
        Ok(())
    }

    async fn update_predicate_init(
        &self,
        collection_id: CollectionId,
        new_predicate: ankql::ast::Predicate,
        new_version: u32,
        rx: Option<tokio::sync::oneshot::Receiver<Vec<Entity>>>,
    ) -> Result<(), RetrievalError> {
        tracing::info!("update_predicate_init starting for predicate {}", self.0.predicate_id);

        let storage_collection = self.0.node.get_storage_collection(&collection_id).await?;
        tracing::info!("update_predicate_init got storage_collection");

        // Get entities from remote or local storage (follow initialize pattern)
        let included_entities = match rx {
            Some(rx) => {
                tracing::info!("update_predicate_init waiting for rx (ephemeral case)");
                match rx.await {
                    Ok(entities) => {
                        tracing::info!("update_predicate_init got {} entities from rx", entities.len());
                        entities
                    }
                    Err(_) => {
                        warn!("Failed to receive remote update for predicate {}, falling back to local", self.0.predicate_id);
                        self.0.node.fetch_entities_from_local_erased(&collection_id, &new_predicate).await?
                    }
                }
            }
            None => {
                tracing::info!("update_predicate_init fetching from local (durable case)");
                let entities = self.0.node.fetch_entities_from_local_erased(&collection_id, &new_predicate).await?;
                tracing::info!("update_predicate_init got {} entities from local", entities.len());
                entities
            }
        };

        tracing::info!(
            "update_predicate_init calling reactor.update_predicate with {} entities for predicate {} with filter {:?}",
            included_entities.len(),
            self.0.predicate_id,
            new_predicate
        );

        // Call reactor.update_predicate via the trait
        self.0
            .node
            .call_reactor_update_predicate(
                self.0.subscription.id(),
                self.0.predicate_id,
                collection_id,
                new_predicate,
                included_entities,
                new_version,
            )
            .map_err(|e| RetrievalError::storage(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        tracing::info!("update_predicate_init completed reactor call for predicate {}", self.0.predicate_id);

        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) { self.node.unsubscribe_remote_predicate(self.predicate_id); }
}

impl<R: View> LiveQuery<R> {
    /// Wait for the LiveQuery to be fully initialized with initial states
    pub async fn wait_initialized(&self) { self.0.wait_initialized().await; }

    pub fn resultset(&self) -> ResultSet<R> { self.0 .0.resultset.wrap::<R>() }

    pub fn loaded(&self) -> bool { self.0 .0.resultset.is_loaded() }

    pub fn ids(&self) -> Vec<proto::EntityId> { self.0 .0.resultset.keys().collect() }
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
            // Convert ReactorUpdate to ChangeSet<R>
            let changeset: ChangeSet<R> = livequery_change_set_from(me.0 .0.resultset.wrap::<R>(), reactor_update);
            listener(changeset);
        })
    }
}

/// Notably, this function does not filter by predicate_id, because it should only be used by LiveQuery, which entails a single-predicate subscription
fn livequery_change_set_from<R: View>(resultset: ResultSet<R>, reactor_update: ReactorUpdate) -> ChangeSet<R>
where R: View {
    use crate::changes::{ChangeSet, ItemChange};

    let mut changes = Vec::new();

    for item in reactor_update.items {
        let view = R::from_entity(item.entity);

        // Determine the change type based on predicate relevance
        // ignore the predicate_id, because it should only be used by LiveQuery, which entails a single-predicate subscription
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
