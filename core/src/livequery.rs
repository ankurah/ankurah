use std::{
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
};

use ankurah_proto::{self as proto, Attested, CollectionId, EntityState};

use ankurah_signals::{
    broadcast::{Broadcast, BroadcastId, Listener, ListenerGuard},
    porcelain::subscribe::{IntoSubscribeListener, SubscriptionGuard},
    Get, Peek, Signal, Subscribe,
};
use tracing::warn;

use crate::{
    changes::ChangeSet,
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
}

#[derive(Clone)]
pub struct LiveQuery<R: View>(EntityLiveQuery, PhantomData<R>);

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
        let predicate_id = proto::PredicateId::new();
        node.policy_agent.can_access_collection(&cdata, &collection_id)?;
        args.predicate = node.policy_agent.filter_predicate(&cdata, &collection_id, args.predicate)?;

        // Store subscription context for event requests
        node.predicate_context.insert(predicate_id, cdata.clone());

        // Create subscription container
        let subscription = node.reactor.subscribe();

        // Add the predicate to the subscription
        let resultset = subscription.add_predicate(predicate_id, &collection_id, args.predicate.clone())?;

        let me = Self(Arc::new(Inner {
            predicate_id,
            node: Box::new(node.clone()),
            peers: Vec::new(),
            subscription,
            resultset,
            has_error: AtomicBool::new(false),
            error: std::sync::Mutex::new(None),
        }));

        let me2 = me.clone();
        let node = node.clone();
        crate::task::spawn(async move {
            if let Err(e) = me2.initialize_query(node, collection_id, predicate_id, args, cdata).await {
                me2.0.has_error.store(true, std::sync::atomic::Ordering::Relaxed);
                *me2.0.error.lock().unwrap() = Some(e);
            }
        });

        Ok(me)
    }
    pub fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    async fn initialize_query<SE, PA>(
        &self,
        node: Node<SE, PA>,
        collection_id: CollectionId,
        predicate_id: proto::PredicateId,
        args: MatchArgs,
        cdata: PA::ContextData,
    ) -> Result<(), RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let storage_collection = node.collections.get(&collection_id).await?;
        let predicate = args.predicate;
        let cached = args.cached;

        let initial_states: Vec<Attested<EntityState>>;
        // Handle remote subscription setup
        if let Some(ref relay) = node.subscription_relay {
            relay.notify_subscribe(predicate_id, collection_id.clone(), predicate.clone(), cdata);

            if !cached {
                // Create oneshot channel to wait for first remote update
                let (tx, rx) = tokio::sync::oneshot::channel();
                node.pending_predicate_subs.insert(predicate_id, tx);

                // Wait for first remote update before initializing
                match rx.await {
                    Err(_) => {
                        // Channel was dropped, proceed with local initialization anyway
                        warn!("Failed to receive first remote update for subscription {}", predicate_id);
                        initial_states = storage_collection.fetch_states(&predicate).await?;
                    }
                    Ok(states) => initial_states = states,
                }
            } else {
                initial_states = storage_collection.fetch_states(&predicate).await?;
            }
        } else {
            initial_states = storage_collection.fetch_states(&predicate).await?;
        }

        // Convert states to entities
        let retriever = LocalRetriever::new(storage_collection.clone());
        let mut initial_entities = Vec::new();
        for state in initial_states {
            let (_, entity) =
                node.entities.with_state(&retriever, state.payload.entity_id, collection_id.clone(), state.payload.state).await?;
            initial_entities.push(entity);
        }

        node.reactor.initialize(self.0.subscription.id(), predicate_id, initial_entities)?;

        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Handle remote subscription cleanup
        // FIXME: Add relay.notify_unsubscribe, node.predicate_context.remove, node.pending_subs.remove
        warn!("LocalSubscription dropped - remote cleanup not yet implemented");
    }
}

// impl Signal for EntityLiveQuery {
//     fn listen(&self, listener: Listener) -> ListenerGuard { self.0.resultset.listen(listener) }

//     fn broadcast_id(&self) -> BroadcastId { self.0.resultset.broadcast_id() }
// }

impl<R: View> LiveQuery<R> {}

// impl<R: View> std::fmt::Debug for LiveQuery<R> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "LiveQuery({:?})", self.local_subscription.predicate_id)
//     }
// }

// Implement Signal trait - delegate to the resultset
impl<R: View> Signal for LiveQuery<R> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0 .0.resultset.listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.0 .0.resultset.broadcast_id() }
}

// Implement Get trait - delegate to ResultSet<R>
impl<R: View + Clone + 'static> Get<Vec<R>> for LiveQuery<R> {
    fn get(&self) -> Vec<R> { self.0 .0.resultset.map::<R>().get() }
}

// Implement Peek trait - delegate to ResultSet<R>
impl<R: View + Clone + 'static> Peek<Vec<R>> for LiveQuery<R> {
    fn peek(&self) -> Vec<R> { self.0 .0.resultset.map().peek() }
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
        let guard = self.0 .0.subscription.subscribe(move |reactor_update: ReactorUpdate| {
            // Convert ReactorUpdate to ChangeSet<R>
            let changeset: ChangeSet<R> = (me.0 .0.resultset.map::<R>(), reactor_update).into();
            listener(changeset);
        });

        guard
    }
}

impl<R: View + Clone> From<(ResultSet<R>, ReactorUpdate)> for ChangeSet<R> {
    fn from((resultset, reactor_update): (ResultSet<R>, ReactorUpdate)) -> Self {
        use crate::changes::{ChangeSet, ItemChange};

        let mut changes = Vec::new();

        for item in reactor_update.items {
            let view = R::from_entity(item.entity.clone());

            // Determine the change type based on predicate relevance
            if let Some((_, membership_change)) = item.predicate_relevance.first() {
                match membership_change {
                    crate::reactor::MembershipChange::Initial => {
                        changes.push(ItemChange::Initial { item: view.clone() });
                    }
                    crate::reactor::MembershipChange::Add => {
                        changes.push(ItemChange::Add { item: view.clone(), events: item.events });
                    }
                    crate::reactor::MembershipChange::Remove => {
                        changes.push(ItemChange::Remove { item: view.clone(), events: item.events });
                    }
                }
            } else {
                // No membership change, just an update
                changes.push(ItemChange::Update { item: view.clone(), events: item.events });
            }
        }

        ChangeSet { changes, resultset }
    }
}
