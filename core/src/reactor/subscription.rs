use super::SubscriptionState;
use ankurah_proto::{self as proto, Attested};
use std::sync::Arc;
use ulid::Ulid;

/// Unique identifier for a reactor subscription
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReactorSubscriptionId(Ulid);

impl ReactorSubscriptionId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl std::fmt::Display for ReactorSubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "RS-{}", self.0) }
}

/// Inner state for ReactorSubscription
pub(crate) struct ReactorSubInner {
    state: Arc<SubscriptionState>,
    reactor: Box<dyn TReactor + Send + Sync>,
}

/// Trait for type-erased reactor functionality needed by ReactorSubscription
trait TReactor {
    fn manage_watchers_recurse(
        &self,
        collection_id: &proto::CollectionId,
        predicate: &ankql::ast::Predicate,
        sub_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
        op: super::WatcherOp,
    );
    //     fn unsubscribe(&self, sub_id: ReactorSubscriptionId);
    //     fn add_predicate_to_state(
    //         &self,
    //         state: &SubscriptionState,
    //         predicate_id: proto::PredicateId,
    //         collection_id: &proto::CollectionId,
    //         predicate: ankql::ast::Predicate,
    //     );
    //     fn initialize_predicate(
    //         &self,
    //         state: &SubscriptionState,
    //         predicate_id: proto::PredicateId,
    //         collection_id: &proto::CollectionId,
    //         initial_states: Vec<Attested<EntityState>>,
    //     ) -> impl std::future::Future<Output = Result<(), RetrievalError>> + Send;
}

impl<SE, PA> TReactor for super::Reactor<SE, PA> {
    fn manage_watchers_recurse(
        &self,
        collection_id: &proto::CollectionId,
        predicate: &ankql::ast::Predicate,
        sub_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
        op: super::WatcherOp,
    ) {
        self.manage_watchers_recurse(collection_id, predicate, sub_id, predicate_id, op);
    }
}
// where
//     SE: StorageEngine + Send + Sync + 'static,
//     PA: PolicyAgent + Send + Sync + 'static,
// {
//     fn unsubscribe(&self, sub_id: ReactorSubscriptionId) { self.unsubscribe(sub_id); }

//     fn add_predicate_to_state(
//         &self,
//         state: &SubscriptionState,
//         predicate_id: proto::PredicateId,
//         collection_id: &proto::CollectionId,
//         predicate: ast::Predicate,
//     ) {
//         state.add_predicate(self, predicate_id, collection_id, predicate);
//     }

//     fn initialize_predicate(
//         &self,
//         state: &SubscriptionState,
//         predicate_id: proto::PredicateId,
//         collection_id: &proto::CollectionId,
//         initial_states: Vec<Attested<EntityState>>,
//     ) -> impl std::future::Future<Output = Result<(), RetrievalError>> + Send {
//         state.initialize(self, predicate_id, collection_id, initial_states)
//     }
// }

impl Drop for ReactorSubInner {
    fn drop(&mut self) {
        // Automatically unsubscribe when the ReactorSubscription is dropped
        self.reactor.unsubscribe(self.state.id);
    }
}

/// A handle to a reactor subscription that automatically cleans up on drop
pub struct ReactorSubscription(Arc<ReactorSubInner>);

impl ReactorSubscription {
    pub fn new(state: SubscriptionState, reactor: Box<dyn TReactor + Send + Sync>) -> Self {
        Self(Arc::new(ReactorSubInner { state, reactor }))
    }

    /// Add a predicate to this subscription
    pub fn add_predicate(&self, predicate_id: proto::PredicateId, collection_id: &proto::CollectionId, predicate: ankql::ast::Predicate) {
        self.0.reactor.manage_watchers_recurse(collection_id, &predicate, self.id, predicate_id, WatcherOp::Add);

        // Add predicate to subscription
        self.predicates
            .lock()
            .unwrap()
            .insert(predicate_id, PredicateState { predicate, initialized: false, matching_entities: Vec::new() });

        // Update predicate mapping
        reactor.0.predicate_subscription_map.insert(predicate_id, self.id);
    }

    /// Remove a predicate from this subscription
    pub fn remove_predicate(&self, predicate_id: &proto::PredicateId) { self.0.state.remove_predicate(predicate_id); }

    /// Initialize a predicate with initial states
    pub async fn initialize(
        &self,
        predicate_id: proto::PredicateId,
        collection_id: &proto::CollectionId,
        initial_states: Vec<Attested<EntityState>>,
    ) -> Result<(), RetrievalError> {
        self.0.reactor.initialize_predicate(&self.0.state, predicate_id, collection_id, initial_states).await
    }
}

impl Clone for ReactorSubscription {
    fn clone(&self) -> Self { ReactorSubscription(self.0.clone()) }
}

// Implement Signal trait for ReactorSubscription
impl Signal for ReactorSubscription {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0.state.listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.0.state.broadcast_id() }
}

// Implement Subscribe trait for ReactorSubscription
impl Subscribe<ReactorUpdate> for ReactorSubscription {
    fn subscribe<F>(&self, listener: F) -> SignalGuard
    where F: ankurah_signals::porcelain::subscribe::IntoSubscribeListener<ReactorUpdate> {
        self.0.state.subscribe(listener)
    }
}
