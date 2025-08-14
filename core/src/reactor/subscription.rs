use crate::reactor::{Reactor, ReactorEntity, ReactorUpdate};

use ankurah_proto::{self as proto};
use ankurah_signals::{
    broadcast::{Broadcast, BroadcastId, Listener, ListenerGuard},
    porcelain::subscribe::{IntoSubscribeListener, Subscribe, SubscriptionGuard},
    Signal,
};
use std::sync::{Arc, Mutex};
use ulid::Ulid;

/// Unique identifier for a reactor subscription. This id is used only within a given reactor / node.
/// it cannot be transported across nodes. Predicate id and Entity id are used for that instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReactorSubscriptionId(Ulid);

impl ReactorSubscriptionId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl std::fmt::Display for ReactorSubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "RS-{}", self.0) }
}

/// Inner state for ReactorSubscription
pub(crate) struct ReactorSubInner<E: ReactorEntity> {
    subscription_id: ReactorSubscriptionId,
    reactor: Reactor<E>,
    broadcast: Broadcast<ReactorUpdate<E, ()>>, // For now, assume no events in updates
}

impl<E: ReactorEntity> Drop for ReactorSubInner<E> {
    fn drop(&mut self) {
        // Automatically unsubscribe when the ReactorSubscription is dropped
        let _ = self.reactor.unsubscribe(self.subscription_id);
    }
}

/// A handle to a reactor subscription that automatically cleans up on drop
pub struct ReactorSubscription<E: ReactorEntity = crate::entity::Entity>(Arc<ReactorSubInner<E>>);

impl<E: ReactorEntity> ReactorSubscription<E> {
    pub fn new(subscription_id: ReactorSubscriptionId, reactor: Reactor<E>) -> Self {
        Self(Arc::new(ReactorSubInner { subscription_id, reactor, broadcast: Broadcast::new() }))
    }

    /// Get the subscription ID
    pub fn id(&self) -> ReactorSubscriptionId { self.0.subscription_id }

    /// Add a predicate to this subscription
    pub fn add_predicate(&self, predicate_id: proto::PredicateId, collection_id: &proto::CollectionId, predicate: ankql::ast::Predicate) {
        self.0.reactor.add_predicate(self.0.subscription_id, predicate_id, collection_id, predicate);
    }

    /// Remove a predicate from this subscription
    pub fn remove_predicate(&self, predicate_id: proto::PredicateId) {
        self.0.reactor.remove_predicate(self.0.subscription_id, predicate_id);
    }

    /// Add entity subscriptions
    pub fn add_entity_subscriptions(&self, entity_ids: impl IntoIterator<Item = proto::EntityId>) {
        let entity_ids: Vec<_> = entity_ids.into_iter().collect();
        self.0.reactor.add_entity_subscriptions(self.0.subscription_id, entity_ids);
    }

    /// Remove entity subscriptions
    pub fn remove_entity_subscriptions(&self, entity_ids: impl IntoIterator<Item = proto::EntityId>) {
        let entity_ids: Vec<_> = entity_ids.into_iter().collect();
        self.0.reactor.remove_entity_subscriptions(self.0.subscription_id, entity_ids);
    }
}

impl<E: ReactorEntity> Clone for ReactorSubscription<E> {
    fn clone(&self) -> Self { ReactorSubscription(self.0.clone()) }
}

// Implement Signal trait
impl<E: ReactorEntity> Signal for ReactorSubscription<E> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0.broadcast.listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.0.broadcast.broadcast_id() }
}

// Implement Subscribe trait
impl<E: ReactorEntity> Subscribe<ReactorUpdate<E, ()>> for ReactorSubscription<E> {
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<ReactorUpdate<E, ()>> {
        let listener = listener.into_subscribe_listener();
        let guard = self.0.broadcast.listen(Box::new(move |update| {
            listener(update);
        }));
        SubscriptionGuard::new(guard)
    }
}
