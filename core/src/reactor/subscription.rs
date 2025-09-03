use crate::{
    error::SubscriptionError,
    reactor::{AbstractEntity, Reactor, ReactorUpdate},
    resultset::EntityResultSet,
};

use ankurah_proto::{self as proto};
use ankurah_signals::{
    broadcast::Broadcast,
    porcelain::subscribe::{IntoSubscribeListener, Subscribe, SubscriptionGuard},
    Signal,
};
use std::sync::Arc;
use ulid::Ulid;

/// Unique identifier for a reactor subscription. This id is used only within a given reactor / node.
/// it cannot be transported across nodes. Predicate id and Entity id are used for that instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReactorSubscriptionId(Ulid);

impl Default for ReactorSubscriptionId {
    fn default() -> Self { Self::new() }
}

impl ReactorSubscriptionId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl std::fmt::Display for ReactorSubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "RS-{}", self.0) }
}

/// Inner state for ReactorSubscription
pub(super) struct ReactorSubInner<E: AbstractEntity, Ev> {
    pub(super) subscription_id: ReactorSubscriptionId,
    pub(super) reactor: Reactor<E, Ev>,
    pub(super) broadcast: Broadcast<ReactorUpdate<E, Ev>>,
}

impl<E: AbstractEntity, Ev> Drop for ReactorSubInner<E, Ev> {
    fn drop(&mut self) {
        // Automatically unsubscribe when the ReactorSubscription is dropped
        let _ = self.reactor.unsubscribe(self.subscription_id);
    }
}

/// A handle to a reactor subscription that automatically cleans up on drop
pub struct ReactorSubscription<E: AbstractEntity = crate::entity::Entity, Ev = ankurah_proto::Attested<ankurah_proto::Event>>(
    pub(super) Arc<ReactorSubInner<E, Ev>>,
);

impl<E: AbstractEntity, Ev: Clone> ReactorSubscription<E, Ev> {
    /// Get the subscription ID
    pub fn id(&self) -> ReactorSubscriptionId { self.0.subscription_id }

    /// Add a predicate to this subscription
    pub fn add_predicate(
        &self,
        predicate_id: proto::PredicateId,
        collection_id: &proto::CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> Result<EntityResultSet<E>, SubscriptionError> {
        self.0.reactor.add_predicate(self.0.subscription_id, predicate_id, collection_id, predicate)
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

impl<E: AbstractEntity, Ev> Clone for ReactorSubscription<E, Ev> {
    fn clone(&self) -> Self { ReactorSubscription(self.0.clone()) }
}

// Implement Subscribe trait for ReactorUpdate
impl<E: AbstractEntity + 'static, Ev: Clone + 'static> Subscribe<ReactorUpdate<E, Ev>> for ReactorSubscription<E, Ev> {
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<ReactorUpdate<E, Ev>> {
        let listener = listener.into_subscribe_listener();
        let guard: ankurah_signals::broadcast::ListenerGuard<ReactorUpdate<E, Ev>> = self.0.broadcast.reference().listen(listener);
        SubscriptionGuard::new(guard)
    }
}
