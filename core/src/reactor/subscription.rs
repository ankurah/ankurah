use super::SubscriptionState;
use ankurah_proto::{self as proto, Attested};
use std::sync::Arc;
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
pub(crate) struct ReactorSubInner {
    subscription_id: ReactorSubscriptionId,
    collection_id: proto::CollectionId,
    reactor: Box<dyn TReactor + Send + Sync>,
}

/// Trait for type-erased reactor functionality needed by ReactorSubscription
trait TReactor {
    fn unsubscribe(&self, sub_id: ReactorSubscriptionId);
    fn add_predicate(
        &self,
        subscription_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
        collection_id: &proto::CollectionId,
        predicate: ankql::ast::Predicate,
    );
    fn remove_predicate(&self, subscription_id: ReactorSubscriptionId, predicate_id: proto::PredicateId);
    fn add_entity_subscriptions(&self, subscription_id: ReactorSubscriptionId, entity_ids: Vec<proto::EntityId>);
    fn remove_entity_subscriptions(&self, subscription_id: ReactorSubscriptionId, entity_ids: Vec<proto::EntityId>);
    // TODO: Add initialize method when it's moved to Reactor
}

impl<SE, PA> TReactor for super::Reactor<SE, PA>
where
    SE: super::StorageEngine + Send + Sync + 'static,
    PA: super::PolicyAgent + Send + Sync + 'static,
{
    fn unsubscribe(&self, sub_id: ReactorSubscriptionId) { self.unsubscribe(sub_id); }

    fn add_predicate(
        &self,
        subscription_id: ReactorSubscriptionId,
        predicate_id: proto::PredicateId,
        collection_id: &proto::CollectionId,
        predicate: ankql::ast::Predicate,
    ) {
        self.add_predicate(subscription_id, predicate_id, collection_id, predicate);
    }

    fn remove_predicate(&self, subscription_id: ReactorSubscriptionId, predicate_id: proto::PredicateId) {
        self.remove_predicate(subscription_id, predicate_id);
    }

    fn add_entity_subscriptions(&self, subscription_id: ReactorSubscriptionId, entity_ids: Vec<proto::EntityId>) {
        self.add_entity_subscriptions(subscription_id, entity_ids);
    }

    fn remove_entity_subscriptions(&self, subscription_id: ReactorSubscriptionId, entity_ids: Vec<proto::EntityId>) {
        self.remove_entity_subscriptions(subscription_id, entity_ids);
    }
}

impl Drop for ReactorSubInner {
    fn drop(&mut self) {
        // Automatically unsubscribe when the ReactorSubscription is dropped
        self.reactor.unsubscribe(self.subscription_id);
    }
}

/// A handle to a reactor subscription that automatically cleans up on drop
pub struct ReactorSubscription(Arc<ReactorSubInner>);

impl ReactorSubscription {
    pub fn new(subscription_id: ReactorSubscriptionId, reactor: Box<dyn TReactor + Send + Sync>) -> Self {
        Self(Arc::new(ReactorSubInner { subscription_id, reactor }))
    }

    /// Get the subscription ID
    pub fn id(&self) -> ReactorSubscriptionId { self.0.subscription_id }

    pub fn collection_id(&self) -> proto::CollectionId { self.0.collection_id }

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

impl Clone for ReactorSubscription {
    fn clone(&self) -> Self { ReactorSubscription(self.0.clone()) }
}

// TODO: Re-implement Signal traits when signals are integrated with the new architecture
