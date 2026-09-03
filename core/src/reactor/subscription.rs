use crate::{
    error::SubscriptionError,
    reactor::{AbstractEntity, Reactor, ReactorUpdate},
    selection::filter::Filterable,
};
use ankql::ast::Resolved;

use ankurah_proto::{self as proto};
use ankurah_signals::{
    broadcast::Broadcast,
    porcelain::subscribe::{IntoSubscribeListener, Subscribe, SubscriptionGuard},
    signal::ListenerGuard,
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
pub(super) struct ReactorSubInner<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> {
    pub(super) subscription_id: ReactorSubscriptionId,
    pub(super) reactor: Reactor<E, Ev>,
    pub(super) broadcast: Broadcast<ReactorUpdate<E, Ev>>,
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Drop for ReactorSubInner<E, Ev> {
    fn drop(&mut self) {
        // Automatically unsubscribe when the ReactorSubscription is dropped
        let _ = self.reactor.unsubscribe(self.subscription_id);
    }
}

/// A handle to a reactor subscription that automatically cleans up on drop
pub struct ReactorSubscription<
    E: AbstractEntity + Filterable + Send + 'static = crate::entity::Entity,
    Ev: Clone + Send + 'static = ankurah_proto::Attested<ankurah_proto::Event>,
>(pub(super) Arc<ReactorSubInner<E, Ev>>);

// TODO Consider adding a weak ref and combining this with subscription_state::Subscription

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> ReactorSubscription<E, Ev> {
    /// Get the subscription ID
    pub fn id(&self) -> ReactorSubscriptionId { self.0.subscription_id }

    /// Remove a predicate from this subscription
    pub fn remove_predicate(&self, query_id: proto::QueryId) -> Result<(), SubscriptionError> {
        self.0.reactor.remove_query(self.0.subscription_id, query_id)?;
        Ok(())
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

// Entity-specific methods for remote subscriptions
impl ReactorSubscription<crate::entity::Entity, ankurah_proto::Attested<ankurah_proto::Event>> {
    /// Install or replace a remote subscriber's versioned query.
    pub async fn upsert_query(
        &self,
        query_id: proto::QueryId,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection<Resolved>,
        included_entities: Vec<crate::entity::Entity>,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<crate::entity::Entity>>,
        version: u32,
    ) -> anyhow::Result<Vec<crate::entity::Entity>> {
        let notify = self.0.reactor.0.notify_lock.clone().lock_owned().await;
        let subscription = self
            .0
            .reactor
            .subscription(self.0.subscription_id)
            .ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", self.0.subscription_id))?;

        let resultset = subscription.register_or_get_query(query_id, collection_id.clone(), gap_fetcher, version)?;

        let mut all_entities =
            subscription.update_query(query_id, collection_id.clone(), selection.clone(), included_entities, version, &mut ())?;

        let gap_fill = subscription.take_gap_fill(query_id)?;
        drop(notify);
        let (_notify, gap_entities) = subscription.finish_gap_fill(gap_fill).await;
        let gap_entities = gap_entities.ok_or_else(|| anyhow::anyhow!("query {query_id} changed while filling its initial result set"))?;
        all_entities.extend(gap_entities);

        resultset.set_loaded(true);

        Ok(all_entities)
    }
}

impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Clone for ReactorSubscription<E, Ev> {
    fn clone(&self) -> Self { ReactorSubscription(self.0.clone()) }
}

// Implement Subscribe trait for ReactorUpdate
impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Subscribe<ReactorUpdate<E, Ev>>
    for ReactorSubscription<E, Ev>
{
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<ReactorUpdate<E, Ev>> {
        let listener = listener.into_subscribe_listener();
        let guard = self.0.broadcast.reference().listen(listener);
        SubscriptionGuard::new(guard.into())
    }
}

// Implement Signal trait - Listener<()> is automatically converted to Listener::Unit
// This allows ReactorSubscription to be tracked by React observers without cloning ReactorUpdate
impl<E: AbstractEntity + Filterable + Send + 'static, Ev: Clone + Send + 'static> Signal for ReactorSubscription<E, Ev> {
    fn listen(&self, listener: ankurah_signals::signal::Listener) -> ListenerGuard {
        use ankurah_signals::broadcast::BroadcastListener;
        self.0.broadcast.reference().listen(BroadcastListener::NotifyOnly(Arc::new(move || listener(())))).into()
    }

    fn broadcast_id(&self) -> ankurah_signals::broadcast::BroadcastId { self.0.broadcast.id() }
}
