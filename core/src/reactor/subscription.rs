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
    /// Add or update a query for a remote subscriber (server-side):
    /// register if new, installing the caller's gap fetcher, then run
    /// the caller-fetched entities through the versioned update flow
    /// and fill gaps. Idempotent per query id. The reactor holds no
    /// node or credential access here: fetching current matches and
    /// wiring a fetcher to a session source are the caller's jobs.
    pub async fn upsert_query(
        &self,
        query_id: proto::QueryId,
        collection_id: proto::ModelId,
        selection: ankql::ast::Selection<Resolved>,
        included_entities: Vec<crate::entity::Entity>,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<crate::entity::Entity>>,
        version: u32,
    ) -> anyhow::Result<Vec<crate::entity::Entity>> {
        let subscription = self
            .0
            .reactor
            .subscription(self.0.subscription_id)
            .ok_or_else(|| anyhow::anyhow!("Subscription {:?} not found", self.0.subscription_id))?;

        // Register if new or get the existing resultset.
        let resultset = subscription.register_or_get_query(query_id, collection_id.clone(), gap_fetcher);

        // Update query - watcher management is handled internally
        let mut all_entities =
            subscription.update_query(query_id, collection_id.clone(), selection.clone(), included_entities, version, &mut ())?;

        // Fill gaps if needed for this specific query (also registers entity watchers)
        // FIXME: Same follow-up — we should confirm whether edit-driven gaps can occur between the
        // storage fetch and notify_change handling, which would make this gap fill mandatory.
        subscription.fill_gaps_for_query_entities(query_id, &mut all_entities).await;

        resultset.set_loaded(true);

        // Return all entities (newly added + gap-filled)
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
