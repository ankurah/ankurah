//! Implements GetEvents for NodeAndContext, allowing event retrieval from local and remote sources.
//! This lives in lineage because event retrieval is a lineage concern, not a context/session concern.

use crate::{
    error::RetrievalError,
    event_dag::{CausalNavigator, NavigationStep},
    policy::PolicyAgent,
    storage::{StorageCollectionWrapper, StorageEngine},
    util::Iterable,
    Node,
};
use ankurah_proto::{self as proto, Attested, Clock, EntityId, EntityState, Event, EventId};
use async_trait::async_trait;
use std::collections::BTreeSet;

#[async_trait]
pub trait Retrieve {
    // Each implementation of Retrieve determines whether to use local or remote storage
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError>;
}

/// Durable node retriever - retrieves everything locally from storage
pub struct LocalRetriever(StorageCollectionWrapper);

impl LocalRetriever {
    pub fn new(collection: StorageCollectionWrapper) -> Self { Self(collection) }
}

#[async_trait]
impl CausalNavigator for LocalRetriever {
    type EID = EventId;
    type Event = ankurah_proto::Event;

    async fn expand_frontier(
        &self,
        frontier_ids: &[Self::EID],
        _budget: usize,
    ) -> Result<NavigationStep<Self::EID, Self::Event>, RetrievalError> {
        // Fetch events for the given frontier IDs
        let events = self.0.get_events(frontier_ids.to_vec()).await?;
        let event_payloads = events.into_iter().map(|e| e.payload).collect();

        Ok(NavigationStep { events: event_payloads, assertions: Vec::new(), consumed_budget: 1 })
    }
}

#[async_trait]
impl Retrieve for LocalRetriever {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
        match self.0.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Ephemeral node retriever - retrieves events remotely, states locally, with multiple contexts for authentication
pub struct EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    pub collection: proto::CollectionId,
    pub node: &'a Node<SE, PA>,
    pub cdata: &'a C,
}

impl<'a, SE, PA, C> EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    pub fn new(collection: proto::CollectionId, node: &'a Node<SE, PA>, cdata: &'a C) -> Self { Self { collection, node, cdata } }
}

#[async_trait]
impl<'a, SE, PA, C> CausalNavigator for EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    type EID = EventId;
    type Event = Event;

    async fn expand_frontier(
        &self,
        frontier_ids: &[Self::EID],
        _budget: usize,
    ) -> Result<NavigationStep<Self::EID, Self::Event>, RetrievalError> {
        // Relations-first expansion could be applied here (future). For now, only fetch events.
        let event_ids = frontier_ids.to_vec();

        // First try to get events from local storage
        let collection = self.node.system.collection(&self.collection).await?;
        let mut events = collection.get_events(event_ids.clone()).await?;
        let mut cost = 1; // Cost for local retrieval

        // Check which IDs are missing from the returned events
        let missing_ids: Vec<_> = event_ids.into_iter().filter(|id| !events.iter().any(|e| e.payload.id() == *id)).collect();

        // If we have missing events and a durable peer, try to fetch them
        if !missing_ids.is_empty() {
            if let Some(peer_id) = self.node.get_durable_peer_random() {
                match self
                    .node
                    .request(
                        peer_id,
                        self.cdata,
                        proto::NodeRequestBody::GetEvents { collection: self.collection.clone(), event_ids: missing_ids },
                    )
                    .await
                    .map_err(|e| RetrievalError::StorageError(format!("Request failed: {}", e).into()))?
                {
                    proto::NodeResponseBody::GetEvents(peer_events) => {
                        for event in peer_events.iter() {
                            collection.add_event(event).await?;
                        }
                        events.extend(peer_events);
                        cost += 1; // Additional cost for remote retrieval
                    }
                    proto::NodeResponseBody::Error(e) => {
                        return Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into()));
                    }
                    _ => {
                        return Err(RetrievalError::StorageError("Unexpected response type from peer".into()));
                    }
                }
            }
        }

        let event_payloads = events.into_iter().map(|e| e.payload).collect();

        Ok(NavigationStep { events: event_payloads, assertions: Vec::new(), consumed_budget: cost })
    }
}

#[async_trait]
impl<'a, SE, PA, C> Retrieve for EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
        let collection = self.node.collections.get(&self.collection).await?;
        match collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
