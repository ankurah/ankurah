//! Implements GetEvents for NodeAndContext, allowing event retrieval from local and remote sources.
//! This lives in lineage because event retrieval is a lineage concern, not a context/session concern.

use crate::{
    context::NodeAndContext,
    error::RetrievalError,
    policy::PolicyAgent,
    storage::{StorageCollectionWrapper, StorageEngine},
};
use ankurah_proto::{self as proto, Attested, Clock, EntityId, EntityState, Event, EventId};
use async_trait::async_trait;

/// a trait for events and eventlike things that can be descended
pub trait TEvent {
    type Id: Eq + PartialEq + Clone;
    type Parent: TClock<Id = Self::Id>;

    fn id(&self) -> Self::Id;
    fn parent(&self) -> &Self::Parent;
}

pub trait TClock {
    type Id: Eq + PartialEq + Clone;
    fn members(&self) -> &[Self::Id];
}

impl TClock for Clock {
    type Id = EventId;
    fn members(&self) -> &[Self::Id] { self.as_slice() }
}

impl TEvent for ankurah_proto::Event {
    type Id = ankurah_proto::EventId;
    type Parent = Clock;

    fn id(&self) -> EventId { self.id() }
    fn parent(&self) -> &Clock { &self.parent }
}

#[async_trait]
pub trait GetEvents {
    type Id: Eq + PartialEq + Clone + std::fmt::Debug + Send + Sync;
    type Event: TEvent<Id = Self::Id>;

    /// Estimate the budget cost for retrieving a batch of events
    /// This allows different implementations to model their cost structure
    fn estimate_cost(&self, _batch_size: usize) -> usize {
        // Default implementation: fixed cost of 1 per batch
        1
    }

    /// retrieve the events from the store OR the remote peer
    async fn retrieve_event(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError>;
}

#[async_trait]
pub trait Retrieve: GetEvents {
    /// get the entity state from the local store, but do not try to retrieve it from the remote peer
    async fn get_local_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError>;
}

#[async_trait]
impl GetEvents for StorageCollectionWrapper {
    type Id = EventId;
    type Event = ankurah_proto::Event;

    async fn retrieve_event(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
        println!("StorageCollectionWrapper.GetEvents {:?}", event_ids);
        // TODO: push the consumption figure to the store, because its not necessarily the same for all stores
        Ok((1, self.0.get_events(event_ids).await?))
    }
}

#[async_trait]
impl Retrieve for StorageCollectionWrapper {
    async fn get_local_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
        match self.0.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl<SE, PA> GetEvents for (proto::CollectionId, &NodeAndContext<SE, PA>)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    type Id = EventId;
    type Event = Event;

    async fn retrieve_event(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
        // First try to get events from local storage
        let collection = self.1.node.system.collection(&self.0).await?;
        let mut events = collection.get_events(event_ids.clone()).await?;
        let mut cost = 1; // Cost for local retrieval

        // Check which IDs are missing from the returned events
        let missing_ids: Vec<_> = event_ids.into_iter().filter(|id| !events.iter().any(|e| e.payload.id() == *id)).collect();

        // If we have missing events and a durable peer, try to fetch them
        if !missing_ids.is_empty() {
            if let Some(peer_id) = self.1.node.get_durable_peer_random() {
                match self
                    .1
                    .node
                    .request(
                        peer_id,
                        &self.1.cdata,
                        proto::NodeRequestBody::GetEvents { collection: self.0.clone(), event_ids: missing_ids },
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

        Ok((cost, events))
    }
}

#[async_trait]
impl<SE, PA> Retrieve for (proto::CollectionId, &NodeAndContext<SE, PA>)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    async fn get_local_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
        let collection = self.1.node.collections.get(&self.0).await?;
        match collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
