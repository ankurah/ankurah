//! Traits and impls for getting state and event data for specific IDs
//! It's generic so we can use it for unit tests as well as the real implementation

use crate::{
    context::NodeAndContext,
    entity::Entity,
    error::RetrievalError,
    policy::PolicyAgent,
    storage::{StorageCollectionWrapper, StorageEngine},
};
use ankurah_proto::{self as proto, Attested, Clock, CollectionId, EntityId, EntityState, Event, EventId};
use async_trait::async_trait;

#[async_trait]
pub trait GetEvents {
    type Id: Eq + PartialEq + Clone + std::fmt::Debug + Send + Sync;
    type Event: TEvent<Id = Self::Id> + std::fmt::Display;

    /// Estimate the budget cost for retrieving a batch of events
    /// This allows different implementations to model their cost structure
    fn estimate_cost(&self, _batch_size: usize) -> usize {
        // Default implementation: fixed cost of 1 per batch
        1
    }

    /// retrieve the events from the store OR the remote peer
    async fn get_events(
        &self,
        collection_id: CollectionId,
        event_ids: Vec<Self::Id>,
    ) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError>;
}

// TODO: make this generic
#[async_trait]
pub trait GetState {
    // Each implementation of Retrieve determines whether to use local or remote storage
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError>;

    /// Estimate the budget cost for retrieving a batch of events
    /// This allows different implementations to model their cost structure
    fn estimate_cost(&self, _batch_size: usize) -> usize {
        // Default implementation: fixed cost of 1 per batch
        1
    }
}

#[async_trait]
pub trait GetEntities {
    async fn get_entities(&self, entity_ids: Vec<EntityId>) -> Result<Vec<Entity>, RetrievalError>;
}

/// a trait for events and eventlike things that can be descended
pub trait TEvent: std::fmt::Display {
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

// LocalGetter either goes away, or becomes a stub. Interfaces using Option<GetEvents + GetState> would be better I think?
// pub struct LocalGetter(StorageCollectionWrapper);

// impl LocalGetter {
//     pub fn new(collection: StorageCollectionWrapper) -> Self { Self(collection) }
// }

// #[async_trait]
// impl GetEvents for LocalGetter {
//     type Id = EventId;
//     type Event = ankurah_proto::Event;

//     async fn get_events(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
//         println!("StorageCollectionWrapper.GetEvents {:?}", event_ids);
//         // TODO: push the consumption figure to the store, because its not necessarily the same for all stores
//         Ok((1, self.0.get_events(event_ids).await?))
//     }
// }

// #[async_trait]
// impl GetState for LocalGetter {
//     async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
//         match self.0.get_state(entity_id).await {
//             Ok(state) => Ok(Some(state)),
//             Err(RetrievalError::EntityNotFound(_)) => Ok(None),
//             Err(e) => Err(e),
//         }
//     }
// }

pub struct DurablePeerGetter<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> {
    nodeandcontext: NodeAndContext<SE, PA>,
}

impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> DurablePeerGetter<SE, PA> {
    pub fn new(nodeandcontext: NodeAndContext<SE, PA>) -> Self { Self { nodeandcontext } }
}

#[async_trait]
impl<SE, PA> GetEvents for DurablePeerGetter<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    type Id = EventId;
    type Event = Event;

    async fn get_events(
        &self,
        collection_id: CollectionId,
        event_ids: Vec<Self::Id>,
    ) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
        let cost = 1; // Cost for local retrieval

        let peer_id =
            self.nodeandcontext.node.get_durable_peer_random().ok_or(RetrievalError::StorageError("No durable peer found".into()))?;

        match self
            .nodeandcontext
            .node
            .request(peer_id, &self.nodeandcontext.cdata, proto::NodeRequestBody::GetEvents { collection: collection_id, event_ids })
            .await?
        {
            proto::NodeResponseBody::GetEvents(peer_events) => Ok((cost, peer_events)),
            proto::NodeResponseBody::Error(e) => {
                return Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into()));
            }
            _ => {
                return Err(RetrievalError::StorageError("Unexpected response type from peer".into()));
            }
        }
    }
}

#[async_trait]
impl<SE, PA> GetState for (proto::CollectionId, &NodeAndContext<SE, PA>)
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    // this used to be called get_local_state
    // and for some reason we have a use case that wants local or remote event fetching, but not the same for state

    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
        let collection = self.1.node.collections.get(&self.0).await?;
        match collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
