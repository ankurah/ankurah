use ankurah_proto::{Attested, CollectionId, EntityId, EntityState, Event, EventId};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

/// LEFT OFF HERE : Audit this stem to stern
use crate::{
    collectionset::CollectionSet,
    context::NodeAndContext,
    error::RetrievalError,
    lineage::GetEvents,
    node::{ContextData, Node, WeakNode},
    policy::PolicyAgent,
    storage::StorageEngine,
};

/// Read-only data broker that abstracts whether data comes from local storage,
/// remote peers, or a combination. Handles the complexity of local vs remote
/// data retrieval while keeping EntityManager agnostic.
#[async_trait]
pub trait DataBroker<C: ContextData>: Send + Sync {
    type EventGetter: GetEvents<Id = EventId, Event = Event> + Send + Sync;

    /// Create an event getter with the provided context data and collection
    fn event_getter(&self, cdata: &C, collection_id: &CollectionId) -> Result<Self::EventGetter, RetrievalError>;

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<EventId>,
        cdata: &C,
    ) -> Result<(usize, HashMap<EventId, Attested<Event>>), RetrievalError>;

    /// Retrieve entity state
    async fn get_state(
        &self,
        collection_id: &CollectionId,
        entity_id: EntityId,
        cdata: &C,
    ) -> Result<Attested<EntityState>, RetrievalError>;

    /// Fetch multiple entity states matching a predicate (used by subscriptions)
    async fn fetch_states(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
        cdata: &C,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError>;
}

/// DataBroker implementation for durable nodes that only access local storage
pub struct LocalDataBroker<SE> {
    collections: CollectionSet<SE>,
}
impl<SE> Clone for LocalDataBroker<SE>
where SE: StorageEngine
{
    fn clone(&self) -> Self { Self { collections: self.collections.clone() } }
}

impl<SE: StorageEngine> LocalDataBroker<SE> {
    pub fn new(collections: CollectionSet<SE>) -> Self { Self { collections } }
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, CD: ContextData> DataBroker<CD> for LocalDataBroker<SE> {
    type EventGetter = Self;

    fn event_getter(&self, _cdata: &CD, _collection_id: &CollectionId) -> Result<Self::EventGetter, RetrievalError> { Ok(self.clone()) }

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<EventId>,
        _cdata: &CD,
    ) -> Result<(usize, HashMap<EventId, Attested<Event>>), RetrievalError> {
        let collection = self.collections.get(collection_id).await?;
        let events = collection.get_events(event_ids).await?;
        Ok((1, events.into_iter().map(|event| (event.payload.id(), event)).collect()))
    }

    async fn get_state(
        &self,
        collection_id: &CollectionId,
        entity_id: EntityId,
        _cdata: &CD,
    ) -> Result<Attested<EntityState>, RetrievalError> {
        let collection = self.collections.get(collection_id).await?;
        collection.get_state(entity_id).await
    }

    async fn fetch_states(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
        _cdata: &CD,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let collection = self.collections.get(collection_id).await?;
        collection.fetch_states(predicate).await
    }
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static> GetEvents for LocalDataBroker<SE> {
    type Id = EventId;
    type Event = Event;

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: impl Iterator<Item = Self::Id> + Send,
    ) -> Result<(usize, HashMap<EventId, Attested<Self::Event>>), RetrievalError> {
        let collection = self.collections.get(collection_id).await?;
        let events = collection.get_events(event_ids.collect()).await?;
        Ok((1, events.into_iter().map(|event| (event.payload.id(), event)).collect()))
    }
}

/// DataBroker implementation for ephemeral nodes that access both local and remote storage
#[derive(Clone)]
pub struct NetworkDataBroker<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> {
    collections: CollectionSet<SE>,
    node: OnceLock<WeakNode<SE, PA>>,
}

impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NetworkDataBroker<SE, PA> {
    pub fn new(collections: CollectionSet<SE>) -> Self { Self { collections, node: OnceLock::new() } }

    pub fn set_node(&self, node: &Node<SE, PA>) -> Result<(), ()> { self.node.set(node.weak()).map_err(|_| ()) }

    pub fn node(&self) -> Result<Node<SE, PA>, RetrievalError> {
        let weak_node = self.node.get().ok_or(RetrievalError::SanityError("Node not set in NetworkDataBroker"))?;
        let node = weak_node.upgrade().ok_or(RetrievalError::SanityError("Node has been dropped"))?;
        Ok(node)
    }
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> DataBroker<PA::ContextData>
    for NetworkDataBroker<SE, PA>
{
    type EventGetter = NetworkEventGetter<SE, PA>;

    fn event_getter(&self, cdata: &PA::ContextData, _collection_id: &CollectionId) -> Result<Self::EventGetter, RetrievalError> {
        Ok(NetworkEventGetter::new(self.clone(), cdata.clone()))
    }

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<EventId>,
        cdata: &PA::ContextData,
    ) -> Result<(usize, HashMap<EventId, Attested<Event>>), RetrievalError> {
        let cost = 1; // Cost for local retrieval

        // try to get the events from the local storage first
        let collection = self.collections.get(collection_id).await?;
        let mut events: HashMap<EventId, Attested<Event>> =
            collection.get_events(event_ids.clone()).await?.into_iter().map(|event| (event.payload.id(), event)).collect();

        // collect the missing event IDs
        let missing_event_ids: Vec<EventId> = event_ids.iter().filter(|id| !events.contains_key(id)).cloned().collect();

        if missing_event_ids.is_empty() {
            Ok((cost, events))
        } else {
            let node = self.node()?;

            let peer_id = node.get_durable_peer_random().ok_or(RetrievalError::StorageError("No durable peer found".into()))?;

            match node
                .request(
                    peer_id,
                    cdata,
                    ankurah_proto::NodeRequestBody::GetEvents { collection: collection_id.clone(), event_ids: missing_event_ids.clone() },
                )
                .await
                .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
            {
                ankurah_proto::NodeResponseBody::GetEvents(peer_events) => {
                    // merge remote events into local events
                    for event in peer_events {
                        events.insert(event.payload.id(), event);
                    }

                    // build result vector in the requested order, validating each event exists
                    let mut result_events = HashMap::with_capacity(event_ids.len());
                    for event_id in event_ids {
                        match events.remove(&event_id) {
                            Some(event) => result_events.insert(event_id, event),
                            None => return Err(RetrievalError::StorageError(format!("Failed to retrieve event: {:?}", event_id).into())),
                        }
                    }

                    Ok((cost, result_events))
                }
                ankurah_proto::NodeResponseBody::Error(e) => Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into())),
                _ => Err(RetrievalError::StorageError("Unexpected response type from peer".into())),
            }
        }
    }

    async fn get_state(
        &self,
        collection_id: &CollectionId,
        entity_id: EntityId,
        _cdata: &PA::ContextData,
    ) -> Result<Attested<EntityState>, RetrievalError> {
        // Try local first, then remote if available
        let collection = self.collections.get(collection_id).await?;

        match collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => {
                // TODO: Try to fetch from remote peers via the WeakNode
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    async fn fetch_states(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
        _cdata: &PA::ContextData,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        todo!()
    }
}

/// Event getter for network access - thin wrapper that delegates to NetworkDataBroker
pub struct NetworkEventGetter<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> {
    broker: NetworkDataBroker<SE, PA>,
    cdata: PA::ContextData,
}

impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NetworkEventGetter<SE, PA> {
    pub fn new(broker: NetworkDataBroker<SE, PA>, cdata: PA::ContextData) -> Self { Self { broker, cdata } }
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> GetEvents for NetworkEventGetter<SE, PA> {
    type Id = EventId;
    type Event = Event;

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: impl Iterator<Item = Self::Id> + Send,
    ) -> Result<(usize, HashMap<EventId, Attested<Self::Event>>), RetrievalError> {
        // Just delegate to the broker
        self.broker.get_events(collection_id, event_ids.collect(), &self.cdata).await
    }
}
