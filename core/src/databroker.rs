use ankurah_proto::{Attested, CollectionId, EntityId, EntityState, Event, EventId, NodeRequestBody, NodeResponseBody};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use tracing::debug;

/// LEFT OFF HERE : Audit this stem to stern
use crate::{
    collectionset::CollectionSet,
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
    fn event_getter(&self, cdata: C) -> Self::EventGetter;

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<EventId>,
        cdata: &C,
    ) -> Result<(usize, HashMap<EventId, Attested<Event>>), RetrievalError>;

    /// Retrieve entity state
    async fn get_states(
        &self,
        collection_id: &CollectionId,
        entity_ids: Vec<EntityId>,
        cdata: &C,
        // allow_cache: bool,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

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
impl<SE: StorageEngine + Send + Sync + 'static, C: ContextData> DataBroker<C> for LocalDataBroker<SE> {
    type EventGetter = Self;

    fn event_getter(&self, _cdata: C) -> Self::EventGetter { self.clone() }

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<EventId>,
        _cdata: &C,
    ) -> Result<(usize, HashMap<EventId, Attested<Event>>), RetrievalError> {
        let collection = self.collections.get(collection_id).await?;
        let events = collection.get_events(event_ids).await?;
        Ok((1, events))
    }

    async fn get_states(
        &self,
        collection_id: &CollectionId,
        entity_ids: Vec<EntityId>,
        _cdata: &C,
        // _allow_cache: bool,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let collection = self.collections.get(collection_id).await?;
        collection.get_states(entity_ids).await
    }

    async fn fetch_states(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
        _cdata: &C,
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
        event_ids: Vec<Self::Id>,
    ) -> Result<(usize, HashMap<EventId, Attested<Self::Event>>), RetrievalError> {
        let collection = self.collections.get(collection_id).await?;
        let events = collection.get_events(event_ids.collect()).await?;
        Ok((1, events))
    }
}

/// DataBroker implementation for ephemeral nodes that access both local and remote storage
#[derive(Clone)]
pub struct NetworkDataBroker<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> {
    collections: CollectionSet<SE>,
    node: OnceLock<WeakNode<SE, PA, Self>>,
}

impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NetworkDataBroker<SE, PA> {
    pub fn new(collections: CollectionSet<SE>) -> Self { Self { collections, node: OnceLock::new() } }

    pub fn set_node(&self, node: &Node<SE, PA, Self>) -> Result<(), ()> { self.node.set(node.weak()).map_err(|_| ()) }

    pub fn node(&self) -> Result<Node<SE, PA, Self>, RetrievalError> {
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

    fn event_getter(&self, cdata: PA::ContextData) -> Self::EventGetter { NetworkEventGetter::new(self.clone(), cdata) }

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<EventId>,
        cdata: &PA::ContextData,
    ) -> Result<(usize, HashMap<EventId, Attested<Event>>), RetrievalError> {
        let cost = 1; // Cost for local retrieval

        // try to get the events from the local storage first
        let collection = self.collections.get(collection_id).await?;
        let mut events = collection.get_events(event_ids.clone()).await?;

        // collect the missing event IDs
        let missing_event_ids: Vec<EventId> = event_ids.iter().filter(|id| !events.contains_key(id)).cloned().collect();

        if missing_event_ids.is_empty() {
            Ok((cost, events))
        } else {
            let node = self.node()?;

            let peer_id = node.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

            match node
                .request(
                    peer_id,
                    cdata,
                    NodeRequestBody::GetEvents { collection: collection_id.clone(), event_ids: missing_event_ids.clone() },
                )
                .await
                .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
            {
                NodeResponseBody::GetEvents(peer_events) => {
                    // merge remote events into local events
                    for event in peer_events {
                        events.insert(event.payload.id(), event);
                    }

                    Ok((cost, events))
                }
                NodeResponseBody::Error(e) => Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into())),
                _ => Err(RetrievalError::StorageError("Unexpected response type from peer".into())),
            }
        }
    }

    async fn get_states(
        &self,
        collection_id: &CollectionId,
        mut entity_ids: Vec<EntityId>,
        cdata: &PA::ContextData,
        // allow_cache: bool,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        // let mut statemap: HashMap<EntityId, Attested<EntityState>> = HashMap::new();
        // if allow_cache {
        //     let collection = self.collections.get(collection_id).await?;

        //     for state in collection.get_states(entity_ids).await? {
        //         statemap.insert(state.payload.entity_id, state);
        //     }

        //     let missing_entity_ids: Vec<EntityId> = entity_ids.iter().filter(|id| !statemap.contains_key(id)).cloned().collect();

        //     if missing_entity_ids.is_empty() {
        //         return Ok(statemap.values().cloned().collect());
        //     }
        //     entity_ids = missing_entity_ids;

        //     // QUESTION: should we return it immediately and background the peer fetch?
        //     // If so, then we are doing a bit more than just retrieving, we're alsos applying that state to the collection
        //     // which might be a separation of concerns issue vs Node/EntityManager
        // }
        let node = self.node()?;
        let peer_id = node.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        println!("🔍 NetworkDataBroker::get_state: collection_id = {collection_id}, entity_ids = {entity_ids:?}");
        match node
            .request(peer_id, cdata, NodeRequestBody::Get { collection: collection_id.clone(), ids: entity_ids })
            .await
            .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
        {
            NodeResponseBody::Get(states) => {
                println!("🔍 Node::get_from_peer: states = {states:?}");
                let collection = self.collections.get(collection_id).await?;

                // do we have the ability to merge states?
                // because that's what we have to do I think
                for state in states {
                    node.policy_agent.validate_received_state(&node, &peer_id, &state)?;
                    // collection.set_state(state).await.map_err(|e| RetrievalError::Other(format!("{:?}", e)))?;
                    // statemap.insert(state.payload.entity_id, state);
                }
                Ok(states)
            }
            NodeResponseBody::Error(e) => {
                debug!("Error from peer fetch: {}", e);
                Err(RetrievalError::Other(format!("{:?}", e)))
            }
            _ => {
                debug!("Unexpected response type from peer get");
                Err(RetrievalError::Other("Unexpected response type".to_string()))
            }
        }
    }

    async fn fetch_states(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
        cdata: &PA::ContextData,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let node = self.node()?;
        let peer_id = node.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

        match node
            .request(peer_id, cdata, NodeRequestBody::Fetch { collection: collection_id.clone(), predicate: predicate.clone() })
            .await
            .map_err(|e| RetrievalError::Other(format!("{:?}", e)))?
        {
            NodeResponseBody::Fetch(states) => {
                let collection = self.collections.get(collection_id).await?;
                // do we have the ability to merge states?
                // because that's what we have to do I think
                for state in states {
                    node.policy_agent.validate_received_state(&node, &peer_id, &state)?;
                    // collection.set_state(state).await.map_err(|e| RetrievalError::Other(format!("{:?}", e)))?;
                }
                let states = collection.fetch_states(predicate).await?;
                Ok(states)
            }
            NodeResponseBody::Error(e) => {
                debug!("Error from peer fetch: {}", e);
                Err(RetrievalError::Other(format!("{:?}", e)))
            }
            _ => {
                debug!("Unexpected response type from peer fetch");
                Err(RetrievalError::Other("Unexpected response type".to_string()))
            }
        }
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
        event_ids: Vec<Self::Id>,
    ) -> Result<(usize, HashMap<EventId, Attested<Self::Event>>), RetrievalError> {
        // Just delegate to the broker
        self.broker.get_events(collection_id, event_ids.collect(), &self.cdata).await
    }
}
