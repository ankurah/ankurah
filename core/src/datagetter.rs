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
    policy::{AccessDenied, PolicyAgent},
    storage::StorageEngine,
};

/// Read-only data broker that abstracts whether data comes from local storage,
/// remote peers, or a combination. Handles the complexity of local vs remote
/// data retrieval while keeping EntityManager agnostic.
#[async_trait]
pub trait DataGetter<C: ContextData>: Send + Sync {
    type EventGetter: GetEvents<Id = EventId, Event = Event> + Send + Sync;

    /// Gives the DataBroker a reference to the Node, which it can use to access the EntityManager and other resources
    /// This can be stored by your implementation of the DataBroker, or just ignore it if you don't need it
    fn bind_node<SE: StorageEngine, PA: PolicyAgent<ContextData = C> + Send + Sync + 'static>(&self, _node: &Node<SE, PA, Self>)
    where Self: Sized {
        // Default implementation does nothing
    }

    /// Create an event getter with the provided context data and collection
    fn event_getter(&self, cdata: C) -> Self::EventGetter;

    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<EventId>,
        cdata: &C,
    ) -> Result<(usize, HashMap<EventId, Attested<Event>>), RetrievalError>;

    async fn get_state(
        &self,
        collection_id: &CollectionId,
        entity_id: &EntityId,
        cdata: &C,
    ) -> Result<Attested<EntityState>, RetrievalError>;

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
pub struct LocalGetter<SE> {
    collections: CollectionSet<SE>,
}
impl<SE> Clone for LocalGetter<SE>
where SE: StorageEngine
{
    fn clone(&self) -> Self { Self { collections: self.collections.clone() } }
}

impl<SE: StorageEngine> LocalGetter<SE> {
    pub fn new(collections: CollectionSet<SE>) -> Self { Self { collections } }
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, C: ContextData> DataGetter<C> for LocalGetter<SE> {
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
    async fn get_state(
        &self,
        collection_id: &CollectionId,
        entity_id: &EntityId,
        cdata: &C,
    ) -> Result<Attested<EntityState>, RetrievalError> {
        let collection = self.collections.get(collection_id).await?;
        collection.get_state(entity_id).await
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
impl<SE: StorageEngine + Send + Sync + 'static> GetEvents for LocalGetter<SE> {
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
pub struct NetworkGetter<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> {
    collections: CollectionSet<SE>,
    node: OnceLock<WeakNode<SE, PA, Self>>,
}

impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NetworkGetter<SE, PA> {
    pub fn new(collections: CollectionSet<SE>) -> Self { Self { collections, node: OnceLock::new() } }

    pub fn node(&self) -> Result<Node<SE, PA, Self>, RetrievalError> {
        let weak_node = self.node.get().ok_or(RetrievalError::SanityError("Node not set in NetworkDataBroker"))?;
        let node = weak_node.upgrade().ok_or(RetrievalError::SanityError("Node has been dropped"))?;
        Ok(node)
    }
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> DataGetter<PA::ContextData>
    for NetworkGetter<SE, PA>
{
    type EventGetter = NetworkEventGetter<SE, PA>;

    fn bind_node<SE2: StorageEngine, PA2: PolicyAgent<ContextData = PA::ContextData> + Send + Sync + 'static>(
        &self,
        node: &Node<SE2, PA2, Self>,
    ) where
        Self: Sized,
    {
        self.node.set(node.weak()).expect("Failed to set node reference in NetworkDataBroker - already set");
    }

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

    async fn get_state(
        &self,
        collection_id: &CollectionId,
        entity_id: &EntityId,
        cdata: &PA::ContextData,
    ) -> Result<Attested<EntityState>, RetrievalError> {
        let states = self.get_states(collection_id, vec![entity_id.clone()], cdata).await?;
        if states.is_empty() {
            Err(RetrievalError::EntityNotFound(entity_id.clone()))
        } else {
            Ok(states[0].clone())
        }
    }

    async fn get_states(
        &self,
        collection_id: &CollectionId,
        mut entity_ids: Vec<EntityId>,
        cdata: &PA::ContextData,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
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
                    node.policy_agent.validate_received_state(&peer_id, &state)?;
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
                    node.policy_agent.validate_received_state(&peer_id, &state)?;
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
    broker: NetworkGetter<SE, PA>,
    cdata: PA::ContextData,
}

impl<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent + Send + Sync + 'static> NetworkEventGetter<SE, PA> {
    pub fn new(broker: NetworkGetter<SE, PA>, cdata: PA::ContextData) -> Self { Self { broker, cdata } }
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
