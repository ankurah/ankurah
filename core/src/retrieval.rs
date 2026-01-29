//! Implements GetEvents for NodeAndContext, allowing event retrieval from local and remote sources.
//! This lives in lineage because event retrieval is a lineage concern, not a context/session concern.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::{
    error::{InternalError, MutationError, NotFound, RetrievalError, StorageError},
    policy::PolicyAgent,
    storage::{StorageCollectionWrapper, StorageEngine},
    util::Iterable,
    Node,
};
use error_stack::Report;

/// Convert StorageError to RetrievalError
fn storage_to_retrieval(e: StorageError) -> RetrievalError {
    match e {
        StorageError::EntityNotFound(id) => RetrievalError::NotFound(NotFound::Entity(id)),
        StorageError::CollectionNotFound(id) => RetrievalError::NotFound(NotFound::Collection(id)),
        other => RetrievalError::Failure(Report::new(other).change_context(InternalError)),
    }
}

/// Convert StorageError to MutationError
fn storage_to_mutation(e: StorageError) -> MutationError {
    MutationError::Failure(Report::new(e).change_context(InternalError))
}

/// Convert RetrievalError to MutationError
fn retrieval_to_mutation(e: RetrievalError) -> MutationError {
    match e {
        RetrievalError::AccessDenied(ad) => MutationError::AccessDenied(ad),
        other => MutationError::Failure(Report::new(crate::error::AnyhowWrapper::from(format!("{}", other))).change_context(InternalError)),
    }
}

/// Convert RequestError to RetrievalError
fn request_to_retrieval(e: crate::error::RequestError) -> RetrievalError {
    match e {
        crate::error::RequestError::AccessDenied(ad) => RetrievalError::AccessDenied(ad),
        other => RetrievalError::Failure(Report::new(other).change_context(InternalError)),
    }
}
use ankurah_proto::{self as proto, Attested, Clock, EntityId, EntityState, Event, EventId};
use async_trait::async_trait;

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
    async fn retrieve_event(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError>;

    /// Stage events for immediate retrieval without storage. Used when applying EventBridge deltas.
    /// Staged events are available for lineage comparison at zero budget cost before being persisted.
    fn stage_events(&self, events: impl IntoIterator<Item = Attested<Self::Event>>);

    /// Mark an event as used. Used when applying EventBridge deltas.
    fn mark_event_used(&self, event_id: &Self::Id);
}

#[async_trait]
pub trait Retrieve: GetEvents {
    // Each implementation of Retrieve determines whether to use local or remote storage
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError>;
}

/// Durable node retriever - retrieves everything locally from storage
#[derive(Clone)]
pub struct LocalRetriever(Arc<LocalRetrieverInner>);
struct LocalRetrieverInner {
    collection: StorageCollectionWrapper,
    // Tuple is (event, was_used)
    staged_events: Mutex<Option<HashMap<EventId, (Attested<Event>, bool)>>>,
}

impl LocalRetriever {
    pub fn new(collection: StorageCollectionWrapper) -> Self {
        Self(Arc::new(LocalRetrieverInner { collection, staged_events: Mutex::new(Some(HashMap::new())) }))
    }

    pub async fn store_used_events(&mut self) -> Result<(), RetrievalError> {
        let staged = { self.0.staged_events.lock().unwrap().take() };

        if let Some(staged) = staged {
            for (_id, (event, used)) in staged.iter() {
                if *used {
                    self.0.collection.add_event(event).await.map_err(storage_to_retrieval)?;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl GetEvents for LocalRetriever {
    type Id = EventId;
    type Event = ankurah_proto::Event;

    async fn retrieve_event(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
        let mut events = Vec::with_capacity(event_ids.len());
        let mut event_ids: HashSet<Self::Id> = event_ids.into_iter().collect();

        // First check staged events (zero cost)
        {
            if let Some(staged) = self.0.staged_events.lock().unwrap().as_mut() {
                event_ids.retain(|id| {
                    if let Some((event, used)) = staged.get_mut(id) {
                        events.push(event.clone());
                        *used = true;
                        false
                    } else {
                        true
                    }
                });
            }
        }

        if event_ids.is_empty() {
            return Ok((0, events));
        }

        // staged events are free
        // cost for local retrieval is 1 per batch

        // Then retrieve from storage if needed
        let stored_events = self.0.collection.get_events(event_ids.into_iter().collect()).await.map_err(storage_to_retrieval)?;
        events.extend(stored_events);

        // TODO: push the consumption figure to the store, because its not necessarily the same for all stores
        Ok((1, events))
    }

    fn stage_events(&self, events: impl IntoIterator<Item = Attested<Self::Event>>) {
        let mut staged = self.0.staged_events.lock().unwrap();
        let staged = staged.get_or_insert_with(|| HashMap::new());

        for event in events.into_iter() {
            staged.insert(event.payload.id(), (event, false));
        }
    }

    fn mark_event_used(&self, event_id: &Self::Id) {
        let mut staged = self.0.staged_events.lock().unwrap();
        let staged = staged.get_or_insert_with(|| HashMap::new());
        staged.get_mut(event_id).map(|(_, used)| {
            *used = true;
        });
    }
}

#[async_trait]
impl Retrieve for LocalRetriever {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
        match self.0.collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(StorageError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(storage_to_retrieval(e)),
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
    // Tuple is (event, was_used)
    staged_events: Mutex<Option<HashMap<EventId, (Attested<Event>, bool)>>>,
}

impl<'a, SE, PA, C> EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    pub fn new(collection: proto::CollectionId, node: &'a Node<SE, PA>, cdata: &'a C) -> Self {
        Self { collection, node, cdata, staged_events: Mutex::new(Some(HashMap::new())) }
    }

    pub async fn store_used_events(&self) -> Result<(), MutationError> {
        let staged = { self.staged_events.lock().unwrap().take() };

        if let Some(staged) = staged {
            // For ephemeral nodes, storing events is optional
            // Only store if we actually want to persist them
            let collection = self.node.system.collection(&self.collection).await.map_err(retrieval_to_mutation)?;
            for (_id, (event, used)) in staged.iter() {
                if *used {
                    collection.add_event(event).await.map_err(storage_to_mutation)?;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<'a, SE, PA, C> GetEvents for EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    type Id = EventId;
    type Event = Event;

    async fn retrieve_event(&self, event_ids: Vec<Self::Id>) -> Result<(usize, Vec<Attested<Self::Event>>), RetrievalError> {
        let mut events = Vec::with_capacity(event_ids.len());
        let mut event_ids: HashSet<Self::Id> = event_ids.into_iter().collect();

        // First check staged events (zero cost)
        {
            if let Some(staged) = self.staged_events.lock().unwrap().as_mut() {
                event_ids.retain(|id| {
                    if let Some((event, used)) = staged.get_mut(id) {
                        events.push(event.clone());
                        *used = true;
                        false
                    } else {
                        true
                    }
                });
            }
        }

        if event_ids.is_empty() {
            return Ok((0, events));
        }

        // staged events are free
        // cost for local retrieval is 1 per batch
        // cost for remote retrieval is 5 per batch

        // Then try to get events from local storage
        let collection = self.node.system.collection(&self.collection).await?;
        // TODO update get_events to take &HashSet
        for event in collection.get_events(event_ids.iter().cloned().collect()).await.map_err(storage_to_retrieval)? {
            event_ids.remove(&event.payload.id());
            events.push(event);
        }

        if event_ids.is_empty() {
            return Ok((1, events));
        }

        // If we have missing events and a durable peer, try to fetch them
        let Some(peer_id) = self.node.get_durable_peer_random() else {
            return Ok((1, events)); // no durable peers - return what we have
        };

        match self
            .node
            .request(
                peer_id,
                self.cdata,
                proto::NodeRequestBody::GetEvents { collection: self.collection.clone(), event_ids: event_ids.into_iter().collect() }, // TODO update ::GetEvents to take HashSet
            )
            .await
            .map_err(request_to_retrieval)?
        {
            proto::NodeResponseBody::GetEvents(peer_events) => {
                for event in peer_events.iter() {
                    collection.add_event(event).await.map_err(storage_to_retrieval)?;
                }
                events.extend(peer_events);
            }
            proto::NodeResponseBody::Error(e) => {
                return Err(request_to_retrieval(crate::error::RequestError::ServerError(e)));
            }
            other => return Err(request_to_retrieval(crate::error::RequestError::UnexpectedResponse(other))),
        }
        Ok((5, events))
    }

    fn stage_events(&self, events: impl IntoIterator<Item = Attested<Self::Event>>) {
        let mut staged = self.staged_events.lock().unwrap();
        let staged = staged.get_or_insert_with(|| HashMap::new());

        for event in events.into_iter() {
            staged.insert(event.payload.id(), (event, false));
        }
    }

    fn mark_event_used(&self, event_id: &Self::Id) {
        let mut staged = self.staged_events.lock().unwrap();
        let staged = staged.get_or_insert_with(|| HashMap::new());
        staged.get_mut(event_id).map(|(_, used)| {
            *used = true;
        });
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
        let collection = self.node.collections.get(&self.collection).await.map_err(storage_to_retrieval)?;
        match collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(StorageError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(storage_to_retrieval(e)),
        }
    }
}
