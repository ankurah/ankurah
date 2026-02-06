//! Implements GetEvents for NodeAndContext, allowing event retrieval from local and remote sources.
//! This lives in lineage because event retrieval is a lineage concern, not a context/session concern.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::{
    error::{MutationError, RetrievalError},
    event_dag::{CausalNavigator, NavigationStep},
    policy::PolicyAgent,
    storage::{StorageCollectionWrapper, StorageEngine},
    util::Iterable,
    Node,
};
use ankurah_proto::{self as proto, Attested, EntityId, EntityState, Event, EventId};
use async_trait::async_trait;

#[async_trait]
pub trait Retrieve {
    // Each implementation of Retrieve determines whether to use local or remote storage
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError>;

    /// Retrieve a single event by ID from storage.
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError>;

    /// Check whether an event exists in storage without fetching the full body.
    /// Default implementation delegates to get_event, but implementors should
    /// override with a more efficient storage-level check when possible.
    async fn event_exists(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        match self.get_event(event_id).await {
            Ok(_) => Ok(true),
            Err(RetrievalError::EventNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// Blanket implementation for shared references - enables passing `&retriever` to functions
/// that take `R: Retrieve`.
#[async_trait]
impl<R: Retrieve + Send + Sync + ?Sized> Retrieve for &R {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<EntityState>>, RetrievalError> {
        (*self).get_state(entity_id).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        (*self).get_event(event_id).await
    }

    async fn event_exists(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        (*self).event_exists(event_id).await
    }
}

/// Trait for staging events before they're stored.
/// This allows events in an EventBridge to reference each other during application.
pub trait EventStaging {
    /// Stage events for potential storage (makes them available for retrieval)
    fn stage_events(&self, events: Vec<Attested<Event>>);

    /// Mark an event as actually used (for staged event tracking)
    fn mark_event_used(&self, event_id: &EventId);
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
                    self.0.collection.add_event(event).await?;
                }
            }
        }

        Ok(())
    }
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
        let events = self.0.collection.get_events(frontier_ids.to_vec()).await?;
        let event_payloads = events.into_iter().map(|e| e.payload).collect();

        Ok(NavigationStep { events: event_payloads, assertions: Vec::new(), consumed_budget: 1 })
    }
}

impl LocalRetriever {
    pub fn stage_events(&self, events: impl IntoIterator<Item = Attested<Event>>) {
        let mut staged = self.0.staged_events.lock().unwrap();
        let staged = staged.get_or_insert_with(|| HashMap::new());

        for event in events.into_iter() {
            staged.insert(event.payload.id(), (event, false));
        }
    }

    pub fn mark_event_used(&self, event_id: &EventId) {
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
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        let events = self.0.collection.get_events(vec![event_id.clone()]).await?;
        events.into_iter().next().map(|e| e.payload).ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }
}

impl EventStaging for LocalRetriever {
    fn stage_events(&self, events: Vec<Attested<Event>>) { LocalRetriever::stage_events(self, events) }

    fn mark_event_used(&self, event_id: &EventId) { LocalRetriever::mark_event_used(self, event_id) }
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
            let collection = self.node.system.collection(&self.collection).await?;
            for (_id, (event, used)) in staged.iter() {
                if *used {
                    collection.add_event(event).await?;
                }
            }
        }

        Ok(())
    }
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
        let mut events: Vec<Attested<Event>> = Vec::with_capacity(frontier_ids.len());
        let mut event_ids: HashSet<EventId> = frontier_ids.iter().cloned().collect();
        let mut cost = 0;

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
            let event_payloads = events.into_iter().map(|e| e.payload).collect();
            return Ok(NavigationStep { events: event_payloads, assertions: Vec::new(), consumed_budget: cost });
        }

        // Then try to get events from local storage
        let collection = self.node.system.collection(&self.collection).await?;
        for event in collection.get_events(event_ids.iter().cloned().collect()).await? {
            event_ids.remove(&event.payload.id());
            events.push(event);
        }
        cost = 1; // Cost for local retrieval

        if event_ids.is_empty() {
            let event_payloads = events.into_iter().map(|e| e.payload).collect();
            return Ok(NavigationStep { events: event_payloads, assertions: Vec::new(), consumed_budget: cost });
        }

        // If we have missing events and a durable peer, try to fetch them
        let Some(peer_id) = self.node.get_durable_peer_random() else {
            let event_payloads = events.into_iter().map(|e| e.payload).collect();
            return Ok(NavigationStep { events: event_payloads, assertions: Vec::new(), consumed_budget: cost });
        };

        match self
            .node
            .request(
                peer_id,
                self.cdata,
                proto::NodeRequestBody::GetEvents { collection: self.collection.clone(), event_ids: event_ids.into_iter().collect() },
            )
            .await?
        {
            proto::NodeResponseBody::GetEvents(peer_events) => {
                for event in peer_events.iter() {
                    collection.add_event(event).await?;
                }
                events.extend(peer_events);
                cost = 5; // Cost for remote retrieval
            }
            proto::NodeResponseBody::Error(e) => {
                return Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into()));
            }
            _ => return Err(RetrievalError::StorageError("Unexpected response type from peer".into())),
        }

        let event_payloads = events.into_iter().map(|e| e.payload).collect();
        Ok(NavigationStep { events: event_payloads, assertions: Vec::new(), consumed_budget: cost })
    }
}

impl<'a, SE, PA, C> EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    pub fn stage_events(&self, events: impl IntoIterator<Item = Attested<Event>>) {
        let mut staged = self.staged_events.lock().unwrap();
        let staged = staged.get_or_insert_with(|| HashMap::new());

        for event in events.into_iter() {
            staged.insert(event.payload.id(), (event, false));
        }
    }

    pub fn mark_event_used(&self, event_id: &EventId) {
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
        let collection = self.node.collections.get(&self.collection).await?;
        match collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        // Try local storage
        let collection = self.node.collections.get(&self.collection).await?;
        let events = collection.get_events(vec![event_id.clone()]).await?;
        if let Some(event) = events.into_iter().next() {
            return Ok(event.payload);
        }

        // Try remote peer
        let Some(peer_id) = self.node.get_durable_peer_random() else {
            return Err(RetrievalError::EventNotFound(event_id.clone()));
        };

        match self
            .node
            .request(
                peer_id,
                self.cdata,
                proto::NodeRequestBody::GetEvents {
                    collection: self.collection.clone(),
                    event_ids: vec![event_id.clone()],
                },
            )
            .await?
        {
            proto::NodeResponseBody::GetEvents(peer_events) => {
                // Store locally for future access
                for event in peer_events.iter() {
                    collection.add_event(event).await?;
                }
                peer_events
                    .into_iter()
                    .next()
                    .map(|e| e.payload)
                    .ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
            }
            proto::NodeResponseBody::Error(e) => {
                Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into()))
            }
            _ => Err(RetrievalError::StorageError("Unexpected response type from peer".into())),
        }
    }
}

impl<'a, SE, PA, C> EventStaging for EphemeralNodeRetriever<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    fn stage_events(&self, events: Vec<Attested<Event>>) { EphemeralNodeRetriever::stage_events(self, events) }

    fn mark_event_used(&self, event_id: &EventId) { EphemeralNodeRetriever::mark_event_used(self, event_id) }
}
