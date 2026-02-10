//! Implements event and state retrieval from local and remote sources.
//!
//! Split into separate traits for event retrieval (`GetEvents`), state retrieval (`GetState`),
//! and event staging/commit (`SuspenseEvents`). This separation enables the staging pattern
//! where incoming events are temporarily staged for BFS discovery before being committed
//! to permanent storage.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{
    error::{MutationError, RetrievalError},
    policy::PolicyAgent,
    storage::{StorageCollectionWrapper, StorageEngine},
    util::Iterable,
    Node,
};
use ankurah_proto::{self as proto, Attested, EntityId, Event, EventId};
use async_trait::async_trait;

// ============================================================================
// TRAITS
// ============================================================================

/// Retrieve events by ID. Implementations may check staging, local storage, or remote peers.
#[async_trait]
pub trait GetEvents {
    /// Retrieve a single event by ID.
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError>;

    /// Check whether an event is in permanent storage (not staging).
    /// Used for creation-uniqueness guards where we need to know if the event
    /// was previously committed, not merely staged.
    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        match self.get_event(event_id).await {
            Ok(_) => Ok(true),
            Err(RetrievalError::EventNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Whether event_stored() is definitive — i.e., returning false means the event
    /// genuinely doesn't exist, not just that it's missing from local cache.
    /// Default is false (safe for ephemeral nodes).
    fn storage_is_definitive(&self) -> bool { false }
}

/// Retrieve entity state snapshots.
#[async_trait]
pub trait GetState {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<proto::EntityState>>, RetrievalError>;
}

/// Extends GetEvents with staging (interior-mutable) and commit capabilities.
/// The caller stages events before comparison, then commits them after policy checks pass.
#[async_trait]
pub trait SuspenseEvents: GetEvents {
    /// Stage an event for BFS discovery. `get_event` will find staged events.
    fn stage_event(&self, event: Event);

    /// Commit a staged event to permanent storage and remove from staging.
    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError>;
}

// ============================================================================
// BLANKET REFERENCE IMPLS
// ============================================================================

#[async_trait]
impl<R: GetEvents + Send + Sync + ?Sized> GetEvents for &R {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        (*self).get_event(event_id).await
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        (*self).event_stored(event_id).await
    }

    fn storage_is_definitive(&self) -> bool {
        (*self).storage_is_definitive()
    }
}

#[async_trait]
impl<R: GetState + Send + Sync + ?Sized> GetState for &R {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<proto::EntityState>>, RetrievalError> {
        (*self).get_state(entity_id).await
    }
}

// ============================================================================
// CONCRETE TYPES
// ============================================================================

/// Local event getter with staging support. Used by durable nodes.
/// `get_event` checks staging first, then permanent storage.
/// `event_stored` checks permanent storage only.
/// `commit_event` persists to storage and removes from staging.
#[derive(Clone)]
pub struct LocalEventGetter {
    collection: StorageCollectionWrapper,
    durable: bool,
    staging: Arc<RwLock<HashMap<EventId, Event>>>,
}

impl LocalEventGetter {
    pub fn new(collection: StorageCollectionWrapper, durable: bool) -> Self {
        Self {
            collection,
            durable,
            staging: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl GetEvents for LocalEventGetter {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        // Check staging first
        {
            let staging = self.staging.read().unwrap();
            if let Some(event) = staging.get(event_id) {
                return Ok(event.clone());
            }
        }
        // Fall back to permanent storage
        let events = self.collection.get_events(vec![event_id.clone()]).await?;
        events.into_iter().next().map(|e| e.payload).ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        // Check permanent storage only (not staging)
        let events = self.collection.get_events(vec![event_id.clone()]).await?;
        Ok(events.into_iter().next().is_some())
    }

    fn storage_is_definitive(&self) -> bool {
        self.durable
    }
}

#[async_trait]
impl SuspenseEvents for LocalEventGetter {
    fn stage_event(&self, event: Event) {
        let mut staging = self.staging.write().unwrap();
        staging.insert(event.id(), event);
    }

    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError> {
        self.collection.add_event(attested).await?;
        let mut staging = self.staging.write().unwrap();
        staging.remove(&attested.payload.id());
        Ok(())
    }
}

/// Cached event getter with staging + remote peer fallback. Used by ephemeral nodes.
/// `get_event` checks staging, then local storage, then remote peer.
/// `event_stored` checks permanent storage only.
/// `commit_event` persists to storage and removes from staging.
pub struct CachedEventGetter<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    collection_id: proto::CollectionId,
    collection: StorageCollectionWrapper,
    node: &'a Node<SE, PA>,
    cdata: &'a C,
    staging: Arc<RwLock<HashMap<EventId, Event>>>,
}

impl<'a, SE, PA, C> CachedEventGetter<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    pub fn new(collection_id: proto::CollectionId, collection: StorageCollectionWrapper, node: &'a Node<SE, PA>, cdata: &'a C) -> Self {
        Self {
            collection_id,
            collection,
            node,
            cdata,
            staging: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl<'a, SE, PA, C> GetEvents for CachedEventGetter<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        // Check staging first
        {
            let staging = self.staging.read().unwrap();
            if let Some(event) = staging.get(event_id) {
                return Ok(event.clone());
            }
        }

        // Try local storage
        let events = self.collection.get_events(vec![event_id.clone()]).await?;
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
                    collection: self.collection_id.clone(),
                    event_ids: vec![event_id.clone()],
                },
            )
            .await?
        {
            proto::NodeResponseBody::GetEvents(peer_events) => {
                // Store locally for future access
                for event in peer_events.iter() {
                    self.collection.add_event(event).await?;
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

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        // Check permanent storage only (not staging)
        let events = self.collection.get_events(vec![event_id.clone()]).await?;
        Ok(events.into_iter().next().is_some())
    }
}

#[async_trait]
impl<'a, SE, PA, C> SuspenseEvents for CachedEventGetter<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    fn stage_event(&self, event: Event) {
        let mut staging = self.staging.write().unwrap();
        staging.insert(event.id(), event);
    }

    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError> {
        self.collection.add_event(attested).await?;
        let mut staging = self.staging.write().unwrap();
        staging.remove(&attested.payload.id());
        Ok(())
    }
}

/// Local state getter. Retrieves entity states from local storage.
/// Reused by both durable and ephemeral paths.
#[derive(Clone)]
pub struct LocalStateGetter {
    collection: StorageCollectionWrapper,
}

impl LocalStateGetter {
    pub fn new(collection: StorageCollectionWrapper) -> Self {
        Self { collection }
    }
}

#[async_trait]
impl GetState for LocalStateGetter {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<proto::EntityState>>, RetrievalError> {
        match self.collection.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
