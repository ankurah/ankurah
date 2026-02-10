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
    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError>;

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
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> { (*self).get_event(event_id).await }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> { (*self).event_stored(event_id).await }

    fn storage_is_definitive(&self) -> bool { (*self).storage_is_definitive() }
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
        Self { collection, durable, staging: Arc::new(RwLock::new(HashMap::new())) }
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

    fn storage_is_definitive(&self) -> bool { self.durable }
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
        Self { collection_id, collection, node, cdata, staging: Arc::new(RwLock::new(HashMap::new())) }
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
                proto::NodeRequestBody::GetEvents { collection: self.collection_id.clone(), event_ids: vec![event_id.clone()] },
            )
            .await?
        {
            proto::NodeResponseBody::GetEvents(peer_events) => {
                // Store locally for future access
                for event in peer_events.iter() {
                    self.collection.add_event(event).await?;
                }
                peer_events.into_iter().next().map(|e| e.payload).ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
            }
            proto::NodeResponseBody::Error(e) => Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into())),
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
    pub fn new(collection: StorageCollectionWrapper) -> Self { Self { collection } }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{StorageCollection, StorageCollectionWrapper};
    use ankurah_proto::{AttestationSet, Attested, Clock, EntityId, EntityState, Event, EventId, OperationSet};
    use async_trait::async_trait;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};

    /// Minimal in-memory storage collection for testing the staging lifecycle.
    /// Only implements `add_event` and `get_events`; other methods panic or
    /// return not-found since they are not exercised by these tests.
    struct MockStorageCollection {
        events: Mutex<HashMap<EventId, Attested<Event>>>,
    }

    impl MockStorageCollection {
        fn new() -> Self { Self { events: Mutex::new(HashMap::new()) } }
    }

    #[async_trait]
    impl StorageCollection for MockStorageCollection {
        async fn set_state(&self, _state: Attested<EntityState>) -> Result<bool, MutationError> { Ok(true) }

        async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> { Err(RetrievalError::EntityNotFound(id)) }

        async fn fetch_states(&self, _selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
            Ok(vec![])
        }

        async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError> {
            let mut events = self.events.lock().unwrap();
            events.insert(entity_event.payload.id(), entity_event.clone());
            Ok(true)
        }

        async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
            let events = self.events.lock().unwrap();
            Ok(event_ids.into_iter().filter_map(|id| events.get(&id).cloned()).collect())
        }

        async fn dump_entity_events(&self, _id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> { Ok(vec![]) }
    }

    /// Create a test event with a deterministic content-hashed ID.
    fn make_test_event(seed: u8, parent_ids: &[EventId]) -> Event {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = seed;
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        Event { entity_id, collection: "test".into(), parent: Clock::from(parent_ids.to_vec()), operations: OperationSet(BTreeMap::new()) }
    }

    // ====================================================================
    // STAGING LIFECYCLE TESTS
    // ====================================================================

    /// Stage an event, then verify `get_event` can find it.
    ///
    /// This exercises the core staging contract: after `stage_event`,
    /// the event must be discoverable via `get_event` (used by BFS
    /// during DAG comparison).
    #[tokio::test]
    async fn test_stage_then_get_event() {
        let collection = StorageCollectionWrapper::new(Arc::new(MockStorageCollection::new()));
        let getter = LocalEventGetter::new(collection, true);

        let event = make_test_event(1, &[]);
        let event_id = event.id();

        // Before staging, event should not be found
        let result = getter.get_event(&event_id).await;
        assert!(result.is_err(), "Event should not be found before staging");

        // Stage the event
        getter.stage_event(event.clone());

        // After staging, get_event should find it
        let retrieved = getter.get_event(&event_id).await.expect("Staged event should be retrievable via get_event");
        assert_eq!(retrieved.id(), event_id, "Retrieved event ID should match staged event ID");
        assert_eq!(retrieved.entity_id, event.entity_id, "Retrieved event should match staged event");
    }

    /// Stage an event, then verify `event_stored` returns false.
    ///
    /// This is a critical safety property: `event_stored` must only
    /// reflect permanent storage, not the staging area. This distinction
    /// is used for creation-uniqueness guards — an event that's merely
    /// staged should not be considered "already stored".
    #[tokio::test]
    async fn test_stage_does_not_affect_event_stored() {
        let collection = StorageCollectionWrapper::new(Arc::new(MockStorageCollection::new()));
        let getter = LocalEventGetter::new(collection, true);

        let event = make_test_event(2, &[]);
        let event_id = event.id();

        // Before staging, event_stored should be false
        assert!(!getter.event_stored(&event_id).await.unwrap(), "event_stored should be false before staging");

        // Stage the event
        getter.stage_event(event);

        // After staging, event_stored should STILL be false
        // (staging is not permanent storage)
        assert!(!getter.event_stored(&event_id).await.unwrap(), "event_stored must return false for staged-but-not-committed events");
    }

    /// Stage an event, commit it, then verify `event_stored` returns true
    /// and the event is no longer in the staging area (but still retrievable
    /// from permanent storage).
    ///
    /// This exercises the full staging lifecycle:
    /// 1. stage_event → discoverable by BFS (get_event) but not "stored"
    /// 2. commit_event → moved to permanent storage, removed from staging
    /// 3. event_stored → now returns true
    /// 4. get_event → still works (now from permanent storage)
    #[tokio::test]
    async fn test_commit_makes_event_stored_true() {
        let collection = StorageCollectionWrapper::new(Arc::new(MockStorageCollection::new()));
        let getter = LocalEventGetter::new(collection, true);

        let event = make_test_event(3, &[]);
        let event_id = event.id();

        // Stage the event
        getter.stage_event(event.clone());

        // Verify staged state: get_event finds it, event_stored does not
        assert!(getter.get_event(&event_id).await.is_ok(), "Staged event should be retrievable");
        assert!(!getter.event_stored(&event_id).await.unwrap(), "event_stored should be false while staged");

        // Commit the event (wrap in Attested for the commit interface)
        let attested = Attested { payload: event, attestations: AttestationSet::default() };
        getter.commit_event(&attested).await.expect("commit_event should succeed");

        // After commit: event_stored should now return true
        assert!(getter.event_stored(&event_id).await.unwrap(), "event_stored must return true after commit_event");

        // get_event should still work (now from permanent storage, not staging)
        let retrieved = getter.get_event(&event_id).await.expect("Event should be retrievable from permanent storage after commit");
        assert_eq!(retrieved.id(), event_id);
    }

    /// Verify that `storage_is_definitive` reflects the durable flag.
    #[tokio::test]
    async fn test_storage_is_definitive_reflects_durable_flag() {
        let collection = StorageCollectionWrapper::new(Arc::new(MockStorageCollection::new()));

        let durable_getter = LocalEventGetter::new(collection.clone(), true);
        assert!(durable_getter.storage_is_definitive(), "Durable getter should report storage as definitive");

        let ephemeral_getter = LocalEventGetter::new(collection, false);
        assert!(!ephemeral_getter.storage_is_definitive(), "Ephemeral getter should report storage as non-definitive");
    }
}
