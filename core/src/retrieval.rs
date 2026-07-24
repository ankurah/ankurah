//! Implements event and state retrieval from local and remote sources.
//!
//! Split into separate traits for event retrieval (`GetEvents`), state
//! retrieval (`GetState`), and in-memory event staging (`SuspenseEvents`).
//! This separation enables incoming events to be discoverable by BFS before
//! the caller appends them to permanent storage.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{error::RetrievalError, policy::PolicyAgent, storage::StorageEngine, util::Iterable, Node};
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
    /// Retrieve the canonical state for `entity_id`, returning `None` when it
    /// is absent from this source.
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<proto::EntityState>>, RetrievalError>;
}

/// Extends [`GetEvents`] with interior-mutable staging for BFS discovery.
///
/// Permanent append belongs to [`StorageEngine::append_events`], after the
/// caller has validated and attested the complete logical event set.
pub trait SuspenseEvents: GetEvents {
    /// Stage an event for BFS discovery. `get_event` will find staged events.
    fn stage_event(&self, event: Event);
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
pub struct LocalEventGetter<SE> {
    storage: Arc<SE>,
    durable: bool,
    staging: Arc<RwLock<HashMap<EventId, Event>>>,
}

impl<SE> LocalEventGetter<SE> {
    /// Construct a local event getter over `storage`.
    ///
    /// `durable` determines whether a negative permanent-storage lookup is
    /// authoritative for creation-uniqueness checks.
    pub fn new(storage: Arc<SE>, durable: bool) -> Self { Self { storage, durable, staging: Arc::new(RwLock::new(HashMap::new())) } }
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static> GetEvents for LocalEventGetter<SE> {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        // Check staging first
        {
            let staging = self.staging.read().unwrap_or_else(|e| e.into_inner());
            if let Some(event) = staging.get(event_id) {
                return Ok(event.clone());
            }
        }
        // Fall back to permanent storage
        let events = self.storage.get_events(vec![event_id.clone()]).await?;
        events.into_iter().next().map(|e| e.payload).ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        // Check permanent storage only (not staging)
        let events = self.storage.get_events(vec![event_id.clone()]).await?;
        Ok(events.into_iter().next().is_some())
    }

    fn storage_is_definitive(&self) -> bool { self.durable }
}

impl<SE: StorageEngine + Send + Sync + 'static> SuspenseEvents for LocalEventGetter<SE> {
    fn stage_event(&self, event: Event) {
        let mut staging = self.staging.write().unwrap_or_else(|e| e.into_inner());
        staging.insert(event.id(), event);
    }
}

/// Cached event getter with staging + remote peer fallback. Used by ephemeral nodes.
/// `get_event` checks staging, then local storage, then remote peer.
/// `event_stored` checks permanent storage only.
pub struct CachedEventGetter<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    collection_id: crate::ModelId,
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
    /// Construct a cache-first event getter for one model-scoped request.
    pub fn new(collection_id: crate::ModelId, node: &'a Node<SE, PA>, cdata: &'a C) -> Self {
        Self { collection_id, node, cdata, staging: Arc::new(RwLock::new(HashMap::new())) }
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
            let staging = self.staging.read().unwrap_or_else(|e| e.into_inner());
            if let Some(event) = staging.get(event_id) {
                return Ok(event.clone());
            }
        }

        // Try local storage
        let events = self.node.storage.get_events(vec![event_id.clone()]).await?;
        if let Some(event) = events.into_iter().next() {
            return Ok(event.payload);
        }

        // Try remote peer
        let Some(peer_id) = self.node.get_durable_peer_random() else {
            return Err(RetrievalError::EventNotFound(event_id.clone()));
        };

        let model = self.collection_id;
        match self
            .node
            .request(peer_id, self.cdata, proto::NodeRequestBody::GetEvents { model: model.clone(), event_ids: vec![event_id.clone()] })
            .await?
        {
            proto::NodeResponseBody::GetEvents { model: response_model, events: peer_events } => {
                if response_model != model {
                    return Err(RetrievalError::Other("GetEvents response crossed the requested model boundary".to_owned()));
                }
                // Store locally for future access
                self.node.storage.append_events(&peer_events).await?;
                peer_events.into_iter().next().map(|e| e.payload).ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
            }
            proto::NodeResponseBody::Error(e) => Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into())),
            _ => Err(RetrievalError::StorageError("Unexpected response type from peer".into())),
        }
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        // Check permanent storage only (not staging)
        let events = self.node.storage.get_events(vec![event_id.clone()]).await?;
        Ok(events.into_iter().next().is_some())
    }
}

impl<'a, SE, PA, C> SuspenseEvents for CachedEventGetter<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    fn stage_event(&self, event: Event) {
        let mut staging = self.staging.write().unwrap_or_else(|e| e.into_inner());
        staging.insert(event.id(), event);
    }
}

/// Local state getter. Retrieves entity states from local storage.
/// Reused by both durable and ephemeral paths.
#[derive(Clone)]
pub struct LocalStateGetter<SE> {
    storage: Arc<SE>,
}

impl<SE> LocalStateGetter<SE> {
    /// Construct a local state getter over the canonical entity store.
    pub fn new(storage: Arc<SE>) -> Self { Self { storage } }
}

#[async_trait]
impl<SE: StorageEngine + Send + Sync + 'static> GetState for LocalStateGetter<SE> {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<proto::EntityState>>, RetrievalError> {
        match self.storage.get_state(entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MutationError;
    use crate::storage::StorageEngine;
    use ankurah_proto::ModelId;
    use ankurah_proto::{AttestationSet, Attested, Clock, EntityId, EntityState, Event, EventId, OperationSet};
    use async_trait::async_trait;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};

    /// Minimal in-memory storage engine for testing the staging lifecycle.
    /// Only implements `add_event` and `get_events`; other methods panic or
    /// return not-found since they are not exercised by these tests.
    struct MockStorageEngine {
        events: Mutex<HashMap<EventId, Attested<Event>>>,
    }

    impl MockStorageEngine {
        fn new() -> Self { Self { events: Mutex::new(HashMap::new()) } }
    }

    #[async_trait]
    impl StorageEngine for MockStorageEngine {
        type Value = ();

        async fn append_events(&self, entity_events: &[Attested<Event>]) -> Result<Vec<bool>, MutationError> {
            let mut events = self.events.lock().unwrap();
            Ok(entity_events.iter().map(|event| events.insert(event.payload.id(), event.clone()).is_none()).collect())
        }

        async fn commit_batch(
            &self,
            batch: crate::storage::StorageWriteBatch,
        ) -> Result<crate::storage::CommitBatchOutcome, MutationError> {
            Ok(crate::storage::CommitBatchOutcome::Committed(crate::storage::StorageCommitResult {
                entities: batch
                    .entities
                    .into_iter()
                    .map(|write| crate::storage::CommittedEntityWrite {
                        entity_id: write.state.payload.entity_id,
                        canonical_changed: true,
                        associations_added: write.associate_with.iter().copied().collect(),
                        materialized_as: write.associate_with.into_iter().collect(),
                    })
                    .collect(),
            }))
        }

        async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> { Err(RetrievalError::EntityNotFound(id)) }

        async fn fetch_states(
            &self,
            _model: &ModelId,
            _selection: &ankql::ast::Selection,
        ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
            Ok(vec![])
        }

        async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
            let events = self.events.lock().unwrap();
            Ok(event_ids.into_iter().filter_map(|id| events.get(&id).cloned()).collect())
        }

        async fn dump_entity_events(&self, _id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> { Ok(vec![]) }

        async fn delete_all(&self) -> Result<bool, MutationError> { Ok(false) }
    }

    /// Create a test event with a deterministic content-hashed ID.
    fn make_test_event(seed: u8, parent_ids: &[EventId]) -> Event {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = seed;
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        Event { entity_id, parent: Clock::from(parent_ids.to_vec()), operations: OperationSet(BTreeMap::new()) }
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
        let storage = Arc::new(MockStorageEngine::new());
        let getter = LocalEventGetter::new(storage, true);

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
        let storage = Arc::new(MockStorageEngine::new());
        let getter = LocalEventGetter::new(storage, true);

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

    /// Stage an event, append it, then verify `event_stored` returns true.
    ///
    /// This exercises the production staging lifecycle:
    /// 1. stage_event → discoverable by BFS (get_event) but not "stored"
    /// 2. append_events → copied to permanent storage
    /// 3. event_stored → now returns true
    /// 4. get_event → still works
    #[tokio::test]
    async fn test_append_makes_event_stored_true() {
        let storage = Arc::new(MockStorageEngine::new());
        let getter = LocalEventGetter::new(storage.clone(), true);

        let event = make_test_event(3, &[]);
        let event_id = event.id();

        // Stage the event
        getter.stage_event(event.clone());

        // Verify staged state: get_event finds it, event_stored does not
        assert!(getter.get_event(&event_id).await.is_ok(), "Staged event should be retrievable");
        assert!(!getter.event_stored(&event_id).await.unwrap(), "event_stored should be false while staged");

        // Append the validated event through the storage boundary.
        let attested = Attested { payload: event, attestations: AttestationSet::default() };
        storage.append_events(&[attested]).await.expect("append_events should succeed");

        // After append: event_stored should now return true
        assert!(getter.event_stored(&event_id).await.unwrap(), "event_stored must return true after append_events");

        // get_event remains available throughout the staging scope.
        let retrieved = getter.get_event(&event_id).await.expect("Event should remain retrievable after append");
        assert_eq!(retrieved.id(), event_id);
    }

    /// Verify that `storage_is_definitive` reflects the durable flag.
    #[tokio::test]
    async fn test_storage_is_definitive_reflects_durable_flag() {
        let storage = Arc::new(MockStorageEngine::new());

        let durable_getter = LocalEventGetter::new(storage.clone(), true);
        assert!(durable_getter.storage_is_definitive(), "Durable getter should report storage as definitive");

        let ephemeral_getter = LocalEventGetter::new(storage, false);
        assert!(!ephemeral_getter.storage_is_definitive(), "Ephemeral getter should report storage as non-definitive");
    }
}
