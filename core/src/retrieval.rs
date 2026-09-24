//! Implements event and state retrieval from local and remote sources.
//!
//! Split into separate traits for event retrieval (`GetEvents`), state retrieval (`GetState`),
//! and event staging (`SuspenseEvents`). This separation enables the staging pattern
//! where incoming events are temporarily staged for BFS discovery before being committed
//! to permanent storage.

use crate::internal::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::util::Iterable;
use crate::storage::StorageTransaction;
use ankurah_proto::{Attested, EntityId, Event, EventId};
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

/// Extends GetEvents with temporary staging for causal comparison.
pub trait SuspenseEvents: GetEvents {
    /// Stage an event for BFS discovery. `get_event` will find staged events.
    fn stage_event(&self, event: Event);
}

// ============================================================================
// GETTER IMPLS
// ============================================================================

#[async_trait]
impl<R: GetEvents + Send + Sync + ?Sized> GetEvents for &R {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> { (*self).get_event(event_id).await }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> { (*self).event_stored(event_id).await }

    fn storage_is_definitive(&self) -> bool { (*self).storage_is_definitive() }
}

#[async_trait]
impl<SE: StorageEngine> GetState for SE {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<proto::EntityState>>, RetrievalError> {
        match StorageEngine::get_state(self, entity_id).await {
            Ok(state) => Ok(Some(state)),
            Err(RetrievalError::EntityNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

// ============================================================================
// CONCRETE TYPES
// ============================================================================

/// Local event getter with staging support. Used by durable nodes.
/// `get_event` checks staging first, then permanent storage.
/// `event_stored` checks permanent storage only.
pub struct LocalEventGetter<SE: StorageEngine> {
    storage: Arc<SE>,
    durable: bool,
    staging: Arc<RwLock<HashMap<EventId, Event>>>,
}

impl<SE: StorageEngine> LocalEventGetter<SE> {
    pub fn new(storage: Arc<SE>, durable: bool) -> Self { Self { storage, durable, staging: Arc::new(RwLock::new(HashMap::new())) } }
}

#[async_trait]
impl<SE: StorageEngine> GetEvents for LocalEventGetter<SE> {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        // Check staging first
        {
            let staging = self.staging.read().unwrap_or_else(|e| e.into_inner());
            if let Some(event) = staging.get(event_id) {
                return Ok(event.clone());
            }
        }
        // Fall back to permanent storage
        let events = self.storage.get_events(vec![event_id.clone()], &ankql::ast::Predicate::True).await?;
        events.into_iter().next().map(|e| e.payload).ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        // Check permanent storage only (not staging)
        let events = self.storage.get_events(vec![event_id.clone()], &ankql::ast::Predicate::True).await?;
        Ok(events.into_iter().next().is_some())
    }

    fn storage_is_definitive(&self) -> bool { self.durable }
}

impl<SE: StorageEngine> SuspenseEvents for LocalEventGetter<SE> {
    fn stage_event(&self, event: Event) {
        let mut staging = self.staging.write().unwrap_or_else(|e| e.into_inner());
        staging.insert(event.id(), event);
    }
}

/// Cached event getter with staging + remote peer fallback. Used by ephemeral nodes.
/// `get_event` checks staging, then local storage, then remote peer.
/// `event_stored` checks permanent storage only.
// FIXME: This getter persists remote misses, while callers persist accepted staged events.
// Make both writes explicit at the caller's storage boundary.
pub struct CachedEventGetter<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
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
    pub fn new(node: &'a Node<SE, PA>, cdata: &'a C) -> Self {
        Self { node, cdata, staging: Arc::new(RwLock::new(HashMap::new())) }
    }
}

/// The event in a peer's `GetEvents` response that actually answers a request
/// for `event_id`: one that recomputes to the id asked for and is structurally
/// well formed.
///
/// Both are the peer's word rather than ours. An event that derives some other
/// id does not answer this request, and caching it under the requested id would
/// make every later reader of that id read the wrong event. A malformed one --
/// a genesis whose content derives a different id, a genesis carrying a parent
/// clock, an update carrying none -- would reach the DAG layers, where its
/// backend diffs and its membership are applied to the requesting entity.
fn answering_event(peer_events: Vec<Attested<Event>>, event_id: &EventId) -> Option<Attested<Event>> {
    peer_events.into_iter().find(|event| event.payload.id() == *event_id && event.payload.validate_structure().is_ok())
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
        let events = self.node.storage.get_events(vec![event_id.clone()], &ankql::ast::Predicate::True).await?;
        if let Some(event) = events.into_iter().next() {
            return Ok(event.payload);
        }

        // Try remote peer
        let Some(peer_id) = self.node.get_durable_peer_random() else {
            return Err(RetrievalError::EventNotFound(event_id.clone()));
        };

        match self
            .node
            .request(peer_id, self.cdata, proto::NodeRequestBody::GetEvents { event_ids: vec![event_id.clone()] })
            .await?
        {
            proto::NodeResponseBody::GetEvents(peer_events) => {
                // Deliberately not checked here: the policy hook
                // (validate_received_event) that the other three peer-event
                // paths call stays with peer-event validation for visibility,
                // together with the ignored coverage in
                // tests/tests/adversarial_wire.rs, and membership
                // admissibility at BFS time is identity-02's.
                let event = answering_event(peer_events, event_id).ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))?;
                let mut transaction = self.node.storage.transaction();
                transaction.add_events(std::slice::from_ref(&event)).await?;
                transaction.commit().await?.committed()?;
                Ok(event.payload)
            }
            proto::NodeResponseBody::Error(e) => Err(RetrievalError::storage(format!("Error from peer: {}", e))),
            _ => Err(RetrievalError::storage("Unexpected response type from peer")),
        }
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        // Check permanent storage only (not staging)
        let events = self.node.storage.get_events(vec![event_id.clone()], &ankql::ast::Predicate::True).await?;
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
pub struct LocalStateGetter<SE: StorageEngine> {
    storage: Arc<SE>,
}

impl<SE: StorageEngine> LocalStateGetter<SE> {
    pub fn new(storage: Arc<SE>) -> Self { Self { storage } }
}

#[async_trait]
impl<SE: StorageEngine> GetState for LocalStateGetter<SE> {
    async fn get_state(&self, entity_id: EntityId) -> Result<Option<Attested<proto::EntityState>>, RetrievalError> {
        GetState::get_state(self.storage.as_ref(), entity_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestStorage;
    use ankurah_proto::{AttestationSet, Attested, Clock, EntityId, Event, EventId, OperationSet};
    use std::sync::Arc;

    /// Create a test event with a deterministic content-hashed ID.
    ///
    /// The nonce comes from the seed rather than from entropy, so every id is
    /// reproducible across runs. A genesis derives its entity id from its own
    /// content, the way the production mint does, so the staging lifecycle runs
    /// over the only genesis shape a node can produce rather than one the
    /// commit funnels would refuse. An update names the seed-derived id
    /// instead, because a fixture's parents are synthetic event ids with no
    /// genesis behind them.
    fn make_test_event(seed: u8, parent_ids: &[EventId]) -> Event {
        let mut entity_id_bytes = [0u8; 32];
        entity_id_bytes[0] = seed;
        let mut nonce = [0u8; 32];
        nonce[0] = seed;

        let parent = Clock::from(parent_ids.to_vec());
        let author = ankurah_proto::AuthorId::Unknown;
        let operations = OperationSet::default();
        let (entity_id, body) = if parent.is_empty() {
            let entity_id = EntityId::from(EventId::from_genesis_parts(&None, &nonce, 0, &author, &operations));
            (entity_id, ankurah_proto::EventBody::Genesis { system: None, nonce, timestamp: 0, author, operations })
        } else {
            (EntityId::from_bytes(entity_id_bytes), ankurah_proto::EventBody::Update { nonce, timestamp: 0, author, operations })
        };
        Event { entity_id, body, parent }
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
        let collection = Arc::new(TestStorage::default());
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
        let collection = Arc::new(TestStorage::default());
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

    #[tokio::test]
    async fn event_stored_observes_storage_not_staging() {
        let storage = Arc::new(TestStorage::default());
        let getter = LocalEventGetter::new(storage.clone(), true);
        let event = make_test_event(3, &[]);
        let event_id = event.id();
        getter.stage_event(event.clone());
        assert!(!getter.event_stored(&event_id).await.unwrap());

        let attested = Attested { payload: event, attestations: AttestationSet::default() };
        let mut transaction = storage.transaction();
        transaction.add_events(&[attested]).await.unwrap();
        transaction.commit().await.unwrap().committed().unwrap();
        assert!(getter.event_stored(&event_id).await.unwrap());
        let fresh_getter = LocalEventGetter::new(storage, true);
        assert_eq!(fresh_getter.get_event(&event_id).await.unwrap().id(), event_id);
    }

    /// Verify that `storage_is_definitive` reflects the durable flag.
    #[tokio::test]
    async fn test_storage_is_definitive_reflects_durable_flag() {
        let collection = Arc::new(TestStorage::default());

        let durable_getter = LocalEventGetter::new(collection.clone(), true);
        assert!(durable_getter.storage_is_definitive(), "Durable getter should report storage as definitive");

        let ephemeral_getter = LocalEventGetter::new(collection, false);
        assert!(!ephemeral_getter.storage_is_definitive(), "Ephemeral getter should report storage as non-definitive");
    }
}
