//! Implements event and state retrieval from local and remote sources.
//!
//! Split into separate traits for event retrieval (`GetEvents`), state retrieval (`GetState`),
//! and event staging/commit (`SuspenseEvents`). This separation enables the staging pattern
//! where incoming events are temporarily staged for BFS discovery before being committed
//! to permanent storage.

use std::sync::Arc;

use crate::{
    entity::WeakEntitySet,
    error::{MutationError, RequestError, RetrievalError},
    ingest::StagingArea,
    policy::PolicyAgent,
    storage::{StorageCollectionWrapper, StorageEngine},
    util::Iterable,
    Node,
};
use ankurah_proto::{self as proto, Attested, EntityId, Event, EventId};
use async_trait::async_trait;

fn checked_event(requested: &EventId, event: Event) -> Result<Event, RetrievalError> {
    let actual = event.id();
    if actual != *requested {
        return Err(RetrievalError::Other(format!("event identity mismatch: requested {requested}, payload recomputed to {actual}")));
    }
    Ok(event)
}

fn validate_event_scope(event: &Event, expected_entity: EntityId, expected_model: EntityId) -> Result<(), MutationError> {
    let event_id = event.id();
    if event.entity_id != expected_entity {
        return Err(crate::error::IngestError::Lineage(crate::error::LineageRejection::EntityMismatch {
            event: event_id,
            expected: expected_entity,
            received: event.entity_id,
        })
        .into());
    }
    if event.model != expected_model {
        return Err(crate::error::IngestError::Lineage(crate::error::LineageRejection::ModelMismatch {
            event: event_id,
            expected: expected_model,
            received: event.model,
        })
        .into());
    }
    Ok(())
}

fn validate_optional_scope(event: &Event, scope: Option<(EntityId, EntityId)>) -> Result<(), RetrievalError> {
    if let Some((expected_entity, expected_model)) = scope {
        validate_event_scope(event, expected_entity, expected_model).map_err(RetrievalError::from)?;
    }
    Ok(())
}

/// Validate a peer's complete response before caching any part of it. This
/// two-pass shape matters for scoped lookups: a wrong-lineage ancestor must
/// never be written into the requested collection before the outer comparison
/// rejects it.
async fn validate_and_cache_peer_events(
    collection: &StorageCollectionWrapper,
    requested: &EventId,
    peer_events: Vec<Attested<Event>>,
    scope: Option<(EntityId, EntityId)>,
) -> Result<Event, RetrievalError> {
    for event in &peer_events {
        checked_event(requested, event.payload.clone())?;
        validate_optional_scope(&event.payload, scope)?;
    }

    let first = peer_events.first().map(|event| event.payload.clone()).ok_or_else(|| RetrievalError::EventNotFound(requested.clone()))?;
    for event in &peer_events {
        collection.add_event(event).await?;
    }
    Ok(first)
}

/// Close the response-delivery-to-cache-write reset window for ordinary
/// requests. The request itself registers under the reset fence, but it
/// releases that guard while awaiting its response. Reacquiring here and
/// holding through cache persistence ensures a queued reset either wipes
/// after these writes or wins first and makes the response stale.
async fn validate_and_cache_peer_events_at_epoch(
    collection: &StorageCollectionWrapper,
    requested: &EventId,
    peer_events: Vec<Attested<Event>>,
    scope: Option<(EntityId, EntityId)>,
    entities: &WeakEntitySet,
    request_epoch: u64,
) -> Result<Event, RetrievalError> {
    let _reset_fence = entities.reset_fence_read().await;
    if entities.reset_epoch() != request_epoch {
        return Err(RequestError::ConnectionLost.into());
    }
    validate_and_cache_peer_events(collection, requested, peer_events, scope).await
}

// ============================================================================
// TRAITS
// ============================================================================

/// Retrieve events by ID. Implementations may check staging, local storage, or remote peers.
#[async_trait]
pub trait GetEvents {
    /// Retrieve a single event by ID.
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError>;

    /// Retrieve an event while binding every implementation-side side effect
    /// (notably remote caching) to an expected entity/model lineage. The
    /// default is sufficient for side-effect-free getters; caching getters
    /// override it so validation happens before persistence.
    async fn get_event_scoped(
        &self,
        event_id: &EventId,
        expected_entity: EntityId,
        expected_model: EntityId,
    ) -> Result<Event, RetrievalError> {
        let event = self.get_event(event_id).await?;
        validate_event_scope(&event, expected_entity, expected_model).map_err(RetrievalError::from)?;
        Ok(event)
    }

    /// Scoped lookup for a caller that already holds the node reset fence.
    /// Side-effect-free getters can use the ordinary implementation. A
    /// networked getter overrides this to avoid recursively acquiring
    /// Tokio's fair RwLock behind a queued reset writer.
    async fn get_event_scoped_fenced(
        &self,
        event_id: &EventId,
        expected_entity: EntityId,
        expected_model: EntityId,
    ) -> Result<Event, RetrievalError> {
        self.get_event_scoped(event_id, expected_entity, expected_model).await
    }

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
    fn stage_event(&self, event: Event) -> Result<(), MutationError>;

    /// Commit a staged event to permanent storage and remove from staging.
    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError>;

    /// The event if it is LOCALLY in hand (staging area or local permanent
    /// storage), else None. NEVER fetches from a peer: this is the parent
    /// resolution admission verification uses (D2-3), and "locally
    /// resolvable" is decided against local knowledge only; a parent that is
    /// not in hand makes the event admit unverified, it does not trigger a
    /// verification fetch.
    async fn get_local_event(&self, event_id: &EventId) -> Result<Option<Event>, RetrievalError>;
}

/// Binds every lineage lookup and commit to the resident identity being
/// mutated. Direct batch members are checked at intake; this wrapper closes
/// the same boundary over stored or remotely fetched ancestors discovered by
/// the comparison walk.
pub(crate) struct ScopedEventGetter<'a, G> {
    inner: &'a G,
    entity: EntityId,
    model: EntityId,
    reset_fence_held: bool,
}

impl<'a, G> ScopedEventGetter<'a, G> {
    pub(crate) fn new(inner: &'a G, entity: EntityId, model: EntityId) -> Self { Self { inner, entity, model, reset_fence_held: false } }

    pub(crate) fn new_fenced(inner: &'a G, entity: EntityId, model: EntityId) -> Self {
        Self { inner, entity, model, reset_fence_held: true }
    }

    fn validate(&self, event: &Event) -> Result<(), MutationError> { validate_event_scope(event, self.entity, self.model) }
}

#[async_trait]
impl<G> GetEvents for ScopedEventGetter<'_, G>
where G: GetEvents + Send + Sync
{
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        let event = if self.reset_fence_held {
            self.inner.get_event_scoped_fenced(event_id, self.entity, self.model).await?
        } else {
            self.inner.get_event_scoped(event_id, self.entity, self.model).await?
        };
        self.validate(&event).map_err(RetrievalError::from)?;
        Ok(event)
    }

    async fn get_event_scoped(
        &self,
        event_id: &EventId,
        expected_entity: EntityId,
        expected_model: EntityId,
    ) -> Result<Event, RetrievalError> {
        let event = self.get_event(event_id).await?;
        validate_event_scope(&event, expected_entity, expected_model).map_err(RetrievalError::from)?;
        Ok(event)
    }

    async fn get_event_scoped_fenced(
        &self,
        event_id: &EventId,
        expected_entity: EntityId,
        expected_model: EntityId,
    ) -> Result<Event, RetrievalError> {
        let event = self.inner.get_event_scoped_fenced(event_id, self.entity, self.model).await?;
        self.validate(&event).map_err(RetrievalError::from)?;
        validate_event_scope(&event, expected_entity, expected_model).map_err(RetrievalError::from)?;
        Ok(event)
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> { self.inner.event_stored(event_id).await }

    fn storage_is_definitive(&self) -> bool { self.inner.storage_is_definitive() }
}

#[async_trait]
impl<G> SuspenseEvents for ScopedEventGetter<'_, G>
where G: SuspenseEvents + Send + Sync
{
    fn stage_event(&self, event: Event) -> Result<(), MutationError> {
        self.validate(&event)?;
        self.inner.stage_event(event)
    }

    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError> {
        self.validate(&attested.payload)?;
        self.inner.commit_event(attested).await
    }

    async fn get_local_event(&self, event_id: &EventId) -> Result<Option<Event>, RetrievalError> {
        let event = self.inner.get_local_event(event_id).await?;
        if let Some(event) = &event {
            self.validate(event).map_err(RetrievalError::from)?;
        }
        Ok(event)
    }
}

// ============================================================================
// BLANKET REFERENCE IMPLS
// ============================================================================

#[async_trait]
impl<R: GetEvents + Send + Sync + ?Sized> GetEvents for &R {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> { (*self).get_event(event_id).await }

    async fn get_event_scoped(
        &self,
        event_id: &EventId,
        expected_entity: EntityId,
        expected_model: EntityId,
    ) -> Result<Event, RetrievalError> {
        (*self).get_event_scoped(event_id, expected_entity, expected_model).await
    }

    async fn get_event_scoped_fenced(
        &self,
        event_id: &EventId,
        expected_entity: EntityId,
        expected_model: EntityId,
    ) -> Result<Event, RetrievalError> {
        (*self).get_event_scoped_fenced(event_id, expected_entity, expected_model).await
    }

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
pub struct LocalEventGetter {
    collection: StorageCollectionWrapper,
    durable: bool,
    staging: Arc<StagingArea>,
}

impl LocalEventGetter {
    /// Private per-call staging, the historical lifetime: everything staged
    /// through this getter is dropped with it unless committed first.
    pub fn new(collection: StorageCollectionWrapper, durable: bool) -> Self {
        Self::with_staging(collection, durable, Arc::new(StagingArea::with_default_cap()))
    }

    /// Shared staging with caller-controlled lifetime. The ingest pipeline
    /// passes the node-scoped area so staged-but-unapplied events survive
    /// across deliveries (gap replay, NeedsState/NeedsEvents buffering).
    pub(crate) fn with_staging(collection: StorageCollectionWrapper, durable: bool, staging: Arc<StagingArea>) -> Self {
        Self { collection, durable, staging }
    }
}

#[async_trait]
impl GetEvents for LocalEventGetter {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        // Check staging first
        if let Some(event) = self.staging.get(event_id) {
            return Ok(event);
        }
        // Fall back to permanent storage
        let events = self.collection.get_events(vec![event_id.clone()]).await?;
        match events.into_iter().next() {
            Some(event) => checked_event(event_id, event.payload),
            None => Err(RetrievalError::EventNotFound(event_id.clone())),
        }
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        // Check permanent storage only (not staging)
        self.collection.has_event(event_id).await
    }

    fn storage_is_definitive(&self) -> bool { self.durable }
}

#[async_trait]
impl SuspenseEvents for LocalEventGetter {
    fn stage_event(&self, event: Event) -> Result<(), MutationError> {
        // The bare-Event form carries no attestations; that is fine for the
        // discovery-only lifetime this trait serves (attestations travel
        // separately to commit_event). Pipeline intake stages Attested
        // events directly on the shared area when retention matters.
        self.staging.try_stage(Attested::opt(event, None)).map_err(Into::into)
    }

    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError> {
        self.collection.add_event(attested).await?;
        self.staging.remove(&attested.payload.id());
        Ok(())
    }

    async fn get_local_event(&self, event_id: &EventId) -> Result<Option<Event>, RetrievalError> {
        if let Some(event) = self.staging.get(event_id) {
            return Ok(Some(event));
        }
        match self.collection.get_events(vec![event_id.clone()]).await?.into_iter().next() {
            Some(event) => checked_event(event_id, event.payload).map(Some),
            None => Ok(None),
        }
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
    staging: Arc<StagingArea>,
    expected_epoch: u64,
}

impl<'a, SE, PA, C> CachedEventGetter<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    /// Private per-call staging, the historical lifetime.
    pub fn new(collection_id: proto::CollectionId, collection: StorageCollectionWrapper, node: &'a Node<SE, PA>, cdata: &'a C) -> Self {
        let expected_epoch = node.entities.reset_epoch();
        Self::new_at_epoch(collection_id, collection, node, cdata, expected_epoch)
    }

    /// Private per-call staging bound to the epoch that admitted the owning
    /// operation. Remote cache writes from another epoch are rejected.
    pub(crate) fn new_at_epoch(
        collection_id: proto::CollectionId,
        collection: StorageCollectionWrapper,
        node: &'a Node<SE, PA>,
        cdata: &'a C,
        expected_epoch: u64,
    ) -> Self {
        Self::with_staging_at_epoch(collection_id, collection, node, cdata, Arc::new(StagingArea::with_default_cap()), expected_epoch)
    }

    /// Shared staging bound to the epoch that admitted the owning update,
    /// fetch, or subscription operation.
    pub(crate) fn with_staging_at_epoch(
        collection_id: proto::CollectionId,
        collection: StorageCollectionWrapper,
        node: &'a Node<SE, PA>,
        cdata: &'a C,
        staging: Arc<StagingArea>,
        expected_epoch: u64,
    ) -> Self {
        Self { collection_id, collection, node, cdata, staging, expected_epoch }
    }

    async fn get_event_with_scope(
        &self,
        event_id: &EventId,
        scope: Option<(EntityId, EntityId)>,
        reset_fence_held: bool,
    ) -> Result<Event, RetrievalError> {
        let request_epoch = self.expected_epoch;
        if self.node.entities.reset_epoch() != request_epoch {
            return Err(RequestError::ConnectionLost.into());
        }

        // Check staging first.
        if let Some(event) = self.staging.get(event_id) {
            validate_optional_scope(&event, scope)?;
            return Ok(event);
        }

        // Try local storage.
        let events = self.collection.get_events(vec![event_id.clone()]).await?;
        if let Some(event) = events.into_iter().next() {
            let event = checked_event(event_id, event.payload)?;
            validate_optional_scope(&event, scope)?;
            return Ok(event);
        }

        // Try a remote peer.
        let Some(peer_id) = self.node.get_durable_peer_random() else {
            return Err(RetrievalError::EventNotFound(event_id.clone()));
        };

        let request = proto::NodeRequestBody::GetEvents { collection: self.collection_id.clone(), event_ids: vec![event_id.clone()] };
        let response = if reset_fence_held {
            self.node.request_fenced(peer_id, self.cdata, request).await?
        } else {
            self.node.request_at_epoch(peer_id, self.cdata, request, request_epoch).await?
        };

        match response {
            proto::NodeResponseBody::GetEvents(peer_events) => {
                if !reset_fence_held {
                    validate_and_cache_peer_events_at_epoch(
                        &self.collection,
                        event_id,
                        peer_events,
                        scope,
                        &self.node.entities,
                        request_epoch,
                    )
                    .await
                } else {
                    // request_fenced's caller owns the outer reset guard
                    // across both the round trip and this cache write.
                    validate_and_cache_peer_events(&self.collection, event_id, peer_events, scope).await
                }
            }
            proto::NodeResponseBody::Error(e) => Err(RetrievalError::StorageError(format!("Error from peer: {}", e).into())),
            _ => Err(RetrievalError::StorageError("Unexpected response type from peer".into())),
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
        self.get_event_with_scope(event_id, None, false).await
    }

    async fn get_event_scoped(
        &self,
        event_id: &EventId,
        expected_entity: EntityId,
        expected_model: EntityId,
    ) -> Result<Event, RetrievalError> {
        self.get_event_with_scope(event_id, Some((expected_entity, expected_model)), false).await
    }

    async fn get_event_scoped_fenced(
        &self,
        event_id: &EventId,
        expected_entity: EntityId,
        expected_model: EntityId,
    ) -> Result<Event, RetrievalError> {
        self.get_event_with_scope(event_id, Some((expected_entity, expected_model)), true).await
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        // Check permanent storage only (not staging)
        self.collection.has_event(event_id).await
    }
}

#[async_trait]
impl<'a, SE, PA, C> SuspenseEvents for CachedEventGetter<'a, SE, PA, C>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData> + Send + Sync + 'a,
{
    fn stage_event(&self, event: Event) -> Result<(), MutationError> {
        self.staging.try_stage(Attested::opt(event, None)).map_err(Into::into)
    }

    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError> {
        self.collection.add_event(attested).await?;
        self.staging.remove(&attested.payload.id());
        Ok(())
    }

    /// Staging then LOCAL storage only: no peer fallback, unlike get_event.
    async fn get_local_event(&self, event_id: &EventId) -> Result<Option<Event>, RetrievalError> {
        if let Some(event) = self.staging.get(event_id) {
            return Ok(Some(event));
        }
        match self.collection.get_events(vec![event_id.clone()]).await?.into_iter().next() {
            Some(event) => checked_event(event_id, event.payload).map(Some),
            None => Ok(None),
        }
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
    use ankurah_proto::{AttestationSet, Attested, EntityId, EntityState, Event, EventId, OperationSet};
    use async_trait::async_trait;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    /// Minimal in-memory storage collection for testing the staging lifecycle.
    /// Only implements `add_event` and `get_events`; other methods panic or
    /// return not-found since they are not exercised by these tests.
    struct MockStorageCollection {
        events: Mutex<HashMap<EventId, Attested<Event>>>,
    }

    impl MockStorageCollection {
        fn new() -> Self { Self { events: Mutex::new(HashMap::new()) } }

        fn insert_under(&self, requested: EventId, event: Event) {
            self.events.lock().unwrap().insert(requested, Attested::opt(event, None));
        }

        fn contains(&self, event_id: &EventId) -> bool { self.events.lock().unwrap().contains_key(event_id) }
    }

    /// Exercises the same response-validation/cache helper as
    /// CachedEventGetter without constructing a networked Node. The unscoped
    /// method intentionally models the historical cache-first behavior; the
    /// scoped override is the production pre-cache path.
    struct SimulatedRemoteCacheGetter {
        collection: StorageCollectionWrapper,
        response: Attested<Event>,
    }

    struct FenceAwareGetter {
        event: Event,
        ordinary_scoped_calls: AtomicUsize,
        fenced_scoped_calls: AtomicUsize,
    }

    #[async_trait]
    impl GetEvents for FenceAwareGetter {
        async fn get_event(&self, _event_id: &EventId) -> Result<Event, RetrievalError> {
            panic!("scoped wrapper must not fall back to an unscoped lookup")
        }

        async fn get_event_scoped(
            &self,
            _event_id: &EventId,
            _expected_entity: EntityId,
            _expected_model: EntityId,
        ) -> Result<Event, RetrievalError> {
            self.ordinary_scoped_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.event.clone())
        }

        async fn get_event_scoped_fenced(
            &self,
            _event_id: &EventId,
            _expected_entity: EntityId,
            _expected_model: EntityId,
        ) -> Result<Event, RetrievalError> {
            self.fenced_scoped_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.event.clone())
        }

        async fn event_stored(&self, _event_id: &EventId) -> Result<bool, RetrievalError> { Ok(false) }
    }

    #[async_trait]
    impl GetEvents for SimulatedRemoteCacheGetter {
        async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
            validate_and_cache_peer_events(&self.collection, event_id, vec![self.response.clone()], None).await
        }

        async fn get_event_scoped(
            &self,
            event_id: &EventId,
            expected_entity: EntityId,
            expected_model: EntityId,
        ) -> Result<Event, RetrievalError> {
            validate_and_cache_peer_events(&self.collection, event_id, vec![self.response.clone()], Some((expected_entity, expected_model)))
                .await
        }

        async fn event_stored(&self, _event_id: &EventId) -> Result<bool, RetrievalError> { Ok(false) }
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

    /// Create a test event with a deterministic content-hashed ID. Parents are
    /// EVENTS (payload-authoritative generation stamping; the registry ban).
    fn make_test_event(seed: u8, parents: &[&Event]) -> Event {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = seed;
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        Event {
            entity_id,
            model: EntityId::from_bytes([0xEE; 16]),
            operations: OperationSet(BTreeMap::new()),
            parent: ankurah_proto::Clock::from(parents.iter().map(|p| p.id()).collect::<Vec<_>>()),
            generation: Event::generation_from_parents(parents.iter().map(|p| p.generation)),
        }
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
        getter.stage_event(event.clone()).unwrap();

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
        getter.stage_event(event).unwrap();

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
        getter.stage_event(event.clone()).unwrap();

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

    /// The executor wraps an already scoped getter after taking the reset
    /// fence. Preserve the fenced signal through both wrappers so a cached
    /// peer lookup reaches Node::request_fenced rather than recursively
    /// acquiring the fair reset RwLock.
    #[tokio::test]
    async fn nested_scoped_lookup_preserves_the_already_fenced_path() {
        let event = make_test_event(8, &[]);
        let event_id = event.id();
        let entity = event.entity_id;
        let model = event.model;
        let getter = FenceAwareGetter { event, ordinary_scoped_calls: AtomicUsize::new(0), fenced_scoped_calls: AtomicUsize::new(0) };
        let inner = ScopedEventGetter::new(&getter, entity, model);
        let outer = ScopedEventGetter::new_fenced(&inner, entity, model);

        outer.get_event(&event_id).await.expect("fenced lineage lookup succeeds");

        assert_eq!(getter.ordinary_scoped_calls.load(Ordering::SeqCst), 0);
        assert_eq!(getter.fenced_scoped_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn remote_response_from_a_dead_epoch_is_rejected_before_cache_write() {
        let event = make_test_event(7, &[]);
        let event_id = event.id();
        let raw = Arc::new(MockStorageCollection::new());
        let collection = StorageCollectionWrapper::new(raw.clone());
        let entities = WeakEntitySet::default();
        let request_epoch = entities.reset_epoch();

        // Models the response-handler-to-caller scheduling window: delivery
        // completed in request_epoch, then reset won before the caller could
        // validate and populate its local cache.
        entities.bump_reset_epoch();
        let error = validate_and_cache_peer_events_at_epoch(
            &collection,
            &event_id,
            vec![Attested::opt(event, None)],
            None,
            &entities,
            request_epoch,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, RetrievalError::RequestError(RequestError::ConnectionLost)));
        assert!(!raw.contains(&event_id), "a response from the purged epoch must not repopulate successor storage");
    }

    #[tokio::test]
    async fn local_getters_reject_a_payload_that_does_not_match_the_requested_id() {
        let raw = Arc::new(MockStorageCollection::new());
        let honest = make_test_event(9, &[]);
        let requested = honest.id();
        let doctored = Event { generation: 7, ..honest };
        assert_ne!(doctored.id(), requested);
        raw.insert_under(requested.clone(), doctored);

        let getter = LocalEventGetter::new(StorageCollectionWrapper::new(raw), true);
        let err = getter.get_event(&requested).await.unwrap_err();
        assert!(err.to_string().contains("event identity mismatch"), "unexpected error: {err}");

        let err = getter.get_local_event(&requested).await.unwrap_err();
        assert!(err.to_string().contains("event identity mismatch"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn scoped_remote_ancestors_are_rejected_before_cache_write() {
        let expected_entity = EntityId::from_bytes([0x11; 16]);
        let expected_model = EntityId::from_bytes([0x22; 16]);
        let wrong_model = Event {
            entity_id: expected_entity,
            model: EntityId::from_bytes([0x23; 16]),
            operations: OperationSet(BTreeMap::new()),
            parent: proto::Clock::default(),
            generation: 1,
        };
        let wrong_entity = Event {
            entity_id: EntityId::from_bytes([0x12; 16]),
            model: expected_model,
            operations: OperationSet(BTreeMap::new()),
            parent: proto::Clock::default(),
            generation: 1,
        };

        for (kind, event, expected_message) in [("model", wrong_model, "belongs to model"), ("entity", wrong_entity, "belongs to entity")] {
            let requested = event.id();
            let raw = Arc::new(MockStorageCollection::new());
            let collection = StorageCollectionWrapper::new(raw.clone());
            let remote = SimulatedRemoteCacheGetter { collection: collection.clone(), response: Attested::opt(event, None) };
            let scoped = ScopedEventGetter::new(&remote, expected_entity, expected_model);

            let err = scoped.get_event(&requested).await.unwrap_err();
            assert!(err.to_string().contains(expected_message), "unexpected {kind} error: {err}");
            assert!(!raw.contains(&requested), "wrong-{kind} remote ancestor must not be cached before rejection");

            let local = LocalEventGetter::new(collection, true);
            assert!(
                matches!(local.get_event(&requested).await, Err(RetrievalError::EventNotFound(id)) if id == requested),
                "wrong-{kind} rejection must leave no poisoned row for subsequent local reads"
            );
        }
    }
}
