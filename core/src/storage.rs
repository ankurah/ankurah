use ankql::ast::{Predicate, Resolved};
use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::Stream;

use crate::error::{MutationError, RetrievalError};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventId, ModelId};

/// One raw logical record emitted by a storage dump.
#[derive(Debug)]
pub enum StorageDumpItem {
    Event(Attested<Event>),
    State(Attested<EntityState>),
}

/// Raw logical export implemented by storage engines that support portable
/// dumps. Physical tables, cursor pages, and materialized values remain an
/// implementation detail of the engine.
#[async_trait]
pub trait StorageDump: StorageEngine {
    type DumpStream: Stream<Item = Result<StorageDumpItem, RetrievalError>> + Send + 'static;

    /// Stream events followed by states without exposing the engine's
    /// physical partitioning.
    async fn dump(&self) -> Result<Self::DumpStream, RetrievalError>;
}

mod catalog;
pub use catalog::CatalogResolver;

mod read;
pub use read::{filter_events, GetStateResult};

/// One atomic storage transaction. Engines may execute writes as they arrive or buffer them until commit.
/// Dropping the handle without committing must leave its writes uncommitted.
#[async_trait]
pub trait StorageTransaction: Send {
    /// Write validated events with this transaction's states.
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError>;

    /// Replace an entity's state, including its memberships and materializations.
    /// `expected_head` is the state this update was derived from, including prior writes in this transaction.
    /// A missing entity has an empty head. Repeated calls for an entity replace its preceding state.
    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>) -> Result<(), MutationError>;

    /// Compare every expected head, then atomically persist events, canonical
    /// states, memberships, and materializations. Any conflict publishes none
    /// of those records and returns the observed stored states.
    async fn commit(self) -> Result<StorageCommitOutcome, MutationError>;
}

/// Successful persistence details for one prepared entity.
// TODO: No caller uses this result; remove it unless per-entity commit reporting proves necessary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedEntityWrite {
    /// Canonical entity identity.
    pub entity_id: EntityId,
    /// Whether the canonical head changed.
    pub canonical_changed: bool,
}

/// Result of an atomically committed storage transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageCommitResult {
    /// One result per entity, in first-write order.
    pub entities: Vec<CommittedEntityWrite>,
}

/// Outcome of attempting an exact-head storage transaction.
#[derive(Debug, Clone)]
pub enum StorageCommitOutcome {
    /// Every expectation matched and the entire batch committed.
    Committed(StorageCommitResult),
    /// At least one expectation differed, so the engine rolled back the
    /// complete batch. Values are canonical states observed while checking
    /// this attempt; `None` means no canonical entity existed when checked.
    Conflict { observed: BTreeMap<EntityId, Option<Attested<EntityState>>> },
}

impl StorageCommitOutcome {
    /// Return the committed result, or a retryable write conflict.
    pub fn committed(self) -> Result<StorageCommitResult, MutationError> {
        match self {
            Self::Committed(result) => Ok(result),
            Self::Conflict { .. } => Err(MutationError::WriteConflict),
        }
    }
}

#[async_trait]
/// Semantic persistence boundary for canonical entity data, events,
/// and model-specific query materializations.
///
/// Physical tables, object stores, trees, naming registries, and
/// entity-to-model association layouts remain private to each implementation.
pub trait StorageEngine: Send + Sync {
    /// The engine's native value representation.
    type Value;

    /// This engine's transaction handle, borrowing only its engine handle.
    type Transaction<'a>: StorageTransaction + 'a
    where Self: 'a;

    /// Begin an atomic storage transaction; the engine controls native transaction timing.
    fn transaction(&self) -> Self::Transaction<'_>;

    /// Retrieve canonical state by entity identity.
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError>;

    /// Keep existing identities whose stored entities match the predicate.
    /// SQL engines can test materializations without decoding entity state.
    async fn filter_entity_ids(&self, ids: &[EntityId], predicate: &Predicate<Resolved>) -> Result<Vec<EntityId>, RetrievalError> {
        let mut matches = Vec::new();
        let ids = ids.iter().copied().collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        for result in self.get_states(ids, predicate).await? {
            if let GetStateResult::Found(state) = result {
                matches.push(state.payload.entity_id);
            }
        }
        Ok(matches)
    }

    /// Fetch entities matching the selection, including its model-membership predicates.
    async fn fetch_states(
        &self,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    /// Retrieve one outcome per requested identity, in input order, distinguishing absence from predicate mismatch.
    /// Engines may batch reads or join materializations; existence and predicate checks must use the same state.
    async fn get_states(&self, ids: Vec<EntityId>, predicate: &Predicate<Resolved>) -> Result<Vec<GetStateResult>, RetrievalError> {
        let mut states = Vec::with_capacity(ids.len());
        for id in ids {
            states.push(match self.get_state(id).await {
                Ok(state) => GetStateResult::matching(state, predicate)?,
                Err(RetrievalError::EntityNotFound(_)) => GetStateResult::NotFound(id),
                Err(e) => return Err(e),
            });
        }
        Ok(states)
    }

    /// Retrieve events whose entities match the predicate; omit missing or nonmatching events.
    /// `True` does not require local entity state, so event-DAG reconstruction can read events before state exists.
    async fn get_events(&self, event_ids: Vec<EventId>, predicate: &Predicate<Resolved>) -> Result<Vec<Attested<Event>>, RetrievalError>;

    /// Retrieve all events for an entity.
    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError>;

    /// Delete all engine-owned entity, event, association, materialization,
    /// and physical-name data while retaining engine compatibility metadata.
    async fn delete_all(&self) -> Result<bool, MutationError>;

    /// List existing model materializations without creating any.
    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> { Ok(Vec::new()) }

    /// Supply optional labels for engines to seed their own persistent physical names.
    /// Injected after node construction; retained weakly to avoid an ownership cycle.
    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn crate::schema::CatalogResolver>) { let _ = resolver; }
}
