use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use tracing::warn;

use crate::error::{MutationError, RetrievalError};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, Event, EventId, ModelId};

/// Helpers for seeding, sanitizing, and deduplicating engine-private names.
pub mod naming;
// The AST->AST rewrite that used to substitute physical addressing names (a
// now-deleted pass) is gone: the AST stays purely logical, and each engine
// resolves a property's physical address from its own durable PropertyId-keyed
// map at emit time (SQL builders for sqlite/postgres; the planner integration
// for sled/idb).

/// One storage-ready canonical entity replacement.
///
/// `state` is the result of applying the transaction's already-durable events
/// to the canonical state identified by `expected_head`. `associate_with`
/// contains model usages introduced by this transaction; the engine unions
/// them with its complete durable association set before refreshing
/// materializations.
#[derive(Debug, Clone)]
pub struct PreparedEntityWrite {
    /// Exact canonical version from which `state` was derived.
    ///
    /// A missing canonical record has the empty genesis clock.
    pub expected_head: Clock,
    /// Proposed canonical state and head.
    pub state: Attested<EntityState>,
    /// Model usages which must be durably associated on commit.
    pub associate_with: BTreeSet<ModelId>,
}

impl PreparedEntityWrite {
    /// Construct a storage-ready entity write.
    pub fn new(expected_head: Clock, state: Attested<EntityState>, associate_with: impl IntoIterator<Item = ModelId>) -> Self {
        Self { expected_head, state, associate_with: associate_with.into_iter().collect() }
    }
}

/// An all-or-nothing collection of prepared canonical entity writes.
#[derive(Debug, Clone, Default)]
pub struct StorageWriteBatch {
    /// At most one final write per entity identity.
    pub entities: Vec<PreparedEntityWrite>,
}

impl StorageWriteBatch {
    /// Construct a batch from prepared entity writes.
    pub fn new(entities: Vec<PreparedEntityWrite>) -> Self { Self { entities } }
}

/// Successful persistence details for one prepared entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedEntityWrite {
    /// Canonical entity identity.
    pub entity_id: EntityId,
    /// Whether the canonical head changed.
    pub canonical_changed: bool,
    /// Model associations newly inserted by this commit.
    pub associations_added: Vec<ModelId>,
    /// Complete model set refreshed from canonical state.
    pub materialized_as: Vec<ModelId>,
}

/// Result of an atomically committed prepared-state batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageCommitResult {
    /// Results in the same order as the submitted entity writes.
    pub entities: Vec<CommittedEntityWrite>,
}

/// Outcome of attempting an exact-head prepared-state batch.
#[derive(Debug, Clone)]
pub enum CommitBatchOutcome {
    /// Every expectation matched and the entire batch committed.
    Committed(StorageCommitResult),
    /// At least one expectation differed, so the engine rolled back the
    /// complete batch. Values are canonical states observed while checking
    /// this attempt; `None` represents an unexpectedly missing entity.
    Conflict { observed: BTreeMap<EntityId, Option<Attested<EntityState>>> },
}

#[async_trait]
/// Semantic persistence boundary for canonical entity data, canonical events,
/// and model-specific query materializations.
///
/// Physical tables, object stores, trees, naming registries, and
/// entity-to-model association layouts remain private to each implementation.
pub trait StorageEngine: Send + Sync {
    /// The engine's native value representation.
    type Value;

    /// Blindly append validated, attested, model-independent events.
    ///
    /// Inserts are idempotent by `EventId`; results preserve input order and
    /// report whether each event was newly inserted.
    async fn append_events(&self, events: &[Attested<Event>]) -> Result<Vec<bool>, MutationError>;

    /// Atomically compare and replace canonical entities, add model
    /// associations, and refresh every affected materialization.
    ///
    /// One mismatched expectation rolls back the complete batch and returns
    /// the canonical states needed for core's causal true-up and retry.
    async fn commit_batch(&self, batch: StorageWriteBatch) -> Result<CommitBatchOutcome, MutationError>;

    /// Retrieve canonical state by entity identity.
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError>;

    /// Fetch canonical states through one model's materialized query surface.
    async fn fetch_states(&self, model: &ModelId, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    /// Retrieve every existing state among `ids`, omitting missing entities.
    async fn get_states(&self, ids: Vec<EntityId>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let mut states = Vec::new();
        for id in ids {
            match self.get_state(id).await {
                Ok(state) => states.push(state),
                Err(RetrievalError::EntityNotFound(_)) => {
                    warn!("Entity not found: {:?}", id);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(states)
    }

    /// Retrieve canonical events by identity.
    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError>;

    /// Retrieve every canonical event for an entity.
    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError>;

    /// Delete all engine-owned entity, event, association, materialization,
    /// and physical-name data while retaining engine compatibility metadata.
    async fn delete_all(&self) -> Result<bool, MutationError>;

    /// List the models that already have durable storage, WITHOUT
    /// creating any. Used by the catalog manager to warm only the catalog
    /// materializations that actually exist, so a schema-less node never
    /// materializes empty `_ankurah_*` trees. The default returns empty,
    /// which is always safe: a caller then simply skips its existence-gated
    /// warm (falling back to live subscription updates) rather than
    /// misbehaving. Engines override this with their real list.
    async fn list_materializations(&self) -> Result<Vec<ModelId>, RetrievalError> { Ok(Vec::new()) }

    /// Inject the catalog resolver. Engines that maintain
    /// human-named materialized structures (postgres/sqlite tables and columns,
    /// indexeddb materialization names and fields) may seed their DURABLE
    /// identity-to-physical-name maps from it; the maps stay engine-owned, the
    /// resolver is only the name source. Sled's identity-derived model tree
    /// names and numeric property slots do not need human-name hints. Called
    /// once from `Node` construction, after the
    /// catalog exists -- the engine object is constructed before the node, so
    /// this cannot be a constructor argument. Weak: the engine must not keep
    /// the catalog (and thus the node) alive. Default no-op for engines with
    /// no human-named structures (memory/test engines).
    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn crate::schema::CatalogResolver>) { let _ = resolver; }
}
