use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use crate::error::{MutationError, RetrievalError};
use ankurah_proto::{Attested, CollectionId, EntityId, EntityState, Event, EventId, SystemRootProof};

/// Result of the storage-wide atomic system-root claim operation.
///
/// The claim is engine metadata, not a collection entity. It serializes root
/// creation across independent Node/SystemManager instances and, for engines
/// that support it, across processes sharing the same database.
#[derive(Debug, Clone, PartialEq)]
pub enum SystemRootClaim {
    /// This caller atomically installed the candidate root.
    Claimed,
    /// A claim already existed; its root is returned without modification.
    Existing(SystemRootProof),
}

pub fn state_name(name: &str) -> String { format!("{}_state", name) }

pub fn event_name(name: &str) -> String { format!("{}_event", name) }

/// The model-definition id a storage bucket stamps on wire envelopes it
/// reconstructs from stored fragments (#330): well-known system/catalog ids
/// first (answerable stone-cold, which is how the catalog itself warms from
/// storage), then the injected catalog resolver. An error means a
/// user-collection envelope is being reconstructed before the catalog warmed;
/// readiness gating makes that unreachable in steady state, and failing loud
/// beats stamping a wrong id.
pub fn bucket_model_id(
    collection: &CollectionId,
    resolver: Option<&dyn crate::schema::CatalogResolver>,
) -> Result<EntityId, RetrievalError> {
    crate::schema::well_known_model_id(collection.as_str())
        .or_else(|| resolver.and_then(|r| r.model_id_for(collection.as_str())))
        .ok_or_else(|| RetrievalError::Other(format!("no model id known for collection '{collection}' (catalog cold?)")))
}

mod column_space;
pub mod naming;
pub use column_space::selection_to_column_space;

#[async_trait]
pub trait StorageEngine: Send + Sync {
    type Value;
    // Opens and/or creates a storage collection.
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError>;
    // Delete all collections and their data from the storage engine
    async fn delete_all_collections(&self) -> Result<bool, MutationError>;

    /// Atomically install `candidate` iff no system-root claim exists.
    async fn claim_system_root(&self, candidate: &SystemRootProof) -> Result<SystemRootClaim, MutationError>;

    /// Read the currently persisted system-root claim, if any.
    async fn system_root_claim(&self) -> Result<Option<SystemRootProof>, RetrievalError>;

    /// Atomically remove the claim only when it still equals `expected`.
    /// Used to clean up a failed create without deleting a winner installed by
    /// another caller.
    async fn release_system_root_claim(&self, expected: &SystemRootProof) -> Result<bool, MutationError>;

    /// List the collections that already have durable storage, WITHOUT
    /// creating any. Used by the catalog manager to warm only the catalog
    /// collections that actually exist, so a schema-less node never
    /// materializes empty `_ankurah_*` trees. The default returns empty,
    /// which is always safe: a caller then simply skips its existence-gated
    /// warm (falling back to live subscription updates) rather than
    /// misbehaving. Engines override this with their real list.
    async fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> { Ok(Vec::new()) }

    /// Inject the catalog resolver. Engines that maintain
    /// human-named materialized structures (postgres/sqlite/indexeddb columns,
    /// sled property slots) seed their DURABLE id-to-name maps from it at
    /// materialization time; the maps stay engine-owned, the resolver is only
    /// the name source. Called once from `Node` construction, after the
    /// catalog exists -- the engine object is constructed before the node, so
    /// this cannot be a constructor argument. Weak: the engine must not keep
    /// the catalog (and thus the node) alive. Default no-op for engines with
    /// no human-named structures (memory/test engines).
    fn set_catalog_resolver(&self, resolver: std::sync::Weak<dyn crate::schema::CatalogResolver>) { let _ = resolver; }
}

#[async_trait]
pub trait StorageCollection: Send + Sync {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError>;
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError>;

    // Fetch raw entity states matching a selection (predicate + order by + limit)
    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    async fn set_states(&self, states: Vec<Attested<EntityState>>) -> Result<(), MutationError> {
        for state in states {
            self.set_state(state).await?;
        }
        Ok(())
    }

    async fn get_states(&self, ids: Vec<EntityId>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let mut states = Vec::new();
        for id in ids {
            match self.get_state(id).await {
                Ok(state) => states.push(state),
                Err(RetrievalError::EntityNotFound(_)) => {
                    warn!("Entity not found: {:?}", id);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(states)
    }

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError>;

    /// Retrieve a list of events
    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError>;

    /// Retrieve all events from the collection
    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError>;
}

/// Manages the storage and state of the collection without any knowledge of the model type
#[derive(Clone)]
pub struct StorageCollectionWrapper(pub(crate) Arc<dyn StorageCollection>);

/// Storage interface for a collection
impl StorageCollectionWrapper {
    pub fn new(bucket: Arc<dyn StorageCollection>) -> Self { Self(bucket) }
}

impl std::ops::Deref for StorageCollectionWrapper {
    type Target = Arc<dyn StorageCollection>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
