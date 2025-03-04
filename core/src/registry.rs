use std::sync::Arc;

use dashmap::{DashMap, Entry};

use crate::entity::{Entity, WeakEntity};
use crate::error::{MutationError, RetrievalError};
use crate::proto;

#[derive(Debug, Clone)]
pub(crate) struct EntityRegistry(Arc<DashMap<proto::ID, WeakEntity>>);

impl EntityRegistry {
    pub fn new() -> Self { Self(Arc::new(DashMap::new())) }

    /// This should be called only by the transaction commit for newly created Entities
    /// This is necessary because Entities created in a transaction scope have no upstream
    /// so when they hand out read Entities, they have to work immediately.
    /// TODO: Discuss. The upside is that you can call .read() on a Mutable. The downside is that the behavior is inconsistent
    /// between newly created Entities and Entities that are created in a transaction scope.
    #[must_use]
    pub fn insert(&self, entity: Entity) -> Result<(), MutationError> {
        match self.0.entry(entity.id().clone()) {
            Entry::Vacant(entry) => {
                entry.insert(entity.weak());
                Ok(())
            }
            Entry::Occupied(_) => Err(MutationError::AlreadyExists),
        }
    }

    /// Register a Entity with the node, with the intention of preventing duplicate resident entities.
    /// Returns true if the Entity was already present.
    /// Note that this does not actually store the entity in the storage engine.
    #[must_use]
    pub fn assert(&self, collection_id: &proto::CollectionId, id: proto::ID, state: &proto::State) -> Result<Entity, RetrievalError> {
        match self.0.entry(id) {
            Entry::Occupied(mut entry) => {
                if let Some(entity) = entry.get().upgrade() {
                    entity.apply_state(state)?;
                    Ok(entity)
                } else {
                    let entity = Entity::from_state(id, collection_id.clone(), state)?;

                    entry.insert(entity.weak());
                    Ok(entity)
                }
            }
            Entry::Vacant(entry) => {
                let entity = Entity::from_state(id, collection_id.clone(), state)?;
                entry.insert(entity.weak());
                Ok(entity)
            }
        }
    }

    pub fn get(&self, id: &proto::ID) -> Option<Entity> {
        //
        self.0.get(id).and_then(|entry| entry.upgrade())
    }
}
