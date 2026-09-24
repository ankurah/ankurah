use super::{entity::Entity, state::{EntityInnerState, EntityState, StateApplyResult}};
use crate::{error::{MutationError, RetrievalError}, retrieval::{GetEvents, GetState}, schema::SystemEpoch};
use ankurah_proto::{EntityId, State};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, Weak},
};

/// The node's committed instance of an entity, constructed only by its registry.
pub struct EntityInner {
    pub(super) id: EntityId,
    pub(super) state: EntityState,
    /// Keeps the weak registration alive and prevents construction outside this module.
    registry: WeakEntitySet,
}

impl EntityInner {
    pub(super) fn system_epoch(&self) -> SystemEpoch { self.registry.system_epoch }
}

impl std::fmt::Debug for EntityInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityInner")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("system_epoch", &self.system_epoch())
            .finish()
    }
}

/// The node's identity registry; only committed Primary instances belong here.
#[derive(Clone)]
pub struct WeakEntitySet {
    entities: Arc<RwLock<BTreeMap<EntityId, Weak<EntityInner>>>>,
    system_epoch: SystemEpoch,
}

impl WeakEntitySet {
    pub(crate) fn new(system_epoch: SystemEpoch) -> Self {
        Self { entities: Arc::new(RwLock::new(BTreeMap::new())), system_epoch }
    }

    pub(crate) fn system_epoch(&self) -> SystemEpoch { self.system_epoch }

    pub fn get(&self, id: &EntityId) -> Option<Entity> {
        self.entities.read().unwrap().get(id)?.upgrade().map(Entity::Primary)
    }

    /// Deliberately load invalid state so adversarial tests can exercise phantom-entity rejection.
    #[cfg(feature = "test-helpers")]
    pub fn conjure_evil_phantom(&self, id: EntityId, model: ankurah_proto::ModelId) -> Entity {
        let state = State { memberships: [model].into_iter().collect(), ..State::default() };
        Entity::Primary(self.get_or_insert_state(id, &state).unwrap().1)
    }

    pub async fn get_or_retrieve<S, E>(&self, state_getter: &S, event_getter: &E, id: &EntityId) -> Result<Option<Entity>, RetrievalError>
    where S: GetState + Send + Sync, E: GetEvents + Send + Sync {
        match self.get(id) {
            Some(entity) => Ok(Some(entity)),
            None => match state_getter.get_state(*id).await? {
                None => Ok(None),
                Some(state) => Ok(Some(self.with_state(state_getter, event_getter, *id, state.payload.state).await?.1)),
            },
        }
    }

    /// Register a persisted creation, transferring its prepared state without decoding it again.
    /// Returns whether the primary already existed; only a new primary takes the prepared state.
    pub(super) fn publish_new(&self, id: EntityId, state: &EntityState) -> Result<(bool, Arc<EntityInner>), MutationError> {
        if state.head().is_empty() { return Err(MutationError::PhantomEntity(id)); }
        let mut entities = self.entities.write().unwrap();
        if let Some(entity) = entities.get(&id).and_then(Weak::upgrade) { return Ok((true, entity)); }
        let entity = Arc::new(EntityInner { id, state: EntityState::new(state.take()), registry: self.clone() });
        entities.insert(id, Arc::downgrade(&entity));
        Ok((false, entity))
    }

    fn get_or_insert_state(&self, id: EntityId, state: &State) -> Result<(bool, Arc<EntityInner>), RetrievalError> {
        let mut entities = self.entities.write().unwrap();
        if let Some(entity) = entities.get(&id).and_then(Weak::upgrade) { return Ok((true, entity)); }
        let state = EntityState::new(EntityInnerState::from_state(state)?);
        let entity = Arc::new(EntityInner { id, state, registry: self.clone() });
        entities.insert(id, Arc::downgrade(&entity));
        Ok((false, entity))
    }

    /// Reconstitute or merge committed state without replacing a retained primary.
    /// Returns `(changed, entity)`; `changed` is `None` when first loading the entity.
    pub async fn with_state<S, E>(
        &self, state_getter: &S, event_getter: &E, id: EntityId, state: State,
    ) -> Result<(Option<bool>, Entity), RetrievalError>
    where S: GetState + Send + Sync, E: GetEvents + Send + Sync {
        let entity = match self.get(&id) {
            Some(entity) => entity.primary()?,
            None => {
                if let Some(stored) = state_getter.get_state(id).await? {
                    self.get_or_insert_state(id, &stored.payload.state)?.1
                } else {
                    match self.get_or_insert_state(id, &state)? {
                        (true, entity) => entity,
                        (false, entity) => return Ok((None, Entity::Primary(entity))),
                    }
                }
            }
        };
        let result = entity.state.apply_state(event_getter, &state).await?;
        Ok((Some(matches!(result, StateApplyResult::Applied)), Entity::Primary(entity)))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::{entity::{LocalTrxEntity, TemporaryEntity}, property::backend::{LWWBackend, PropertyBackend}, value::Value};
    use ankurah_proto::{AuthorId, EventId, PropertyId};
    use std::sync::atomic::AtomicBool;

    #[test]
    fn resident_and_temporary_values_preserve_property_ids_and_missing_values() -> anyhow::Result<()> {
        let entities = WeakEntitySet::new(SystemEpoch::allocate());
        let state = EntityState::default();
        let backend = state.get_backend::<LWWBackend>()?;
        let expected = BTreeMap::from([
            (PropertyId::EntityId(EntityId::from_bytes([2; 32])), Some(Value::String("value".into()))),
            (PropertyId::System(ankurah_proto::SystemProperty::Name), None),
        ]);
        for (property, value) in &expected { backend.set(*property, value.clone()); }
        let event_id = EventId::from_bytes([3; 32]);
        backend.apply_operations_with_event(&backend.to_operations()?.unwrap(), event_id.clone())?;
        state.set_head(event_id.into());
        let entity = Entity::Primary(entities.get_or_insert_state(EntityId::from_bytes([1; 32]), &state.to_state()?)?.1);
        assert_eq!(entity.values()?, expected.into_iter().collect::<Vec<_>>());
        let temporary = TemporaryEntity::new(entity.id(), &entity.to_state()?)?;
        assert_eq!(temporary.values(), entity.values()?);
        Ok(())
    }

    #[test]
    fn forks_retain_their_primary_and_nodes_keep_independent_instances() -> anyhow::Result<()> {
        let original = WeakEntitySet::new(SystemEpoch::allocate());
        let replacement = WeakEntitySet::new(SystemEpoch::allocate());
        let id = EntityId::from_bytes([1; 32]);
        let state = State { head: EventId::from_bytes([2; 32]).into(), ..State::default() };
        let resident = Entity::Primary(original.get_or_insert_state(id, &state)?.1);
        let fresh = Entity::Primary(replacement.get_or_insert_state(id, &state)?.1);
        assert_ne!(resident, fresh);
        assert_ne!(resident.system_epoch(), fresh.system_epoch());
        let fork = LocalTrxEntity::edit(&resident, AuthorId::Unknown, Arc::new(AtomicBool::new(true)))?;
        drop(resident);
        assert!(original.get(&id).is_some());
        drop(fork);
        assert!(original.get(&id).is_none());
        assert_eq!(replacement.get(&id), Some(fresh.clone()));
        let registry = Arc::downgrade(&replacement.entities);
        drop(replacement);
        assert_eq!(fresh.primary()?.registry.get(&id), Some(fresh.clone()));
        drop(fresh);
        assert!(registry.upgrade().is_none(), "the registry must not form a cycle with its primaries");
        Ok(())
    }
}
