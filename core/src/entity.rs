use crate::lineage;
use crate::{
    error::{LineageError, MutationError, RetrievalError, StateError},
    model::View,
    property::{Backends, PropertyValue},
    storage::StorageCollectionWrapper,
    Node,
};
use ankql::selection::filter::Filterable;
use ankurah_proto::{clock, Clock, CollectionId, EntityId, Event, EventId, OperationSet, State};
use anyhow::anyhow;
use std::collections::{btree_map::Entry, BTreeMap};
use std::sync::{Arc, Weak};
use tracing::debug;

/// An entity represents a unique thing within a collection. Entity can only be constructed via a WeakEntitySet
/// which provides duplication guarantees.
#[derive(Debug, Clone)]
pub struct Entity(Arc<EntityInner>);

// TODO optimize this to be faster for scanning over entries in a collection
/// Used only for reconstituting state to filter database results. No duplication guarantees are provided
pub struct TemporaryEntity(Arc<EntityInner>);

#[derive(Debug)]
pub struct EntityInner {
    pub id: EntityId,
    pub collection: CollectionId,
    pub(crate) backends: Backends,
    head: std::sync::Mutex<Clock>,
    pub(crate) upstream: Option<Entity>,
}

impl std::ops::Deref for Entity {
    type Target = EntityInner;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::ops::Deref for TemporaryEntity {
    type Target = EntityInner;

    fn deref(&self) -> &Self::Target { &self.0 }
}

/// A weak reference to an entity
pub struct WeakEntity(Weak<EntityInner>);

impl WeakEntity {
    pub fn upgrade(&self) -> Option<Entity> { self.0.upgrade().map(|inner| Entity(inner)) }
}

impl Entity {
    pub fn id(&self) -> EntityId { self.id.clone() }

    // This is intentionally private - only WeakEntitySet should be constructing Entities
    fn weak(&self) -> WeakEntity { WeakEntity(Arc::downgrade(&self.0)) }

    pub fn collection(&self) -> &CollectionId { &self.collection }

    pub fn backends(&self) -> &Backends { &self.backends }

    pub fn head(&self) -> Clock { self.head.lock().unwrap().clone() }

    pub fn to_state(&self) -> Result<State, StateError> { self.backends.to_state_buffers() }

    // used by the Model macro
    pub fn create(id: EntityId, collection: CollectionId, backends: Backends) -> Self {
        Self(Arc::new(EntityInner { id, collection, backends, head: std::sync::Mutex::new(Clock::default()), upstream: None }))
    }

    /// This must remain private - ONLY WeakEntitySet should be constructing Entities
    fn from_state(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(state)?;
        Ok(Self(Arc::new(EntityInner { id, collection, backends, head: std::sync::Mutex::new(state.head.clone()), upstream: None })))
    }

    /// Collect an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit.
    /// TODO: We need to think about rollbacks
    pub fn commit(&self) -> Result<Option<Event>, MutationError> {
        let operations = self.backends.to_operations()?;
        if operations.is_empty() {
            Ok(None)
        } else {
            let event = {
                let mut head = self.head.lock().unwrap();
                let operations = OperationSet(operations);
                let event = Event {
                    // id: EventId::from_parts(&self.id, &operations, &head),
                    entity_id: self.id.clone(),
                    collection: self.collection.clone(),
                    operations,
                    parent: head.clone(),
                };

                println!("Entity.commit pre-lock: {:?}", event);
                // Set the head to the event's ID
                *head = Clock::new([event.id()]);
                println!("Entity.commit post-lock: {:?}", event);
                event
            };

            debug!("Entity.commit {}", self);
            Ok(Some(event))
        }
    }

    pub fn view<V: View>(&self) -> Option<V> {
        if self.collection() != &V::collection() {
            None
        } else {
            Some(V::from_entity(self.clone()))
        }
    }

    /// Attempt to apply an event to the entity
    #[cfg_attr(feature = "instrument", tracing::instrument(level="debug", skip_all, fields(entity = %self, event = %event)))]
    pub async fn apply_event(&self, collection: &StorageCollectionWrapper, event: &Event) -> Result<bool, MutationError> {
        let head = self.head();
        let new_head = event.id().into();
        match crate::lineage::compare(collection, &head, &new_head, 100).await? {
            lineage::Ordering::Equal => {
                debug!("Equal - skip");
                return Ok(false);
            }
            lineage::Ordering::Descends => {
                debug!("Descends - apply");
                for (backend_name, operations) in event.operations.iter() {
                    // TODO - backends and Entity should not have two copies of the head. Figure out how to unify them
                    self.backends.apply_operations((*backend_name).to_owned(), operations, &new_head, &event.parent /* , context*/)?;
                }
                *self.head.lock().unwrap() = new_head.clone();
                *self.backends.head.lock().unwrap() = new_head;
                Ok(true)
            }
            lineage::Ordering::NotDescends { meet } => {
                debug!("NotDescends - skip");
                return Ok(false);
            }
            lineage::Ordering::Incomparable => {
                // total apples and oranges - take a hike buddy
                Err(LineageError::Incomparable.into())
            }
            lineage::Ordering::PartiallyDescends { meet } => {
                // TODO - figure out how to handle this. I don't think we need to materialize a new event
                // but it requires that we update the propertybackends to support this
                Err(LineageError::PartiallyDescends { meet }.into())
            }
            lineage::Ordering::BudgetExceeded { subject_frontier, other_frontier } => {
                Err(LineageError::BudgetExceeded { subject_frontier, other_frontier }.into())
            }
        }
    }

    pub async fn apply_state(&self, collection: &StorageCollectionWrapper, state: &State) -> Result<bool, MutationError> {
        let head = self.head();
        let new_head = state.head.clone();

        match crate::lineage::compare(collection, &head, &new_head, 100).await? {
            lineage::Ordering::Equal => {
                debug!("Equal - skip");
                return Ok(false);
            }
            lineage::Ordering::Descends => {
                self.backends.apply_state(&state)?;
                *self.head.lock().unwrap() = new_head;
                Ok(true)
            }
            lineage::Ordering::NotDescends { meet } => {
                debug!("NotDescends - skip {self} meet: {meet:?}");
                return Ok(false);
            }
            lineage::Ordering::Incomparable => {
                // total apples and oranges - take a hike buddy
                Err(LineageError::Incomparable.into())
            }
            lineage::Ordering::PartiallyDescends { meet } => {
                // TODO - figure out how to handle this. I don't think we need to materialize a new event
                // but it requires that we update the propertybackends to support this
                Err(LineageError::PartiallyDescends { meet }.into())
            }
            lineage::Ordering::BudgetExceeded { subject_frontier, other_frontier } => {
                Err(LineageError::BudgetExceeded { subject_frontier, other_frontier }.into())
            }
        }
    }

    /// Create a snapshot of the Entity which is detached from this one, and will not receive the updates this one does
    pub fn snapshot(&self) -> Self {
        Self(Arc::new(EntityInner {
            id: self.id.clone(),
            collection: self.collection.clone(),
            backends: self.backends.fork(),
            head: std::sync::Mutex::new(self.head.lock().unwrap().clone()),
            upstream: Some(self.clone()),
        }))
    }

    pub fn values(&self) -> Vec<(String, Option<PropertyValue>)> {
        let backends = self.backends.backends.lock().unwrap();
        backends
            .values()
            .flat_map(|backend| {
                backend
                    .property_values()
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.clone()))
                    .collect::<Vec<(String, Option<PropertyValue>)>>()
            })
            .collect()
    }
}

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity({}/{}) = {}", self.collection, self.id, self.head.lock().unwrap())
    }
}

impl Filterable for Entity {
    fn collection(&self) -> &str { self.collection.as_str() }

    /// TODO Implement this as a typecasted value. eg value<T> -> Option<Result<T>>
    /// where None is returned if the property is not found, and Err is returned if the property is found but is not able to be typecasted
    /// to the requested type. (need to think about the rust type system here more)
    fn value(&self, name: &str) -> Option<String> {
        if name == "id" {
            Some(self.id.to_string())
        } else {
            // Iterate through backends to find one that has this property
            let backends = self.backends.backends.lock().unwrap();
            backends.values().find_map(|backend| match backend.property_value(&name.to_owned()) {
                Some(value) => match value {
                    PropertyValue::String(s) => Some(s),
                    PropertyValue::I16(i) => Some(i.to_string()),
                    PropertyValue::I32(i) => Some(i.to_string()),
                    PropertyValue::I64(i) => Some(i.to_string()),
                    PropertyValue::Object(items) => Some(String::from_utf8_lossy(&items).to_string()),
                    PropertyValue::Binary(items) => Some(String::from_utf8_lossy(&items).to_string()),
                },
                None => None,
            })
        }
    }
}

impl TemporaryEntity {
    pub fn new(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(state)?;
        Ok(Self(Arc::new(EntityInner { id, collection, backends, head: std::sync::Mutex::new(state.head.clone()), upstream: None })))
    }
}

// TODO - clean this up and consolidate with Entity somehow, while still preventing anyone from creating unregistered (non-temporary) Entities
impl Filterable for TemporaryEntity {
    fn collection(&self) -> &str { self.0.collection.as_str() }

    fn value(&self, name: &str) -> Option<String> {
        if name == "id" {
            Some(self.0.id.to_string())
        } else {
            // Iterate through backends to find one that has this property
            let backends = self.0.backends.backends.lock().unwrap();
            backends.values().find_map(|backend| match backend.property_value(&name.to_owned()) {
                Some(value) => match value {
                    PropertyValue::String(s) => Some(s),
                    PropertyValue::I16(i) => Some(i.to_string()),
                    PropertyValue::I32(i) => Some(i.to_string()),
                    PropertyValue::I64(i) => Some(i.to_string()),
                    PropertyValue::Object(items) => Some(String::from_utf8_lossy(&items).to_string()),
                    PropertyValue::Binary(items) => Some(String::from_utf8_lossy(&items).to_string()),
                },
                None => None,
            })
        }
    }
}

impl std::fmt::Display for TemporaryEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TemporaryEntity({}/{}) = {}", &self.collection, self.id, self.head.lock().unwrap())
    }
}

/// A set of entities held weakly
#[derive(Clone, Default)]
pub struct WeakEntitySet(Arc<std::sync::RwLock<BTreeMap<EntityId, WeakEntity>>>);
impl WeakEntitySet {
    pub fn get(&self, id: EntityId) -> Option<Entity> {
        let entities = self.0.read().unwrap();
        // TODO: call policy agent with cdata
        if let Some(entity) = entities.get(&id) {
            entity.upgrade()
        } else {
            None
        }
    }

    /// Create a brand new entity, and add it to the set
    pub fn create(&self, collection: CollectionId) -> Entity {
        let mut entities = self.0.write().unwrap();
        let id = EntityId::new();
        let entity = Entity::create(id, collection, Backends::new());
        entities.insert(id, entity.weak());
        entity
    }

    pub async fn with_state(
        &self,
        collection: &StorageCollectionWrapper,
        id: EntityId,
        collection_id: CollectionId,
        state: State,
    ) -> Result<Entity, RetrievalError> {
        let entity = {
            let mut entities = self.0.write().unwrap();
            match entities.entry(id) {
                Entry::Vacant(_) => {
                    let entity = Entity::from_state(id, collection_id.to_owned(), &state)?;
                    entities.insert(id, entity.weak());
                    entity
                }
                Entry::Occupied(o) => match o.get().upgrade() {
                    Some(entity) => {
                        if entity.collection != collection_id {
                            return Err(RetrievalError::Anyhow(anyhow!("collection mismatch {} {collection_id}", entity.collection)));
                        }
                        entity
                    }
                    None => {
                        let entity = Entity::from_state(id, collection_id.to_owned(), &state)?;
                        entities.insert(id, entity.weak());
                        // just created the entity with the state, so we do not need to apply the state
                        return Ok(entity);
                    }
                },
            }
        };

        // if we're here, we've retrieved the entity from the set and need to apply the state
        entity.apply_state(collection, &state).await?;

        Ok(entity)
    }
}
