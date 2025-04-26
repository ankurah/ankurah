use crate::{error::RetrievalError, property::Backends, property::PropertyValue};
use ankql::selection::filter::Filterable;
use ankurah_proto::{Clock, CollectionId, Event, State, ID};
use anyhow::{anyhow, Result};
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
    pub id: ID,
    pub collection: CollectionId,
    pub(crate) backends: Backends,
    head: std::sync::Mutex<Clock>,
    pub upstream: Option<Entity>,
}

impl std::ops::Deref for Entity {
    type Target = EntityInner;

    fn deref(&self) -> &Self::Target { &self.0 }
}

/// A weak reference to an entity
pub struct WeakEntity(Weak<EntityInner>);

impl WeakEntity {
    pub fn upgrade(&self) -> Option<Entity> { self.0.upgrade().map(|inner| Entity(inner)) }
}

impl Entity {
    fn weak(&self) -> WeakEntity { WeakEntity(Arc::downgrade(&self.0)) }

    pub fn collection(&self) -> CollectionId { self.collection.clone() }

    pub fn backends(&self) -> &Backends { &self.backends }

    pub fn to_state(&self) -> Result<State> { self.backends.to_state_buffers() }

    // used by the Model macro
    pub fn create(id: ID, collection: CollectionId, backends: Backends) -> Self {
        Self(Arc::new(EntityInner { id, collection, backends, head: std::sync::Mutex::new(Clock::default()), upstream: None }))
    }

    /// This must remain private - ONLY WeakEntitySet should be constructing Entities
    fn from_state(id: ID, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(state)?;
        Ok(Self(Arc::new(EntityInner { id, collection, backends, head: std::sync::Mutex::new(state.head.clone()), upstream: None })))
    }

    /// Collect an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit.
    /// TODO: We need to think about rollbacks
    pub fn commit(&self) -> Result<Option<Event>> {
        let operations = self.backends.to_operations()?;
        if operations.is_empty() {
            Ok(None)
        } else {
            let event = {
                let event = Event {
                    id: ID::new(),
                    entity_id: self.id.clone(),
                    collection: self.collection.clone(),
                    operations,
                    parent: self.head.lock().unwrap().clone(),
                };

                // Set the head to the event's ID
                *self.head.lock().unwrap() = Clock::new([event.id]);
                event
            };

            debug!("Entity.commit {}", self);
            Ok(Some(event))
        }
    }

    pub fn apply_event(&self, event: &Event) -> Result<()> {
        /*
           case A: event precursor descends the current head, then set entity clock to singleton of event id
           case B: event precursor is concurrent to the current head, push event id to event head clock.
           case C: event precursor is descended by the current head
        */
        let head = Clock::new([event.id]);
        for (backend_name, operations) in &event.operations {
            // TODO - backends and Entity should not have two copies of the head. Figure out how to unify them
            self.backends.apply_operations((*backend_name).to_owned(), operations, &head, &event.parent /* , context*/)?;
        }
        // TODO figure out how to test this
        debug!("Entity.apply_event {}", event);

        *self.head.lock().unwrap() = head.clone();
        // Hack
        *self.backends.head.lock().unwrap() = head;

        Ok(())
    }

    /// HACK - we probably shouldn't be stomping on the backends like this
    pub fn apply_state(&self, state: &State) -> Result<(), RetrievalError> {
        self.backends.apply_state(state)?;
        Ok(())
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
                    PropertyValue::Bool(i) => Some(i.to_string()),
                    PropertyValue::Object(items) => Some(String::from_utf8_lossy(&items).to_string()),
                    PropertyValue::Binary(items) => Some(String::from_utf8_lossy(&items).to_string()),
                    PropertyValue::DateTime(date_time) => Some(date_time.format("%Y-%m-%dT%H:%M:%S%.f%z").to_string()),
                },
                None => None,
            })
        }
    }
}

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity({}/{}) = {}", self.collection, self.id, self.head.lock().unwrap())
    }
}

impl TemporaryEntity {
    pub fn new(id: ID, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
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
                    PropertyValue::Bool(i) => Some(i.to_string()),
                    PropertyValue::Object(items) => Some(String::from_utf8_lossy(&items).to_string()),
                    PropertyValue::Binary(items) => Some(String::from_utf8_lossy(&items).to_string()),
                    PropertyValue::DateTime(date_time) => Some(date_time.format("%Y-%m-%dT%H:%M:%S%.f%z").to_string()),
                },
                None => None,
            })
        }
    }
}

/// A set of entities held weakly
#[derive(Clone, Default)]
pub struct WeakEntitySet(Arc<std::sync::RwLock<BTreeMap<ID, WeakEntity>>>);
impl WeakEntitySet {
    pub fn get(&self, id: ID) -> Option<Entity> {
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
        let id = ID::new();
        let entity = Entity::create(id, collection, Backends::new());
        entities.insert(id, entity.weak());
        entity
    }

    pub fn with_state(&self, id: ID, collection_id: CollectionId, state: State) -> Result<Entity, RetrievalError> {
        let mut entities = self.0.write().unwrap();
        match entities.entry(id) {
            Entry::Vacant(_) => {
                let entity = Entity::from_state(id, collection_id.to_owned(), &state)?;
                entities.insert(id, entity.weak());
                Ok(entity)
            }
            Entry::Occupied(o) => match o.get().upgrade() {
                Some(entity) => {
                    if entity.collection == collection_id {
                        entity.apply_state(&state)?;
                        Ok(entity)
                    } else {
                        Err(RetrievalError::Anyhow(anyhow!("collection mismatch {} {collection_id}", entity.collection)))
                    }
                }
                None => {
                    let entity = Entity::from_state(id, collection_id.to_owned(), &state)?;
                    entities.insert(id, entity.weak());
                    Ok(entity)
                }
            },
        }
    }
}
