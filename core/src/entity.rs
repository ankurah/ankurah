use crate::lineage::{self, GetEvents, Retrieve};
use crate::{
    error::{LineageError, MutationError, RetrievalError, StateError},
    model::View,
    property::{Backends, PropertyValue},
    storage::StorageCollectionWrapper,
};
use ankql::selection::filter::Filterable;
use ankurah_proto::{Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State};
use anyhow::anyhow;
use std::collections::{btree_map::Entry, BTreeMap};
use std::sync::{Arc, Weak};
use tracing::{debug, error, warn};

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
    // TODO when a transaction creates a downstream entity, we needf to provide a weak reference back to the transaction
    // so we can error in the case that the transaction has been committed/rolled back (dropped)
    // This is necessary for JsMutables (to be created) which cannot have a borrow of the transaction
    // Standard Mutables will have a borrow of the transaction so it should not be possible for them to outlive the transaction
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
    pub fn upgrade(&self) -> Option<Entity> { self.0.upgrade().map(Entity) }
}

impl Entity {
    pub fn id(&self) -> EntityId { self.id }

    // This is intentionally private - only WeakEntitySet should be constructing Entities
    fn weak(&self) -> WeakEntity { WeakEntity(Arc::downgrade(&self.0)) }

    pub fn collection(&self) -> &CollectionId { &self.collection }

    pub fn backends(&self) -> &Backends { &self.backends }

    pub fn head(&self) -> Clock { self.head.lock().unwrap().clone() }

    pub fn to_state(&self) -> Result<State, StateError> {
        let state_buffers = self.backends.to_state_buffers()?;
        Ok(State { state_buffers, head: self.head() })
    }

    pub fn to_entity_state(&self) -> Result<EntityState, StateError> {
        let state = self.to_state()?;
        Ok(EntityState { entity_id: self.id(), collection: self.collection.clone(), state })
    }

    // used by the Model macro
    pub fn create(id: EntityId, collection: CollectionId, backends: Backends) -> Self {
        Self(Arc::new(EntityInner { id, collection, backends, head: std::sync::Mutex::new(Clock::default()), upstream: None }))
    }

    /// This must remain private - ONLY WeakEntitySet should be constructing Entities
    fn from_state(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(&state.state_buffers)?;
        Ok(Self(Arc::new(EntityInner { id, collection, backends, head: std::sync::Mutex::new(state.head.clone()), upstream: None })))
    }

    /// Generate an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit. Notably this does not apply the head to the entity, which must be done
    /// using commit_head
    pub(crate) fn generate_commit_event(&self) -> Result<Option<Event>, MutationError> {
        let operations = self.backends.take_accumulated_operations()?;
        if operations.is_empty() {
            Ok(None)
        } else {
            let operations = OperationSet(operations);
            let event = Event { entity_id: self.id, collection: self.collection.clone(), operations, parent: self.head() };
            Ok(Some(event))
        }
    }

    /// Updates the head of the entity to the given clock, which should come exclusively from generate_commit_event
    pub(crate) fn commit_head(&self, new_head: Clock) { *self.head.lock().unwrap() = new_head; }

    pub fn view<V: View>(&self) -> Option<V> {
        if self.collection() != &V::collection() {
            None
        } else {
            Some(V::from_entity(self.clone()))
        }
    }

    /// Attempt to apply an event to the entity
    #[cfg_attr(feature = "instrument", tracing::instrument(level="debug", skip_all, fields(entity = %self, event = %event)))]
    pub async fn apply_event<G>(&self, getter: &G, event: &Event) -> Result<bool, MutationError>
    where G: GetEvents<Id = EventId, Event = Event> {
        debug!("apply_event head: {event} to {self}");
        let current_entity_head = self.head(); // Capture current head before any potential modifications

        if current_entity_head.is_empty() && event.is_entity_create() {
            // this is the creation event for a new entity, so we simply accept it
            for (backend_name, operations) in event.operations.iter() {
                self.backends.apply_operations((*backend_name).to_owned(), operations)?;
            }
            *self.head.lock().unwrap() = event.id().into();
            return Ok(true);
        }

        let budget = 100; // TODO: Make budget configurable or dynamic
        match crate::lineage::compare_unstored_event(getter, event, &current_entity_head, budget).await? {
            lineage::Ordering::Equal => {
                debug!("Event {} is Equal to current head {}, skipping.", event.id(), current_entity_head);
                Ok(false)
            }
            lineage::Ordering::Descends { unapplied } => {
                debug!(
                    "Event {} Descends current head {}. Applying unapplied path ({} events) and event itself.",
                    event.id(),
                    current_entity_head,
                    unapplied.len()
                );
                for item in unapplied {
                    // Each item.event is an event in the path from current_entity_head to event.parent()
                    // These need to be applied in order. The `compare_unstored_event` and `compare` logic
                    // should provide them in causal order (oldest to newest).
                    // The `item.concurrency` might be relevant for LWW if those concurrent events affect the same properties.
                    debug!("Applying operations from unapplied event: {}", item.event.id());
                    for (backend_name, operations) in item.event.operations.iter() {
                        self.backends.apply_operations((*backend_name).to_owned(), operations)?;
                    }
                }
                // After applying the path, the original `event` is the new tip.
                // Its operations were included if it was the last item in `unapplied` (or if `unapplied` only contained it for direct descent).
                // If `compare_unstored_event` ensures the original `event` is the last in `unapplied` if it's part of the sequence,
                // then this separate application might be redundant OR `unapplied` is only up to parent.
                // Given `compare_unstored_event` structure, `unapplied` goes up to `event` itself.
                // So, no need to apply `event.operations` separately here if it's already the last in `unapplied`.

                *self.head.lock().unwrap() = event.id().into(); // Set head to the incoming event's ID
                Ok(true)
            }
            lineage::Ordering::Ascends => {
                debug!("Event {} Ascends current head {} (event is older), skipping.", event.id(), current_entity_head);
                Ok(false)
            }
            lineage::Ordering::Other { unapplied } => {
                // 'unapplied' contains events from the new event's branch that are not known to the current_entity_head.
                // These events lead up to the incoming 'event'.
                // The incoming 'event' itself should be the last one in this sequence or represented by it.
                debug!(
                    "Event {} is Other (concurrent or divergent) with current head {}. Applying its branch ({} events).",
                    event.id(),
                    current_entity_head,
                    unapplied.len()
                );
                for item in unapplied {
                    // item.event is on the new branch. item.concurrency are events from the current_entity_head's branch that are concurrent.
                    // For LWW, backends need to handle this when applying operations.
                    debug!("Applying operations from Other branch event: {}", item.event.id());
                    for (backend_name, operations) in item.event.operations.iter() {
                        // The backend needs to be smart enough to use LWW rules if applicable, potentially using item.concurrency for context.
                        // For now, we just apply the operations from item.event.
                        self.backends.apply_operations((*backend_name).to_owned(), operations)?;
                    }
                }
                // After applying the operations from the incoming event's branch, augment the head.
                // The original `event` is the tip of this new branch.
                let mut head_guard = self.head.lock().unwrap();
                head_guard.insert(event.id());
                // It might be necessary to also remove elements from head_guard that are now superseded
                // if 'unapplied' implies that 'event' makes some parts of current_entity_head obsolete.
                // This depends on the exact semantics of Ordering::Other.
                // For now, simple augmentation.
                debug!("Augmented head with {} due to Other ordering. New head: {}", event.id(), *head_guard);
                Ok(true) // Indicate change was made
            }
            lineage::Ordering::Incomparable => {
                warn!("Event {} is Incomparable with current head {}, skipping.", event.id(), current_entity_head);
                Err(LineageError::Incomparable.into())
            }
            lineage::Ordering::BudgetExceeded { subject_frontier, other_frontier } => {
                error!(
                    "Budget exceeded comparing event {} with current head {}. Subject frontier: {:?}, Other frontier: {:?}",
                    event.id(),
                    current_entity_head,
                    subject_frontier,
                    other_frontier
                );
                Err(LineageError::BudgetExceeded { original_budget: budget, subject_frontier, other_frontier }.into())
            }
        }
    }

    pub async fn apply_state<G>(&self, getter: &G, state: &State) -> Result<bool, MutationError>
    where G: GetEvents<Id = EventId, Event = Event> {
        let head = self.head();
        let new_head = state.head.clone();

        debug!("{self} apply_state - new head: {new_head}");
        let budget = 100;

        match crate::lineage::compare(getter, &new_head, &head, budget).await? {
            lineage::Ordering::Equal => {
                debug!("{self} apply_state - heads are equal, skipping");
                Ok(false)
            }
            lineage::Ordering::Descends { unapplied: _ } => {
                // TODO: Handle unapplied path for state changes if necessary.
                // For apply_state, if new_head Descends current_head, it means the new_state
                // is a direct successor or has a lineage of successors.
                // The `unapplied` path might be relevant if we need to ensure intermediate states
                // are also valid or trigger some logic, but typically apply_state just sets to the target state.
                debug!("{self} apply_state - new head {} descends from current {}, applying state", state.head, self.head());
                self.backends.apply_state(state)?;
                *self.head.lock().unwrap() = state.head.clone();
                Ok(true)
            }
            lineage::Ordering::Ascends => {
                warn!(
                    "{self} apply_state - new head {} ascends current_head {} (new state is older), skipping application.",
                    state.head,
                    self.head()
                );
                Ok(false)
            }
            lineage::Ordering::Other { unapplied: _ } => {
                // TODO: Define behavior for Other in apply_state.
                // This implies the new state is concurrent or divergent.
                // A simple 'apply_state' might not be appropriate.
                // It might require merging backend states, which is complex.
                // For now, treating as a non-apply.
                warn!("{self} apply_state - new head {} is Other relative to current_head {}. State application for 'Other' is not fully defined, skipping.", state.head, self.head());
                Ok(false)
            }
            lineage::Ordering::Incomparable => {
                error!("{self} apply_state - new head {} is Incomparable with current_head {}.", state.head, self.head());
                Err(LineageError::Incomparable.into())
            }
            lineage::Ordering::BudgetExceeded { subject_frontier, other_frontier } => {
                tracing::warn!(
                    "{self} apply_state - budget exceeded comparing new head {} with current {}. Subject: {:?}, Other: {:?}",
                    state.head,
                    self.head(),
                    subject_frontier,
                    other_frontier
                );
                Err(LineageError::BudgetExceeded { original_budget: budget, subject_frontier, other_frontier }.into())
            }
        }
    }

    /// Create a snapshot of the Entity which is detached from this one, and will not receive the updates this one does
    pub fn snapshot(&self) -> Self {
        Self(Arc::new(EntityInner {
            id: self.id,
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
        write!(f, "Entity({}/{} {})", self.collection, self.id.to_base64_short(), self.head.lock().unwrap())
    }
}

impl Filterable for Entity {
    fn collection(&self) -> &str { self.collection.as_str() }

    /// TODO Implement this as a typecasted value. eg value<T> -> Option<Result<T>>
    /// where None is returned if the property is not found, and Err is returned if the property is found but is not able to be typecasted
    /// to the requested type. (need to think about the rust type system here more)
    fn value(&self, name: &str) -> Option<String> {
        if name == "id" {
            Some(self.id.to_base64())
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
                },
                None => None,
            })
        }
    }
}

impl TemporaryEntity {
    pub fn new(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(&state.state_buffers)?;
        Ok(Self(Arc::new(EntityInner { id, collection, backends, head: std::sync::Mutex::new(state.head.clone()), upstream: None })))
    }
}

// TODO - clean this up and consolidate with Entity somehow, while still preventing anyone from creating unregistered (non-temporary) Entities
impl Filterable for TemporaryEntity {
    fn collection(&self) -> &str { self.0.collection.as_str() }

    fn value(&self, name: &str) -> Option<String> {
        if name == "id" {
            Some(self.0.id.to_base64())
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
    pub fn get(&self, id: &EntityId) -> Option<Entity> {
        let entities = self.0.read().unwrap();
        // TODO: call policy agent with cdata
        if let Some(entity) = entities.get(id) {
            entity.upgrade()
        } else {
            None
        }
    }

    pub async fn get_or_retrieve(
        &self,
        collection_id: &CollectionId,
        collection: &StorageCollectionWrapper,
        id: &EntityId,
    ) -> Result<Option<Entity>, RetrievalError> {
        // do it in two phases to avoid holding the lock while waiting for the collection
        match self.get(id) {
            Some(entity) => Ok(Some(entity)),
            None => match collection.get_state(*id).await {
                Err(RetrievalError::EntityNotFound(_)) => Ok(None),
                Err(e) => Err(e),
                Ok(state) => {
                    // technically someone could have added the entity since we last checked, so it's better to use the
                    // with_state method to re-check
                    let (_, entity) = self.with_state(collection, *id, collection_id.to_owned(), state.payload.state).await?;
                    Ok(Some(entity))
                }
            },
        }
    }
    /// Returns a resident entity, or fetches it from storage, or finally creates if neither of the two are found
    pub async fn get_retrieve_or_create(
        &self,
        collection_id: &CollectionId,
        collection: &StorageCollectionWrapper,
        id: &EntityId,
    ) -> Result<Entity, RetrievalError> {
        match self.get_or_retrieve(collection_id, collection, id).await? {
            Some(entity) => Ok(entity),
            None => {
                let mut entities = self.0.write().unwrap();
                // TODO: call policy agent with cdata
                if let Some(entity) = entities.get(id) {
                    if let Some(entity) = entity.upgrade() {
                        return Ok(entity);
                    }
                }
                let entity = Entity::create(*id, collection_id.to_owned(), Backends::new());
                entities.insert(*id, entity.weak());
                Ok(entity)
            }
        }
    }
    /// Create a brand new entity, and add it to the set
    pub fn create(&self, collection: CollectionId) -> Entity {
        let mut entities = self.0.write().unwrap();
        let id = EntityId::new();
        let entity = Entity::create(id, collection, Backends::new());
        entities.insert(*id, entity.weak());
        entity
    }

    /// Returns a tuple of (changed, entity)
    /// changed is Some(true) if the entity was changed, Some(false) if it already exists and the state was not applied
    /// None if the entity was not previously on the local node (either in the WeakEntitySet or in storage)
    pub async fn with_state<R>(
        &self,
        retriever: &R,
        id: EntityId,
        collection_id: CollectionId,
        state: State,
    ) -> Result<(Option<bool>, Entity), RetrievalError>
    where
        R: Retrieve<Id = EventId, Event = Event>,
    {
        let entity = {
            let mut entities = self.0.write().unwrap();
            match entities.entry(id) {
                Entry::Vacant(_) => None,
                Entry::Occupied(o) => match o.get().upgrade() {
                    Some(entity) => {
                        debug!("Entity {id} was resident");
                        if entity.collection != collection_id {
                            return Err(RetrievalError::Anyhow(anyhow!("collection mismatch {} {collection_id}", entity.collection)));
                        }
                        Some(entity)
                    }
                    None => {
                        debug!("Entity {id} was deallocated");
                        None
                    }
                },
            }
        };

        // Handle the case where entity is not resident (either vacant or deallocated)
        let Some(entity) = entity else {
            // Check if there's existing state in storage before creating a new entity
            if let Some(existing_state) = retriever.get_local_state(id).await? {
                debug!("Found existing state in storage for {id}");
                let entity = Entity::from_state(id, collection_id.to_owned(), &existing_state.payload.state)?;

                {
                    self.0.write().unwrap().insert(id, entity.weak());
                }

                // We need to apply the new state to check lineage
                let changed = entity.apply_state(retriever, &state).await?;
                return Ok((Some(changed), entity));
            } else {
                debug!("No existing state in storage for {id}");
                let entity = Entity::from_state(id, collection_id.to_owned(), &state)?;

                {
                    self.0.write().unwrap().insert(id, entity.weak());
                }

                return Ok((None, entity));
            }
        };

        // if we're here, we've retrieved the entity from the set and need to apply the state
        let changed = entity.apply_state(retriever, &state).await?;
        Ok((Some(changed), entity))
    }
}
