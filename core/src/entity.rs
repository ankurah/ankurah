use crate::lineage::{self, GetEvents, Retrieve};
use crate::{
    error::{LineageError, MutationError, RetrievalError, StateError},
    model::View,
    property::backend::{backend_from_string, PropertyBackend},
    reactor::AbstractEntity,
    value::Value,
};
use ankql::selection::filter::Filterable;
use ankurah_proto::{Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tracing::{debug, error, info, warn};

/// An entity represents a unique thing within a collection. Entity can only be constructed via a WeakEntitySet
/// which provides duplication guarantees.
#[derive(Debug, Clone)]
pub struct Entity(Arc<EntityInner>);

// TODO optimize this to be faster for scanning over entries in a collection
/// Used only for reconstituting state to filter database results. No duplication guarantees are provided
pub struct TemporaryEntity(Arc<EntityInner>);

/// Combined state for atomic updates of head and backends
#[derive(Debug)]
struct EntityInnerState {
    head: Clock,
    // TODO: remove interior mutability from backends; make mutation methods take &mut self
    backends: BTreeMap<String, Arc<dyn PropertyBackend>>,
}

impl EntityInnerState {
    /// Apply operations to a specific backend within this state
    /// TODO: backends currently rely on interior mutability; refactor to externalize mutability
    fn apply_operations(&mut self, backend_name: String, operations: &Vec<ankurah_proto::Operation>) -> Result<(), MutationError> {
        if let Some(backend) = self.backends.get(&backend_name) {
            backend.apply_operations(operations)?;
        } else {
            let backend = backend_from_string(&backend_name, None)?;
            backend.apply_operations(operations)?;
            self.backends.insert(backend_name, backend);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct EntityInner {
    pub id: EntityId,
    pub collection: CollectionId,
    /// Combined state RwLock for atomic head/backends updates
    state: std::sync::RwLock<EntityInnerState>,
    pub(crate) kind: EntityKind,
    /// Broadcast for notifying Signal subscribers about entity changes
    pub(crate) broadcast: ankurah_signals::broadcast::Broadcast,
}

#[derive(Debug)]
pub enum EntityKind {
    Primary,                                                     // New or resident entity - TODO delineate these
    Transacted { trx_alive: Arc<AtomicBool>, upstream: Entity }, // Transaction fork with liveness tracking
}

impl std::ops::Deref for Entity {
    type Target = EntityInner;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::ops::Deref for TemporaryEntity {
    type Target = EntityInner;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl PartialEq for Entity {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
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

    pub fn head(&self) -> Clock { self.state.read().unwrap().head.clone() }

    /// Check if this entity is writable (i.e., it's a transaction fork that's still alive)
    pub fn is_writable(&self) -> bool {
        match &self.kind {
            EntityKind::Primary => false, // Primary entities are read-only
            EntityKind::Transacted { trx_alive, .. } => trx_alive.load(Ordering::Acquire),
        }
    }

    pub fn to_state(&self) -> Result<State, StateError> {
        let state = self.state.read().expect("other thread panicked, panic here too");
        let mut state_buffers = BTreeMap::default();
        for (name, backend) in &state.backends {
            let state_buffer = backend.to_state_buffer()?;
            state_buffers.insert(name.clone(), state_buffer);
        }
        let state_buffers = ankurah_proto::StateBuffers(state_buffers);
        Ok(State { state_buffers, head: state.head.clone() })
    }

    pub fn to_entity_state(&self) -> Result<EntityState, StateError> {
        let state = self.to_state()?;
        Ok(EntityState { entity_id: self.id(), collection: self.collection.clone(), state })
    }

    // used by the Model macro
    pub fn create(id: EntityId, collection: CollectionId) -> Self {
        Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState { head: Clock::default(), backends: BTreeMap::default() }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
        }))
    }

    /// This must remain private - ONLY WeakEntitySet should be constructing Entities
    fn from_state(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        let mut backends = BTreeMap::new();
        for (name, state_buffer) in state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }

        Ok(Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState { head: state.head.clone(), backends }),
            kind: EntityKind::Primary,
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
        })))
    }

    /// Generate an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit. Notably this does not apply the head to the entity, which must be done
    /// using commit_head
    pub(crate) fn generate_commit_event(&self) -> Result<Option<Event>, MutationError> {
        let state = self.state.read().expect("other thread panicked, panic here too");
        let mut operations = BTreeMap::<String, Vec<ankurah_proto::Operation>>::new();
        for (name, backend) in &state.backends {
            if let Some(ops) = backend.to_operations()? {
                operations.insert(name.clone(), ops);
            }
        }

        if operations.is_empty() {
            Ok(None)
        } else {
            let operations = OperationSet(operations);
            let event = Event { entity_id: self.id, collection: self.collection.clone(), operations, parent: state.head.clone() };
            Ok(Some(event))
        }
    }

    /// Updates the head of the entity to the given clock, which should come exclusively from generate_commit_event
    pub(crate) fn commit_head(&self, new_head: Clock) {
        // TODO figure out how to implement CAS with the backend state
        // probably need an increment for local edits
        self.state.write().unwrap().head = new_head;
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
    pub async fn apply_event<G>(&self, getter: &G, event: &Event) -> Result<bool, MutationError>
    where G: GetEvents<Id = EventId, Event = Event> {
        debug!("apply_event head: {event} to {self}");

        // Check for entity creation under the mutex to avoid TOCTOU race
        if event.is_entity_create() {
            let mut state = self.state.write().unwrap();
            // Re-check if head is still empty now that we hold the lock
            if state.head.is_empty() {
                // this is the creation event for a new entity, so we simply accept it
                for (backend_name, operations) in event.operations.iter() {
                    state.apply_operations(backend_name.clone(), operations)?;
                }
                state.head = event.id().into();
                drop(state); // Release lock before broadcast
                             // Notify Signal subscribers about the change
                self.broadcast.send(());
                return Ok(true);
            }
            // If head is no longer empty, fall through to normal lineage comparison
        }

        let mut head = self.head();
        // Retry loop to handle head changes between lineage comparison and mutation
        const MAX_RETRIES: usize = 5;
        let budget = 10;

        for attempt in 0..MAX_RETRIES {
            match crate::lineage::compare_unstored_event(getter, event, &head, budget).await? {
                lineage::Ordering::Equal => {
                    info!("Equal - skip");
                    return Ok(false);
                }
                lineage::Ordering::Descends => {
                    info!("Descends - apply (attempt {})", attempt + 1);
                    let new_head = event.id().into();
                    // Atomic update: apply operations and set head under single lock
                    {
                        let mut state = self.state.write().unwrap();
                        // Re-check that head hasn't changed since lineage comparison
                        if state.head != head {
                            debug!("Head changed during lineage comparison, retrying...");
                            let current_head = state.head.clone();
                            drop(state);
                            head = current_head;
                            continue;
                        }
                        for (backend_name, operations) in event.operations.iter() {
                            // IMPORTANT TODO - we might descend the entity head by more than one event,
                            // therefore we need to play forward events one by one, not just the latest
                            // set operations, the current head and the parent of that.
                            // We have to play forward all events from the current entity/backend state
                            // to the latest event. At this point we know this lineage is connected,
                            // else we would get back an Ordering::Incomparable.
                            // So we just need to fiure out how to scan through those events here.
                            // Probably the easiest thing would be to update Ordering::Descends to provide
                            // the list of events to play forward, but ideally we'd do it on a streaming
                            // basis instead. of materializing what could potentially be a large number
                            // of events in some cases.
                            // we can't do this in the lineage comparison, because its looking backwards
                            // not forwards - and we need to replay the events in order.
                            // Maybe the best approach is to have Descends return the list of event ids
                            // connecting the current head to the new event, and use a modest LRU cache
                            // on the event geter (which needs to abstract storage collection anyway,
                            // due to the need for remote event retrieval)
                            // ...so we get a cache hit in the most common cases, and it can paginate the rest.
                            state.apply_operations(backend_name.clone(), operations)?;
                        }
                        state.head = new_head;
                    }
                    // Notify Signal subscribers about the change
                    self.broadcast.send(());
                    return Ok(true);
                }
                lineage::Ordering::NotDescends { meet: _ } => {
                    // HACK - this is totally wrong, and should be handled by the Visitor discussed in concurrent-updates/spec.md
                    info!("NotDescends - applying (attempt {})", attempt + 1);
                    // Atomic update: apply operations and augment head under single lock
                    {
                        let mut state = self.state.write().unwrap();
                        // Re-check that head hasn't changed since lineage comparison
                        if state.head != head {
                            warn!("Head changed during lineage comparison, retrying...");
                            let current_head = state.head.clone();
                            drop(state);
                            head = current_head;
                            continue;
                        }
                        for (backend_name, operations) in event.operations.iter() {
                            state.apply_operations(backend_name.clone(), operations)?;
                        }
                        // concurrent - so augment the head
                        state.head.insert(event.id());
                    }
                    return Ok(false);
                }
                lineage::Ordering::Incomparable => {
                    // total apples and oranges - take a hike buddy
                    error!("{self} apply_event - Incomparable. This should essentially never happen among good actors.");
                    return Err(LineageError::Incomparable.into());
                }
                lineage::Ordering::PartiallyDescends { meet } => {
                    error!("PartiallyDescends - skipping this event, but we should probably be handling this");
                    // TODO - figure out how to handle this. I don't think we need to materialize a new event
                    // but it requires that we update the propertybackends to support this
                    return Err(LineageError::PartiallyDescends { meet }.into());
                }
                lineage::Ordering::BudgetExceeded { subject_frontier, other_frontier } => {
                    error!(
                        "BudgetExceeded subject_frontier: {}, other_frontier: {}",
                        subject_frontier.iter().map(|id| id.to_base64_short()).collect::<Vec<String>>().join(", "),
                        other_frontier.iter().map(|id| id.to_base64_short()).collect::<Vec<String>>().join(", ")
                    );
                    return Err(LineageError::BudgetExceeded { original_budget: budget, subject_frontier, other_frontier }.into());
                }
            }
        }

        // If we've exhausted retries, return a budget exceeded error
        Err(LineageError::BudgetExceeded {
            original_budget: MAX_RETRIES,
            subject_frontier: std::collections::BTreeSet::from([event.id()]),
            other_frontier: std::collections::BTreeSet::new(),
        }
        .into())
    }

    pub async fn apply_state<G>(&self, getter: &G, state: &State) -> Result<bool, MutationError>
    where G: GetEvents<Id = EventId, Event = Event> {
        let head = self.head();
        let new_head = state.head.clone();

        debug!("{self} apply_state - new head: {new_head}");
        let budget = 10;

        match crate::lineage::compare(getter, &new_head, &head, budget).await? {
            lineage::Ordering::Equal => {
                debug!("{self} apply_state - heads are equal, skipping");
                Ok(false)
            }
            lineage::Ordering::Descends => {
                debug!("{self} apply_state - new head descends from current, applying");
                // TODO: Consider playing forward the event lineage instead of overwriting backends.
                // Current approach has a security vulnerability: a malicious peer could send a state
                // buffer that disagrees with the actual event operations. Playing forward events would
                // ensure the state always matches the verified event lineage.

                // Atomic update: overwrite backends and set head under single lock
                {
                    let mut entity_state = self.state.write().unwrap();
                    for (name, state_buffer) in state.state_buffers.iter() {
                        let backend = backend_from_string(name, Some(state_buffer))?;
                        entity_state.backends.insert(name.to_owned(), backend);
                    }
                    entity_state.head = new_head;
                }
                // Notify Signal subscribers about the change
                self.broadcast.send(());
                Ok(true)
            }
            lineage::Ordering::NotDescends { meet } => {
                warn!("{self} apply_state - new head {new_head} does not descend {head}, meet: {meet:?}");
                Ok(false)
            }
            lineage::Ordering::Incomparable => {
                error!("{self} apply_state - heads are incomparable. This should essentially never happen among good actors.");
                // total apples and oranges - take a hike buddy
                Err(LineageError::Incomparable.into())
            }
            lineage::Ordering::PartiallyDescends { meet } => {
                error!("{self} apply_state - heads partially descend, meet: {meet:?}");
                // TODO - figure out how to handle this. I don't think we need to materialize a new event
                // but it requires that we update the propertybackends to support this
                Err(LineageError::PartiallyDescends { meet }.into())
            }
            lineage::Ordering::BudgetExceeded { subject_frontier, other_frontier } => {
                tracing::warn!("{self} apply_state - budget exceeded. subject: {subject_frontier:?}, other: {other_frontier:?}");
                Err(LineageError::BudgetExceeded { original_budget: budget, subject_frontier, other_frontier }.into())
            }
        }
    }

    /// Create a snapshot of the Entity which is detached from this one, and will not receive the updates this one does
    /// The trx_alive parameter tracks whether the transaction that owns this snapshot is still alive
    pub fn snapshot(&self, trx_alive: Arc<AtomicBool>) -> Self {
        // Inline fork logic
        let state = self.state.read().expect("other thread panicked, panic here too");
        let mut forked = BTreeMap::new();
        for (name, backend) in &state.backends {
            forked.insert(name.clone(), backend.fork());
        }

        Self(Arc::new(EntityInner {
            id: self.id,
            collection: self.collection.clone(),
            state: std::sync::RwLock::new(EntityInnerState { head: state.head.clone(), backends: forked }),
            kind: EntityKind::Transacted { trx_alive, upstream: self.clone() },
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
        }))
    }

    /// Get a reference to the entity's broadcast for Signal implementations
    pub fn broadcast(&self) -> &ankurah_signals::broadcast::Broadcast { &self.broadcast }

    /// Get a specific backend, creating it if it doesn't exist
    pub fn get_backend<P: PropertyBackend>(&self) -> Result<Arc<P>, RetrievalError> {
        let backend_name = P::property_backend_name();
        let mut state = self.state.write().expect("other thread panicked, panic here too");
        if let Some(backend) = state.backends.get(&backend_name) {
            let upcasted = backend.clone().as_arc_dyn_any();
            Ok(upcasted.downcast::<P>().unwrap()) // TODO: handle downcast error
        } else {
            let backend = backend_from_string(&backend_name, None)?;
            let upcasted = backend.clone().as_arc_dyn_any();
            let typed_backend = upcasted.downcast::<P>().unwrap(); // TODO handle downcast error
            state.backends.insert(backend_name, backend);
            Ok(typed_backend)
        }
    }

    pub fn values(&self) -> Vec<(String, Option<Value>)> {
        let state = self.state.read().expect("other thread panicked, panic here too");
        state
            .backends
            .values()
            .flat_map(|backend| {
                backend
                    .property_values()
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.clone()))
                    .collect::<Vec<(String, Option<Value>)>>()
            })
            .collect()
    }
}

// Implement AbstractEntity for Entity (used by reactor)
impl AbstractEntity for Entity {
    fn collection(&self) -> ankurah_proto::CollectionId { self.collection.clone() }

    fn id(&self) -> &ankurah_proto::EntityId { &self.id }

    fn value(&self, field: &str) -> Option<crate::value::Value> {
        if field == "id" {
            Some(crate::value::Value::EntityId(self.id))
        } else {
            // Iterate through backends to find one that has this property
            let state = self.state.read().expect("other thread panicked, panic here too");
            state.backends.values().find_map(|backend| backend.property_value(&field.into()))
        }
    }
}

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity({}/{} {:#})", self.collection, self.id.to_base64_short(), self.head())
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
            let state = self.state.read().expect("other thread panicked, panic here too");
            state.backends.values().find_map(|backend| match backend.property_value(&name.to_owned()) {
                Some(value) => match value {
                    Value::String(s) => Some(s),
                    Value::I16(i) => Some(i.to_string()),
                    Value::I32(i) => Some(i.to_string()),
                    Value::I64(i) => Some(i.to_string()),
                    Value::F64(i) => Some(i.to_string()),
                    Value::Bool(i) => Some(i.to_string()),
                    Value::EntityId(entity_id) => Some(entity_id.to_base64()),
                    Value::Object(items) => Some(String::from_utf8_lossy(&items).to_string()),
                    Value::Binary(items) => Some(String::from_utf8_lossy(&items).to_string()),
                },
                None => None,
            })
        }
    }
}

impl TemporaryEntity {
    pub fn new(id: EntityId, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        // Inline from_state_buffers logic
        let mut backends = BTreeMap::new();
        for (name, state_buffer) in state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }

        Ok(Self(Arc::new(EntityInner {
            id,
            collection,
            state: std::sync::RwLock::new(EntityInnerState { head: state.head.clone(), backends }),
            kind: EntityKind::Primary,
            // slightly annoying that we need to populate this, given that it won't be used
            broadcast: ankurah_signals::broadcast::Broadcast::new(),
        })))
    }
    pub fn values(&self) -> Vec<(String, Option<Value>)> {
        let state = self.0.state.read().expect("other thread panicked, panic here too");
        state.backends.values().flat_map(|backend| backend.property_values()).collect()
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
            let state = self.0.state.read().expect("other thread panicked, panic here too");
            state.backends.values().find_map(|backend| match backend.property_value(&name.to_owned()) {
                Some(value) => match value {
                    Value::String(s) => Some(s),
                    Value::I16(i) => Some(i.to_string()),
                    Value::I32(i) => Some(i.to_string()),
                    Value::I64(i) => Some(i.to_string()),
                    Value::F64(i) => Some(i.to_string()),
                    Value::Bool(i) => Some(i.to_string()),
                    Value::EntityId(entity_id) => Some(entity_id.to_base64()),
                    Value::Object(items) => Some(String::from_utf8_lossy(&items).to_string()),
                    Value::Binary(items) => Some(String::from_utf8_lossy(&items).to_string()),
                },
                None => None,
            })
        }
    }
}

impl std::fmt::Display for TemporaryEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TemporaryEntity({}/{}) = {}", &self.collection, self.id, self.0.state.read().unwrap().head)
    }
}

// TODO - Implement TOCTOU Race condition tests. Require real backend state mutations to be meaningful. punting that for now
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

    pub async fn get_or_retrieve<R>(
        &self,
        retriever: &R,
        collection_id: &CollectionId,
        id: &EntityId,
    ) -> Result<Option<Entity>, RetrievalError>
    where
        R: Retrieve<Id = EventId, Event = Event> + Send + Sync,
    {
        // do it in two phases to avoid holding the lock while waiting for the collection
        match self.get(id) {
            Some(entity) => Ok(Some(entity)),
            None => match retriever.get_state(*id).await {
                Ok(None) => Ok(None),
                Ok(Some(state)) => {
                    // technically someone could have added the entity since we last checked, so it's better to use the
                    // with_state method to re-check
                    let (_, entity) = self.with_state(retriever, *id, collection_id.to_owned(), state.payload.state).await?;
                    Ok(Some(entity))
                }
                Err(e) => Err(e),
            },
        }
    }
    /// Returns a resident entity, or fetches it from storage, or finally creates if neither of the two are found
    pub async fn get_retrieve_or_create<R>(
        &self,
        retriever: &R,
        collection_id: &CollectionId,
        id: &EntityId,
    ) -> Result<Entity, RetrievalError>
    where
        R: Retrieve<Id = EventId, Event = Event> + Send + Sync,
    {
        match self.get_or_retrieve(retriever, collection_id, id).await? {
            Some(entity) => Ok(entity),
            None => {
                let mut entities = self.0.write().unwrap();
                // TODO: call policy agent with cdata
                if let Some(entity) = entities.get(id) {
                    if let Some(entity) = entity.upgrade() {
                        return Ok(entity);
                    }
                }
                let entity = Entity::create(*id, collection_id.to_owned());
                entities.insert(*id, entity.weak());
                Ok(entity)
            }
        }
    }
    /// Create a brand new entity, and add it to the set
    pub fn create(&self, collection: CollectionId) -> Entity {
        let mut entities = self.0.write().unwrap();
        let id = EntityId::new();
        let entity = Entity::create(id, collection);
        entities.insert(id, entity.weak());
        entity
    }

    /// Get or create entity after async operations, checking for race conditions
    /// Returns (existed, entity) where existed is true if the entity was already present
    fn private_get_or_create(&self, id: EntityId, collection_id: &CollectionId, state: &State) -> Result<(bool, Entity), RetrievalError> {
        let mut entities = self.0.write().unwrap();
        if let Some(existing_weak) = entities.get(&id) {
            if let Some(existing_entity) = existing_weak.upgrade() {
                debug!("Entity {id} was created by another thread during async work, using that one");
                return Ok((true, existing_entity));
            }
        }
        let entity = Entity::from_state(id, collection_id.to_owned(), state)?;
        entities.insert(id, entity.weak());
        Ok((false, entity))
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
        let entity = match self.get(&id) {
            Some(entity) => entity, // already resident
            None => {
                // not yet resident. We have to retrieve our baseline state before applying the new state
                if let Some(stored_state) = retriever.get_state(id).await? {
                    // get a resident entity for this retrieved state. It's possible somebody frontran us to create it
                    // but we don't actually care, so we ignore the created flag
                    self.private_get_or_create(id, &collection_id, &stored_state.payload.state)?.1
                } else {
                    // no stored state, so we can use the given state directly
                    match self.private_get_or_create(id, &collection_id, &state)? {
                        (true, entity) => entity, // some body frontran us to create it, so we have to apply the new state
                        (false, entity) => {
                            // we just created it with the given state, so there's nothing to apply. early return
                            return Ok((None, entity));
                        }
                    }
                }
            }
        };

        // if we're here, we've retrieved the entity from the set and need to apply the state
        let changed = entity.apply_state(retriever, &state).await?;
        Ok((Some(changed), entity))
    }
}
