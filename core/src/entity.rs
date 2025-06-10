use crate::changes::EntityChange;
use crate::collectionset::CollectionSet;
use crate::datagetter::{DataGetter, LocalGetter};
use crate::lineage::{self, GetEvents};
use crate::reactor::Reactor;
use crate::storage::StorageEngine;
use crate::{
    error::{LineageError, MutationError, RetrievalError, StateError},
    model::View,
    policy::PolicyAgent,
    property::{Backends, PropertyValue},
};
use ankql::selection::filter::Filterable;
use ankurah_proto::{Attested, Clock, CollectionId, EntityId, EntityState, Event, EventId, OperationSet, State};
use std::collections::BTreeMap;
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

    // Only EntityManager should be creating Entities
    fn create(id: EntityId, collection: CollectionId, backends: Backends) -> Self {
        Self(Arc::new(EntityInner { id, collection, backends, head: std::sync::Mutex::new(Clock::default()), upstream: None }))
    }

    /// Only EntityManager should be constructing Entities from states
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
    /// Only EntityManager should be calling this
    fn commit_head(&self, new_head: Clock) { *self.head.lock().unwrap() = new_head; }

    pub fn view<V: View>(&self) -> Option<V> {
        if self.collection() != &V::collection() {
            None
        } else {
            Some(V::from_entity(self.clone()))
        }
    }

    /// Attempt to apply an event to the entity. The event_getter is used to traverse the event DAG as needed between the current head and the event.
    #[cfg_attr(feature = "instrument", tracing::instrument(level="debug", skip_all, fields(entity = %self, event = %event)))]
    async fn apply_event<G>(&self, event_getter: &G, event: &Event) -> Result<bool, MutationError>
    where G: GetEvents<Id = EventId, Event = Event> {
        debug!("apply_event head: {event} to {self}");

        if event.entity_id != self.id {
            return Err(MutationError::DifferentEntity);
        }
        let head = self.head();

        if head.is_empty() && event.is_entity_create() {
            // this is the creation event for a new entity, so we simply accept it
            for (backend_name, operations) in event.operations.iter() {
                self.backends.apply_operations((*backend_name).to_owned(), operations)?;
            }
            *self.head.lock().unwrap() = event.id().into();
            return Ok(true);
        }

        let budget = 100;
        match crate::lineage::compare_unstored_event(event_getter, event, &head, budget).await? {
            lineage::Ordering::Equal => {
                debug!("Equal - skip");
                Ok(false)
            }
            lineage::Ordering::Descends => {
                debug!("Descends - apply");
                let new_head = event.id().into();
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
                    // on the event getter (which needs to abstract storage collection anyway,
                    // due to the need for remote event retrieval)
                    // ...so we get a cache hit in the most common cases, and it can paginate the rest.

                    // QUESTION: Do we actually need to apply the operations in order?
                    // it seems to me that we should be able to apply the operations in any order,
                    // and the backends should be required to handle it. albeit with ordering details
                    // provided to apply_operations for LRU and others without their own internal ordering.
                    self.backends.apply_operations((*backend_name).to_owned(), operations)?;
                }
                *self.head.lock().unwrap() = new_head;
                Ok(true)
            }
            lineage::Ordering::NotDescends { meet: _ } => {
                // TODO - continue concurrency branch, issue #117
                debug!("NotDescends - applying");
                for (backend_name, operations) in event.operations.iter() {
                    self.backends.apply_operations((*backend_name).to_owned(), operations)?;
                }
                // concurrent - so augment the head
                self.head.lock().unwrap().insert(event.id());
                Ok(false)
            }
            lineage::Ordering::Incomparable => {
                // total apples and oranges - take a hike buddy
                Err(LineageError::Incomparable.into())
            }
            lineage::Ordering::PartiallyDescends { meet } => {
                error!("PartiallyDescends - skipping this event, but we should probably be handling this");
                // TODO - figure out how to handle this. I don't think we need to materialize a new event
                // but it requires that we update the propertybackends to support this
                Err(LineageError::PartiallyDescends { meet }.into())
            }
            lineage::Ordering::BudgetExceeded { subject_frontier, other_frontier } => {
                Err(LineageError::BudgetExceeded { original_budget: budget, subject_frontier, other_frontier }.into())
            }
        }
    }

    async fn can_apply_state<G>(&self, getter: &G, state: &State) -> Result<bool, MutationError>
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
            lineage::Ordering::Descends => {
                debug!("{self} apply_state - new head descends from current, applying");
                Ok(true)
            }
            lineage::Ordering::NotDescends { meet } => {
                warn!("{self} apply_state - new head {new_head} does not descend {head}, meet: {meet:?}");
                Ok(false)
            }
            lineage::Ordering::Incomparable => {
                error!("{self} apply_state - heads are incomparable");
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
    fn apply_state(&self, state: &State) -> Result<bool, MutationError> {
        self.backends.apply_state(state)?;
        *self.head.lock().unwrap() = state.head.clone();
        Ok(true)
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

/// The abiter of all entities in the system (except those which are forked during a transaction)
pub struct EntityManager<SE, PA, D>(Arc<Inner<SE, PA, D>>);

pub struct Inner<SE, PA, D> {
    resident: std::sync::RwLock<BTreeMap<EntityId, WeakEntity>>,
    reactor: Reactor<SE, PA>,
    policy_agent: PA,
    data_getter: D,
    collections: CollectionSet<SE>,
}

impl<SE, PA, D> Clone for EntityManager<SE, PA, D> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        DG: DataGetter<PA::ContextData> + Send + Sync + 'static,
    > EntityManager<SE, PA, DG>
{
    pub fn new(data_getter: DG, collections: CollectionSet<SE>, reactor: Reactor<SE, PA>, policy_agent: PA) -> Self {
        Self(Arc::new(Inner { resident: std::sync::RwLock::new(BTreeMap::new()), data_getter, collections, reactor, policy_agent }))
    }

    /// Returns a resident entity, or uses the getter
    pub async fn get(&self, collection_id: &CollectionId, id: &EntityId, cdata: &PA::ContextData) -> Result<Entity, RetrievalError> {
        // Uncomment this when entity residency is implicitly a subscription
        // match self.get_resident(id) {
        //     Some(entity) => Ok(Some(entity)),
        //     None =>

        let state = self.0.data_getter.get_state(collection_id, id, cdata).await?;
        let entity = self.apply_state(state, cdata).await?;
        Ok(entity)

        // }
    }

    /// Returns a resident entity, or None if it's not resident
    fn get_resident(&self, id: &EntityId) -> Option<Entity> {
        let entities = self.0.resident.read().unwrap();

        if let Some(entity) = entities.get(id) {
            entity.upgrade()
        } else {
            None
        }
    }

    /// Returns a resident entity, or gets it from the provided getter, or finally creates if neither of the two are found
    async fn get_or_create(&self, collection_id: &CollectionId, id: &EntityId, cdata: &PA::ContextData) -> Result<Entity, RetrievalError> {
        match self.get(collection_id, id, cdata).await {
            Ok(entity) => Ok(entity),
            Err(RetrievalError::EntityNotFound(_)) => {
                let mut entities = self.0.resident.write().unwrap();
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
            Err(e) => Err(e),
        }
    }
    /// Create a brand new entity and add it to the set
    pub fn create(&self, collection: CollectionId) -> Entity {
        let mut entities = self.0.resident.write().unwrap();
        let id = EntityId::new();
        let entity = Entity::create(id, collection, Backends::new());
        entities.insert(id, entity.weak());
        entity
    }

    /// Apply events to their respective entities, using the default DataGetter to traverse the event graph
    pub async fn apply_events(
        &self,
        cdata: &PA::ContextData,
        events: impl Iterator<Item = Attested<Event>>,
    ) -> Result<bool, MutationError> {
        let getter = self.0.data_getter.event_getter(cdata.clone());
        self.apply_events_with_getter(cdata, events, getter).await
    }

    /// Use the provided getter to apply the events which may or may not differ from the data broker's default getter
    pub async fn apply_events_with_getter<G: GetEvents<Id = EventId, Event = Event>>(
        &self,
        cdata: &PA::ContextData,
        events: impl IntoIterator<Item = Attested<Event>>,
        getter: G,
    ) -> Result<bool, MutationError> {
        let mut changed = false;

        let mut changes = Vec::new();

        // TODO - handle the case where we're applying multiple events to the same entity, which may not be in the correct order
        // Should probably be handled by pre-caching the events in the data getter, then recursing in apply_event

        for mut event in events {
            let collection = self.0.collections.get(&event.payload.collection).await?;

            // When applying events for a remote transaction, we should only look at the local storage for the lineage
            // If we are missing events necessary to connect the lineage, that's their responsibility to include in the transaction.
            let entity = self.get_or_create(&event.payload.collection, &event.payload.entity_id, cdata).await?;

            // we have the entity, so we can check access, optionally atteste, and apply/save the event;
            if let Some(attestation) = self.0.policy_agent.check_event(cdata, &entity, &event.payload)? {
                event.attestations.push(attestation);
            }
            if entity.apply_event(&getter, &event.payload).await? {
                changed = true;
                let state = entity.to_state()?;
                let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
                let attestation = self.0.policy_agent.attest_state(&entity_state);
                let attested = Attested::opt(entity_state, attestation);
                collection.add_event(&event).await?;
                collection.set_state(attested).await?;
                changes.push(EntityChange::new(entity, Some(event))?);
            }
        }

        self.0.reactor.notify_change(changes);

        Ok(changed)
    }

    /// Fetch multiple entities matching a predicate, hydrating them through apply_state
    pub async fn fetch(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
        cdata: &PA::ContextData,
    ) -> Result<Vec<Entity>, RetrievalError> {
        let matching_states = self.0.data_getter.fetch_states(collection_id, predicate, cdata).await?;

        let mut entities = Vec::new();
        for state in matching_states {
            let entity = self.apply_state(state, cdata).await?;
            entities.push(entity);
        }

        Ok(entities)
    }

    /// A version of fetch that only fetches from local storage, and does not require cdata
    pub async fn fetch_local(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
    ) -> Result<Vec<Entity>, RetrievalError> {
        let collection = self.0.collections.get(collection_id).await?;
        let local_getter = LocalGetter::new(self.0.collections.clone());
        let mut changes = Vec::new();

        let entities = Vec::new();
        for state in collection.fetch_states(predicate).await? {
            let entity = match self.get_resident(&state.payload.entity_id) {
                Some(entity) => entity,
                None => {
                    let entity = Entity::from_state(state.payload.entity_id, collection_id.clone(), &state.payload.state)?;
                    self.0.resident.write().unwrap().insert(state.payload.entity_id, entity.weak());
                    entity
                }
            };

            // LocalData as GetEvents, not DataBroker
            if entity.can_apply_state(&local_getter, &state.payload.state).await? {
                entity.apply_state(&state.payload.state)?;
                changes.push(EntityChange::new(entity.clone(), None)?);
            }
        }

        if !changes.is_empty() {
            self.0.reactor.notify_change(changes);
        }

        Ok(entities)
    }

    /// Attempts to apply an EntityState to the locally resident Entity (if any) and to the collection.
    ///
    /// This method ensures that entity state is only written to StorageCollection if it descends
    /// the current state for that entity. It centralizes reading and writing of local state.
    ///
    /// Returns (changed, entity) where:
    /// - changed is Some(true) if the entity was changed
    /// - changed is Some(false) if it already exists and the state was not applied
    /// - changed is None if the entity was not previously on the local node
    pub async fn apply_state(
        &self,
        attested_entity_state: Attested<EntityState>,
        cdata: &PA::ContextData,
    ) -> Result<Entity, MutationError> {
        match self.apply_state_inner(attested_entity_state, cdata).await {
            Ok((true, entity)) => {
                self.0.reactor.notify_change(vec![EntityChange::new(entity.clone(), None)?]);
                Ok(entity)
            }
            Ok((false, entity)) => Ok(entity),
            Err(e) => Err(e),
        }
    }
    pub async fn apply_states(
        &self,
        attested_entity_states: Vec<Attested<EntityState>>,
        cdata: &PA::ContextData,
    ) -> Result<Vec<Entity>, MutationError> {
        let mut results = Vec::new();
        let mut changes = Vec::new();

        for attested_entity_state in attested_entity_states {
            let (changed, entity) = self.apply_state_inner(attested_entity_state, cdata).await?;

            // Only create EntityChange for entities that actually changed
            if changed {
                changes.push(EntityChange::new(entity.clone(), None)?);
            }

            results.push(entity);
        }

        // Notify reactor of all changes at once
        if !changes.is_empty() {
            self.0.reactor.notify_change(changes);
        }

        Ok(results)
    }

    /// TODO: optimize this so that non-resident state retrieals are batched for apply_states
    /// (requires splitting this back out into apply_states and apply_state)
    async fn apply_state_inner(
        &self,
        attested_entity_state: Attested<EntityState>,
        cdata: &PA::ContextData,
    ) -> Result<(bool, Entity), MutationError> {
        let id = attested_entity_state.payload.entity_id;
        let collection_id = &attested_entity_state.payload.collection;
        let state = &attested_entity_state.payload.state;

        // Check if the entity is already resident
        match self.get_resident(&id) {
            Some(entity) => {
                if entity.collection() != collection_id {
                    return Err(MutationError::General(format!("collection mismatch {} {collection_id}", entity.collection()).into()));
                }
                debug!("Entity {id} was resident");
                if entity.can_apply_state(&self.0.data_getter.event_getter(cdata.clone()), state).await? {
                    // we can only apply the state if it descends from the current head, which means it changed
                    entity.apply_state(state)?;
                    let collection = self.0.collections.get(collection_id).await?;
                    collection.set_state(attested_entity_state).await?;

                    // Resident entity change detection is based on the entity's head
                    return Ok((true, entity));
                } else {
                    return Ok((false, entity));
                }
            }
            None => {
                // Retrieve what we have from local storage only
                let collection = self.0.collections.get(collection_id).await?;
                if let Some(existing_state) = collection.get_state(id).await.ok() {
                    debug!("Found existing state in storage for {id}");
                    let entity = Entity::from_state(id, collection_id.clone(), &existing_state.payload.state)?;

                    // Register the entity as resident
                    self.0.resident.write().unwrap().insert(id, entity.weak());

                    // Check lineage and apply if needed - use data_broker for event retrieval
                    let changed = entity.can_apply_state(&self.0.data_getter.event_getter(cdata.clone()), state).await?;
                    if changed {
                        entity.apply_state(state)?;
                        collection.set_state(attested_entity_state).await?;
                    }
                    Ok((changed, entity))
                } else {
                    debug!("No existing state in storage for {id}");
                    let entity = Entity::from_state(id, collection_id.clone(), state)?;

                    // Register the entity as resident and store the new state
                    self.0.resident.write().unwrap().insert(id, entity.weak());
                    collection.set_state(attested_entity_state).await?;

                    Ok((true, entity))
                }
            }
        }
    }

    pub async fn commit_events(&self, entity_attested_events: Vec<(Entity, Attested<Event>)>) -> Result<(), MutationError> {
        // All peers confirmed, now we can update local state
        let mut changes: Vec<EntityChange> = Vec::new();
        let local_getter = LocalGetter::new(self.0.collections.clone());
        for (entity, attested_event) in entity_attested_events {
            let collection = self.0.collections.get(&attested_event.payload.collection).await?;
            collection.add_event(&attested_event).await?;
            entity.commit_head(Clock::new([attested_event.payload.id()]));

            let collection_id = &attested_event.payload.collection;
            // If this entity has an upstream, propagate the changes
            if let Some(ref upstream) = entity.upstream {
                // the LocalGetter should have all the events necessary to apply the event to the upstream
                upstream.apply_event(&local_getter, &attested_event.payload).await?;
            }

            // Persist

            let state = entity.to_state()?;

            let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
            let attestation = self.0.policy_agent.attest_state(&entity_state);
            let attested = Attested::opt(entity_state, attestation);
            collection.set_state(attested).await?;

            changes.push(EntityChange::new(entity.clone(), Some(attested_event))?);
        }

        // Notify reactor of ALL changes
        self.0.reactor.notify_change(changes);

        Ok(())
    }

    /// Apply a mix of events and states without duplicate reactor notifications
    /// This is used by subscription updates where each item may have events, state, or both
    pub async fn apply_events_and_states(
        &self,
        updates: Vec<(Option<Attested<Event>>, Option<Attested<EntityState>>)>,
        cdata: &PA::ContextData,
    ) -> Result<Vec<(bool, Entity)>, MutationError> {
        let mut results = Vec::new();
        let mut changes = Vec::new();

        // TODO - cache events in DataGetter so we don't retrieve any more than we need to

        for (event, state) in updates {
            let mut entity_changed = false;
            let mut change_event = None;

            // Handle state first if present
            let entity = if let Some(attested_state) = state {
                let (changed, entity) = self.apply_state_inner(attested_state, cdata).await?;
                if changed {
                    entity_changed = true;
                }
                entity
            } else if let Some(ref attested_event) = event {
                // If no state but we have an event, get or create the entity
                self.get_or_create(&attested_event.payload.collection, &attested_event.payload.entity_id, cdata).await?
            } else {
                // Neither state nor event - skip this update
                continue;
            };

            // Handle event if present
            if let Some(attested_event) = event {
                let getter = self.0.data_getter.event_getter(cdata.clone());
                if entity.apply_event(&getter, &attested_event.payload).await? {
                    entity_changed = true;
                    change_event = Some(attested_event.clone());

                    // Only store the event in collection if it applied successfully
                    let collection = self.0.collections.get(&attested_event.payload.collection).await?;
                    collection.add_event(&attested_event).await?;

                    // Update entity state in storage
                    let state = entity.to_state()?;
                    let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
                    let attestation = self.0.policy_agent.attest_state(&entity_state);
                    let attested = Attested::opt(entity_state, attestation);
                    collection.set_state(attested).await?;
                }
            }

            // Collect change for reactor notification
            if entity_changed {
                changes.push(EntityChange::new(entity.clone(), change_event)?);
            }

            results.push((entity_changed, entity));
        }

        // Single reactor notification for all changes
        if !changes.is_empty() {
            self.0.reactor.notify_change(changes);
        }

        Ok(results)
    }
}
