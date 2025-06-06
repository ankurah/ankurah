use crate::changes::EntityChange;
use crate::collectionset::CollectionSet;
use crate::databroker::DataBroker;
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

    // TODO before merge - mark this as private and do all state/event application in the EntityManager to make sure it gets
    // store in the local storage

    /// Attempt to apply an event to the entity
    #[cfg_attr(feature = "instrument", tracing::instrument(level="debug", skip_all, fields(entity = %self, event = %event)))]
    async fn apply_event<G>(&self, getter: &G, event: &Event) -> Result<bool, MutationError>
    where G: GetEvents<Id = EventId, Event = Event> {
        debug!("apply_event head: {event} to {self}");
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
        match crate::lineage::compare_unstored_event(getter, event, &head, budget).await? {
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
                    // on the event geter (which needs to abstract storage collection anyway,
                    // due to the need for remote event retrieval)
                    // ...so we get a cache hit in the most common cases, and it can paginate the rest.
                    self.backends.apply_operations((*backend_name).to_owned(), operations)?;
                }
                *self.head.lock().unwrap() = new_head;
                Ok(true)
            }
            lineage::Ordering::NotDescends { meet: _ } => {
                // TODOs:
                // [ ] we should probably have some rules about when a non-descending (concurrent) event is allowed. In a way, this is the same question as Descends with a gap, as discussed above
                // [ ] update LWW backend determine which value to keep based on its deterministic last write win strategy. (just lexicographic winner, I think?)
                // [ ] differentiate NotDescends into Ascends and Concurrent. we should ignore the former, and apply the latter
                // just doing the needful for now
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

    async fn can_apply_state<G>(&self, getter: G, state: &State) -> Result<bool, MutationError>
    where G: GetEvents<Id = EventId, Event = Event> {
        let head = self.head();
        let new_head = state.head.clone();

        debug!("{self} apply_state - new head: {new_head}");
        let budget = 100;

        match crate::lineage::compare(&getter, &new_head, &head, budget).await? {
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
impl<SE, PA, D> Clone for EntityManager<SE, PA, D> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

pub struct Inner<SE, PA, D> {
    resident: std::sync::RwLock<BTreeMap<EntityId, WeakEntity>>,
    reactor: Reactor<SE, PA>,
    policy_agent: PA,
    data_broker: D,
    collections: CollectionSet<SE>,
}

impl<
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
        D: DataBroker<PA::ContextData> + Send + Sync + 'static,
    > EntityManager<SE, PA, D>
{
    pub fn new(data_broker: D, collections: CollectionSet<SE>, reactor: Reactor<SE, PA>, policy_agent: PA) -> Self {
        Self(Arc::new(Inner { resident: std::sync::RwLock::new(BTreeMap::new()), data_broker, collections, reactor, policy_agent }))
    }
    /// Returns a resident entity, or None if it's not resident
    pub fn get_resident(&self, id: &EntityId) -> Option<Entity> {
        let entities = self.0.resident.read().unwrap();
        // TODO: call policy agent with cdata
        if let Some(entity) = entities.get(id) {
            entity.upgrade()
        } else {
            None
        }
    }

    /// Returns a resident entity, or uses the getter
    pub async fn get(
        &self,
        collection_id: &CollectionId,
        id: &EntityId,
        cdata: &PA::ContextData,
    ) -> Result<Option<Entity>, RetrievalError> {
        // do it in two phases to avoid holding the lock while waiting for the collection
        // match self.get_resident(id) {
        //     Some(entity) => Ok(Some(entity)),
        //     None => match self.0.data_broker.get_states(collection_id, *id, cdata).await {
        //         Ok(None) => Ok(None),
        //         Ok(Some(state)) => {
        //             let (_, entity) = self.apply_state(state, cdata).await?;
        //             Ok(Some(entity))
        //         }
        //         Err(e) => Err(e),
        //     },
        // }
        todo!()
    }
    /// Returns a resident entity, or gets it from the provided getter, or finally creates if neither of the two are found
    pub async fn get_or_create(
        &self,
        collection_id: &CollectionId,
        id: &EntityId,
        cdata: &PA::ContextData,
    ) -> Result<Entity, RetrievalError> {
        match self.get(collection_id, id, cdata).await? {
            Some(entity) => Ok(entity),
            None => {
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
        }
    }
    /// Create a brand new entity, and add it to the set
    pub fn create(&self, collection: CollectionId) -> Entity {
        let mut entities = self.0.resident.write().unwrap();
        let id = EntityId::new();
        let entity = Entity::create(id, collection, Backends::new());
        entities.insert(id, entity.weak());
        entity
    }

    /// use the data broker's default getter
    pub async fn apply_events(
        &self,
        cdata: &PA::ContextData,
        events: impl Iterator<Item = Attested<Event>>,
    ) -> Result<bool, MutationError> {
        let getter = self.0.data_broker.event_getter(cdata.clone());
        self.apply_events_with_getter(cdata, events, getter).await
    }

    /// Use the provided getter to apply the events which may or may not differ from the data broker's default getter
    pub async fn apply_events_with_getter<G: GetEvents<Id = EventId, Event = Event>>(
        &self,
        cdata: &PA::ContextData,
        events: impl Iterator<Item = Attested<Event>>,
        getter: G,
    ) -> Result<bool, MutationError> {
        let mut changed = false;

        let mut changes = Vec::new();

        for event in events.iter_mut() {
            let collection = self.collections.get(&event.payload.collection).await?;

            // When applying events for a remote transaction, we should only look at the local storage for the lineage
            // If we are missing events necessary to connect the lineage, that's their responsibility to include in the transaction.
            let entity = self.entities.get_or_create(&getter, &event.payload.collection, &event.payload.entity_id).await?;

            // we have the entity, so we can check access, optionally atteste, and apply/save the event;
            if let Some(attestation) = self.policy_agent.check_event(self, cdata, &entity, &event.payload)? {
                event.attestations.push(attestation);
            }
            if entity.apply_event(&getter, &event.payload).await? {
                let state = entity.to_state()?;
                let entity_state = EntityState { entity_id: entity.id(), collection: entity.collection().clone(), state };
                let attestation = self.policy_agent.attest_state(self, &entity_state);
                let attested = Attested::opt(entity_state, attestation);
                collection.add_event(event).await?;
                collection.set_state(attested).await?;
                changes.push(EntityChange::new(entity.clone(), event.clone())?);
            }
        }

        self.reactor.notify_change(changes);

        Ok(changed)
    }

    /// Fetch multiple entities matching a predicate, hydrating them through apply_state
    pub async fn fetch(
        &self,
        collection_id: &CollectionId,
        predicate: &ankql::ast::Predicate,
        cdata: &PA::ContextData,
    ) -> Result<Vec<Entity>, RetrievalError> {
        let matching_states = self.0.data_broker.fetch_states(collection_id, predicate, cdata).await?;

        let mut entities = Vec::new();
        for state in matching_states {
            let (_, entity) = self
                .apply_state(state, cdata)
                .await
                .map_err(|e| RetrievalError::Other(format!("Failed to process entity state: {}", e)))?;
            entities.push(entity);
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
    ) -> Result<(Option<bool>, Entity), MutationError> {
        let id = attested_entity_state.payload.entity_id;
        let collection_id = &attested_entity_state.payload.collection;
        let state = &attested_entity_state.payload.state;

        // Check if the entity is already resident
        if let Some(entity) = self.get_resident(&id) {
            // Validate that the resident entity belongs to the correct collection
            if entity.collection() != collection_id {
                return Err(MutationError::General(format!("collection mismatch {} {collection_id}", entity.collection()).into()));
            }

            debug!("Entity {id} was resident");

            // Apply the state if lineage allows - use data_broker for event retrieval
            if entity.can_apply_state(self.0.data_broker.event_getter(cdata.clone()), state).await? {
                // we can only apply the state if it descends from the current head, which means it changed
                entity.apply_state(state)?;
                let collection = self.0.collections.get(collection_id).await?;
                collection.set_state(attested_entity_state).await?;

                // LEFT OFF HERE - determine if we should be notifying the reactor here
                // yes, we should be notifying the reactor here

                return Ok((Some(true), entity));
            } else {
                return Ok((Some(false), entity));
            }
        }

        // Entity is not resident - check if there's existing state in storage
        let collection = self.0.collections.get(collection_id).await?;

        if let Some(existing_state) = collection.get_state_useeeee_dataaa_broker(id, &*self.0.data_broker, cdata).await.ok() {
            debug!("Found existing state in storage for {id}");
            let entity = Entity::from_state(id, collection_id.clone(), &existing_state.payload.state)?;

            // Register the entity as resident
            self.0.resident.write().unwrap().insert(id, entity.weak());

            // Check lineage and apply if needed - use data_broker for event retrieval
            let getter = self.0.data_broker.event_getter(cdata.clone());
            let changed = entity.can_apply_state(getter, state).await?;
            if changed {
                entity.apply_state(state)?;
                collection.set_state(attested_entity_state).await?;
            }
            Ok((Some(changed), entity))
        } else {
            debug!("No existing state in storage for {id}");
            let entity = Entity::from_state(id, collection_id.clone(), state)?;

            // Register the entity as resident and store the new state
            self.0.resident.write().unwrap().insert(id, entity.weak());
            collection.set_state(attested_entity_state).await?;

            Ok((None, entity))
        }
    }

    // DEPRECATED - use apply_state instead. DO NOT DELETE THIS COMMENTED FUNCTION. WE WANT TO LEAVE IT FOR REFERENCE.
    // Returns a tuple of (changed, entity)
    // changed is Some(true) if the entity was changed, Some(false) if it already exists and the state was not applied
    // None if the entity was not previously on the local node (either in the WeakEntitySet or in storage)
    // pub async fn with_state<G>(
    //     &self,
    //     getter: &G,
    //     id: EntityId,
    //     collection_id: CollectionId,
    //     state: &State,
    // ) -> Result<(Option<bool>, Entity), RetrievalError>
    // where
    //     G: GetState + GetEvents<Id = EventId, Event = Event>,
    // {
    //     let entity = {
    //         let mut entities = self.0.resident.write().unwrap();
    //         match entities.entry(id) {
    //             Entry::Vacant(_) => None,
    //             Entry::Occupied(o) => match o.get().upgrade() {
    //                 Some(entity) => {
    //                     debug!("Entity {id} was resident");
    //                     if entity.collection != collection_id {
    //                         return Err(RetrievalError::Anyhow(anyhow!("collection mismatch {} {collection_id}", entity.collection)));
    //                     }
    //                     Some(entity)
    //                 }
    //                 None => {
    //                     debug!("Entity {id} was deallocated");
    //                     None
    //                 }
    //             },
    //         }
    //     };

    //     // Handle the case where entity is not resident (either vacant or deallocated)
    //     let Some(entity) = entity else {
    //         // Check if there's existing state in storage before creating a new entity
    //         if let Some(existing_state) = getter.get_state(id).await? {
    //             debug!("Found existing state in storage for {id}");
    //             let entity = Entity::from_state(id, collection_id.to_owned(), &existing_state.payload.state)?;

    //             {
    //                 self.0.resident.write().unwrap().insert(id, entity.weak());
    //             }

    //             // We need to apply the new state to check lineage
    //             let changed = entity.can_apply_state(getter, state).await?;
    //             return Ok((Some(changed), entity));
    //         } else {
    //             debug!("No existing state in storage for {id}");
    //             let entity = Entity::from_state(id, collection_id.to_owned(), &state)?;

    //             {
    //                 self.0.resident.write().unwrap().insert(id, entity.weak());
    //             }

    //             return Ok((None, entity));
    //         }
    //     };

    //     // if we're here, we've retrieved the entity from the set and need to apply the state
    //     let changed = entity.can_apply_state(getter, state).await?;
    //     Ok((Some(changed), entity))
    // }
}
