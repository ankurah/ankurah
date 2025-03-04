use crate::error::{MutationError, RetrievalError, StateError};
use ankql::selection::filter::Filterable;
use ankurah_proto::{Clock, CollectionId, Event, State, ID};
use std::sync::{Arc, Mutex, Weak};

use crate::model::View;
use crate::property::Backends;
use tracing::info;

/// An entity represents a unique thing within a collection
#[derive(Debug, Clone)]
pub struct Entity(Arc<Inner>);

#[derive(Debug, Clone)]
pub struct WeakEntity(Weak<Inner>);

#[derive(Debug)]
struct Inner {
    pub id: ID,
    pub collection: CollectionId,
    pub(crate) backends: Backends,
    head: Mutex<Clock>,
    pub upstream: Option<Entity>,
}

impl WeakEntity {
    pub fn upgrade(&self) -> Option<Entity> { self.0.upgrade().map(Entity) }
}

impl Entity {
    pub fn id(&self) -> &ID { &self.0.id }
    pub(crate) fn upstream(&self) -> Option<&Entity> { self.0.upstream.as_ref() }
    pub(crate) fn weak(&self) -> WeakEntity { WeakEntity(Arc::downgrade(&self.0)) }

    pub fn collection(&self) -> &CollectionId { &self.0.collection }

    pub(crate) fn backends(&self) -> &Backends { &self.0.backends }

    pub(crate) fn to_state(&self) -> Result<State, StateError> { self.0.backends.to_state_buffers() }

    // used by the Model macro
    pub fn create(id: ID, collection: CollectionId, backends: Backends) -> Self {
        Self(Arc::new(Inner { id, collection, backends, head: Mutex::new(Clock::default()), upstream: None }))
    }
    pub fn from_state(id: ID, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(state)?;

        Ok(Self(Arc::new(Inner { id, collection, backends, head: Mutex::new(state.head.clone()), upstream: None })))
    }

    /// Collect an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit.
    /// TODO: We need to think about rollbacks
    pub fn commit(&self) -> Result<Option<Event>, MutationError> {
        let operations = self.0.backends.to_operations()?;
        if operations.is_empty() {
            Ok(None)
        } else {
            let event = {
                let event = Event {
                    id: ID::new(),
                    entity_id: self.0.id.clone(),
                    collection: self.0.collection.clone(),
                    operations,
                    parent: self.0.head.lock().unwrap().clone(),
                };

                // Set the head to the event's ID
                *self.0.head.lock().unwrap() = Clock::new([event.id]);
                event
            };

            info!("Commit {}", self);
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

    // pub fn property<T: Property>(&self, name: &str) -> Option<&T> { self.backends.get_property::<T>(name) }

    /*
        entity1: [], head: [],
        event1: ["blah"], precursors: [],
        entity1: ["blah"], head: [event1],

        event2: [], precursor: [event1],
        event3: [], precursor: [event1],
        event4: [], precursor: [event2, event3],
        [event4] == [event4, event3, event2, event1]

        enum ClockOrder {
            Descends,
            Concurrent,
            IsDescendedBy,
            Divergent,
            ComparisonBudgetExceeded,
        }

        impl Clock {
            pub async fn compare(&self, other: &Self, node: &Node) -> ClockOrdering {

            }
        }
    */

    pub fn apply_event(&self, event: &Event) -> Result<(), MutationError> {
        /*
           case A: event precursor descends the current head, then set entity clock to singleton of event id
           case B: event precursor is concurrent to the current head, push event id to event head clock.
           case C: event precursor is descended by the current head
        */
        let head = Clock::new([event.id]);
        for (backend_name, operations) in &event.operations {
            // TODO - backends and Entity should not have two copies of the head. Figure out how to unify them
            self.0.backends.apply_operations((*backend_name).to_owned(), operations, &head, &event.parent /* , context*/)?;
        }
        // TODO figure out how to test this
        // info!("Apply event {}", event);

        *self.0.head.lock().unwrap() = head.clone();
        // Hack
        *self.0.backends.head.lock().unwrap() = head;
        // info!("Apply event MARK 2 new head {}", self.0.head.lock().unwrap());

        Ok(())
    }

    /// HACK - we probably shouldn't be stomping on the backends like this
    pub fn apply_state(&self, state: &State) -> Result<(), RetrievalError> {
        self.0.backends.apply_state(state)?;
        Ok(())
    }

    /// Create a snapshot of the Entity which is detached from this one, and will not receive the updates this one does
    pub fn snapshot(&self) -> Entity {
        Entity(Arc::new(Inner {
            id: self.0.id.clone(),
            collection: self.0.collection.clone(),
            backends: self.0.backends.fork(),
            head: Mutex::new(self.0.head.lock().unwrap().clone()),
            upstream: Some(self.clone()),
        }))
    }
}

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity({}/{}) = {}", self.0.collection, self.0.id, self.0.head.lock().unwrap())
    }
}

impl Filterable for Entity {
    fn collection(&self) -> &str { self.0.collection.as_str() }

    /// TODO Implement this as a typecasted value. eg value<T> -> Option<Result<T>>
    /// where None is returned if the property is not found, and Err is returned if the property is found but is not able to be typecasted
    /// to the requested type. (need to think about the rust type system here more)
    fn value(&self, name: &str) -> Option<String> {
        if name == "id" {
            Some(self.0.id.to_string())
        } else {
            // Iterate through backends to find one that has this property
            self.0.backends.backends.lock().unwrap().values().find_map(|backend| backend.get_property_value_string(name))
        }
    }
}
