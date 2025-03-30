use ankurah_proto::{Clock, CollectionId, Event, State, ID};
use tracing::debug;

use std::sync::Arc;
use std::sync::Mutex;

use crate::property::PropertyValue;
use crate::{error::RetrievalError, property::Backends};

use anyhow::Result;

use ankql::selection::filter::Filterable;

/// An entity represents a unique thing within a collection
#[derive(Debug)]
pub struct Entity {
    pub id: ID,
    pub collection: CollectionId,
    pub(crate) backends: Backends,
    head: Arc<Mutex<Clock>>,
    pub upstream: Option<Arc<Entity>>,
}

impl Entity {
    pub fn collection(&self) -> CollectionId { self.collection.clone() }

    pub fn backends(&self) -> &Backends { &self.backends }

    pub fn to_state(&self) -> Result<State> { self.backends.to_state_buffers() }

    // used by the Model macro
    pub fn create(id: ID, collection: CollectionId, backends: Backends) -> Self {
        Self { id, collection, backends, head: Arc::new(Mutex::new(Clock::default())), upstream: None }
    }
    pub fn from_state(id: ID, collection: CollectionId, state: &State) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(state)?;

        Ok(Self { id, collection, backends, head: Arc::new(Mutex::new(state.head.clone())), upstream: None })
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
    pub fn snapshot(self: &Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            id: self.id.clone(),
            collection: self.collection.clone(),
            backends: self.backends.fork(),
            head: Arc::new(Mutex::new(self.head.lock().unwrap().clone())),
            upstream: Some(self.clone()),
        })
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

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entity({}/{}) = {}", self.collection, self.id, self.head.lock().unwrap())
    }
}
