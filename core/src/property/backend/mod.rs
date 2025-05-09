use ankurah_proto::{Operation, State, StateBuffers};
use anyhow::Result;
use std::any::Any;
use std::fmt::Debug;
use std::sync::MutexGuard;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub mod entity_ref;
pub mod lww;
//pub mod pn_counter;
pub mod yrs;
use crate::error::{MutationError, RetrievalError, StateError};

pub use entity_ref::RefBackend;
pub use lww::LWWBackend;
//pub use pn_counter::PNBackend;
pub use yrs::YrsBackend;

use super::{PropertyName, PropertyValue};

pub trait PropertyBackend: Any + Send + Sync + Debug + 'static {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static>;
    fn as_debug(&self) -> &dyn Debug;
    fn fork(&self) -> Box<dyn PropertyBackend>;

    fn properties(&self) -> Vec<PropertyName>;
    fn property_value(&self, property_name: &PropertyName) -> Option<PropertyValue> {
        let mut map = self.property_values();
        map.remove(property_name).flatten()
    }
    fn property_values(&self) -> BTreeMap<PropertyName, Option<PropertyValue>>;

    /// Unique property backend identifier.
    fn property_backend_name() -> String
    where Self: Sized;

    /// Get the latest state buffer for this property backend.
    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError>;
    /// Construct a property backend from a state buffer.
    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized;

    /// Retrieve operations applied to this backend since the last time we called this method.
    fn to_operations(&self) -> Result<Vec<Operation>, MutationError>;

    fn apply_operations(&self, operations: &Vec<Operation>) -> Result<(), MutationError>;
}

/// Holds the property backends inside of entities.
#[derive(Debug)]
pub struct Backends {
    pub backends: Arc<Mutex<BTreeMap<String, Arc<dyn PropertyBackend>>>>,
}

// This is where this gets a bit tough.
// PropertyBackends should either have a concrete type of some sort,
// or if they can take a generic, they should also take a `Vec<u8>`.

// TODO: Implement a property backend type registry rather than this hardcoded nonsense.
pub fn backend_from_string(name: &str, buffer: Option<&Vec<u8>>) -> Result<Arc<dyn PropertyBackend>, RetrievalError> {
    if name == "yrs" {
        let backend = match buffer {
            Some(buffer) => YrsBackend::from_state_buffer(buffer)?,
            None => YrsBackend::new(),
        };
        Ok(Arc::new(backend))
    } else if name == "lww" {
        let backend = match buffer {
            Some(buffer) => LWWBackend::from_state_buffer(buffer)?,
            None => LWWBackend::new(),
        };
        Ok(Arc::new(backend))
    } else if name == "ref" {
        let backend = match buffer {
            Some(buffer) => RefBackend::from_state_buffer(buffer)?,
            None => RefBackend::new(),
        };
        Ok(Arc::new(backend))
    }
    /*else if name == "pn" {
        let backend = match buffer {
            Some(buffer) => PNBackend::from_state_buffer(buffer)?,
            None => PNBackend::new(),
        };
        Ok(Arc::new(backend))
    } */
    else {
        panic!("unknown backend: {:?}", name);
    }
}

impl Default for Backends {
    fn default() -> Self { Self::new() }
}

impl Backends {
    pub fn new() -> Self { Self { backends: Arc::new(Mutex::new(BTreeMap::default())) } }

    fn backends_lock(&self) -> MutexGuard<BTreeMap<String, Arc<dyn PropertyBackend>>> {
        self.backends.lock().expect("other thread panicked, panic here too")
    }

    pub fn get<P: PropertyBackend>(&self) -> Result<Arc<P>, RetrievalError> {
        let backend_name = P::property_backend_name();
        let backend = self.get_raw(backend_name)?;
        let upcasted = backend.as_arc_dyn_any();
        Ok(upcasted.downcast::<P>().unwrap())
    }

    pub fn get_raw(&self, backend_name: String) -> Result<Arc<dyn PropertyBackend>, RetrievalError> {
        let mut backends = self.backends_lock();
        if let Some(backend) = backends.get(&backend_name) {
            Ok(backend.clone())
        } else {
            let backend = backend_from_string(&backend_name, None)?;
            backends.insert(backend_name, backend.clone());
            Ok(backend)
        }
    }

    /// Fork the data behind the backends.
    pub fn fork(&self) -> Backends {
        let backends = self.backends_lock();
        let mut forked = BTreeMap::new();
        for (name, backend) in &*backends {
            forked.insert(name.clone(), backend.fork().into());
        }

        Self { backends: Arc::new(Mutex::new(forked)) }
    }

    fn insert(&self, backend_name: String, backend: Arc<dyn PropertyBackend>) {
        let mut backends = self.backends_lock();
        backends.insert(backend_name, backend);
    }

    pub fn to_state_buffers(&self) -> Result<StateBuffers, StateError> {
        let backends = self.backends_lock();
        let mut state_buffers = BTreeMap::default();
        for (name, backend) in &*backends {
            let state_buffer = backend.to_state_buffer()?;
            state_buffers.insert(name.clone(), state_buffer);
        }
        Ok(StateBuffers(state_buffers))
    }

    pub fn from_state_buffers(state_buffers: &StateBuffers) -> Result<Self, RetrievalError> {
        let backends = Backends::new();
        for (name, state_buffer) in state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }
        Ok(backends)
    }

    pub fn take_accumulated_operations(&self) -> Result<BTreeMap<String, Vec<Operation>>, MutationError> {
        let backends = self.backends_lock();
        let mut operations = BTreeMap::<String, Vec<Operation>>::new();
        for (name, backend) in &*backends {
            operations.insert(name.clone(), backend.to_operations()?);
        }

        Ok(operations)
    }

    pub fn apply_operations(&self, backend_name: String, operations: &Vec<Operation>) -> Result<(), MutationError> {
        let backend = self.get_raw(backend_name)?;
        backend.apply_operations(operations)?;
        Ok(())
    }

    /// HACK - this should be based on a play forward of events
    pub fn apply_state(&self, state: &State) -> Result<(), MutationError> {
        let mut backends = self.backends_lock();
        for (name, state_buffer) in state.state_buffers.iter() {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }
        Ok(())
    }

    pub fn property_values(&self) -> BTreeMap<PropertyName, Option<PropertyValue>> {
        let backends = self.backends_lock();
        let mut map = BTreeMap::new();
        for (_, backend) in backends.iter() {
            let values = backend.property_values();
            for (property, value) in values {
                if map.contains_key(&property) {
                    panic!("Property '{:?}' is in multiple property backends", property);
                }

                map.insert(property, value);
            }
        }
        map
    }
}
