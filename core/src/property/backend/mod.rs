use ankurah_proto::{Clock, Operation, State};
use anyhow::Result;
use std::any::Any;
use std::fmt::Debug;
use std::sync::MutexGuard;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::storage::Materialized;

pub mod lww;
pub mod pn_counter;
pub mod yrs;
use crate::error::RetrievalError;
use crate::Node;
pub use lww::LWWBackend;
pub use pn_counter::PNBackend;
pub use yrs::YrsBackend;

use super::PropertyName;

pub trait PropertyBackend: Any + Send + Sync + Debug + 'static {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static>;
    fn downcasted(self: Arc<Self>, name: &str) -> BackendDowncasted {
        let upcasted = self.as_arc_dyn_any();
        match name {
            "yrs" => BackendDowncasted::Yrs(upcasted.downcast::<YrsBackend>().unwrap()),
            "lww" => BackendDowncasted::LWW(upcasted.downcast::<LWWBackend>().unwrap()),
            "pn" => BackendDowncasted::PN(upcasted.downcast::<PNBackend>().unwrap()),
            _ => BackendDowncasted::Unknown(upcasted),
        }
    }
    fn as_debug(&self) -> &dyn Debug;
    fn fork(&self) -> Box<dyn PropertyBackend>;

    fn properties(&self) -> Vec<String>;
    fn materialized(&self) -> BTreeMap<PropertyName, Materialized>;

    // TODO: This should be a specific typecast, not just a string
    fn get_property_value_string(&self, property_name: &str) -> Option<String>;

    /// Unique property backend identifier.
    fn property_backend_name() -> String
    where Self: Sized;

    /// Get the latest state buffer for this property backend.
    fn to_state_buffer(&self) -> Result<Vec<u8>>;
    /// Construct a property backend from a state buffer.
    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized;

    /// Retrieve operations applied to this backend since the last time we called this method.
    fn to_operations(&self) -> anyhow::Result<Vec<Operation>>;
    fn apply_operations(&self, operations: &Vec<Operation>, current_head: &Clock, event_precursors: &Clock, node: &Node) -> anyhow::Result<()>;
}

pub enum BackendDowncasted {
    Yrs(Arc<YrsBackend>),
    LWW(Arc<LWWBackend>),
    PN(Arc<PNBackend>),
    Unknown(Arc<dyn Any>),
}

// impl Event {
//     pub fn push(&mut self, property_backend: &'static str, operation: Operation) {
//         match self.operations.entry(property_backend.to_owned()) {
//             Entry::Occupied(mut entry) => {
//                 entry.get_mut().push(operation);
//             }
//             Entry::Vacant(entry) => {
//                 entry.insert(vec![operation]);
//             }
//         }
//     }

//     pub fn extend(&mut self, property_backend: &'static str, operations: Vec<Operation>) {
//         match self.operations.entry(property_backend.to_owned()) {
//             Entry::Occupied(mut entry) => {
//                 entry.get_mut().extend(operations);
//             }
//             Entry::Vacant(entry) => {
//                 entry.insert(operations);
//             }
//         }
//     }
// }

/// Holds the property backends inside of entities.
#[derive(Debug)]
pub struct Backends {
    pub backends: Arc<Mutex<BTreeMap<String, Arc<dyn PropertyBackend>>>>,
    pub head: Arc<Mutex<Clock>>,
}

// This is where this gets a bit tough.
// PropertyBackends should either have a concrete type of some sort,
// or if they can take a generic, they should also take a `Vec<u8>`.
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
    } else if name == "pn" {
        let backend = match buffer {
            Some(buffer) => PNBackend::from_state_buffer(buffer)?,
            None => PNBackend::new(),
        };
        Ok(Arc::new(backend))
    } else {
        panic!("unknown backend: {:?}", name);
    }
}

impl Default for Backends {
    fn default() -> Self { Self::new() }
}

impl Backends {
    pub fn new() -> Self { Self { backends: Arc::new(Mutex::new(BTreeMap::default())), head: Arc::new(Mutex::new(Clock::default())) } }

    fn backends_lock(&self) -> MutexGuard<BTreeMap<String, Arc<dyn PropertyBackend>>> {
        self.backends.lock().expect("other thread panicked, panic here too")
    }

    pub fn get<P: PropertyBackend>(&self) -> Result<Arc<P>, RetrievalError> {
        let backend_name = P::property_backend_name();
        let backend = self.get_raw(backend_name)?;
        let upcasted = backend.as_arc_dyn_any();
        Ok(upcasted.downcast::<P>().unwrap())
    }

    pub fn get_with_name(&self, backend_name: String) -> Result<BackendDowncasted, RetrievalError> {
        let backend = self.get_raw(backend_name.clone())?;
        Ok(backend.downcasted(&backend_name))
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

    pub fn downcasted(&self) -> Vec<BackendDowncasted> {
        let backends = self.backends_lock();
        backends.iter().map(|(name, backend)| backend.clone().downcasted(name)).collect()
    }

    /// Fork the data behind the backends.
    pub fn fork(&self) -> Backends {
        let backends = self.backends_lock();
        let mut forked = BTreeMap::new();
        for (name, backend) in &*backends {
            forked.insert(name.clone(), backend.fork().into());
        }

        Self { backends: Arc::new(Mutex::new(forked)), head: Arc::new(Mutex::new(self.head.lock().unwrap().clone())) }
    }

    fn insert(&self, backend_name: String, backend: Arc<dyn PropertyBackend>) {
        let mut backends = self.backends_lock();
        backends.insert(backend_name, backend);
    }

    pub fn to_state_buffers(&self) -> Result<State> {
        let backends = self.backends_lock();
        let mut state_buffers = BTreeMap::default();
        for (name, backend) in &*backends {
            let state_buffer = backend.to_state_buffer()?;
            state_buffers.insert(name.clone(), state_buffer);
        }
        Ok(State { state_buffers, head: self.head.lock().unwrap().clone() })
    }

    pub fn from_state_buffers(entity_state: &State) -> Result<Self, RetrievalError> {
        let backends = Backends::new();
        for (name, state_buffer) in &entity_state.state_buffers {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }
        *backends.head.lock().unwrap() = entity_state.head.clone();
        Ok(backends)
    }

    pub fn to_operations(&self) -> Result<BTreeMap<String, Vec<Operation>>> {
        let backends = self.backends_lock();
        let mut operations = BTreeMap::<String, Vec<Operation>>::new();
        for (name, backend) in &*backends {
            operations.insert(name.clone(), backend.to_operations()?);
        }

        Ok(operations)
    }

    pub fn apply_operations(
        &self,
        backend_name: String,
        operations: &Vec<Operation>,
        current_head: &Clock,
        event_precursors: &Clock,
        node: &Node,
    ) -> Result<()> {
        let backend = self.get_raw(backend_name)?;
        backend.apply_operations(operations, current_head, event_precursors, node)?;
        Ok(())
    }

    /// HACK - this should be based on a play forward of events
    pub fn apply_state(&self, state: &State) -> Result<(), RetrievalError> {
        let mut backends = self.backends_lock();
        for (name, state_buffer) in &state.state_buffers {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }
        *self.head.lock().unwrap() = state.head.clone();
        Ok(())
    }
}
