use std::{
    any::Any,
    collections::{btree_map::Entry, BTreeMap},
    fmt::Debug,
    sync::{Arc, Mutex},
};

pub mod lww;
pub mod pn_counter;
pub mod yrs;
pub use lww::LWWBackend;
pub use pn_counter::PNBackend;
pub use yrs::YrsBackend;

use crate::{
    error::RetrievalError,
    storage::{Materialized, RecordState},
    ID,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

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

    /// Unique property backend identifier.
    fn property_backend_name() -> String
    where
        Self: Sized;

    /// Get the latest state buffer for this property backend.
    fn to_state_buffer(&self) -> Result<Vec<u8>>;
    /// Construct a property backend from a state buffer.
    fn from_state_buffer(
        state_buffer: &Vec<u8>,
    ) -> std::result::Result<Self, crate::error::RetrievalError>
    where
        Self: Sized;

    /// Retrieve operations applied to this backend since the last time we called this method.
    // TODO: Should this take a precursor id?
    fn to_operations(&self /*precursor: ULID*/) -> anyhow::Result<Vec<Operation>>;
    fn apply_operations(&self, operations: &Vec<Operation>) -> anyhow::Result<()>;
}

pub enum BackendDowncasted {
    Yrs(Arc<YrsBackend>),
    LWW(Arc<LWWBackend>),
    PN(Arc<PNBackend>),
    Unknown(Arc<dyn Any>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordEvent {
    pub id: ID,
    pub bucket_name: &'static str,
    pub operations: BTreeMap<String, Vec<Operation>>,
}

impl RecordEvent {
    pub fn new(id: ID, bucket_name: &'static str) -> Self {
        Self {
            id: id,
            bucket_name: bucket_name,
            operations: BTreeMap::default(),
        }
    }

    pub fn id(&self) -> ID {
        self.id
    }

    pub fn bucket_name(&self) -> &'static str {
        self.bucket_name
    }

    pub fn is_empty(&self) -> bool {
        let mut empty = true;
        for (_, operations) in &self.operations {
            if operations.len() > 0 {
                empty = false;
            }
        }

        empty
    }

    pub fn push(&mut self, property_backend: &'static str, operation: Operation) {
        match self.operations.entry(property_backend.to_owned()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(operation);
            }
            Entry::Vacant(entry) => {
                entry.insert(vec![operation]);
            }
        }
    }

    pub fn extend(&mut self, property_backend: &'static str, operations: Vec<Operation>) {
        match self.operations.entry(property_backend.to_owned()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().extend(operations);
            }
            Entry::Vacant(entry) => {
                entry.insert(operations);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Operation {
    pub diff: Vec<u8>,
}

/// Holds the property backends inside of records.
#[derive(Debug)]
pub struct Backends {
    pub backends: Arc<Mutex<BTreeMap<String, Arc<dyn PropertyBackend>>>>,
}

// This is where this gets a bit tough.
// PropertyBackends should either have a concrete type of some sort,
// or if they can take a generic, they should also take a `Vec<u8>`.
pub fn backend_from_string(
    name: &str,
    buffer: Option<&Vec<u8>>,
) -> Result<Arc<dyn PropertyBackend>, RetrievalError> {
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
    } else {
        panic!("unknown backend: {:?}", name);
    }
}

impl Backends {
    pub fn new() -> Self {
        Self {
            backends: Arc::new(Mutex::new(BTreeMap::default())),
        }
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

    pub fn get_raw(
        &self,
        backend_name: String,
    ) -> Result<Arc<dyn PropertyBackend>, RetrievalError> {
        let mut backends = self.backends.lock().unwrap();
        if let Some(backend) = backends.get(&backend_name) {
            Ok(backend.clone())
        } else {
            let backend = backend_from_string(&backend_name, None)?;
            backends.insert(backend_name, backend.clone());
            Ok(backend)
        }
    }

    pub fn downcasted(&self) -> Vec<BackendDowncasted> {
        let backends = self.backends.lock().unwrap();
        backends
            .iter()
            .map(|(name, backend)| backend.clone().downcasted(name))
            .collect()
    }

    /// Fork the data behind the backends.
    pub fn fork(&self) -> Backends {
        let backends = self.backends.lock().unwrap();
        let mut forked = BTreeMap::new();
        for (name, backend) in &*backends {
            forked.insert(name.clone(), backend.fork().into());
        }

        Self {
            backends: Arc::new(Mutex::new(forked)),
        }
    }

    fn insert(&self, backend_name: String, backend: Arc<dyn PropertyBackend>) {
        let mut backends = self.backends.lock().expect("Backends lock failed");
        backends.insert(backend_name, backend);
    }

    pub fn to_state_buffers(&self) -> Result<RecordState> {
        let backends = self.backends.lock().expect("Backends lock failed");
        let mut state_buffers = BTreeMap::default();
        for (name, backend) in &*backends {
            let state_buffer = backend.to_state_buffer()?;
            state_buffers.insert(name.clone(), state_buffer);
        }
        Ok(RecordState { state_buffers })
    }

    pub fn from_state_buffers(record_state: &RecordState) -> Result<Self, RetrievalError> {
        let backends = Backends::new();
        for (name, state_buffer) in &record_state.state_buffers {
            let backend = backend_from_string(name, Some(state_buffer))?;
            backends.insert(name.to_owned(), backend);
        }
        Ok(backends)
    }

    pub fn to_operations(&self) -> Result<BTreeMap<String, Vec<Operation>>> {
        let backends = self.backends.lock().unwrap();
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
    ) -> Result<()> {
        let backend = self.get_raw(backend_name)?;
        backend.apply_operations(operations)?;
        Ok(())
    }
}
