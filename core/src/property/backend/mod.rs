use ankurah_proto::Operation;
use anyhow::Result;
use std::any::Any;
use std::fmt::Debug;
use std::{collections::BTreeMap, sync::Arc};

pub mod lww;
//pub mod pn_counter;
pub mod yrs;
use crate::error::{MutationError, RetrievalError, StateError};
pub use lww::LWWBackend;
//pub use pn_counter::PNBackend;
pub use yrs::YrsBackend;

use super::{PropertyName, Value};

// TODO - implement a property backend value iterator so we don't have to alloc a HashMap for every call to values()

pub trait PropertyBackend: Any + Send + Sync + Debug + 'static {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static>;
    fn as_debug(&self) -> &dyn Debug;
    fn fork(&self) -> Arc<dyn PropertyBackend>;

    fn properties(&self) -> Vec<PropertyName>;
    fn property_value(&self, property_name: &PropertyName) -> Option<Value> {
        let mut map = self.property_values();
        map.remove(property_name).flatten()
    }
    fn property_values(&self) -> BTreeMap<PropertyName, Option<Value>>;

    /// Unique property backend identifier.
    fn property_backend_name() -> String
    where Self: Sized;

    /// Get the latest state buffer for this property backend.
    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError>;
    /// Construct a property backend from a state buffer.
    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized;

    /// Retrieve operations applied to this backend since the last time we called this method.
    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError>;

    fn apply_operations(&self, operations: &Vec<Operation>) -> Result<(), MutationError>;

    /// Listen to changes for a specific field managed by this backend.
    /// Auto-creates the broadcast if it doesn't exist yet.
    /// Returns a subscription guard that will unsubscribe when dropped.
    fn listen_field(
        &self,
        field_name: &PropertyName,
        listener: ankurah_signals::broadcast::Listener,
    ) -> ankurah_signals::broadcast::ListenerGuard;
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
