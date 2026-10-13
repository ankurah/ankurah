use crate::internal::prelude::*;
use ankurah_proto::BackendOperation as Operation;
use anyhow::Result;
use std::any::Any;
use std::fmt::Debug;
use std::{collections::BTreeMap, sync::Arc};

// The dormant pn_counter backend (issues #37/#38, a commutative i64
// counter) was deleted rather than revived: it predated the July trait
// factorization by 19 compile errors, and a counter cannot satisfy the
// conformance kit's cross-order determinism law without being rebuilt as a
// provenance-tracking, idempotent op-counter (apply_layer summing deltas is
// non-idempotent and the counter kept no per-event id to de-duplicate on).
// See issue #267's conformance kit discussion for the full rationale
// and what a future counter backend must provide.
#[cfg(test)]
mod conformance;
pub mod lww;
pub mod yrs;
use crate::error::StateError;
use crate::event_dag::EventLayer;
pub use lww::LWWBackend;
pub use yrs::YrsBackend;

use super::Value;
use ankurah_proto::PropertyId;

// TODO - implement a property backend value iterator so we don't have to alloc a HashMap for every call to values()

pub trait PropertyBackend: Any + Send + Sync + Debug + 'static {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static>;
    fn as_debug(&self) -> &dyn Debug;
    fn fork(&self) -> Arc<dyn PropertyBackend>;

    fn properties(&self) -> Vec<PropertyId>;
    fn property_value(&self, property: &PropertyId) -> Option<Value> {
        let mut map = self.property_values();
        map.remove(property).flatten()
    }
    fn property_values(&self) -> BTreeMap<PropertyId, Option<Value>>;

    /// Unique property backend identifier.
    fn property_backend_name() -> &'static str
    where Self: Sized;

    /// Get the latest state buffer for this property backend.
    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError>;
    /// Construct a property backend from a state buffer.
    fn from_state_buffer(state_buffer: &Vec<u8>) -> std::result::Result<Self, crate::error::RetrievalError>
    where Self: Sized;

    /// Retrieve operations applied to this backend since the last time we called this method.
    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError>;

    /// Apply this backend's operations from a layer of concurrent events.
    /// Layers are applied in causal order. Event order within a layer must not
    /// affect the result; operation order within each event must be preserved.
    fn apply_layer(&self, layer: &EventLayer) -> Result<(), MutationError>;

    /// Listen to changes for a specific field managed by this backend.
    /// Auto-creates the broadcast if it doesn't exist yet.
    /// Returns a subscription guard that will unsubscribe when dropped.
    fn listen_field(&self, property: &PropertyId, listener: ankurah_signals::signal::Listener) -> ankurah_signals::signal::ListenerGuard;
}

// This is where this gets a bit tough.
// PropertyBackends should either have a concrete type of some sort,
// or if they can take a generic, they should also take a `Vec<u8>`.

// TODO: Implement a property backend type registry rather than this hardcoded nonsense.
/// Fire the change broadcast for each changed field that has subscribers.
pub(crate) fn notify_changed_fields<'a>(
    field_broadcasts: &std::sync::Mutex<std::collections::BTreeMap<PropertyId, ankurah_signals::broadcast::Broadcast>>,
    changed: impl IntoIterator<Item = &'a PropertyId>,
) {
    let broadcasts = field_broadcasts.lock().expect("field_broadcasts lock is poisoned");
    for property in changed {
        if let Some(broadcast) = broadcasts.get(property) {
            broadcast.send(());
        }
    }
}

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
    } else {
        Err(RetrievalError::InvalidState(format!("unknown backend: {name}")))
    }
}
