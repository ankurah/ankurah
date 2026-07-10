use ankurah_proto::{EventId, Operation};
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
// See the RFC #267 conformance kit and the C3 PR body for the full rationale
// and what a future counter backend must provide.
#[cfg(test)]
mod conformance;
pub mod lww;
pub mod yrs;
use crate::error::{MutationError, RetrievalError, StateError};
use crate::event_dag::EventLayer;
pub use lww::LWWBackend;
pub use yrs::YrsBackend;

use super::{PropertyKey, Value};

// TODO - implement a property backend value iterator so we don't have to alloc a HashMap for every call to values()

pub trait PropertyBackend: Any + Send + Sync + Debug + 'static {
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static>;
    fn as_debug(&self) -> &dyn Debug;
    fn fork(&self) -> Arc<dyn PropertyBackend>;

    fn properties(&self) -> Vec<PropertyKey>;
    fn property_value(&self, key: &PropertyKey) -> Option<Value> {
        let mut map = self.property_values();
        map.remove(key).flatten()
    }
    fn property_values(&self) -> BTreeMap<PropertyKey, Option<Value>>;

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

    /// Apply operations without event tracking.
    /// Used when loading from state buffer (no associated event).
    fn apply_operations(&self, operations: &[Operation]) -> Result<(), MutationError>;

    /// Apply operations with event tracking.
    ///
    /// This tracks which event set each property value, enabling per-property
    /// conflict resolution when concurrent events arrive later.
    ///
    /// For CRDT backends (like Yrs), this is equivalent to `apply_operations`
    /// since CRDTs handle concurrency internally.
    ///
    /// For LWW backends, this tracks the event_id for each modified property.
    fn apply_operations_with_event(&self, operations: &[Operation], event_id: EventId) -> Result<(), MutationError> {
        // Default implementation ignores event_id (suitable for CRDTs)
        let _ = event_id;
        self.apply_operations(operations)
    }

    /// Apply a layer of concurrent events.
    ///
    /// All events in `layer.already_applied` and `layer.to_apply` are mutually concurrent
    /// (same causal depth from meet). The backend receives ALL events for
    /// complete context, but only needs to mutate state for `to_apply` events.
    ///
    /// # Contract
    /// - All events in the layer are mutually concurrent (no causal relationship)
    /// - `already_applied` events are in the current state (for context/comparison)
    /// - `to_apply` events are new and need processing
    /// - Backend MUST implement this method (no default)
    ///
    /// # For LWW backends
    /// Determine per-property winner by causal dominance among candidates,
    /// using `layer.compare(...)` to decide causal relationship. Use
    /// lexicographic EventId only for truly concurrent candidates.
    ///
    /// # For CRDT backends (Yrs)
    /// Apply all operations from `to_apply` events. Order within layer doesn't
    /// matter (CRDTs are commutative). Can ignore `already_applied`.
    fn apply_layer(&self, layer: &EventLayer) -> Result<(), MutationError>;

    /// Listen to changes for a specific field managed by this backend.
    /// Auto-creates the broadcast if it doesn't exist yet.
    /// Returns a subscription guard that will unsubscribe when dropped.
    fn listen_field(&self, key: &PropertyKey, listener: ankurah_signals::signal::Listener) -> ankurah_signals::signal::ListenerGuard;

    /// The keys currently staged (uncommitted) in this backend, so the commit
    /// path can resolve a transient [`PropertyKey::Name`] to its
    /// [`PropertyKey::Id`] before the event is generated (the PropertyKey
    /// amendment, #289). Default empty: backends that resolve keys at write
    /// time (yrs) stage nothing to re-key.
    fn uncommitted_keys(&self) -> Vec<PropertyKey> { Vec::new() }

    /// Move a staged value from `from` to `to` at commit (Name -> Id
    /// resolution). A pure key operation -- the catalog-aware caller decides
    /// the mapping, so the backend never sees the catalog. Default no-op.
    fn rekey(&self, from: &PropertyKey, to: PropertyKey) { let _ = (from, to); }
}

// This is where this gets a bit tough.
// PropertyBackends should either have a concrete type of some sort,
// or if they can take a generic, they should also take a `Vec<u8>`.

// TODO: Implement a property backend type registry rather than this hardcoded nonsense.
/// Fire the change broadcast for each named field that has subscribers.
pub(crate) fn notify_changed_fields<'a>(
    field_broadcasts: &std::sync::Mutex<std::collections::BTreeMap<crate::property::PropertyKey, ankurah_signals::broadcast::Broadcast>>,
    changed: impl IntoIterator<Item = &'a crate::property::PropertyKey>,
) {
    let broadcasts = field_broadcasts.lock().expect("field_broadcasts lock is poisoned");
    for key in changed {
        if let Some(broadcast) = broadcasts.get(key) {
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
        Err(RetrievalError::Other(format!("unknown backend: {}", name)))
    }
}
