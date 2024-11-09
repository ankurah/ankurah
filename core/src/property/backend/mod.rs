use std::sync::Arc;

pub mod yrs;
pub use yrs::YrsBackend;

/// Holds the property backends inside of records.
pub struct Backends {
    // Probably should be an `Option` since not all records will use each backend?
    // Otherwise we might want to upcast this into something like `BTreeMap<BackendIdentifier, Box<dyn PropertyBackend>>`.
    pub yrs: Arc<YrsBackend>,
    // extend this with any backends needed.
}
