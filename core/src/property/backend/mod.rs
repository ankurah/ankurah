use std::sync::Arc;

pub mod yrs;
pub use yrs::YrsBackend;

/// Temporary structure for constructing fields.
pub struct Backends {
    pub yrs: Arc<YrsBackend>,
}
