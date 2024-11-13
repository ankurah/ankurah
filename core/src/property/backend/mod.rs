use std::sync::Arc;

pub mod yrs;
pub use yrs::YrsBackend;

use crate::{error::RetrievalError, model::RecordInner, storage::RecordState};

use anyhow::Result;

/// Holds the property backends inside of records.
#[derive(Debug, Clone)]
pub struct Backends {
    // Probably should be an `Option` since not all records will use each backend?
    // Otherwise we might want to upcast this into something like `BTreeMap<BackendIdentifier, Box<dyn PropertyBackend>>`.
    pub yrs: Arc<YrsBackend>,
    // extend this with any backends needed.
}

impl Backends {
    pub fn new(inner: Arc<RecordInner>) -> Self {
        let yrs = Arc::new(YrsBackend::new(inner.clone()));
        Self {
            yrs,
        }
    }

    pub fn from_state_buffers(inner: Arc<RecordInner>, record_state: &RecordState) -> Result<Self, RetrievalError> {
        let yrs = Arc::new(YrsBackend::from_state_buffer(inner.clone(), &record_state.yrs_state_buffer)?);
        Ok(Self {
            yrs,
        })
    }
}