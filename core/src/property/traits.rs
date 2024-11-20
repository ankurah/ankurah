use anyhow::Result;

use crate::property::backend::Backends;

pub trait InitializeWith<T> {
    fn initialize_with(backends: &Backends, property_name: &'static str, value: &T) -> Self;
}

pub trait StateSync {
    /// Apply an update to the field from an event/operation
    fn apply_update(&self, update: &[u8]) -> Result<()>;

    /// Retrieve the current state of the field, suitable for storing in the materialized record
    fn state(&self) -> Vec<u8>;

    /// Retrieve the pending update for this field since the last call to this method
    fn get_pending_update(&self) -> Option<Vec<u8>>;
}
