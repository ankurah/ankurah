use crate::{model::Model, storage::StorageBucket};

// WIP

/// Manages the storage and state of the collection without any knowledge of the model type
#[derive(Clone)]
pub struct RawCollection {
    pub name: String,
    pub bucket: Box<dyn StorageBucket>,
}

/// API surface for a collection
pub struct CollectionHandle<M: Model> {
    pub name: String,
    pub raw: RawCollection,
}

impl RawCollection {
    pub fn new(name: String, bucket: Box<dyn StorageBucket>) -> Self {
        Self { name, bucket }
    }
}

impl<M: Model> CollectionHandle<M> {
    pub fn new(name: String, raw: &RawCollection) -> Self {
        Self {
            name,
            raw: raw.clone(),
        }
    }
}
