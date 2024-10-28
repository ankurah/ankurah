use std::{marker::PhantomData, sync::Arc};

use tokio::sync::Mutex;

use crate::{model::Model, storage::StorageBucket};

// WIP

/// Manages the storage and state of the collection without any knowledge of the model type
#[derive(Clone)]
pub struct RawCollection {
    pub bucket: Arc<Box<dyn StorageBucket>>,
}

impl RawCollection {
    pub fn new(bucket: Box<dyn StorageBucket>) -> Self {
        Self { bucket: Arc::new(bucket) }
    }
}

/// Immutable API surface for a collection
pub struct Collection<M: Model> {
    pub name: String,
    pub raw: RawCollection,
    _marker: PhantomData<M>,
}

impl<'a, M: Model> Collection<M> {
    pub fn new(name: &str, raw: &RawCollection) -> Self {
        Self {
            name: name.to_owned(),
            raw: raw.clone(),
            _marker: PhantomData,
        }
    }
}

/// Mutable API surface for a collection
pub struct CollectionMut<'a, M: Model> {
    pub name: String,
    pub raw: &'a mut RawCollection,
    _marker: PhantomData<M>,
}

impl<'a, M: Model> CollectionMut<'a, M> {
    pub fn new(name: &str, raw: &'a mut RawCollection) -> Self {
        Self {
            name: name.to_owned(),
            raw: raw,
            _marker: PhantomData,
        }
    }
}
