use std::{borrow::Cow, marker::PhantomData};

use crate::{model::Model, storage::StorageBucket};

// WIP

/// Manages the storage and state of the collection without any knowledge of the model type
pub struct RawCollection {
    pub name: String,
    pub bucket: Box<dyn StorageBucket>,
}

impl RawCollection {
    pub fn new(name: String, bucket: Box<dyn StorageBucket>) -> Self {
        Self { name, bucket }
    }
}

/// Immutable API surface for a collection
pub struct Collection<'a, M: Model> {
    pub name: String,
    pub raw: &'a RawCollection,
    _marker: PhantomData<M>,
}

impl<'a, M: Model> Collection<'a, M> {
    pub fn new(name: &str, raw: &'a RawCollection) -> Self {
        Self {
            name: name.to_owned(),
            raw: raw,
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
