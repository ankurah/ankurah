use std::{
    marker::PhantomData,
    sync::{Arc, Weak},
};

use crate::{
    model::Entity,
    property::{
        backend::YrsBackend,
        traits::{FromActiveType, FromEntity, InitializeWith, PropertyError},
        PropertyName,
    },
};

#[derive(Debug)]
pub struct YrsString<Projected> {
    // ideally we'd store the yrs::TransactionMut in the Transaction as an ExtendableOp or something like that
    // and call encode_update_v2 on it when we're ready to commit
    // but its got a lifetime of 'doc and that requires some refactoring
    pub property_name: PropertyName,
    pub backend: Weak<YrsBackend>,
    phantom: PhantomData<Projected>,
}

// Starting with basic string type operations
impl<Projected> YrsString<Projected> {
    pub fn new(property_name: PropertyName, backend: Arc<YrsBackend>) -> Self {
        Self { property_name, backend: Arc::downgrade(&backend), phantom: PhantomData }
    }
    pub fn backend(&self) -> Arc<YrsBackend> { self.backend.upgrade().expect("Expected `Yrs` property backend to exist") }
    pub fn value(&self) -> Option<String> { self.backend().get_string(&self.property_name) }
    pub fn insert(&self, index: u32, value: &str) { self.backend().insert(&self.property_name, index, value); }
    pub fn delete(&self, index: u32, length: u32) { self.backend().delete(&self.property_name, index, length); }
    pub fn overwrite(&self, start: u32, length: u32, value: &str) {
        self.backend().delete(&self.property_name, start, length);
        self.backend().insert(&self.property_name, start, value);
    }
    pub fn replace(&self, value: &str) {
        self.backend().delete(&self.property_name, 0, self.value().unwrap_or_default().len() as u32);
        self.backend().insert(&self.property_name, 0, value);
    }
}

impl<Projected> FromEntity for YrsString<Projected> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<YrsBackend>().expect("YrsBackend should exist");
        Self::new(property_name, backend)
    }
}

impl<Projected, S: FromActiveType<YrsString<Projected>>> FromActiveType<YrsString<Projected>> for Option<S> {
    fn from_active(active: YrsString<Projected>) -> Result<Self, PropertyError> {
        match S::from_active(active) {
            Ok(value) => Ok(Some(value)),
            Err(PropertyError::Missing) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl<Projected> FromActiveType<YrsString<Projected>> for String {
    fn from_active(active: YrsString<Projected>) -> Result<Self, PropertyError> {
        match active.value() {
            Some(value) => Ok(value),
            None => Err(PropertyError::Missing),
        }
    }
}

impl<'a, Projected> FromActiveType<YrsString<Projected>> for std::borrow::Cow<'a, str> {
    fn from_active(active: YrsString<Projected>) -> Result<Self, PropertyError> {
        match active.value() {
            Some(value) => Ok(Self::from(value)),
            None => Err(PropertyError::Missing),
        }
    }
}

impl<Projected> InitializeWith<String> for YrsString<Projected> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &String) -> Self {
        let new_string = Self::from_entity(property_name, entity);
        new_string.insert(0, value);
        new_string
    }
}

impl<Projected> InitializeWith<Option<String>> for YrsString<Projected> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &Option<String>) -> Self {
        let new_string = Self::from_entity(property_name, entity);
        if let Some(value) = value {
            new_string.insert(0, value);
        }
        new_string
    }
}
