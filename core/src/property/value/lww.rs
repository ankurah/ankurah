use std::{marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    model::Entity,
    property::{
        backend::LWWBackend,
        traits::{FromActiveType, FromEntity, PropertyError},
        InitializeWith, Property, PropertyName, PropertyValue,
    },
};

pub struct LWW<T: Property> {
    pub property_name: PropertyName,
    pub backend: Arc<LWWBackend>,

    phantom: PhantomData<T>,
}

impl<T: Property> std::fmt::Debug for LWW<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LWW").field("property_name", &self.property_name).finish()
    }
}

impl<T: Property> LWW<T> {
    pub fn set(&self, value: &T) -> Result<(), PropertyError> {
        let value = value.into_value()?;
        self.backend.set(self.property_name.clone(), value);
        Ok(())
    }

    pub fn get(&self) -> Result<T, PropertyError> {
        let value = self.get_value();
        T::from_value(value)
    }

    pub fn get_value(&self) -> Option<PropertyValue> { self.backend.get(&self.property_name) }
}

impl<T: Property> FromEntity for LWW<T> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<LWWBackend>().expect("LWW Backend should exist");
        Self { property_name: property_name, backend: backend, phantom: PhantomData }
    }
}

impl<T: Property> FromActiveType<LWW<T>> for T {
    fn from_active(active: LWW<T>) -> Result<Self, PropertyError>
    where Self: Sized {
        active.get()
    }
}

impl<T: Property> InitializeWith<T> for LWW<T> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &T) -> Self {
        let new = Self::from_entity(property_name, entity);
        new.set(value).unwrap();
        new
    }
}
