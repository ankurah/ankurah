use std::{marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    model::Entity,
    property::{
        backend::LWWBackend,
        traits::{FromActiveType, FromEntity, PropertyError},
        InitializeWith, PropertyName, PropertyValue,
    },
};

pub struct LWW<T>
where
    T: TryFrom<PropertyValue, Error=PropertyError>,
    for<'a> &'a T: TryInto<PropertyValue, Error=PropertyError>,
{
    pub property_name: PropertyName,
    pub backend: Arc<LWWBackend>,

    phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for LWW<T>
where
    T: TryFrom<PropertyValue, Error=PropertyError>,
    for<'a> &'a T: TryInto<PropertyValue, Error=PropertyError>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LWW").field("property_name", &self.property_name).finish()
    }
}

impl<T> LWW<T>
where
    T: TryFrom<PropertyValue, Error=PropertyError>,
    for<'a> &'a T: TryInto<PropertyValue, Error=PropertyError>,
{
    pub fn set(&self, value: &T) -> Result<(), PropertyError> {
        let value: PropertyValue = value.try_into()?;
        self.backend.set(self.property_name.clone(), value);
        Ok(())
    }

    pub fn get(&self) -> Result<T, PropertyError> { 
        let value = self.get_value()?;
        value.try_into()
    }

    pub fn get_value(&self) -> Result<PropertyValue, PropertyError> {
        match self.backend.get(self.property_name.clone()) {
            Some(value) => Ok(value),
            None => Err(PropertyError::Missing),
        }
    }
}

impl<T> FromEntity for LWW<T>
where
    T: TryFrom<PropertyValue, Error=PropertyError>,
    for<'a> &'a T: TryInto<PropertyValue, Error=PropertyError>,
{
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<LWWBackend>().expect("LWW Backend should exist");
        Self { property_name: property_name, backend: backend, phantom: PhantomData }
    }
}

impl<T> FromActiveType<LWW<T>> for T
where
    T: TryFrom<PropertyValue, Error=PropertyError>,
    for<'a> &'a T: TryInto<PropertyValue, Error=PropertyError>,
{
    fn from_active(active: LWW<T>) -> Result<Self, PropertyError>
    where Self: Sized {
        active.get()
    }
}

impl<T> InitializeWith<T> for LWW<T>
where
    T: TryFrom<PropertyValue, Error=PropertyError>,
    for<'a> &'a T: TryInto<PropertyValue, Error=PropertyError>,
{
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &T) -> Self {
        let new = Self::from_entity(property_name, entity);
        new.set(value).unwrap();
        new
    }
}
