use std::{marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    model::Entity,
    property::{
        backend::LWWBackend,
        traits::{FromActiveType, FromEntity, PropertyError},
        InitializeWith, PropertyName,
    },
};

pub struct LWW<T>
where T: serde::Serialize + for<'de> serde::Deserialize<'de>
{
    pub property_name: PropertyName,
    pub backend: Arc<LWWBackend>,

    phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for LWW<T>
where T: serde::Serialize + for<'de> serde::Deserialize<'de>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LWW").field("property_name", &self.property_name).finish()
    }
}

impl<T> LWW<T>
where T: Serialize + for<'a> Deserialize<'a>
{
    pub fn set(&self, value: &T) -> anyhow::Result<()> {
        let value = bincode::serialize(value)?;
        self.backend.set(self.property_name.clone(), value);
        Ok(())
    }

    pub fn get(&self) -> Result<T, PropertyError> {
        match self.backend.get(self.property_name.clone()) {
            Some(bytes) => bincode::deserialize::<T>(&bytes).map_err(|err| PropertyError::DeserializeError(err)),
            None => Err(PropertyError::Missing),
        }
    }
}

impl<T> FromEntity for LWW<T>
where T: serde::Serialize + for<'de> serde::Deserialize<'de>
{
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<LWWBackend>().expect("LWW Backend should exist");
        Self { property_name: property_name, backend: backend, phantom: PhantomData }
    }
}

impl<T> FromActiveType<LWW<T>> for T
where T: serde::Serialize + for<'de> serde::Deserialize<'de>
{
    fn from_active(active: LWW<T>) -> Result<Self, PropertyError>
    where Self: Sized {
        active.get()
    }
}

impl<T> InitializeWith<T> for LWW<T>
where T: Serialize + for<'a> Deserialize<'a>
{
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &T) -> Self {
        let new = Self::from_entity(property_name, entity);
        new.set(value).unwrap();
        new
    }
}
