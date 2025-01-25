use std::{marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    model::Entity,
    property::{backend::LWWBackend, traits::FromEntity, InitializeWith, PropertyName},
};

use super::ProjectedValue;

pub struct LWW<T> {
    pub property_name: PropertyName,
    pub backend: Arc<LWWBackend>,

    phantom: PhantomData<T>,
}

impl<T> LWW<T>
where T: Serialize + for<'a> Deserialize<'a>
{
    pub fn set(&self, value: &T) -> anyhow::Result<()> {
        let value = bincode::serialize(value)?;
        self.backend.set(self.property_name.clone(), value);
        Ok(())
    }
}

impl<T> ProjectedValue for LWW<T> {
    type Projected = T;
    fn projected(&self) -> Self::Projected {
        //self.value()
        todo!()
    }
}

impl<T> FromEntity for LWW<T> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        Self { property_name: property_name, backend: entity.backends().get::<LWWBackend>().unwrap(), phantom: PhantomData }
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
