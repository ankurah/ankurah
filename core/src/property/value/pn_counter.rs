use std::{
    marker::PhantomData,
    sync::{Arc, Weak},
};

use crate::{
    entity::Entity,
    property::{
        backend::{pn_counter::PNValue, PNBackend},
        traits::{FromActiveType, FromEntity, InitializeWith, PropertyError},
        PropertyName,
    },
};

#[derive(Debug)]
pub struct PNCounter<I>
where I: Into<PNValue> + From<PNValue> + Copy + Clone
{
    pub property_name: PropertyName,
    pub backend: Weak<PNBackend>,
    phantom: PhantomData<I>,
}

// Starting with basic string type operations
impl<I> PNCounter<I>
where I: Into<PNValue> + From<PNValue> + Copy + Clone
{
    pub fn new(property_name: PropertyName, backend: Arc<PNBackend>) -> Self {
        Self { property_name, backend: Arc::downgrade(&backend), phantom: PhantomData }
    }
    pub fn backend(&self) -> Arc<PNBackend> { self.backend.upgrade().expect("Expected `PN` property backend to exist") }
    pub fn add(&self, amount: impl Into<PNValue>) { self.backend().add(self.property_name.clone(), amount.as_i64()); }
}

impl<I: From<PNValue>> PNCounter<I>
where I: Into<PNValue> + From<PNValue> + Copy + Clone
{
    pub fn value(&self) -> I {
        let pn_value = self.backend().get(self.property_name.clone());
        I::from(pn_value)
    }
}

impl<I> FromEntity for PNCounter<I>
where I: Into<PNValue> + From<PNValue> + Copy + Clone
{
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<PNBackend>().expect("PNBackend should exist");
        Self::new(property_name, backend)
    }
}

impl<I> FromActiveType<PNCounter<I>> for I
where I: Into<PNValue> + From<PNValue> + Copy + Clone
{
    fn from_active(active: PNCounter<I>) -> Result<Self, PropertyError>
    where Self: Sized {
        Ok(active.value())
    }
}

impl<I> InitializeWith<I> for PNCounter<I>
where I: Into<PNValue> + From<PNValue> + Copy + Clone
{
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &I) -> Self {
        let new = Self::from_entity(property_name, entity);
        new.add(*value);
        new
    }
}
