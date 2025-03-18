use std::{marker::PhantomData, sync::Arc};

use ankurah_proto::ID;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::wasm_bindgen;

use crate::{
    model::{Entity, Model},
    property::{
        backend::RefBackend,
        traits::{FromActiveType, FromEntity, PropertyError},
        InitializeWith, Property, PropertyName, PropertyValue,
    },
    transaction::Transaction,
};

#[derive(Debug, Copy, Clone, Default)]
#[wasm_bindgen()]
pub struct RefTest {
    id: Option<ID>,
}

impl RefTest {
    pub fn id(id: ID) -> Self { Self { id: Some(id) } }
    pub fn empty() -> Self { Self { id: None } }
    pub fn optional(id: Option<ID>) -> Self { Self { id: id } }
    pub fn get(&self) -> Option<ID> { self.id }
}

#[derive(Serialize, Deserialize)]
pub struct Ref<M: Model> {
    id: Option<ID>,
    phantom: PhantomData<M>,
}

impl<M: Model> Ref<M> {
    pub fn id(id: ID) -> Self { Self { id: Some(id), phantom: PhantomData } }
    pub fn empty() -> Self { Self { id: None, phantom: PhantomData } }
    pub fn optional(id: Option<ID>) -> Self { Self { id: id, phantom: PhantomData } }
    pub fn get(&self) -> Option<ID> { self.id }
}

impl<M: Model> std::fmt::Debug for Ref<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_tuple("Ref").field(&self.id).finish() }
}

pub trait ModelRef {
    type Model: Model;
}

impl<M: Model> ModelRef for Ref<M> {
    type Model = M;
}

pub struct ActiveRef<M: ModelRef> {
    pub property_name: PropertyName,
    pub backend: Arc<RefBackend>,

    phantom: PhantomData<M>,
}

impl<M: ModelRef> std::fmt::Debug for ActiveRef<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ref").field("property_name", &self.property_name).finish()
    }
}

impl<M: ModelRef> ActiveRef<M> {
    pub fn set(&self, id: Option<ID>) -> Result<(), PropertyError> {
        self.backend.set(self.property_name.clone(), id);
        Ok(())
    }

    pub async fn edit<'rec, 'trx: 'rec>(
        &self,
        trx: &'trx Transaction,
    ) -> Result<Option<<M::Model as Model>::Mutable<'rec>>, PropertyError> {
        match self.get_value() {
            Some(id) => {
                let rec = trx.edit::<M::Model>(id).await?;
                Ok(Some(rec))
            }
            None => Ok(None),
        }
    }

    pub fn get_value(&self) -> Option<ID> { self.backend.get(&self.property_name) }
}

impl<M: ModelRef> FromEntity for ActiveRef<M> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<RefBackend>().expect("Ref Backend should exist");
        Self { property_name: property_name, backend: backend, phantom: PhantomData }
    }
}

impl<M: Model> FromActiveType<ActiveRef<Ref<M>>> for Ref<M> {
    fn from_active(active: ActiveRef<Ref<M>>) -> Result<Self, PropertyError>
    where Self: Sized {
        Ok(Ref::<M>::optional(active.get_value()))
    }
}

impl<M: Model> InitializeWith<Ref<M>> for ActiveRef<Ref<M>> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &Ref<M>) -> Self {
        let new = Self::from_entity(property_name, entity);
        new.set(value.get()).unwrap();
        new
    }
}
