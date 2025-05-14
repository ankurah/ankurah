use std::{marker::PhantomData, sync::Arc};

use ankurah_proto::EntityId;
use serde::{Deserialize, Serialize};

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

use crate::{
    context::Context,
    entity::Entity,
    error::RetrievalError,
    model::Model,
    property::{
        backend::RefBackend,
        traits::{FromActiveType, FromEntity, PropertyError},
        InitializeWith, Property, PropertyName, PropertyValue,
    },
    transaction::Transaction,
};

#[derive(Serialize, Deserialize)]
pub struct Ref<M: Model> {
    pub id: EntityId,
    pub phantom: PhantomData<M>,
}

impl<M: Model> Property for Ref<M> {
    fn into_value(&self) -> Result<Option<crate::property::PropertyValue>, PropertyError> {
        Ok(Some(PropertyValue::EntityId(self.id.clone())))
    }

    fn from_value(value: Option<crate::property::PropertyValue>) -> Result<Self, PropertyError> {
        if let Some(PropertyValue::EntityId(id)) = value {
            Ok(Ref { id, phantom: PhantomData })
        } else {
            Err(PropertyError::InvalidValue { value: value.map_or_else(|| "None".to_string(), |v| v.to_string()), ty: "Ref".to_string() })
        }
    }
}

pub struct ActiveRef<M: Model> {
    pub property_name: PropertyName,
    pub backend: Arc<RefBackend>,
    // pub context: Context,
    phantom: PhantomData<M>,
}
pub struct OptionalActiveRef<M: Model> {
    pub property_name: PropertyName,
    pub backend: Arc<RefBackend>,
    // pub context: Context,
    phantom: PhantomData<M>,
}

pub struct MutableRef<M: Model> {
    pub property_name: PropertyName,
    pub backend: Arc<RefBackend>,
    phantom: PhantomData<M>,
}

pub struct OptionalMutableRef<M: Model> {
    pub property_name: PropertyName,
    pub backend: Arc<RefBackend>,
    phantom: PhantomData<M>,
}

impl<M: Model> ActiveRef<M> {
    /// Temporarily passing in context to get the entity - this should probably be removed
    /// in favor of an EntityAndContext type of some kind that transports the context along side the entity
    pub async fn get(&self, context: &Context) -> Result<M::View, RetrievalError> {
        let id = self.entity_id()?;
        let rec = context.get::<M::View>(id).await?;
        Ok(rec)
    }
    pub async fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx Transaction) -> Result<M::Mutable<'rec>, PropertyError> {
        let id = self.entity_id()?;
        let rec = trx.get::<M>(&id).await?;
        Ok(rec)
    }

    pub fn entity_id(&self) -> Result<EntityId, PropertyError> { self.backend.get(&self.property_name).ok_or(PropertyError::Missing) }
}

impl<M: Model> OptionalActiveRef<M> {
    pub async fn get(&self, context: &Context) -> Result<Option<M::View>, RetrievalError> {
        match self.entity_id()? {
            Some(id) => Ok(Some(context.get::<M::View>(id).await?)),
            None => Ok(None),
        }
    }
    pub async fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx Transaction) -> Result<Option<M::Mutable<'rec>>, PropertyError> {
        match self.entity_id()? {
            Some(id) => Ok(Some(trx.get::<M>(&id).await?)),
            None => Ok(None),
        }
    }

    pub fn entity_id(&self) -> Result<Option<EntityId>, PropertyError> { Ok(self.backend.get(&self.property_name)) }
}

impl<M: Model> MutableRef<M> {
    pub async fn get(&self, context: &Context) -> Result<M::View, RetrievalError> {
        let id = self.entity_id()?;
        let rec = context.get::<M::View>(id).await?;
        Ok(rec)
    }
    pub async fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx Transaction) -> Result<M::Mutable<'rec>, PropertyError> {
        let id = self.entity_id()?;
        let rec = trx.get::<M>(&id).await?;
        Ok(rec)
    }
    pub fn set(&self, id: impl Into<EntityId>) -> Result<(), PropertyError> {
        self.backend.set(self.property_name.clone(), Some(id.into()));
        Ok(())
    }

    pub fn entity_id(&self) -> Result<EntityId, PropertyError> { self.backend.get(&self.property_name).ok_or(PropertyError::Missing) }
}

impl<M: Model> OptionalMutableRef<M> {
    pub async fn get(&self, context: &Context) -> Result<Option<M::View>, RetrievalError> {
        match self.entity_id()? {
            Some(id) => Ok(Some(context.get::<M::View>(id).await?)),
            None => Ok(None),
        }
    }
    pub async fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx Transaction) -> Result<Option<M::Mutable<'rec>>, PropertyError> {
        match self.entity_id()? {
            Some(id) => Ok(Some(trx.get::<M>(&id).await?)),
            None => Ok(None),
        }
    }
    pub fn set(&self, id: impl Into<EntityId>) -> Result<(), PropertyError> {
        self.backend.set(self.property_name.clone(), Some(id.into()));
        Ok(())
    }

    pub fn entity_id(&self) -> Result<Option<EntityId>, PropertyError> { Ok(self.backend.get(&self.property_name)) }
}

impl<M: Model> FromEntity for ActiveRef<M> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<RefBackend>().expect("Ref Backend should exist");
        Self { property_name: property_name, backend: backend, phantom: PhantomData }
    }
}

impl<M: Model> FromEntity for OptionalActiveRef<M> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<RefBackend>().expect("Ref Backend should exist");
        Self { property_name: property_name, backend: backend, phantom: PhantomData }
    }
}

impl<M: Model> InitializeWith<Ref<M>> for ActiveRef<M> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &Ref<M>) -> Self {
        let mutable = MutableRef::from_entity(property_name.clone(), entity);
        mutable.set(value.id).unwrap();
        Self::from_entity(property_name, entity)
    }
}

impl<M: Model> InitializeWith<Ref<M>> for OptionalActiveRef<M> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &Ref<M>) -> Self {
        let mutable = OptionalMutableRef::from_entity(property_name.clone(), entity);
        mutable.set(value.id).unwrap();
        Self::from_entity(property_name, entity)
    }
}

impl<M: Model> InitializeWith<Ref<M>> for MutableRef<M> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &Ref<M>) -> Self {
        let new = Self::from_entity(property_name, entity);
        new.set(value.id).unwrap();
        new
    }
}

impl<M: Model> InitializeWith<Ref<M>> for OptionalMutableRef<M> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &Ref<M>) -> Self {
        let new = Self::from_entity(property_name, entity);
        new.set(value.id).unwrap();
        new
    }
}

impl<M: Model> FromEntity for MutableRef<M> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<RefBackend>().expect("Ref Backend should exist");
        Self { property_name: property_name, backend: backend, phantom: PhantomData }
    }
}

impl<M: Model> FromEntity for OptionalMutableRef<M> {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self {
        let backend = entity.backends().get::<RefBackend>().expect("Ref Backend should exist");
        Self { property_name: property_name, backend: backend, phantom: PhantomData }
    }
}

impl<M: Model> FromActiveType<ActiveRef<M>> for Ref<M> {
    fn from_active(active: ActiveRef<M>) -> Result<Self, PropertyError>
    where Self: Sized {
        Ok(Ref { id: active.entity_id()?, phantom: PhantomData })
    }
}

impl<M: Model> std::fmt::Debug for Ref<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_tuple("Ref").field(&self.id).finish() }
}

impl<M: Model> std::fmt::Debug for ActiveRef<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ref").field("property_name", &self.property_name).finish()
    }
}
impl<M: Model> std::fmt::Display for ActiveRef<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Ref {}: {}", self.property_name, self.entity_id()?) }
}
