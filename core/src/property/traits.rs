use ankurah_proto::{Clock, ClockOrdering};
use anyhow::Result;

use crate::{
    error::RetrievalError,
    model::Entity,
    property::PropertyName,
};

use thiserror::Error;

use super::PropertyValue;

pub trait InitializeWith<T> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &T) -> Self;
}

#[derive(Error, Debug)]
pub enum PropertyError {
    #[error("property is missing")]
    Missing,
    #[error("deserialization error: {0}")]
    DeserializeError(Box<dyn std::error::Error + Send + Sync>),
    #[error("retrieval error: {0}")]
    RetrievalError(crate::error::RetrievalError),
    #[error("invalid variant `{given}` for `{ty}`")]
    InvalidVariant { given: PropertyValue, ty: String },
    #[error("invalid value `{value}` for `{ty}`")]
    InvalidValue { value: String, ty: String },
}

#[cfg(feature = "wasm")]
impl Into<wasm_bindgen::JsValue> for PropertyError {
    fn into(self) -> wasm_bindgen::JsValue { wasm_bindgen::JsValue::from_str(&self.to_string()) }
}

impl From<RetrievalError> for PropertyError {
    fn from(retrieval: RetrievalError) -> Self { PropertyError::RetrievalError(retrieval) }
}

pub trait FromEntity {
    fn from_entity(property_name: PropertyName, entity: &Entity) -> Self;
}

pub trait FromActiveType<A> {
    fn from_active(active: A) -> Result<Self, PropertyError>
    where Self: Sized;
}

pub fn compare_clocks(clock: &Clock, other: &Clock /*, context: &Box<dyn TContext>*/) -> ClockOrdering {
    let ulid1 = clock.as_slice().iter().max();
    let ulid2 = other.as_slice().iter().max();

    if ulid1 > ulid2 {
        ClockOrdering::Child
    } else if ulid1 < ulid2 {
        ClockOrdering::Parent
    } else {
        ClockOrdering::Sibling
    }
}

/*
impl<A, T> FromActiveType<A> for Option<T>
where T: FromActiveType<A> {
    fn from_active(active: Result<A, PropertyError>) -> Result<Option<T>, PropertyError> {
        match T::from_active(active) {
            Ok(projected) => {
                Ok(Some(projected))
            }
            Err(PropertyError::Missing) => Ok(None),
            Err(err) => Err(err),
        }
    }
}
*/
