use anyhow::Result;

use crate::{entity::Entity, error::RetrievalError, property::PropertyName, value::CastError};

use thiserror::Error;

use super::Value;

pub trait InitializeWith<T> {
    fn initialize_with(entity: &Entity, property_name: PropertyName, value: &T) -> Self;
}

#[derive(Error, Debug)]
pub enum PropertyError {
    #[error("property is missing")]
    Missing,

    // #[error("property is missing: {name} in collection: {collection}")]
    // NotFoundInBackend { backend: &'static str, name: PropertyName },
    #[error("serialization error: {0}")]
    SerializeError(Box<dyn std::error::Error + Send + Sync>),
    #[error("deserialization error: {0}")]
    DeserializeError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("retrieval error: {0}")]
    RetrievalError(crate::error::RetrievalError),
    #[error("invalid variant `{given}` for `{ty}`")]
    InvalidVariant { given: Value, ty: String },
    #[error("invalid value `{value}` for `{ty}`")]
    InvalidValue { value: String, ty: String },
    #[error("transaction is no longer alive")]
    TransactionClosed,

    #[error("cast error: {0}")]
    CastError(CastError),
}

impl PartialEq for PropertyError {
    fn eq(&self, other: &Self) -> bool { self.to_string() == other.to_string() }
}

impl From<PropertyError> for std::fmt::Error {
    fn from(_: PropertyError) -> std::fmt::Error { std::fmt::Error }
}

#[cfg(feature = "wasm")]
impl From<PropertyError> for wasm_bindgen::JsValue {
    fn from(val: PropertyError) -> Self { wasm_bindgen::JsValue::from_str(&val.to_string()) }
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
