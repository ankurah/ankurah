use crate::internal::prelude::*;
use anyhow::Result;

use crate::entity::LocalTrxEntity;
use crate::property::PropertyId;
use crate::value::CastError;

use thiserror::Error;

use super::Value;

/// Write a model field's initial value into the backend that will store it.
// TODO: Separate backend initialization from the mutable-handle type used to select it.
pub trait InitializeWith<T> {
    fn initialize_with(entity: &LocalTrxEntity, property: PropertyId, value: &T);
}

#[derive(Error, Debug)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum PropertyError {
    #[error("property is missing")]
    Missing,

    // #[error("property is missing: {name} in collection: {collection}")]
    // NotFoundInBackend { backend: &'static str, name: PropertyId },
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

    /// The field has no usable binding in the system epoch this entity belongs to.
    #[error("field '{field}' of model '{model}' is not resolved under this entity's system epoch")]
    Unresolved { model: &'static str, field: &'static str },

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

impl From<serde_json::Error> for PropertyError {
    fn from(e: serde_json::Error) -> Self { PropertyError::SerializeError(Box::new(e)) }
}

/// Implemented by every active type (the wrapper a model field compiles
/// to): names the property backend that stores its data, exactly as that
/// backend registers itself at runtime. The derive reads this const into
/// the compiled descriptor's `backend` field, so the fact is declared by
/// the active type itself and tabulated nowhere.
pub trait ActiveType {
    const BACKEND: &'static str;
}

pub trait FromLocalTrxEntity: Sized {
    fn from_local_entity(property: PropertyId, entity: &LocalTrxEntity) -> Result<Self, PropertyError>;
}
