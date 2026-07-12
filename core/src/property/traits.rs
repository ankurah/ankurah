use anyhow::Result;

use crate::{
    entity::Entity,
    error::{MutationError, RetrievalError},
    property::PropertyAddress,
    value::CastError,
};

use thiserror::Error;

use super::Value;

pub trait InitializeWith<T> {
    /// Construct and initialize an active field from its compiled address.
    /// Implementations must honor `explicit_id` when present; taking the
    /// address as the required API (rather than a name-only default) keeps
    /// third-party active types from silently discarding explicit identity.
    fn initialize_with(entity: &Entity, property: PropertyAddress, value: &T) -> Result<Self, MutationError>
    where Self: Sized;
}

#[derive(Error, Debug)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
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

    /// A per-value cast failure against the property's canonical value_type
    /// (rfc.md 5.6, the canonical value_type ruling): the type PAIR was
    /// admitted at registration, but this particular value does not fit
    /// (numeric overflow, unparseable string). Surfaces at the write that
    /// staged the value or the read that projects it -- never at policy or
    /// query evaluation, which read leniently.
    #[error("not castable: {0}")]
    NonCastable(CastError),

    /// A property reference that neither the admitted compiled binding nor the
    /// catalog defines. Predicate building fails closed rather than treating a
    /// typo as NULL (RFC 5.3 in specs/model-property-metadata/rfc.md).
    #[error("unknown property '{name}' in collection '{collection}'")]
    UnknownProperty { collection: String, name: String },

    /// The queried collection has no model in the catalog, this binary
    /// carries a compiled schema for it, and FIRST-USE REGISTRATION could
    /// not resolve the doubt (policy denied the definition, or no durable
    /// peer is reachable). Surfaced as a loud error on query and fetch
    /// paths: a lagging catalog cache
    /// cannot prove emptiness, and idling on an unanswerable subscription
    /// helps no one -- retry once the schema is registered or connectivity
    /// returns.
    #[error("collection '{collection}' is not registered and could not be registered from this node")]
    UnregisteredCollection { collection: String },
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

pub trait FromEntity {
    /// Bind an active field to an entity using its complete compiled address.
    /// Implementations must preserve an explicit id for reads, writes, and
    /// listener identity rather than resolving only by display name.
    fn from_entity(property: PropertyAddress, entity: &Entity) -> Self;
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
