//! Public error types for ankurah.
//!
//! These are the only error types exposed to FFI (wasm_bindgen, uniffi).
//! Internal errors are mapped to `Failure` variants at API boundaries.

pub mod internal;

// Re-export internal types for crate-internal use
pub(crate) use internal::{
    AnyhowWrapper, ApplyError, ApplyErrorCause, ApplyErrorItem, LineageError, RequestError,
    SubscriptionError, ValidationError,
};

// Re-export storage types publicly for storage implementation crates
pub use internal::{StateError, StorageError};

use ankurah_proto::{CollectionId, EntityId};
use error_stack::{Report, ResultExt};
use thiserror::Error;

use crate::policy::AccessDenied;

/// Marker context for errors that cross the public API boundary.
/// The actual error chain is preserved in the Report via error-stack.
#[derive(Debug, Error)]
#[error("internal error")]
pub struct InternalError;

/// Error type for retrieval operations.
///
/// Returned from: `Context::get`, `fetch`, `query`; `Transaction::get`
#[derive(Debug, Error)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum RetrievalError {
    /// Entity or collection not found
    #[error("{0}")]
    NotFound(NotFound),

    /// Access denied by policy
    #[error("access denied")]
    AccessDenied(AccessDenied),

    /// Invalid query syntax or semantics
    #[error("invalid query: {0}")]
    InvalidQuery(QueryError),

    /// Query timed out (retried internally but deadline exceeded)
    #[error("timeout")]
    Timeout,

    /// Internal failure - use `.diagnostic()` for details
    #[error("{0:?}")]
    Failure(Report<InternalError>),
}

impl RetrievalError {
    /// Get diagnostic string if this is a Failure
    pub fn diagnostic(&self) -> Option<String> {
        match self {
            Self::Failure(report) => Some(format!("{report:?}")),
            _ => None,
        }
    }
}

#[cfg(feature = "wasm")]
impl From<RetrievalError> for wasm_bindgen::JsValue {
    fn from(err: RetrievalError) -> Self {
        err.to_string().into()
    }
}

#[derive(Debug, Error)]
pub enum NotFound {
    #[error("entity not found: {0:?}")]
    Entity(EntityId),

    #[error("collection not found: {0}")]
    Collection(CollectionId),
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("parse error: {0}")]
    Parse(ankql::error::ParseError),

    #[error("invalid filter: {0}")]
    Filter(String),
}

impl From<ankql::error::ParseError> for QueryError {
    fn from(err: ankql::error::ParseError) -> Self {
        QueryError::Parse(err)
    }
}

impl From<ankql::error::ParseError> for RetrievalError {
    fn from(err: ankql::error::ParseError) -> Self {
        RetrievalError::InvalidQuery(QueryError::Parse(err))
    }
}

/// Error type for mutation operations.
///
/// Returned from: `Transaction::create`, `commit`; `Context::commit_local_trx`
#[derive(Debug, Error)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum MutationError {
    /// Access denied by policy
    #[error("access denied")]
    AccessDenied(AccessDenied),

    /// Entity already exists (create conflict)
    #[error("already exists")]
    AlreadyExists,

    /// The mutation is invalid (user error)
    #[error("invalid update: {0}")]
    InvalidUpdate(String),

    /// Peer rejected the transaction
    #[error("rejected")]
    Rejected,

    /// Commit timed out (retried internally but deadline exceeded)
    #[error("timeout")]
    Timeout,

    /// Internal failure - use `.diagnostic()` for details
    #[error("{0:?}")]
    Failure(Report<InternalError>),
}

impl MutationError {
    /// Get diagnostic string if this is a Failure
    pub fn diagnostic(&self) -> Option<String> {
        match self {
            Self::Failure(report) => Some(format!("{report:?}")),
            _ => None,
        }
    }
}

#[cfg(feature = "wasm")]
impl From<MutationError> for wasm_bindgen::JsValue {
    fn from(err: MutationError) -> Self {
        err.to_string().into()
    }
}

impl From<AccessDenied> for MutationError {
    fn from(err: AccessDenied) -> Self {
        MutationError::AccessDenied(err)
    }
}

impl From<AccessDenied> for RetrievalError {
    fn from(err: AccessDenied) -> Self {
        RetrievalError::AccessDenied(err)
    }
}

impl From<crate::property::PropertyError> for MutationError {
    fn from(err: crate::property::PropertyError) -> Self {
        MutationError::Failure(Report::new(err).change_context(InternalError))
    }
}

/// Error type for property access operations.
///
/// Returned from: property accessors on `View` and `Mutable` types
#[derive(Debug, Error)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum PropertyError {
    /// Property not set
    #[error("missing property")]
    Missing,

    /// Value is wrong type
    #[error("invalid variant: expected {ty}, got {given}")]
    InvalidVariant { given: String, ty: String },

    /// Value can't be parsed
    #[error("invalid value '{value}' for type {ty}")]
    InvalidValue { value: String, ty: String },

    /// Tried to mutate after transaction ended
    #[error("transaction closed")]
    TransactionClosed,

    /// Failed to serialize value
    #[error("serialize error: {0}")]
    SerializeError(String),

    /// Failed to deserialize value
    #[error("deserialize error: {0}")]
    DeserializeError(String),

    /// Internal failure - use `.diagnostic()` for details
    #[error("{0:?}")]
    Failure(Report<InternalError>),
}

impl PropertyError {
    /// Get diagnostic string if this is a Failure
    pub fn diagnostic(&self) -> Option<String> {
        match self {
            Self::Failure(report) => Some(format!("{report:?}")),
            _ => None,
        }
    }
}

#[cfg(feature = "wasm")]
impl From<PropertyError> for wasm_bindgen::JsValue {
    fn from(val: PropertyError) -> Self {
        wasm_bindgen::JsValue::from_str(&val.to_string())
    }
}

impl From<PropertyError> for std::fmt::Error {
    fn from(_: PropertyError) -> std::fmt::Error {
        std::fmt::Error
    }
}

impl From<serde_json::Error> for PropertyError {
    fn from(e: serde_json::Error) -> Self {
        PropertyError::DeserializeError(e.to_string())
    }
}
