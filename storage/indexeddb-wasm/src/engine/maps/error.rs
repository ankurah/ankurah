//! How a map call fails on the IndexedDB engine, apart from a write conflict.

use ankurah_core::error::MutationError;
use ankurah_core::storage::LockName;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{DomException, IdbRequest};

/// A map call's failure: IndexedDB's own, a record in the maps' object
/// stores that the engine didn't write, or a lock's version that a commit
/// can't raise.
#[derive(Debug, thiserror::Error)]
pub enum MapError {
    /// IndexedDB refused a call or failed a request, with the DOMException
    /// it reported: its name, such as `QuotaExceededError`, and its message.
    #[error("IndexedDB failed a map call: {name}: {message}")]
    IndexedDb { name: String, message: String },

    /// A map's entry, a lock's version or the reset epoch, read from the
    /// database, isn't a value the engine writes there.
    #[error("the database holds a {what} the engine didn't write")]
    Damaged { what: &'static str },

    /// A commit under the lock `name`, whose version is the last a lock
    /// keeps, which the commit can't raise.
    #[error("the lock {name:?} is at the last version a lock keeps, so no commit under it can land")]
    LastVersion { name: LockName },
}

impl MapError {
    /// The failure IndexedDB threw as `error`.
    pub(super) fn thrown(error: JsValue) -> Self {
        match error.dyn_into::<DomException>() {
            Ok(exception) => Self::of(&exception),
            Err(error) => Self::IndexedDb { name: "Error".to_owned(), message: crate::error::extract_message(error) },
        }
    }

    /// Why `request` failed, once its error event has fired.
    pub(super) fn of_request(request: &IdbRequest) -> Self {
        match request.error() {
            Ok(Some(exception)) => Self::of(&exception),
            Ok(None) | Err(_) => Self::unexplained("a request failed"),
        }
    }

    /// Whether IndexedDB refused a request because the transaction it was
    /// made in is no longer active, as once control has gone back to the
    /// event loop with none of its requests pending.
    pub(super) fn is_inactive_transaction(&self) -> bool {
        matches!(self, Self::IndexedDb { name, .. } if name == "TransactionInactiveError")
    }

    fn of(exception: &DomException) -> Self { Self::IndexedDb { name: exception.name(), message: exception.message() } }

    /// What IndexedDB gave no DOMException for: `what` happened.
    pub(super) fn unexplained(what: &str) -> Self {
        Self::IndexedDb { name: "UnknownError".to_owned(), message: format!("{what} without saying why") }
    }
}

impl From<MapError> for MutationError {
    fn from(error: MapError) -> Self { MutationError::General(Box::new(error)) }
}
