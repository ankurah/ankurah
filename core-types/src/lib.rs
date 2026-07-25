//! Dependency-leaf durable identities and values shared by the query,
//! protocol, core, and storage crates.

#![deny(missing_docs)]

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

mod entity_id;
mod error;
mod model_id;
mod property_id;
mod value;
mod value_type;

pub use entity_id::EntityId;
pub use error::{DecodeError, IdParseError};
pub use model_id::{ModelId, SystemModel};
pub use property_id::{PropertyId, SystemProperty};
pub use value::{CastError, Value, ValueParseError};
pub use value_type::ValueType;
