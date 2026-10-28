//! Dependency-leaf durable identities and values shared by the query,
//! protocol, core, and storage crates, with how values sort and the key specs
//! that order an index's entries.

#![deny(missing_docs)]

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

pub mod collation;
mod entity_id;
mod error;
pub mod key_spec;
mod model_id;
mod path;
mod property_id;
mod property_path;
mod value;
mod value_type;

pub use entity_id::EntityId;
pub use error::{DecodeError, IdParseError};
pub use model_id::{ModelId, SystemModel};
pub use path::Path;
pub use property_id::{PropertyId, SystemProperty};
pub use property_path::PropertyPath;
pub use value::{CastError, Value, ValueParseError};
pub use value_type::ValueType;
