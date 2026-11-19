//! Dependency-leaf durable identities and values shared by the query,
//! protocol, core, and storage crates.

#![deny(missing_docs)]

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

pub mod collation;
mod entity_id;
mod error;
pub mod key_spec;
mod model_id;
mod node_id;
pub mod ordering;
mod path;
mod property_id;
mod property_path;
mod scan_direction;
mod value;
mod value_type;

pub use entity_id::EntityId;
pub use error::{DecodeError, IdParseError};
pub use model_id::{ModelId, SystemModel};
pub use node_id::NodeId;
pub use path::Path;
pub use property_id::{PropertyId, SystemProperty};
pub use property_path::PropertyPath;
pub use scan_direction::ScanDirection;
pub use value::{CastError, Value, ValueParseError};
pub use value_type::ValueType;
