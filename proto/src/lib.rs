#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

pub mod auth;
pub mod clock;
pub mod data;
pub mod domain_tag;
mod encoding;
pub mod error;
pub mod human_id;
pub mod id;
pub mod identity;
pub mod message;
pub mod operation;
pub mod peering;
pub mod request;
mod subscription;
pub mod time;
pub mod transaction;
pub mod update;

#[cfg(feature = "postgres")]
pub mod postgres;
pub mod registration;

#[cfg(feature = "wasm")]
pub mod wasm;

pub use ankurah_core_types::{ModelId, PropertyId, SystemModel, SystemProperty};

pub use auth::*;
pub use clock::*;
pub use data::*;
pub use domain_tag::*;
pub use error::*;
pub use id::*;
pub use identity::*;
pub use message::*;
pub use operation::*;
pub use peering::*;
pub use registration::*;
pub use request::*;
pub use subscription::QueryId;
pub use transaction::*;
pub use update::*;
