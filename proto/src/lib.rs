#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

pub mod attestation;
pub mod auth;
pub mod carried_body;
pub mod clock;
pub mod domain_tag;
mod encoding;
pub mod error;
pub mod event;
pub mod event_id;
pub mod human_id;
pub mod id;
pub mod identity;
pub mod message;
pub mod operation;
pub mod peering;
pub mod request;
pub mod state;
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

pub use attestation::*;
pub use auth::*;
pub use carried_body::*;
pub use clock::*;
pub use domain_tag::*;
pub use encoding::UndecodableBytes;
pub use error::*;
pub use event::*;
pub use event_id::*;
pub use id::*;
pub use identity::*;
pub use message::*;
pub use operation::*;
pub use peering::*;
pub use registration::*;
pub use request::*;
pub use state::*;
pub use subscription::QueryId;
pub use transaction::*;
pub use update::*;
