//! Serialized vocabulary exchanged between Ankurah nodes and storage layers.
//!
//! This crate gathers identities, clocks, entities, events, requests,
//! responses, subscription updates, schema registration declarations, and
//! attestations into one protocol surface, re-exporting their canonical types
//! from this module root. It owns data shape and encoding compatibility, not
//! transport, authorization, catalog allocation, or runtime application of the
//! messages; those responsibilities live in connectors and `ankurah-core`.

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

pub mod auth;
pub mod clock;
pub mod collection;
pub mod data;
pub mod error;
pub mod human_id;
pub mod id;
pub mod message;
pub mod peering;
pub mod request;
mod subscription;
pub mod sys;
pub mod transaction;
pub mod update;

#[cfg(feature = "postgres")]
pub mod postgres;
pub mod registration;

#[cfg(feature = "wasm")]
pub mod wasm;

pub use ankurah_core_types::{ModelId, PropertyId, SystemModel, SystemProperty, UniqueFieldId, UniqueStructId};

pub use auth::*;
pub use clock::*;
pub use collection::*;
pub use data::*;
pub use error::*;
pub use id::*;
pub use message::*;
pub use peering::*;
pub use registration::*;
pub use request::*;
pub use subscription::QueryId;
pub use transaction::*;
pub use update::*;
