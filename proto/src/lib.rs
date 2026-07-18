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

#[cfg(feature = "wasm")]
pub mod wasm;

/// The resolved property vocabulary is defined in ankql (the AST carries it,
/// and ankql cannot depend on proto), but it is protocol vocabulary: resolved
/// selections ride the wire and engines key their durable maps on it. Proto
/// republishes it so protocol consumers name it from the protocol crate.
pub use ankql::ast::{PropertyId, PropertyPath};

pub use auth::*;
pub use clock::*;
pub use collection::*;
pub use data::*;
pub use error::*;
pub use id::*;
pub use message::*;
pub use peering::*;
pub use request::*;
pub use subscription::QueryId;
pub use transaction::*;
pub use update::*;
