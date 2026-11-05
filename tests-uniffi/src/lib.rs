//! A model crate built the way an app builds one for UniFFI bindings (Swift, Kotlin): with
//! `ankurah/uniffi`, its own `uniffi` dependency and scaffolding, and no `react-native`. Its tests
//! drive the generated bindings from Rust.
//!
//! This crate is not a default workspace member: enabling `ankurah/uniffi` would give every other
//! model crate in the build UniFFI bindings, which need a scaffolding of their own. Run its tests
//! with `cargo test -p ankurah-tests-uniffi`.

use ankurah::{EntityId, Model, Ref};
use serde::{Deserialize, Serialize};

uniffi::setup_scaffolding!();

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Artist {
    pub name: String,
}

/// An album refers to other entities each way a model can: a `Ref` to a model this crate
/// defines, and a plain `EntityId` for an entity whose model it cannot name, such as one in
/// another system; each required and optional.
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub artist: Ref<Artist>,
    pub producer: Option<Ref<Artist>>,
    pub label: EntityId,
    pub distributor: Option<EntityId>,
}
