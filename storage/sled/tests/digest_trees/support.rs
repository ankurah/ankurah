//! A Sled database and the engine shared by its test's callers, using core's
//! digest-tree fixtures.

use std::sync::Arc;

use ankurah::core::sync::conformance::fixtures;
use ankurah_digest_tree::Ristretto255;
use ankurah_storage_sled::SledStorageEngine;

/// A sled database of the test's own. sled lets one process open a
/// database, so the test's nodes share the one engine on it.
pub struct Database {
    engine: Arc<SledStorageEngine>,
}

impl Database {
    /// A database that lasts as long as the test holds it.
    pub fn start() -> anyhow::Result<Self> { Ok(Self { engine: Arc::new(SledStorageEngine::new_test()?) }) }

    /// The engine every node reaches the database through.
    pub fn engine(&self) -> &Arc<SledStorageEngine> { &self.engine }
}

/// A tree of fixture items on Sled.
pub type Tree = fixtures::Tree<SledStorageEngine, Ristretto255>;
