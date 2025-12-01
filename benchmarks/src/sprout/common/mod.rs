//! Common fixtures for benchmark workloads: entities, watchers, dataset builders.

pub mod entities;
pub mod watcher;

pub use entities::{Album, AlbumView, Pet, PetView};
pub use watcher::BenchWatcher;

