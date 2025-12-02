//! Common fixtures for benchmark workloads: entities, watchers, dataset builders.

pub mod entities;
pub mod watcher;

pub use entities::{Album, AlbumView, Artist, ArtistView};
pub use watcher::BenchWatcher;
