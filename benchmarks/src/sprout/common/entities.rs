//! Entity definitions for benchmark workloads.

use ankurah::Model;
use serde::{Deserialize, Serialize};

/// Album entity for benchmarking create/fetch/query operations.
#[derive(Model, Debug, Clone, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
    pub artist_id: String,
    pub foo: i32,
    pub bar: String,
    pub status: String,
    pub age: i32,
    pub score: i32,
    pub timestamp: i64,
}

/// Artist entity exercises additional predicate shapes.
#[derive(Model, Debug, Clone, Serialize, Deserialize)]
pub struct Artist {
    pub name: String,
    pub genre: String,
    pub active: bool,
    pub popularity: i32,
}

impl Album {
    /// Creates a new album with the given index for dataset seeding.
    pub fn with_index(index: usize) -> Self {
        Self {
            name: format!("Album {}", index),
            year: format!("{}", 1990 + (index % 35)),
            artist_id: format!("artist-{}", index % 50),
            foo: (index % 200) as i32,
            bar: format!("Bar {}", index % 75),
            status: if index % 2 == 0 { "active".into() } else { "inactive".into() },
            age: (20 + (index % 40)) as i32,
            score: (index % 120) as i32,
            timestamp: 1_600_000_000 + index as i64,
        }
    }

    /// Creates an album tied to a specific artist identifier.
    pub fn with_artist(index: usize, artist_id: impl Into<String>) -> Self {
        Self { artist_id: artist_id.into(), ..Self::with_index(index) }
    }
}

impl Artist {
    /// Creates a new artist with the given index for dataset seeding.
    pub fn with_index(index: usize) -> Self {
        Self {
            name: format!("Artist {}", index),
            genre: match index % 4 {
                0 => "rock".into(),
                1 => "pop".into(),
                2 => "electronic".into(),
                _ => "jazz".into(),
            },
            active: index % 3 != 0,
            popularity: (index % 100) as i32,
        }
    }
}

// Re-export the generated View types (they are generated in the same module by the Model macro)
pub use self::{AlbumView, ArtistView};
