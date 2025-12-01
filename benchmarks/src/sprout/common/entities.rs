//! Entity definitions for benchmark workloads.

use ankurah::Model;
use serde::{Deserialize, Serialize};

/// Album entity for benchmarking create/fetch/query operations.
#[derive(Model, Debug, Clone, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}

/// Pet entity for benchmarking create/fetch/query operations.
#[derive(Model, Debug, Clone, Serialize, Deserialize)]
pub struct Pet {
    pub name: String,
    pub age: String,
}

impl Album {
    /// Creates a new album with the given index for dataset seeding.
    pub fn with_index(index: usize) -> Self { Self { name: format!("Album {}", index), year: format!("{}", 2000 + (index % 25)) } }
}

impl Pet {
    /// Creates a new pet with the given index for dataset seeding.
    pub fn with_index(index: usize) -> Self { Self { name: format!("Pet {}", index), age: format!("{}", index % 20) } }
}

// Re-export the generated View types (they are generated in the same module by the Model macro)
pub use self::{AlbumView, PetView};
