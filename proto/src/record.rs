use serde::{Deserialize, Serialize};

pub trait Record: serde::de::DeserializeOwned {
    type ID: Clone;
    fn id(&self) -> &Self::ID;
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}
impl Direction {
    pub fn inverse(&self) -> Self {
        match self {
            Direction::Ascending => Direction::Descending,
            Direction::Descending => Direction::Ascending,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PaginatedCursor {
    // We are pointing to the key after this one
    After(Vec<u8>),
    // We are pointing to the key before this one
    Before(Vec<u8>),
    // We are pointing to this key and forward
    StartingWith(Vec<u8>),
    // We are pointing to this key and backwards
    EndingWith(Vec<u8>),
}
