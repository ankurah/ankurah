use serde::{Deserialize, Serialize};

/// What an entity in the `_ankurah_system` collection describes, JSON-encoded in its item property.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Item {
    Collection {
        name: String,
    },
    #[serde(other)]
    Other,
}
