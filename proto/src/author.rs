use serde::{Deserialize, Serialize};

use crate::id::EntityId;

/// Who an event's creator says wrote it.
///
/// Every event carries one, and it sits inside the event's id hash, so the
/// claim cannot be edited without producing a different event. Nothing binds
/// an author cryptographically yet, so every event this binary mints writes
/// [`AuthorId::Unknown`]: a field that looks like a checked claim while
/// nothing checks it is worse than saying outright that no author is
/// established.
///
/// VARIANT ORDER IS PART OF EVERY EVENT ID. bincode writes the variant index
/// positionally, so reordering these silently re-derives every event id and
/// every entity id in existence. Add new variants at the end.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AuthorId {
    /// The entity named here wrote the event.
    Id(EntityId),
    /// The system root wrote the event.
    Root,
    /// No author is established for this event.
    Unknown,
}

impl std::fmt::Display for AuthorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Id(id) => write!(f, "{id:#}"),
            Self::Root => f.write_str("root"),
            Self::Unknown => f.write_str("unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The encoded form of each [`AuthorId`] variant, pinned. bincode writes
    /// the variant index positionally and that index is hashed into every
    /// event id, so a reordering here would silently re-derive every id in
    /// existence; this test is what makes that reordering fail loudly.
    #[test]
    fn author_variant_order_is_pinned() {
        let id = EntityId::from_bytes([7; 32]);
        assert_eq!(bincode::serialize(&AuthorId::Id(id)).unwrap(), [0u32.to_le_bytes().as_slice(), id.to_bytes().as_slice()].concat());
        assert_eq!(bincode::serialize(&AuthorId::Root).unwrap(), 1u32.to_le_bytes());
        assert_eq!(bincode::serialize(&AuthorId::Unknown).unwrap(), 2u32.to_le_bytes());

        for author in [AuthorId::Id(id), AuthorId::Root, AuthorId::Unknown] {
            let bytes = bincode::serialize(&author).unwrap();
            assert_eq!(bincode::deserialize::<AuthorId>(&bytes).unwrap(), author);
        }
    }
}
