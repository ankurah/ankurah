use std::collections::BTreeSet;

use ankurah_core::EntityId;

/// The bytes a node announces for the System entities it holds: each entity's 32-byte id, in
/// ascending order.
pub(crate) fn encode(held: &BTreeSet<EntityId>) -> Vec<u8> { held.iter().flat_map(|id| id.to_bytes()).collect() }

/// An announcement that is not a whole number of entity ids.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0} announced bytes are not a whole number of 32-byte entity ids")]
pub(crate) struct UnreadableAnnouncement(pub usize);

/// The System entity ids a peer announced.
pub(crate) fn decode(announcement: &[u8]) -> Result<BTreeSet<EntityId>, UnreadableAnnouncement> {
    let (ids, remainder) = announcement.as_chunks::<32>();
    if !remainder.is_empty() {
        return Err(UnreadableAnnouncement(announcement.len()));
    }
    Ok(ids.iter().copied().map(EntityId::from_bytes).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_announcement_reads_back_the_ids_it_was_made_from() {
        let held = BTreeSet::from([EntityId::from_bytes([2; 32]), EntityId::from_bytes([1; 32])]);
        let announcement = encode(&held);
        assert_eq!(announcement, [[1u8; 32], [2; 32]].concat(), "ids in ascending order, 32 bytes each");
        assert_eq!(decode(&announcement), Ok(held));
        assert_eq!(decode(&[]), Ok(BTreeSet::new()));
    }

    #[test]
    fn an_announcement_that_is_not_a_whole_number_of_ids_is_unreadable() {
        assert_eq!(decode(&[7; 33]), Err(UnreadableAnnouncement(33)));
        assert_eq!(decode(&[7; 31]), Err(UnreadableAnnouncement(31)));
    }
}
