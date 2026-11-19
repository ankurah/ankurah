//! A tree's entry in the engine's trees map: which incarnation of the tree is
//! current, the generation its rows are at, and its maintenance status.
//!
//! An entry is seventeen bytes: one that says whether the tree is valid (0)
//! invalid (1), or still being built (2), then its incarnation and generation, each
//! eight bytes big-endian.

use super::names::{trees_map, Incarnation, TreeId};
use crate::storage::{MapRead, MapRows};
use crate::sync::{Error, TreePart};

/// A tree's entry: what every storage transaction reads, under the tree's
/// lock, before it reads or writes the tree's rows, and what every read of the
/// tree checks first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::sync::storage_adapter) struct TreeEntry {
    /// The tree's current incarnation, which names its maps.
    pub(in crate::sync::storage_adapter) incarnation: Incarnation,

    /// How many writes have changed the tree's rows since its creation, which
    /// is generation 0.
    pub(in crate::sync::storage_adapter) generation: u64,

    pub(in crate::sync::storage_adapter) status: Status,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::sync::storage_adapter) enum Status {
    Ready = 0,
    /// Refused maintenance requires replacement, not continuation.
    Invalid = 1,
    /// Partial initial membership can be resumed by a full canonical scan.
    /// Ordinary readers cannot use it until a completed cursor is published.
    Building = 2,
}

impl TreeEntry {
    /// `entry`, the tree's entry as a read found it, if a handle of
    /// `incarnation` may read and change the tree: `None` when the tree is
    /// retired, its entry absent, of another incarnation, invalid, or still building.
    pub(in crate::sync::storage_adapter) fn current(entry: Option<Self>, incarnation: Incarnation) -> Option<Self> {
        entry.filter(|entry| entry.incarnation == incarnation && entry.status == Status::Ready)
    }

    pub(in crate::sync::storage_adapter) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(17);
        bytes.push(self.status as u8);
        bytes.extend_from_slice(&self.incarnation.to_bytes());
        bytes.extend_from_slice(&self.generation.to_be_bytes());

        bytes
    }

    /// The entry these bytes hold, or `None` for bytes that are no entry.
    pub(in crate::sync::storage_adapter) fn decode(bytes: &[u8]) -> Option<Self> {
        let (&validity, rest) = bytes.split_first()?;
        let (incarnation, generation) = rest.split_first_chunk::<8>()?;
        let generation: [u8; 8] = generation.try_into().ok()?;

        let status = match validity {
            0 => Status::Ready,
            1 => Status::Invalid,
            2 => Status::Building,
            _ => return None,
        };

        Some(Self { incarnation: Incarnation::from_bytes(*incarnation), generation: u64::from_be_bytes(generation), status })
    }
}

/// The read of these trees' entries: one get of the trees map.
pub(in crate::sync::storage_adapter) fn read<'a>(ids: impl IntoIterator<Item = &'a TreeId>) -> MapRead {
    MapRead::Get { map: trees_map(), keys: ids.into_iter().map(TreeId::entry_key).collect() }
}

/// Each tree's entry, from `answer`, the answer to a [`read`] of `ids`:
/// `None` where a tree has none, and [`Error::Damaged`] where its
/// entry doesn't decode, which concerns that tree alone.
pub(in crate::sync::storage_adapter) fn found(ids: &[&TreeId], answer: Option<MapRows>) -> Result<Vec<Found>, Error> {
    let values = match answer {
        Some(MapRows::Got(values)) if values.len() == ids.len() => values,
        _ => return Err(Error::AnswerMisfit),
    };

    let decoded = ids.iter().zip(values).map(|(id, value)| {
        value
            .map(|bytes| {
                TreeEntry::decode(&bytes).ok_or_else(|| Error::Damaged { tree: (*id).clone(), part: TreePart::Entry, cause: None })
            })
            .transpose()
    });

    Ok(decoded.collect())
}

/// One tree's entry, as a read of it found it.
pub(in crate::sync::storage_adapter) type Found = Result<Option<TreeEntry>, Error>;

/// One tree's entry, from `answers`, the answers to a call whose one read was
/// a [`read`] of `id` alone: `None` where the tree has none.
pub(in crate::sync::storage_adapter) fn found_one(id: &TreeId, answers: Vec<MapRows>) -> Found {
    let [answer]: [MapRows; 1] = answers.try_into().map_err(|_| Error::AnswerMisfit)?;
    found(&[id], Some(answer))?.pop().expect("an entry for the one id")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_decodes_to_itself_and_nothing_else_decodes() {
        let entry = TreeEntry { incarnation: Incarnation::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]), generation: 9, status: Status::Invalid };
        let bytes = entry.encode();

        assert_eq!(bytes, [1, 1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 9]);
        assert_eq!(TreeEntry::decode(&bytes), Some(entry));

        assert_eq!(TreeEntry::decode(&bytes[..16]), None, "a truncated entry");
        assert_eq!(TreeEntry::decode(&[bytes.as_slice(), &[0]].concat()), None, "an entry with a byte to spare");
        assert_eq!(TreeEntry::decode(&[[2].as_slice(), &bytes[1..]].concat()), Some(TreeEntry { status: Status::Building, ..entry }));
        assert_eq!(TreeEntry::decode(&[[3].as_slice(), &bytes[1..]].concat()), None, "an unknown status");
    }
}
