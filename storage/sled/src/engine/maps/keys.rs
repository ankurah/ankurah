//! Where each map's entries lie in the one sled tree that holds every map:
//! each under its map's name, the name's length first, and then its key.
//!
//! The length comes first so that no map's name runs into another's or into
//! a key: the entry of the map `m` under `ab` and that of the map `ma` under
//! `b` lie apart. Every entry of a map then begins with the same bytes, which
//! begin no other map's entry, so a map's entries lie together, in the order
//! of their keys.

use std::ops::Bound;

use ankurah_core::storage::MapId;

/// The bytes every entry of one map begins with in the maps' sled tree.
pub(super) struct MapPrefix(Vec<u8>);

impl MapPrefix {
    /// The prefix of `map`'s entries: its name's length, in eight bytes
    /// big-endian, then its name.
    pub(super) fn of(map: &MapId) -> Self {
        let name = map.as_bytes();
        let mut prefix = Vec::with_capacity(8 + name.len());
        prefix.extend_from_slice(&(name.len() as u64).to_be_bytes());
        prefix.extend_from_slice(name);

        Self(prefix)
    }

    /// The prefix's bytes, which every entry of the map begins with.
    pub(super) fn as_bytes(&self) -> &[u8] { &self.0 }

    /// The sled key of the map's entry under `key`.
    pub(super) fn entry(&self, key: &[u8]) -> Vec<u8> { [self.0.as_slice(), key].concat() }

    /// The key within the map of the entry under `entry`, a sled key that
    /// begins with the prefix.
    pub(super) fn key_of<'e>(&self, entry: &'e [u8]) -> &'e [u8] { &entry[self.0.len()..] }

    /// The sled keys between which lie the map's entries whose keys lie
    /// between `start` and `end`.
    pub(super) fn bounds(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        let start = match start {
            Bound::Included(key) => Bound::Included(self.entry(key)),
            Bound::Excluded(key) => Bound::Excluded(self.entry(key)),
            Bound::Unbounded => Bound::Included(self.0.clone()),
        };

        let end = match end {
            Bound::Included(key) => Bound::Included(self.entry(key)),
            Bound::Excluded(key) => Bound::Excluded(self.entry(key)),
            Bound::Unbounded => self.past_every_entry(),
        };

        (start, end)
    }

    /// The bound past every entry of the map and before any later map's: the
    /// least key that doesn't begin with the prefix and follows it.
    fn past_every_entry(&self) -> Bound<Vec<u8>> {
        let mut past = self.0.clone();
        while let Some(last) = past.pop() {
            if last < u8::MAX {
                past.push(last + 1);
                return Bound::Excluded(past);
            }
        }

        Bound::Unbounded
    }
}
