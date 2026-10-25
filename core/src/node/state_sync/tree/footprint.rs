//! What the LRU charges a resident tree for its memory: a fixed base, each
//! member's index rows, and the digest tree's rows over the members.

use super::TreeEntry;
use ankurah_digest_tree::{Digest, LeafElement, Ristretto255, BUCKET_CAPACITY};
use ankurah_proto::{EntityId, EventId};
use std::{mem::size_of, sync::Arc};

/// An ordered map's share of each row beyond the key and value it stores:
/// node headers, edges and unused slots.
const MAP_ROW_BYTES: usize = 32;
/// One entity id in an ordered map or set, with the row's share of the map.
pub(crate) const ID_ROW_BYTES: usize = size_of::<EntityId>() + MAP_ROW_BYTES;
/// One Clock entry: a `u32` generation and an event id.
pub(crate) const CLOCK_ENTRY_BYTES: usize = size_of::<(u32, EventId)>();
/// A tree's cost before it has any member: its maps, its spec and its digest tree.
const EMPTY_TREE_BYTES: usize = 4096;
/// The header of a shared allocation: its strong and weak counts.
const ARC_HEADER_BYTES: usize = 2 * size_of::<usize>();
/// A member's index rows, beside its address bytes and Clock entries: its
/// entity id and entry in `entries`, its address handle and entity id in
/// `members`, and the header of the address both share.
const INDEX_ROW_BYTES: usize = 2 * ID_ROW_BYTES + size_of::<TreeEntry>() + size_of::<Arc<[u8]>>() + ARC_HEADER_BYTES;
/// One digest-tree node row: a prefix encoding of at most 16 bytes, the
/// node's digest, the two vectors holding them and the row's share of the map.
const NODE_ROW_BYTES: usize = 16 + Digest::<Ristretto255>::ROW_LEN + 2 * size_of::<Vec<u8>>() + MAP_ROW_BYTES;
/// A member's digest rows, beside the copies of its address and key they
/// hold: its leaf row's head hash and element, its lookup row's entity id and
/// key length, the four vectors holding them, the two rows' shares of their
/// maps, and its share of the node rows, of which a tree holds at most one for
/// every quarter of a bucket of leaves.
const DIGEST_ROW_BYTES: usize = 32
    + LeafElement::<Ristretto255>::ROW_LEN
    + size_of::<EntityId>()
    + size_of::<u32>()
    + 4 * size_of::<Vec<u8>>()
    + 2 * MAP_ROW_BYTES
    + NODE_ROW_BYTES.div_ceil(BUCKET_CAPACITY / 4);

/// Running totals of what a tree's members hold, so that its charge is known
/// without visiting them.
#[derive(Default)]
pub(super) struct Footprint {
    members: usize,
    address_bytes: usize,
    clock_entries: usize,
}

impl Footprint {
    pub(super) fn add(&mut self, entry: &TreeEntry) {
        self.members += 1;
        self.address_bytes += entry.address().len();
        self.clock_entries += entry.head.len();
    }

    pub(super) fn remove(&mut self, entry: &TreeEntry) {
        self.members -= 1;
        self.address_bytes -= entry.address().len();
        self.clock_entries -= entry.head.len();
    }

    /// The bytes charged for the members' index, and for their digest rows
    /// when `digests` are built.
    pub(super) fn bytes(&self, digests: bool) -> usize {
        let index = self
            .members
            .saturating_mul(INDEX_ROW_BYTES)
            .saturating_add(self.address_bytes)
            .saturating_add(self.clock_entries.saturating_mul(CLOCK_ENTRY_BYTES));
        // A leaf row's key is the whole address; a lookup row's value holds the key.
        let digest_rows =
            if digests { self.members.saturating_mul(DIGEST_ROW_BYTES).saturating_add(self.address_bytes.saturating_mul(2)) } else { 0 };
        EMPTY_TREE_BYTES.saturating_add(index).saturating_add(digest_rows)
    }
}
