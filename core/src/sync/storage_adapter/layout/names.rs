//! What the storage adapter names in an engine's maps and locks, and the bytes of
//! each name.
//!
//! Every name begins with a byte that says what it names:
//!
//! - the engine's trees map is `t` alone, and keys each tree's entry by the
//!   tree's id, as its UTF-8 bytes;
//! - a tree's rows map is `r`, its cache log `l`, its before-image history `h`,
//!   and its member lookup `m`, each followed by the tree's incarnation, eight
//!   bytes big-endian;
//! - a tree's lock is `k`, followed by the tree's id as its UTF-8 bytes.
//!
//! A tree's maps are named by its incarnation alone, so the name an engine
//! keeps beside each of the tree's rows is nine bytes, however long the
//! tree's id. Its lock is named by its id alone, so that a storage
//! transaction can take it before it knows whether the tree exists, and lock
//! names order as the ids do.

use std::fmt;

use crate::storage::{LockName, MapId};

const TREES: u8 = b't';
const ROWS: u8 = b'r';
const LOG: u8 = b'l';
const MEMBERS: u8 = b'm';
const HISTORY: u8 = b'h';
const LOCK: u8 = b'k';

/// A tree's name among one engine's digest trees, which its owner chooses.
/// Ids order as their text's bytes do, and so do the trees' locks.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TreeId(String);

impl TreeId {
    /// The tree named `id`.
    pub fn new(id: impl Into<String>) -> Self { Self(id.into()) }

    /// The id's text.
    pub fn as_str(&self) -> &str { &self.0 }

    /// The lock a storage transaction takes before it reads or writes the
    /// tree's entry, rows or change log.
    pub(in crate::sync::storage_adapter) fn lock(&self) -> LockName { LockName::new(tagged(LOCK, self.0.as_bytes())) }

    /// The key of the tree's entry in the trees map.
    pub(in crate::sync::storage_adapter) fn entry_key(&self) -> Vec<u8> { self.0.as_bytes().to_vec() }
}

impl fmt::Display for TreeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.0) }
}

/// The engine's trees map, which holds each tree's entry under its id.
pub(in crate::sync::storage_adapter) fn trees_map() -> MapId { MapId::new([TREES]) }

/// One creation of a tree under its id: a random number, minted when the tree
/// is created and kept in its entry, that names the tree's maps. A tree
/// dropped and created again under the same id shares no map with the tree
/// before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(in crate::sync::storage_adapter) struct Incarnation(u64);

impl Incarnation {
    /// A new incarnation, for a tree being created.
    pub(in crate::sync::storage_adapter) fn mint() -> Self { Self(rand::random()) }

    /// The map that holds the tree's rows.
    pub(in crate::sync::storage_adapter) fn rows(self) -> MapId { MapId::new(tagged(ROWS, &self.to_bytes())) }

    /// The map that holds the tree's change log.
    pub(in crate::sync::storage_adapter) fn log(self) -> MapId { MapId::new(tagged(LOG, &self.to_bytes())) }

    /// The manager's member keys, heads and continuation cursor.
    pub(in crate::sync::storage_adapter) fn members(self) -> MapId { MapId::new(tagged(MEMBERS, &self.to_bytes())) }

    /// Before-images retained for captured comparison versions.
    pub(in crate::sync::storage_adapter) fn history(self) -> MapId { MapId::new(tagged(HISTORY, &self.to_bytes())) }

    /// Its eight bytes, big-endian.
    pub(in crate::sync::storage_adapter) fn to_bytes(self) -> [u8; 8] { self.0.to_be_bytes() }

    /// The incarnation these eight bytes hold.
    pub(in crate::sync::storage_adapter) fn from_bytes(bytes: [u8; 8]) -> Self { Self(u64::from_be_bytes(bytes)) }
}

/// `body` behind the byte `tag`.
fn tagged(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut name = Vec::with_capacity(1 + body.len());
    name.push(tag);
    name.extend_from_slice(body);

    name
}
