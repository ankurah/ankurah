//! The first byte of every map name and lock name that core writes. The byte
//! says which part of core owns the name, so each owner takes its bytes from
//! this one table and no two owners' names collide. Engines give the bytes no
//! meaning.

/// The digest trees' map of tree entries.
pub(crate) const TREES: u8 = b't';

/// A digest tree's map of rows.
pub(crate) const TREE_ROWS: u8 = b'r';

/// A digest tree's change log.
pub(crate) const TREE_LOG: u8 = b'l';

/// A digest tree's member lookup.
pub(crate) const TREE_MEMBERS: u8 = b'm';

/// A digest tree's captured before-images.
pub(crate) const TREE_HISTORY: u8 = b'h';

/// A digest tree's lock.
pub(crate) const TREE_LOCK: u8 = b'k';

/// The node's keypair: its map and its lock.
pub(crate) const NODE_KEYPAIR: u8 = b'n';
