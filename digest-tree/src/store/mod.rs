//! Where a tree keeps its rows.

use std::future::Future;
use std::ops::Bound;

use ankurah_core_types::key_spec::KeySpec;

use crate::{Digest, DigestGroup, NodePrefix, ScanDirection, StoreError};

mod encode;
mod memory;
#[cfg(feature = "sled")]
mod sled;

#[cfg(feature = "sled")]
pub use self::sled::SledStore;
pub use memory::MemoryStore;

/// One tree's storage: an ordered map from row keys to rows, behind a cheap,
/// cloneable handle whose clones share the same rows.
///
/// A store holds exactly one tree. Keeping several trees apart is the
/// implementer's job.
///
/// # What a tree relies on
///
/// A commit reads the rows its changes touch, sums new digests from them, and
/// writes the new rows in one batch. Those sums are right only if
///
/// - each batch lands whole or not at all;
/// - commits run one at a time: each reads every row the commits before it
///   wrote, and no other batch lands between a commit's reads and its own
///   batch;
/// - no read of the tree sees part of a commit.
///
/// The tree keeps the last two itself for one tree and its clones, both for a
/// batch it writes through [`write`](Self::write), provided every read sees
/// the batch as soon as `write` returns, and for one its owner writes from a
/// [staged commit](crate::Transaction::stage) in the owner's own storage
/// transaction. A store keeps them itself, however it chooses, when trees
/// opened separately or other processes write through it, since those don't
/// wait for each other. A store that keeps rows in memory takes in a staged
/// commit's rows only once that storage transaction commits.
pub trait Store<K, V>: Clone + Send + Sync + 'static {
    /// The row under `key`.
    fn get(&self, key: &K) -> impl Future<Output = Result<Option<V>, StoreError>> + Send;

    /// At most `limit` rows whose keys lie between the bounds, in key order or
    /// reversed. A caller reads on from the last key it was given.
    fn range(
        &self,
        start: Bound<&K>,
        end: Bound<&K>,
        direction: ScanDirection,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(K, V)>, StoreError>> + Send;

    /// Put and delete these rows together: all of them, or none.
    fn write(&self, items: Vec<WriteItem<K, V>>) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// One write in a batch.
#[derive(Clone, Debug)]
pub enum WriteItem<K, V> {
    /// Store `V` under `K`, replacing any row there.
    Put(K, V),

    /// Remove any row under `K`.
    Delete(K),
}

/// How a store that pages to disk should keep a row in memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Residency {
    /// Keep it in memory while the tree is open: the tree reads it on almost
    /// every operation.
    Always,

    /// Keep it while recently used.
    Recent,
}

/// The key of a row a tree keeps. Keys of one kind sort together.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowKey {
    /// The tree's metadata: its key spec and digest group.
    Meta,

    /// A subtree's digest, by the subtree's prefix.
    Node(NodePrefix),

    /// A member, by its address.
    Leaf(Vec<u8>),
}

impl RowKey {
    /// How a store that pages to disk should keep this row in memory.
    pub fn residency(&self) -> Residency {
        match self {
            Self::Meta | Self::Node(_) => Residency::Always,
            Self::Leaf(_) => Residency::Recent,
        }
    }
}

/// A row a tree keeps, for a tree whose key spec names properties of type
/// `K`. Its contents are the tree's business; a store keeps it whole, and a
/// byte store encodes it with [`Encode`].
#[derive(Clone, Debug)]
pub enum Row<K, L, G: DigestGroup> {
    /// The tree's metadata.
    Meta(Meta<K>),

    /// A subtree's digest.
    Node(Digest<G>),

    /// A member's payload and leaf element.
    Leaf(LeafRow<L, G>),
}

/// The tree's metadata row: its key spec and the digest group its sums use.
#[derive(Clone, Debug)]
pub struct Meta<K> {
    pub(crate) key_spec: KeySpec<K>,
    pub(crate) group: u8,
}

/// A member's row: its payload, and its element in the tree's group.
#[derive(Clone, Debug)]
pub struct LeafRow<L, G: DigestGroup> {
    pub(crate) payload: L,
    pub(crate) element: G::Element,
}

/// The byte encoding a store that keeps bytes uses for row keys and rows.
/// Encodings of [`RowKey`]s sort as the keys do.
pub trait Encode: Sized {
    /// The value's bytes.
    fn encode(&self) -> Vec<u8>;

    /// The value these bytes encode. Bytes that don't decode are a permanent
    /// failure.
    fn decode(bytes: &[u8]) -> Result<Self, StoreError>;
}

#[cfg(test)]
mod tests;
