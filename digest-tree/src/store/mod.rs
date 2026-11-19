//! Where a tree keeps its rows.

use std::future::Future;
use std::ops::Bound;

use ankurah_core_types::key_spec::KeySpec;

use crate::{Digest, DigestGroup, NodePrefix, ScanDirection, StoreError};

mod encode;
#[cfg(any(test, feature = "test-support"))]
mod locked;
#[cfg(any(test, feature = "test-support"))]
mod memory;
mod overlay;

#[cfg(any(test, feature = "test-support"))]
pub use locked::{Locked, LockedRead, LockedTrx, OrderedMap};
#[cfg(any(test, feature = "test-support"))]
pub use memory::{MemoryMap, MemoryStore};
pub(crate) use overlay::Overlay;

/// One tree's storage: an ordered map from row keys to rows, behind a cheap,
/// cloneable handle whose clones share the same rows.
///
/// A store binds reads and writes to one tree, so keys need no tree id and
/// an unbounded range visits only that tree. This keeps storage namespaces
/// out of the tree algorithms. Several stores may share a backend, cache,
/// and outer transaction; each handle selects its tree within them.
/// Keeping those trees apart is the implementer's job.
///
/// # What a tree relies on
///
/// The tree writes through its owner's transaction on the store
/// ([`Trx`](Self::Trx)), lent for one commit or creation. A commit reads the
/// rows its changes touch, sums new digests from them, and then writes the
/// new rows in one batch. Those sums are right only if
///
/// - the batch lands with the owner's transaction, whole, or not at all;
/// - while the tree reads and writes through the transaction, and until the
///   transaction ends, no other transaction's write to the tree lands: a
///   store keeps this by making other writers wait, or by refusing to commit
///   one transaction once another's write has landed;
/// - the transaction's reads see the tree as the last landed commit left it,
///   and see the transaction's own writes, ranges included.
///
/// The owner keeps the tree consistent with the records it describes. It may
/// commit both in one storage transaction, or publish a rebuildable tree in
/// memory and persist it later. With deferred persistence, the durable tree
/// rows and their recovery checkpoint must describe the same source version
/// and land atomically. The owner must reconcile subsequent source changes
/// before serving a recovered tree. A memory commit is still an atomic tree
/// commit; deferring persistence does not permit partially visible writes.
///
/// Each of the tree's own reads, a count, the digests or the pages at some
/// spans, an audit, or one batch of a range read, begins a read of the store
/// of its own ([`begin_read`](Self::begin_read)). A range read begins one for
/// each batch, so that it holds none open while its caller pauses.
///
/// A store that keeps rows in memory takes in a write's rows only once the
/// owner's transaction has committed: what it keeps of the write until then
/// is its [`Pending`](Self::Pending), which the owner hands back to
/// [`landed`](Self::landed) through the tree's
/// [`Applied::landed`](crate::Applied::landed) or
/// [`Created::landed`](crate::Created::landed).
pub trait Store<K, V>: Clone + Send + Sync + 'static {
    /// A read the tree begins itself, for one of its own reads: a count, the
    /// digests of some spans, the pages at some spans, the members within some
    /// spans, an audit, or one batch of a range read.
    type Read: ReadTrx<K, V>;

    /// The owner's transaction on this store, as the owner lends it to the
    /// tree for one commit or one creation.
    type Trx<'a>: StoreTrx<K, V, Pending = Self::Pending>
    where Self: 'a;

    /// What the store keeps of a transaction's write until the owner's commit
    /// is known, and takes in at [`landed`](Self::landed).
    type Pending: Send + 'static;

    /// Begin a read that sees one consistent committed state for all its rows.
    /// Include every tree commit completed before this call began. Commits concurrent
    /// with snapshot acquisition may be included, but never partially.
    fn begin_read(&self) -> impl Future<Output = Result<Self::Read, StoreError>> + Send;

    /// Whether `trx` is a transaction on this store's rows, rather than on
    /// another store's.
    fn owns(&self, trx: &Self::Trx<'_>) -> bool;

    /// The owner's transaction that a write returned `pending` from has
    /// committed, and its rows have landed: a store that keeps rows in memory
    /// may take them in now. It touches memory only, so it is synchronous and
    /// can't fail.
    fn landed(&self, pending: Self::Pending);
}

/// Reads of one tree's rows, all through one read or one transaction, so that
/// each sees the rows as that read or transaction does.
pub trait ReadTrx<K, V>: Send {
    /// The row under `key`.
    fn get(&mut self, key: &K) -> impl Future<Output = Result<Option<V>, StoreError>> + Send;

    /// The row under each of `keys`, one per key and in their order, in one
    /// request where the store can.
    fn get_many(&mut self, keys: &[K]) -> impl Future<Output = Result<Vec<Option<V>>, StoreError>> + Send;

    /// The first `limit` rows whose keys lie between the bounds, in key order
    /// or reversed: fewer only where fewer lie between them.
    fn range(
        &mut self,
        start: Bound<&K>,
        end: Bound<&K>,
        direction: ScanDirection,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(K, V)>, StoreError>> + Send;
}

/// The owner's transaction on a store, as one tree sees it: its reads see the
/// rows as the transaction does, its own writes included, and its write lands
/// when the owner commits the transaction, never before.
pub trait StoreTrx<K, V>: ReadTrx<K, V> {
    /// What the store keeps of the write until the owner's commit is known.
    type Pending;

    /// A commit's one write, possibly empty, made after all its reads: these
    /// puts and deletes land together when the owner commits, all of them or
    /// none. Later reads through the same transaction see them.
    fn write(&mut self, batch: Vec<WriteItem<K, V>>) -> impl Future<Output = Result<Self::Pending, StoreError>> + Send;
}

/// A read lent to a tree is a read too, so that its owner keeps it.
impl<K, V, T: ReadTrx<K, V>> ReadTrx<K, V> for &mut T {
    fn get(&mut self, key: &K) -> impl Future<Output = Result<Option<V>, StoreError>> + Send { (**self).get(key) }

    fn get_many(&mut self, keys: &[K]) -> impl Future<Output = Result<Vec<Option<V>>, StoreError>> + Send { (**self).get_many(keys) }

    fn range(
        &mut self,
        start: Bound<&K>,
        end: Bound<&K>,
        direction: ScanDirection,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<(K, V)>, StoreError>> + Send {
        (**self).range(start, end, direction, limit)
    }
}

/// A transaction lent to a tree is a transaction too, so that its owner keeps
/// it, to write its own records and to commit it.
impl<K, V, T: StoreTrx<K, V>> StoreTrx<K, V> for &mut T {
    type Pending = T::Pending;

    fn write(&mut self, batch: Vec<WriteItem<K, V>>) -> impl Future<Output = Result<Self::Pending, StoreError>> + Send {
        (**self).write(batch)
    }
}

/// One write in a batch.
#[derive(Clone, Debug)]
pub enum WriteItem<K, V> {
    /// Store `V` under `K`, replacing any row there.
    Put(K, V),

    /// Remove any row under `K`.
    Delete(K),
}

/// A hint for a store's cache policy. A store may evict any row to respect its
/// memory budget, or keep no cache at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Residency {
    /// Prefer keeping it while the tree is open: the tree reads it on almost
    /// every operation.
    Always,

    /// Prefer keeping it while recently used.
    Recent,
}

/// The key of a row a tree keeps. Keys of one kind sort together.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowKey {
    /// The tree's metadata: its key spec and digest group.
    Meta,

    /// A node's row, by the node's prefix.
    Node(NodePrefix),

    /// A member, by its address.
    Leaf(Vec<u8>),
}

impl RowKey {
    /// The suggested cache policy for this row.
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
///
/// # Rows by key
///
/// Two trees compare by exchanging pages ([`Page`](crate::Page)), yet a store
/// holds a tree as rows by key: one for its metadata, one for each node of its
/// trie, and one for each member. Rows are what an ordered map holds well. A
/// change to one member rewrites that member's row and the rows of the nodes
/// on its path, where a stored page would be rewritten whole, with every
/// member of its bucket, for the same change. A bucket's members, in address
/// order, are one run of keys, which an ordered map reads as it reads any
/// range. And each step down the trie reads one node by its key, since a node
/// with children records in its row where it branches, so a store may keep
/// some of a tree's rows in memory without keeping all of them. A page is
/// built from these rows when another tree asks for it, and is kept nowhere.
#[derive(Clone, Debug)]
pub enum Row<K, L, G: DigestGroup> {
    /// The tree's metadata.
    Meta(Meta<K>),

    /// A subtree's digest, and where its node branches.
    Node(NodeRow<G>),

    /// A member's payload and leaf element.
    Leaf(LeafRow<L, G>),
}

/// A node's row: the digest of the members beneath it, and where the node
/// branches. The trie compresses its paths, so a child's prefix can't be
/// worked out from its parent's; recording where a node branches lets the
/// trie read its children by their keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRow<G: DigestGroup> {
    /// The digest of the members beneath the node.
    pub(crate) digest: Digest<G>,

    /// For a node with children, the prefix they share: its children are this
    /// prefix followed by 0 and by 1. `None` for a bucket.
    pub(crate) branches_at: Option<NodePrefix>,
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
