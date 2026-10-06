//! Tree storage: what an engine keeps for each digest tree, and the atomic
//! batch through which the refresher changes it.
//!
//! Digest trees let two stores find where they differ by comparing a few
//! digests instead of every entity. The core owns their mathematics (leaf
//! points, digest sums, splits and merges) and uses the engine as ordered
//! storage: points and digests reach the engine as opaque byte strings in the
//! core's storage codec, so no engine sees a curve.
//!
//! Each registered tree serves one [`HashedIndex`] and holds:
//!
//! - folded rows, one per key-and-entity pair the index files, ordered by
//!   [leaf address](leaf_address) (the canonical key, then the entity id), each
//!   holding the entity's sorted head, its leaf point and the position of the
//!   change that last set the row;
//! - tombstones in the same address space, each recording that an entity left
//!   a key and the position at which it did. Node watermarks rise with them, so
//!   a partner learns of removals; they stay until pruned;
//! - the lookup from an entity id to its keys in the index. A commit names
//!   only the entity, so this lookup is how the refresher finds the old leaf
//!   to subtract when an entity changes or moves. The engine maintains it from
//!   the folded rows, so the two never disagree;
//! - node rows by [`NodePrefix`], each holding a digest, a leaf count and a
//!   modification watermark. The core adds digests and counts and raises the
//!   watermark to the largest position beneath the node; the engine stores
//!   what it is given;
//! - the [`TreeCell`]: the build generation, the build status and the fold
//!   position.
//!
//! The commit log ([`super::log`]) feeds the trees. A tree starts at the
//! stable position where it was registered: the engine serializes
//! registration with commits, so every log row at or above that position
//! carries the tree's keys. Every position a tree records belongs to the log
//! incarnation of its cell; a reset removes every tree and leaves only a fresh
//! entity-id tree.
//!
//! A [`TreeBatch`] changes one tree atomically. Reads through it are
//! consistent with each other and with the cell it is compared against, and
//! see the batch's own earlier writes. Its commit applies every write together
//! if the cell still equals the one the caller expected, and none otherwise;
//! a caller that meets a conflict starts over from the cell it observed.
//!
//! The store keeps one prune horizon for all its trees: the position below
//! which some tree may hold no tombstone for a removal, because tombstones
//! there were pruned, or because a tree was registered or rebuilt there and
//! never saw the removals before it. A partner whose cursor lies below the
//! horizon gets no watermark shortcut and compares the whole scope, and a
//! rebuild sets every watermark to at least the horizon.

mod index;
mod prefix;

use std::ops::Bound;

use ankurah_proto::{Clock, EntityId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use index::{HashedIndex, KeyDerivationError};
pub use prefix::{NodePrefix, NodePrefixError};

use super::log::{CommitLog, LogIncarnation, LogPosition};

/// Names one registration of a tree within a store. Not reused within a log
/// incarnation, so a handle to an unregistered tree fails instead of reaching
/// its successor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TreeId(pub u64);

/// A registered tree and the index it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeRegistration {
    pub id: TreeId,
    pub index: HashedIndex,
}

/// Whether sessions and claims may use a tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildStatus {
    /// Still filling: no session or claim uses it yet.
    Building,
    /// Filled and caught up with the log once; published.
    Ready,
}

/// A tree's progress, which every batch compares and replaces as one value.
///
/// The generation is part of the comparison so that a batch prepared for an
/// abandoned build fails even when the new build happens to sit at the same
/// fold position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeCell {
    /// Which build the tree's rows belong to; each restart takes the next.
    pub generation: u64,
    pub status: BuildStatus,
    /// The fold position: every log row below it is folded into the tree, and
    /// none at or above it. It advances by whole positions only.
    pub folded: LogPosition,
}

/// The tree's record of one key-and-entity pair: the inputs of its leaf and
/// the leaf's point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoldedRow {
    /// The canonical key the index files the entity under.
    pub key: Vec<u8>,
    pub entity_id: EntityId,
    /// The entity's head, whose hash the leaf carries.
    pub head: Clock,
    /// The leaf's point, opaque bytes in the core's storage codec.
    pub point: Vec<u8>,
    /// The position of the change that last set this row.
    pub position: LogPosition,
}

impl FoldedRow {
    pub fn address(&self) -> Vec<u8> { leaf_address(&self.key, self.entity_id) }
}

/// What a removal leaves under the key an entity left.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    /// The key the entity left.
    pub key: Vec<u8>,
    pub entity_id: EntityId,
    /// The position of the removal.
    pub position: LogPosition,
}

impl Tombstone {
    pub fn address(&self) -> Vec<u8> { leaf_address(&self.key, self.entity_id) }
}

/// A node's summary of the leaves beneath it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRow {
    /// The sum of the leaf points beneath the node, opaque bytes in the core's
    /// storage codec.
    pub digest: Vec<u8>,
    /// The number of leaves beneath the node.
    pub count: u64,
    /// The highest position at which anything beneath the node changed,
    /// removals included.
    pub watermark: LogPosition,
}

/// Where a tree files a key-and-entity pair: the canonical key followed by the
/// entity id. Folded rows and tombstones are ordered by it, and a node's
/// prefix is a run of its leading bits.
pub fn leaf_address(key: &[u8], entity_id: EntityId) -> Vec<u8> {
    let mut address = Vec::with_capacity(key.len() + 32);
    address.extend_from_slice(key);
    address.extend_from_slice(&entity_id.to_bytes());
    address
}

/// A range of leaf addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressRange {
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
}

impl AddressRange {
    pub fn new(start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> Self { Self { start, end } }

    pub fn all() -> Self { Self::new(Bound::Unbounded, Bound::Unbounded) }

    /// Every address beneath `prefix`: the leaves of a bucket, or of any node.
    pub fn under(prefix: &NodePrefix) -> Self {
        let (start, end) = prefix.address_bounds();
        Self::new(Bound::Included(start), end.map_or(Bound::Unbounded, Bound::Excluded))
    }

    /// The rest of this range after `address`, to read the next page.
    pub fn after(&self, address: &[u8]) -> Self { Self::new(Bound::Excluded(address.to_vec()), self.end.clone()) }

    pub fn contains(&self, address: &[u8]) -> bool {
        let above_start = match &self.start {
            Bound::Included(start) => address >= start.as_slice(),
            Bound::Excluded(start) => address > start.as_slice(),
            Bound::Unbounded => true,
        };
        let below_end = match &self.end {
            Bound::Included(end) => address <= end.as_slice(),
            Bound::Excluded(end) => address < end.as_slice(),
            Bound::Unbounded => true,
        };
        above_start && below_end
    }
}

/// The result of committing a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeBatchOutcome {
    /// The cell matched, every write applied, and this is the new cell.
    Committed(TreeCell),
    /// The cell had moved; no write applied.
    Conflict { observed: TreeCell },
}

/// Why a tree storage operation failed.
#[derive(Debug, thiserror::Error)]
pub enum TreeStorageError {
    #[error("no tree {0:?} is registered")]
    UnknownTree(TreeId),
    #[error("the entity-id tree cannot be unregistered")]
    PermanentTree,
    #[error("a position from log incarnation {found:?} cannot enter a tree of incarnation {expected:?}")]
    IncarnationMismatch { expected: LogIncarnation, found: LogIncarnation },
    #[error("the fold position cannot move back from {current:?} to {requested:?}")]
    FoldBackwards { current: LogPosition, requested: LogPosition },
    #[error("storage error: {0}")]
    Storage(Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Reads of one tree. Through a [`TreeStorage::reader`] each read is atomic on
/// its own; through a [`TreeBatch`] every read belongs to the batch's snapshot.
#[async_trait]
pub trait TreeRead: Send + Sync {
    async fn cell(&self) -> Result<TreeCell, TreeStorageError>;

    /// The folded row of one key-and-entity pair.
    async fn row(&self, key: &[u8], entity_id: EntityId) -> Result<Option<FoldedRow>, TreeStorageError>;

    /// Every folded row of an entity, one per key the index files it under, in
    /// address order: the entity-to-keys lookup.
    async fn entity_rows(&self, entity_id: EntityId) -> Result<Vec<FoldedRow>, TreeStorageError>;

    /// The first `limit` folded rows in `range`, in address order. Reading all
    /// of them from some address on, a page at a time, is the forward scan.
    async fn rows(&self, range: &AddressRange, limit: usize) -> Result<Vec<FoldedRow>, TreeStorageError>;

    /// The first `limit` tombstones in `range`, in address order.
    async fn tombstones(&self, range: &AddressRange, limit: usize) -> Result<Vec<Tombstone>, TreeStorageError>;

    async fn node(&self, prefix: &NodePrefix) -> Result<Option<NodeRow>, TreeStorageError>;

    /// The stored nodes nearest beneath `prefix`, in prefix order: every node
    /// row `prefix` properly contains with no stored row between them. Where
    /// path compression skipped levels, a child lies several levels down.
    async fn children(&self, prefix: &NodePrefix) -> Result<Vec<(NodePrefix, NodeRow)>, TreeStorageError>;
}

/// An atomic change to one tree. Dropping it without committing applies
/// nothing.
#[async_trait]
pub trait TreeBatch: TreeRead {
    /// Write the folded row at its address, replacing any row there.
    async fn put_row(&mut self, row: FoldedRow) -> Result<(), TreeStorageError>;

    async fn delete_row(&mut self, key: &[u8], entity_id: EntityId) -> Result<(), TreeStorageError>;

    /// Write the tombstone at its address, replacing any tombstone there.
    async fn put_tombstone(&mut self, tombstone: Tombstone) -> Result<(), TreeStorageError>;

    /// Remove every tombstone positioned below `below` and raise the store's
    /// prune horizon to at least `below`, so that no partner relies on the
    /// removals they recorded.
    async fn prune_tombstones(&mut self, below: LogPosition) -> Result<(), TreeStorageError>;

    /// Write the node row at `prefix`, replacing any row there.
    async fn put_node(&mut self, prefix: NodePrefix, node: NodeRow) -> Result<(), TreeStorageError>;

    async fn delete_node(&mut self, prefix: &NodePrefix) -> Result<(), TreeStorageError>;

    /// Advance the fold position to `position`, which must not lie below it:
    /// every log row below `position` is now folded into the tree.
    async fn set_folded(&mut self, position: LogPosition) -> Result<(), TreeStorageError>;

    /// Publish the tree for sessions and claims.
    async fn publish(&mut self) -> Result<(), TreeStorageError>;

    /// Start the tree's build over from `folded`: remove every row, take the
    /// next generation, mark the tree building, and raise the store's prune
    /// horizon to at least `folded`, since the new build records no removal
    /// before it. A build that fills from a snapshot starts here, with the
    /// snapshot's position.
    async fn restart(&mut self, folded: LogPosition) -> Result<(), TreeStorageError>;

    /// Apply every write if the tree's cell still equals `expected`, and none
    /// otherwise.
    async fn commit(self, expected: &TreeCell) -> Result<TreeBatchOutcome, TreeStorageError>;
}

/// The digest trees an engine keeps beside its commit log.
#[async_trait]
pub trait TreeStorage: CommitLog {
    type Reader<'a>: TreeRead + 'a
    where Self: 'a;

    type Batch<'a>: TreeBatch + 'a
    where Self: 'a;

    /// Register a tree for `index`, or return the tree already serving it. A
    /// new tree starts building at generation 0 from the stable position, and
    /// the store's prune horizon rises to that position.
    async fn register_tree(&self, index: HashedIndex) -> Result<TreeRegistration, TreeStorageError>;

    /// Remove a tree with all its rows. The entity-id tree is permanent.
    async fn unregister_tree(&self, tree: TreeId) -> Result<(), TreeStorageError>;

    /// Every registered tree, in id order.
    async fn trees(&self) -> Result<Vec<TreeRegistration>, TreeStorageError>;

    /// The position below which some tree may hold no tombstone for a removal.
    async fn prune_horizon(&self) -> Result<LogPosition, TreeStorageError>;

    /// Reads of one tree outside any batch. A read may wait while a batch on
    /// the same tree is open, so a task holding a batch reads through it.
    async fn reader(&self, tree: TreeId) -> Result<Self::Reader<'_>, TreeStorageError>;

    /// Begin a batch on one tree. It may wait while another batch on the same
    /// tree is open.
    async fn batch(&self, tree: TreeId) -> Result<Self::Batch<'_>, TreeStorageError>;
}
