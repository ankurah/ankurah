//! The maps that hold a tree's rows, and the transaction over them: three
//! ordered maps of byte keys to byte values, read consistently and written on
//! conditions.
//!
//! A tree keeps its rows in the three maps a [`MapName`] names, and reads and
//! writes them through [`MapAccess`] within a transaction. The tree encodes
//! every key and value itself, in the `layout` module's layouts, so the maps hold
//! bytes and nothing else.
//!
//! A transaction is the unit of consistency. Every read within it observes one
//! consistent state of all three maps, the state the transaction began with,
//! together with its own earlier writes, so a reader that reads a tree node's
//! base row and then its delta rows never sees a compaction land between the
//! two. Every write is conditional on what the transaction read, so that
//! nothing depends on the order in which concurrent transactions commit, and a
//! transaction that fails leaves nothing half-applied:
//! [`insert`](MapAccess::insert) needs the key absent, and
//! [`put_if`](MapAccess::put_if) and
//! [`delete_if`](MapAccess::delete_if) need the value the transaction
//! sees there. [`commit`](MapTransaction::commit) applies every write
//! together, and only if every condition still holds against the maps as they
//! stand then. A condition that fails, when its write is made or at the
//! commit, fails the whole transaction with [`TreeError::Conflict`] and
//! applies nothing, and the writer retries from fresh reads in a new
//! transaction. Dropping a transaction without committing it rolls it back. A
//! value that only ever accumulates beside others goes in by
//! [`append`](MapAccess::append), under a key no other append takes, so
//! that appends never conflict.
//!
//! A storage engine implements both traits to keep a tree in its own store;
//! access is async so that such an implementation can await I/O.
//! [`MemoryTransaction`](crate::MemoryTransaction) implements them in memory,
//! where every operation completes at once.

use std::ops::Bound;

use async_trait::async_trait;

use crate::error::TreeError;

pub(crate) mod layout;
mod memory;
mod rows;

pub use memory::{MemoryMaps, MemoryTransaction};
pub(crate) use rows::EncodedLeaf;
pub use rows::LeafRow;

/// The bounds of a range scan: a lower and an upper bound on the key, as
/// [`std::ops::RangeBounds`] gives them.
pub type ByteBounds<'a> = (Bound<&'a [u8]>, Bound<&'a [u8]>);

/// The three maps that hold a tree's rows, each an ordered map of byte keys to
/// byte values in the layout the tree gives them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MapName {
    /// The leaf rows, by leaf address.
    Leaves,
    /// The keys each entity is filed under, by entity id: what
    /// [`TreeTransaction::entity_leaves`](crate::TreeTransaction::entity_leaves)
    /// reads, what keeps the count of entities filed under more than one key,
    /// which a Selection's scope requires to be zero, and what an audit checks
    /// against the leaves. Every leaf insertion and removal keeps it in step.
    /// Its reserved empty key holds that count.
    Lookup,
    /// The tree node rows, by prefix: a base row per tree node and the delta
    /// rows appended since its base was last compacted.
    Nodes,
}

/// Why a transaction can no longer commit, which its commit reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    /// A write's condition failed, or an operation found what it expected
    /// changed: the commit reports [`TreeError::Conflict`], and the writer
    /// retries from fresh reads.
    Conflict,
    /// An operation failed for another reason, or was cancelled or panicked
    /// before it finished: the commit reports [`TreeError::Poisoned`].
    Poisoned,
}

impl From<Failure> for TreeError {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Conflict => TreeError::Conflict,
            Failure::Poisoned => TreeError::Poisoned,
        }
    }
}

/// Access to one tree's three maps within a transaction.
///
/// An adapter may borrow an outer transaction containing other trees and
/// records. All reads and writes use that same transaction, including its
/// earlier writes. Dropping the adapter neither commits nor undoes them.
/// [`MapTransaction`] additionally owns the right to commit.
#[async_trait]
pub trait MapAccess: Send {
    /// Permanently prevent the owning transaction from committing any writes,
    /// its commit reporting the first failure recorded. A borrowed adapter
    /// must forward this to its owner, so dropping the adapter or opening
    /// another view cannot clear a failed operation.
    fn poison(&mut self, failure: Failure);

    /// Keep the tree topology this transaction read valid until commit.
    /// Ordinary leaf changes may still commit concurrently.
    fn watch_structure(&mut self);

    /// Publish a topology change only if no writes have committed since this
    /// transaction began. Transactions watching the old topology must retry.
    fn change_structure(&mut self);

    /// The value stored under `key` in `map`.
    async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError>;

    /// The first `limit` entries of `map` whose keys lie within `bounds`, in
    /// key order. Reading on from the last key returned, a page at a time,
    /// reads them all.
    async fn scan(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError>;

    /// Count entries within `bounds`, stopping at `limit`. This observes the
    /// same transaction view as [`Self::scan`], including earlier writes.
    /// Adapters may override the scan-based default to avoid copying row data.
    async fn count_up_to(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<usize, TreeError> {
        Ok(self.scan(map, bounds, limit).await?.len())
    }

    /// The last `limit` entries within `bounds`, in descending key order.
    async fn scan_reverse(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError>;

    /// Store `value` under `key` in `map`, where the transaction sees nothing:
    /// [`TreeError::Conflict`] when it sees a value.
    async fn insert(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError>;

    /// Store `value` under `key` in `map` if the value the transaction sees
    /// there is exactly `expected`, `None` meaning absent:
    /// [`TreeError::Conflict`] otherwise.
    async fn put_if(&mut self, map: MapName, key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> Result<(), TreeError>;

    /// Remove the value under `key` in `map` if the transaction sees exactly
    /// `expected` there: [`TreeError::Conflict`] otherwise, an absent value
    /// included.
    async fn delete_if(&mut self, map: MapName, key: &[u8], expected: &[u8]) -> Result<(), TreeError>;

    /// Store `value` in `map` under `key_prefix` followed by a suffix no other
    /// append takes, never empty: a counter, random bytes, or the
    /// transaction's identity with a count within it. The tree never reads the
    /// suffix; it needs only that values appended under one prefix take
    /// distinct keys and scan as one range, which the prefix gives.
    async fn append(&mut self, map: MapName, key_prefix: &[u8], value: &[u8]) -> Result<(), TreeError>;
}

/// An owned transaction over a tree's maps, able to commit its writes.
/// Borrowed [`MapAccess`] views leave commit to their outer owner.
#[async_trait]
pub trait MapTransaction: MapAccess {
    /// Apply every write the transaction made, together, if every condition
    /// still holds: [`TreeError::Conflict`] otherwise, with nothing applied.
    async fn commit(self) -> Result<(), TreeError>;
}

#[async_trait]
impl<T: MapAccess + ?Sized> MapAccess for &mut T {
    fn poison(&mut self, failure: Failure) { (**self).poison(failure); }
    fn watch_structure(&mut self) { (**self).watch_structure(); }
    fn change_structure(&mut self) { (**self).change_structure(); }

    async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError> { (**self).get(map, key).await }

    async fn scan(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        (**self).scan(map, bounds, limit).await
    }

    async fn count_up_to(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<usize, TreeError> {
        (**self).count_up_to(map, bounds, limit).await
    }

    async fn scan_reverse(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError> {
        (**self).scan_reverse(map, bounds, limit).await
    }

    async fn insert(&mut self, map: MapName, key: &[u8], value: &[u8]) -> Result<(), TreeError> { (**self).insert(map, key, value).await }

    async fn put_if(&mut self, map: MapName, key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> Result<(), TreeError> {
        (**self).put_if(map, key, expected, value).await
    }

    async fn delete_if(&mut self, map: MapName, key: &[u8], expected: &[u8]) -> Result<(), TreeError> {
        (**self).delete_if(map, key, expected).await
    }

    async fn append(&mut self, map: MapName, key_prefix: &[u8], value: &[u8]) -> Result<(), TreeError> {
        (**self).append(map, key_prefix, value).await
    }
}
