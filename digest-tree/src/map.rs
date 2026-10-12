//! The maps that hold a tree's rows, and the transaction over them: three
//! ordered maps of byte keys to byte values, read consistently and written on
//! conditions.
//!
//! A tree keeps its rows in the three maps a [`MapName`] names, and reads and
//! writes them only within a [`MapTransaction`]. The tree encodes every key and
//! value itself ([`layout`](crate::layout)), so the maps hold bytes and nothing
//! else.
//!
//! A transaction is the unit of consistency. Every read within it observes one
//! consistent state of all three maps, the state the transaction began with,
//! together with its own earlier writes, so a reader that reads a tree node's
//! base row and then its delta rows never sees a compaction land between the
//! two. Every write is conditional on what the transaction read, so that
//! nothing depends on the order in which concurrent transactions commit, and a
//! transaction that fails leaves nothing half-applied:
//! [`insert`](MapTransaction::insert) needs the key absent, and
//! [`put_if`](MapTransaction::put_if) and
//! [`delete_if`](MapTransaction::delete_if) need the value the transaction
//! sees there. [`commit`](MapTransaction::commit) applies every write
//! together, and only if every condition still holds against the maps as they
//! stand then. A condition that fails, when its write is made or at the
//! commit, fails the whole transaction with [`TreeError::Conflict`] and
//! applies nothing, and the writer retries from fresh reads in a new
//! transaction. Dropping a transaction without committing it rolls it back. A
//! value that only ever accumulates beside others goes in by
//! [`append`](MapTransaction::append), under a key no other append takes, so
//! that appends never conflict.
//!
//! The memory-resident maps, [`MemoryMaps`](crate::MemoryMaps), implement the
//! trait today, and other storage can implement it later; the trait is async
//! for that storage's sake, and in memory the operations complete at once.

use std::ops::Bound;

use async_trait::async_trait;

use crate::error::TreeError;

/// The bounds of a range scan: a lower and an upper bound on the key, as
/// [`std::ops::RangeBounds`] gives them.
pub type ByteBounds<'a> = (Bound<&'a [u8]>, Bound<&'a [u8]>);

/// The three maps that hold a tree's rows, each an ordered map of byte keys to
/// byte values in the layout [`layout`](crate::layout) gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MapName {
    /// The leaf rows, by leaf address.
    Leaves,
    /// The keys each entity is filed under, by entity id, so that a change,
    /// which names only the entity, finds the leaves it must subtract or
    /// move.
    Lookup,
    /// The tree node rows, by prefix: a base row per tree node and the delta
    /// rows appended since its base was last compacted.
    Nodes,
}

/// One transaction over a tree's three maps.
#[async_trait]
pub trait MapTransaction: Send {
    /// The value stored under `key` in `map`.
    async fn get(&mut self, map: MapName, key: &[u8]) -> Result<Option<Vec<u8>>, TreeError>;

    /// The first `limit` entries of `map` whose keys lie within `bounds`, in
    /// key order. Reading on from the last key returned, a page at a time,
    /// reads them all.
    async fn scan(&mut self, map: MapName, bounds: ByteBounds<'_>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TreeError>;

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

    /// Apply every write the transaction made, together, if every condition
    /// still holds: [`TreeError::Conflict`] otherwise, with nothing applied.
    async fn commit(self) -> Result<(), TreeError>;
}
