//! Digest trees for Ankurah: one tree over any set of leaves.
//!
//! A digest tree summarizes a set of leaves so that two trees over the same
//! leaf set can find the leaves they hold differently by comparing digests
//! instead of leaves. Each leaf is filed at an address, its canonical key
//! followed by its entity id, and the tree groups the leaves by the leading
//! bits of their addresses: a tree node, named by a prefix of those bits,
//! holds the digest of every leaf beneath it, so equal digests let a
//! comparison skip the tree node's whole subtree. The leaf set may be the
//! entries of a query index, which gives the index tree, or the leaves a
//! Selection admits, which give a tree of its own: a tree whose spec carries a
//! predicate, derived from the Selection alone. The tree is the same either
//! way.
//!
//! A [`DigestTree`] is the spec that defines it ([`TreeSpec`]) and the
//! memory-resident maps that hold its rows ([`MemoryMaps`]), with its leaves
//! summed in a group from the [`digest`] module, its type parameter:
//! ristretto255 unless the tree names another, such as LtHash16. Its
//! operations run in transactions: [`DigestTree::begin`] gives a
//! [`TreeTransaction`], whose reads observe one consistent state of the maps
//! and its own writes, and whose writes, each conditional on what it read,
//! land together when it commits, or not at all, with [`TreeError::Conflict`],
//! after which the writer retries from fresh reads.
//!
//! A tree node's base row is written when the tree is built or restructured;
//! ordinary changes append a delta row beside it under a key of
//! its own, which readers sum and compaction folds into the base from time to
//! time, so no change rewrites a hot tree node's row and no two changes
//! conflict on one. A structural change retries after any intervening write,
//! and writers using the previous topology retry after it changes.
//! Every stored element and digest carries its group's codec
//! version byte, so a row written in another group does not decode.
//! Deltas without a base cannot establish a subtree's total: reads and
//! compaction report [`TreeError::IncompleteNode`] instead of assuming zero.
//!
//! Callers evaluate admission against their entity state and supply canonical
//! keys and head hashes. [`CanonicalCodec`] encodes those keys; [`TreeRequest`]
//! and [`TreeSpec::serves`] prove supported predicate implications. A
//! Selection compares a scope over its tree using [`TreeSpanDigest`] values:
//! each carries an exact [`TreeSpan`], its digest and a prefix for refinement.
//!
//! # Modules
//!
//! - `tree`: the [`DigestTree`] and the [`TreeTransaction`] that carries the
//!   tree's operations: leaf rows, tree node rows, changes, builds, audits and
//!   scoped reads.
//! - `selection`: the [`TreeSpec`], what defines a tree, the [`TreeRequest`] a
//!   Selection makes of one, and whether and how a tree serves it.
//! - [`digest`]: the [`Leaf`], the [`DigestGroup`] a tree sums leaves in, its
//!   two groups, and the [`Digest`].
//! - `address`: the [`CanonicalCodec`], the [leaf address](leaf_address), the
//!   [`NodePrefix`] that names a tree node and the [`TreeSpan`] that names a
//!   run of leaves.
//! - `store`: [`MapAccess`] to the three maps a [`MapName`] names, the byte
//!   layout of their rows, and the [`MemoryMaps`] that hold a tree in memory.
//! - `error`: the [`TreeError`].
//!
//! [`TreeTransaction::apply_change`] maintains leaves, entity lookups and
//! ancestor summaries together, including bucket splits and merges. Build and
//! audit operate on admitted leaves. Selection scopes resolve exact bounds and
//! limits within one snapshot, and expose counts, digests, clipped span digests and
//! paginated leaves. Direct address scopes also support multi-key leaf sets.
//! This crate has no Ankurah node or storage-engine dependency.

#![deny(missing_docs)]

mod address;
pub mod digest;
mod error;
mod selection;
mod store;
mod tree;

pub use address::{leaf_address, CanonicalCodec, NodePrefix, NodePrefixError, TreeSpan, TreeSpanDigest};
pub use digest::{Digest, DigestGroup, HeadHash, Leaf, LeafElement, LtHash16, Ristretto255};
pub use error::TreeError;
pub use selection::{PropertyTypes, TreeMatch, TreeRequest, TreeSpec};
pub use store::{ByteBounds, LeafRow, MapAccess, MapName, MapTransaction, MemoryMaps, MemoryTransaction, NodeRow};
pub use tree::{AuditCounts, DigestTree, LeafChange, LeafCursor, LeafInput, LeafPage, TreeScope, TreeTransaction, BUCKET_CAPACITY};
