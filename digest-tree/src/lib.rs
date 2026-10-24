//! Digest trees: comparing two sets of leaves by comparing sums, and
//! descending only where the sums differ.
//!
//! A leaf is one entity filed under one key, with the hash of its head. A
//! digest tree sums its leaves' elements, each a cryptographic hash of one
//! leaf into a group from the [`digest`] module, so that two trees over the
//! same leaves hold the same sums, and trees whose sums differ hold different
//! leaves. Each leaf is filed at an address, its canonical key
//! ([`CanonicalCodec`]) followed by its entity id, and the tree groups the
//! leaves by the leading bits of their addresses: a tree node, named by a
//! [`NodePrefix`], holds the digest of every leaf beneath it. Two peers
//! compare a span of addresses by its digest ([`TreeSpanDigest`]), refine a
//! span whose digests differ into the spans of the tree nodes beneath it, and
//! exchange leaves only where the narrowest spans still differ.
//!
//! A [`DigestTree`] is defined by its [`TreeSpec`]: the predicate that admits
//! its leaves and the key that orders them. The request a Selection makes
//! ([`TreeRequest`]) reads any tree whose spec [serves](TreeSpec::serves) it,
//! as spans of that tree's leaves, so compatible Selections share one tree.
//! Callers evaluate admission against their entity state and supply
//! canonical keys and head hashes; the crate depends on no Ankurah node or
//! storage engine.
//!
//! A tree's rows live in three ordered byte maps ([`MapName`]), in memory
//! ([`MemoryMaps`]) or in any store that implements [`MapAccess`]. Its
//! operations run in transactions: [`DigestTree::begin`] gives a
//! [`TreeTransaction`], whose reads observe one consistent state of the maps
//! and its own writes, and whose writes, each conditional on what it read,
//! land together when it commits, or not at all, with [`TreeError::Conflict`],
//! after which the writer retries from fresh reads. A change applied alone
//! appends a delta row beside each tree node's base row it changes, so
//! changes running at once never conflict on a base row, and the writer folds
//! the deltas into the bases with [`TreeTransaction::compact_node`]. A batch
//! of changes ([`TreeTransaction::apply_changes`]), the path for a tree's only
//! writer, writes each base row once and leaves no delta behind. Every stored
//! element and digest carries its group's codec version byte, so a row
//! written in another group does not decode, and deltas without a base report
//! [`TreeError::IncompleteNode`] rather than stand for a sum.
//!
//! # Modules
//!
//! - `address`: where a tree files each leaf, and how runs of leaves are
//!   named: the [`CanonicalCodec`], the [leaf address](leaf_address), the
//!   [`NodePrefix`], and the [`TreeSpan`] with its [`TreeSpanDigest`].
//! - `selection`: whether and how a tree serves a Selection: the
//!   [`TreeSpec`], the [`TreeRequest`], the proof that a tree serves a
//!   request, and the spans a request's limit selects.
//! - `store`: the bytes a tree keeps and the transactions over them:
//!   [`MapAccess`], [`MapTransaction`], the [`MemoryMaps`], and the layout of
//!   every key and value.
//! - `tree`: the [`DigestTree`] and the [`TreeTransaction`] that carries its
//!   operations: leaf rows, tree node rows, changes, builds, audits and
//!   scoped reads.
//! - [`digest`]: the cryptography: the [`Leaf`], the [`DigestGroup`] a tree
//!   sums leaves in, its two groups, and the [`Digest`].
//! - `error`: the [`TreeError`].

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
pub use store::{ByteBounds, Failure, LeafRow, MapAccess, MapName, MapTransaction, MemoryMaps, MemoryTransaction};
pub use tree::{AuditCounts, DigestTree, LeafChange, LeafCursor, LeafInput, LeafPage, TreeScope, TreeTransaction, BUCKET_CAPACITY};
