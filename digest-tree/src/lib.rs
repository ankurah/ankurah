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
//! A tree node's base row is written when the tree is built; every later
//! change to the tree node is a delta row appended beside it under a key of
//! its own, which readers sum and compaction folds into the base from time to
//! time, so no change rewrites a hot tree node's row and no two changes
//! conflict on one. Every stored element and digest carries its group's codec
//! version byte, so a row written in another group does not decode.
//!
//! The tree reads no entity state, evaluates no predicate and encodes no key:
//! it receives addresses and head hashes and keeps the rows they go into. A
//! Selection compares a scope over its tree: a head array of tree node
//! prefixes, with key bounds where the span cuts a tree node.
//!
//! # Modules
//!
//! - [`tree`]: the [`DigestTree`] and the [`TreeTransaction`] that carries
//!   the tree's operations.
//! - [`spec`]: the [`TreeSpec`], what defines a tree.
//! - [`digest`]: the [`Leaf`], the [`DigestGroup`] a tree sums leaves in, its
//!   two groups, and the [`Digest`].
//! - [`prefix`]: the [`NodePrefix`], the name of a tree node.
//! - [`rows`]: the rows a tree keeps, the [`LeafRow`] per leaf and the
//!   [`NodeRow`] per tree node, with the [leaf address](leaf_address) they are
//!   filed under.
//! - [`map`]: the [`MapTransaction`], a transaction over the three maps a
//!   [`MapName`] names, with its consistent reads and conditional writes.
//! - [`layout`]: the byte layout of every key and value in the maps.
//! - [`memory`]: the [`MemoryMaps`] that hold a tree in memory, and their
//!   [`MemoryTransaction`].
//! - [`error`]: the [`TreeError`].
//!
//! Later tasks add applying a leaf change (`change`), the build from a set of
//! leaves and the audit that recomputes a tree from its rows (`build`,
//! `audit`), the scope a Selection compares, a head array over byte bounds
//! (`scope`), and the service that holds trees by spec (`service`).

#![deny(missing_docs)]

pub mod digest;
pub mod error;
pub mod layout;
pub mod map;
pub mod memory;
pub mod prefix;
pub mod rows;
pub mod spec;
pub mod tree;

pub use digest::{Digest, DigestGroup, HeadHash, Leaf, LeafElement, LtHash16, Ristretto255};
pub use error::TreeError;
pub use map::{ByteBounds, MapName, MapTransaction};
pub use memory::{MemoryMaps, MemoryTransaction};
pub use prefix::{NodePrefix, NodePrefixError};
pub use rows::{leaf_address, AddressRange, LeafRow, NodeRow};
pub use spec::TreeSpec;
pub use tree::{DigestTree, TreeTransaction};
