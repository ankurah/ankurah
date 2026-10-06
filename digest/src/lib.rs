//! Additive digests for Ankurah's digest trees.
//!
//! Two members find out which entities they hold differently by comparing
//! digests instead of entities: one digest stands for every leaf beneath a node
//! of a digest tree, so equal digests let a session skip that node's whole
//! subtree. This crate holds the cryptography behind that comparison. The core
//! builds the leaves and sums the points this crate produces; no storage engine
//! depends on this crate, and no engine code performs curve operations.
//!
//! # Construction
//!
//! - A [`Leaf`] is one entity filed under one key of one index: the canonical
//!   index key bytes (empty for the entity-id index), the entity id, and the
//!   [`HeadHash`] of the entity's head.
//! - [`Leaf::point`] hashes the leaf's canonical encoding ([`Leaf::encode`]) to
//!   an element of ristretto255 (RFC 9496) with `hash_to_ristretto255` from
//!   RFC 9380, under the domain separation tag [`LEAF_DST`].
//! - A [`Digest`] is the sum of the points of a multiset of leaves together
//!   with the number of leaves summed. Adding a leaf to a node adds its point,
//!   removing one subtracts it, and a commit that changes an entity's head
//!   subtracts the old leaf's point and adds the new one. Addition commutes and
//!   associates, so a set of leaves has one digest whatever order the leaves
//!   arrived in and however a tree groups them.
//!
//! # Security
//!
//! Equal digests stand for equal sets of leaves because producing two different
//! sets of leaves with the same sum is as hard as computing a discrete
//! logarithm in ristretto255. That is Theorem 1 of Maitin-Shepard, Tibouchi and
//! Aranha, "Elliptic Curve Multiset Hash" (2017), under its assumptions: the
//! hash into the group is a random oracle over an efficiently samplable
//! encoding, the model RFC 9380's `hash_to_ristretto255` is built for; the
//! group has prime order, as ristretto255 does; and every multiplicity in the
//! difference of the two multisets is below the group order. The order is about
//! 2^252, so generic attacks need about 2^126 group operations: roughly the
//! 128-bit classical security level, under those assumptions.
//!
//! The guarantee is for sets. A multiset may repeat a leaf, and adding one leaf
//! as many times as the group order cancels it, so sums over unrestricted
//! multisets are not collision resistant. Leaves are unique by construction: a
//! leaf names its key and its entity, and a tree holds one leaf per distinct
//! key and entity. The code that folds commits into a tree must enforce that
//! uniqueness; with it, set-collision resistance is all a comparison needs. An
//! entity filed under two keys is two distinct leaves with distinct points, not
//! two copies of one leaf.
//!
//! # Encodings
//!
//! The wire, and anything signed or fingerprinted, carries a point only in its
//! canonical 32-byte encoding (RFC 9496 section 4.3.2): see
//! [`Digest::to_wire_bytes`]. Stored rows carry the same encoding behind a
//! codec version byte: see [`Digest::to_row_bytes`] and
//! [`LeafPoint::to_row_bytes`].
//!
//! The other candidate for rows was the curve library's internal coordinates,
//! copied as they lie in memory. With them a node update costs what adding a
//! difference in memory costs, about 70 ns in this crate's benchmark, where the
//! canonical encoding costs about 4.9 µs: 2.4 µs to decompress, the addition,
//! and 2.4 µs to compress. But curve25519-dalek keeps those coordinates private
//! and lays them out differently on 32-bit and 64-bit targets, so storing them
//! would mean unsafe copies of a private layout that any release of the library
//! may change, and a damaged row could be caught only by an audit. The
//! canonical encoding is portable across targets and library versions, is
//! refused on read when damaged, since decompression accepts only canonical
//! encodings of group elements, and keeps a digest row at 41 bytes rather than
//! 169.
//!
//! The cost falls on the refresher, off the commit path: one decompress and
//! one compress per node a folded batch touches, and the same pair for a
//! leaf point kept in a row, which makes keeping one cost about what hashing
//! the old leaf again would (5.5 µs with its head hash). If the refresher's
//! measurements call for a faster layout, it takes a new version byte; trees
//! are derived state, so a build that meets a row version it cannot read
//! rebuilds that tree from its leaves.

#![deny(missing_docs)]

mod codec;
mod hash_to_curve;
mod leaf;
mod point;

pub use codec::{DecodeError, DIGEST_ROW_LEN, DIGEST_WIRE_LEN, LEAF_POINT_ROW_LEN};
pub use leaf::{HeadHash, Leaf, HEAD_TAG, LEAF_DST};
pub use point::{Digest, LeafPoint};
