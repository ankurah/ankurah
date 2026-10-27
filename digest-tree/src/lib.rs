//! Digest trees: a set of members kept in key order, with a digest over every
//! subtree, so that two sets can be compared by comparing digests and narrowed
//! down to where they differ.
//!
//! # The model
//!
//! - A **member** is anything the tree is told to hold ([`Member`]): it has an
//!   id ([`MemberId`]), a key it makes under the tree's key spec, and a
//!   [`Payload`] the tree keeps with it and covers in its digest.
//! - A member's **key** is bytes in its owner's collation: they sort as the
//!   owner orders its members, any mix of directions across the key's parts
//!   included. The tree never reads the key spec or a key's parts; it only
//!   orders keys. A member's **address** is its key and then its id, each
//!   escaped and ended, so that addresses sort by key and then by id
//!   ([`leaf_address`]).
//! - Every subtree holds the **digest** ([`Digest`]) of the members beneath
//!   it: a sum of one group element per member, and a count. Two trees holding
//!   the same members, with the same keys and payloads, hold the same digests,
//!   so a [`Span`] of addresses can be compared by its digest ([`SpanDigest`])
//!   and refined into smaller spans only where digests differ.
//! - The tree holds what it is told. Its owner decides which members belong,
//!   and tells it in a [`Transaction`]: an insert, or an update or a removal
//!   of the version the tree holds, as an index's upkeep removes an entity's
//!   old key. The tree never evaluates whether something should be a member.
//!
//! # Owning and reading
//!
//! A [`DigestTree`] is built and changed by its owner. A [`DigestTreeView`]
//! reads it: the whole tree, or the part a [`ViewPlan`] selects. Views are
//! live: each read sees a complete state of the tree, and two reads may see
//! different states. A scan of a view's [members](DigestTreeView::members)
//! reads a batch at a time, and each batch sees one state.
//!
//! A tree is right only while it matches its owner's records, such as the
//! entities its members stand for. The owner saves each transaction's changes
//! and the record changes they come from so that the two are guaranteed to
//! match, never one without the other; how it does that is up to the owner
//! and its store.
//!
//! # Storage
//!
//! A tree keeps its rows in a [`Store`], an ordered map behind a cheap,
//! cloneable handle that holds exactly one tree. The crate provides two
//! reference implementations: [`MemoryStore`], and, with the `sled` feature,
//! `SledStore`, which keeps some rows in memory and the rest on disk. Other
//! stores live with whoever needs them. An owner that saves a tree's changes
//! in its own storage transaction, so that they commit with its records,
//! [stages](Transaction::stage) each of the tree's transactions and writes the
//! staged rows there. [`Store`] lists what a tree relies on from its store.

#![deny(missing_docs)]

mod bytes;
pub mod digest;
mod error;
#[cfg(test)]
mod fixtures;
mod member;
mod span;
mod store;
mod tree;

pub use digest::{Digest, DigestGroup, LtHash16, Ristretto255};
pub use error::{Error, StoreError};
pub use member::{Member, MemberId, Payload};
pub use span::{leaf_address, NodePrefix, Span, SpanDigest};
#[cfg(feature = "sled")]
pub use store::SledStore;
pub use store::{Encode, MemoryStore, Residency, Row, RowKey, Store, WriteItem};
pub use tree::{DigestTree, DigestTreeView, Leaf, Members, ScanDirection, StagedCommit, Transaction, ViewPlan};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
