//! Digest trees: a set of members kept in key order, with a digest over every
//! subtree, so that two sets can be compared by comparing digests and narrowed
//! down to where they differ.
//!
//! # The model
//!
//! - A **member** is anything the tree is told to hold ([`Member`]): it has an
//!   id ([`MemberId`]), a value for each part of the tree's key spec, and a
//!   [`Payload`] the tree keeps with it and covers in its digest.
//! - A member's **key** is the tree's encoding of its values under the key
//!   spec, a [`KeySpec`](ankurah_core_types::key_spec::KeySpec): each
//!   value checked against its part's type, and the parts sorting in turn,
//!   each in its own direction, with members that hold no value placed by
//!   the part's nulls order. A member's **address** is its key and then its
//!   id, each escaped and ended, so that addresses sort by key and then by id
//!   ([`leaf_address`]). A [`KeyRange`] of values names a span of addresses,
//!   and every read takes either ([`IntoSpan`]).
//! - Every subtree holds the **digest** ([`Digest`]) of the members beneath
//!   it: a sum of one group element per member, and a count. Two trees holding
//!   the same members, with the same keys and payloads, hold the same digests,
//!   so a [`Span`] of addresses can be compared by its digest and subdivided
//!   into smaller spans only where digests differ. A **span digest**
//!   ([`SpanDigest`]) is that same digest with the span's bounds attached.
//!   Whatever gets another tree's pages for one comparison, a connection to
//!   the node that holds it or, in one process, its view, answers one read
//!   ([`PageGetter::get_pages`]): the tree's **page** ([`Page`]) at each span
//!   asked, its span digests one level beneath the span where it has structure
//!   there, its members where it has none. So a view finds the members that
//!   differ from another tree's ([`differences`](DigestTreeView::differences))
//!   by comparing each page's digests with its own and asking for the pages
//!   beneath those that differ, a whole level per request, down to the
//!   members. The other side only reads, and the getter judges how much to
//!   ask of it; the crate compares and descends.
//! - The tree holds what it is told. Its owner decides which members belong,
//!   and tells it in a [`Transaction`]: an insert, or an update or a removal
//!   of the version the tree holds, as an index's upkeep removes an entity's
//!   old key. The tree never evaluates whether something should be a member.
//!
//! # Owning and reading
//!
//! A [`DigestTree`] is created and changed by its owner. A [`DigestTreeView`]
//! reads it: the whole tree, or the part within some spans or key ranges
//! ([`select`](DigestTree::select)). Views are live: each read sees one
//! committed state of the tree, and two reads may see different states. A
//! [range read](DigestTreeView::range) streams a view's members a batch at a
//! time, and each batch sees one state.
//!
//! A tree is right only while it matches its owner's records, such as the
//! entities its members stand for. So its rows land in the same transaction
//! as the record changes they come from, never one without the other: the
//! owner lends the tree its transaction on the store, the tree writes its
//! rows through it, and they land when the owner commits it.
//!
//! # Storage
//!
//! A tree keeps its rows in a [`Store`], an ordered map behind a cheap,
//! cloneable handle that holds exactly one tree. The tree writes through its
//! owner's transaction on the store ([`StoreTrx`]), and reads through reads
//! it begins itself ([`ReadTrx`]); [`Store`] lists what a tree relies on from
//! both. An owner that keeps a tree's rows beside its own records creates the
//! tree with [`create_in`](DigestTree::create_in), and commits each of the
//! tree's transactions by [preparing](Transaction::prepare) it and
//! [applying](Prepared::apply) it in its own transaction; once that
//! transaction commits, it tells the store the rows
//! [landed](Applied::landed). Stores live with whoever needs them.
//!
//! The crate's introduction (`tests/intro.rs`) and its tests keep their trees
//! in `MemoryStore`, behind the `test-support` feature: a store wholly in
//! memory that owns its own transactions, so that a tree on it is created
//! with `DigestTree::create` and its transactions are committed with
//! `Transaction::commit`, each in one call.

#![deny(missing_docs)]

// The unit tests share the top-level tests' fixtures, whose files name the
// crate as they do.
#[cfg(test)]
extern crate self as ankurah_digest_tree;

mod bytes;
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod common;
pub mod digest;
mod error;
#[cfg(test)]
mod fixtures;
mod key;
mod member;
mod span;
mod store;
mod tree;

pub use digest::{Digest, DigestGroup, LtHash16, Ristretto255};
pub use error::{Error, StoreError};
pub use key::{IntoSpan, IntoSpans, KeyRange};
pub use member::{Member, MemberId, Payload};
pub use span::{leaf_address, NodePrefix, Span, SpanDigest};
pub use store::{Encode, ReadTrx, Residency, Row, RowKey, Store, StoreTrx, WriteItem};
#[cfg(any(test, feature = "test-support"))]
pub use store::{Locked, LockedRead, LockedTrx, MemoryMap, MemoryStore, OrderedMap};
pub use tree::{
    Applied, Comparison, ComparisonError, Created, Difference, DigestTree, DigestTreeView, IntoScanRange, Leaf, MemberStream, Page,
    PageGetter, PageMisfit, Prepared, ScanDirection, ScanRange, Transaction, WeakDigestTree,
};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
