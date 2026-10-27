//! The tree its owner builds and changes, and the views that read it.
//!
//! A tree lives in its store's rows. `rows` stages an operation's changes on
//! top of them, `trie` reads the trie they hold and lays out new subtrees,
//! `write` creates, builds and changes a tree, and `read` holds the views.

use std::marker::PhantomData;
use std::sync::Arc;

use async_lock::RwLock;

use crate::{DigestGroup, Error, MemberId, Payload, Row, RowKey, Span, Store};

#[cfg(any(test, feature = "test-support"))]
mod audit;
#[cfg(test)]
mod fixtures;
mod read;
mod rows;
mod trie;
mod write;

#[cfg(any(test, feature = "test-support"))]
pub use audit::Audit;
pub use read::{DigestTreeView, Leaf, Members, ScanRange};
pub use write::{StagedCommit, Transaction};

use rows::StagedRows;

/// Which way a read runs over the byte order of what it reads: a store's row
/// keys, or a tree's addresses. Ascending and descending belong to the parts
/// of an owner's key, which its collation encodes into the key's bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanDirection {
    /// Smallest first.
    Forward,

    /// Largest first.
    Reverse,
}

/// The part of a tree a view reads: spans of addresses, or, with a limit,
/// the window of their first or last members.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewPlan {
    /// The spans of addresses the view covers.
    pub spans: Vec<Span>,

    /// Which end of the spans a limit takes its members from: for
    /// [`Forward`](ScanDirection::Forward) the first in address order, for
    /// [`Reverse`](ScanDirection::Reverse) the last. Without a limit it has
    /// no effect. A key's parts may each sort ascending or descending, but
    /// that is the key's own, made by the owner's collation, as a descending
    /// part of an index inverts its bytes.
    pub direction: ScanDirection,

    /// Cover at most this many members, from the end `direction` picks: a
    /// window.
    pub limit: Option<u64>,
}

/// A digest tree, held by its owner, who builds and changes it.
///
/// `K` is the owner's key spec, which the tree hands to its members to make
/// their keys and otherwise never reads; `I` is the member id type, `L` the
/// payload type, `G` the digest group, and `S` the store. A handle is cheap
/// to clone, and clones are the same tree: they share its store and its lock.
/// Open one tree per store and clone it to share it.
pub struct DigestTree<K, I, L, G: DigestGroup, S> {
    inner: Arc<Inner<K, S>>,
    _members: MemberTypes<I, L, G>,
}

/// The member id, payload and group types a tree holds, which only its rows
/// mention. A function returning them keeps the tree `Send` and `Sync`
/// without owning any.
type MemberTypes<I, L, G> = PhantomData<fn() -> (I, L, G)>;

struct Inner<K, S> {
    key_spec: K,
    store: S,

    /// Taken for reading by each read, and for writing by each commit from
    /// before its changes are checked until its rows are written, or, for a
    /// staged commit, until the staged commit is dropped. Shared, so that a
    /// staged commit can hold it apart from any handle.
    lock: Arc<RwLock<()>>,
}

impl<K, I, L, G: DigestGroup, S> Clone for DigestTree<K, I, L, G, S> {
    fn clone(&self) -> Self { Self { inner: Arc::clone(&self.inner), _members: PhantomData } }
}

impl<K, I, L, G, S> DigestTree<K, I, L, G, S>
where
    K: Clone + Send + Sync + 'static,
    I: MemberId,
    L: Payload,
    G: DigestGroup,
    S: Store<RowKey, Row<K, L, G>>,
{
    /// The tree a store already holds, keyed by the spec stored with it:
    /// [`Error::NoTree`] for a store that holds none, and
    /// [`Error::WrongGroup`] for a tree summed in another digest group.
    ///
    /// Open one tree per store and clone it to share it, as
    /// [`create`](Self::create) explains.
    pub async fn open(store: S) -> Result<Self, Error> {
        let meta = StagedRows::new(&store).meta().await?.ok_or(Error::NoTree)?;
        if meta.group != G::ROW_VERSION {
            return Err(Error::WrongGroup);
        }

        Ok(Self::holding(store, meta.key_spec))
    }

    fn holding(store: S, key_spec: K) -> Self {
        Self { inner: Arc::new(Inner { key_spec, store, lock: Arc::new(RwLock::new(())) }), _members: PhantomData }
    }

    /// The spec the tree is keyed by.
    pub fn key_spec(&self) -> &K { &self.inner.key_spec }
}
