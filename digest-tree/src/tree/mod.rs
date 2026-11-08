//! The tree its owner creates and changes, and the views that read it.
//!
//! A tree lives in its store's rows. `rows` keeps an operation's changes on
//! top of them, `trie` reads the trie they hold and lays out new subtrees,
//! `write` creates a tree and commits changes to it, and `read` holds the
//! views.

use std::marker::PhantomData;
use std::sync::Arc;

use ankurah_core_types::key_spec::KeySpec;

use crate::key::check_spec;
use crate::{DigestGroup, Error, MemberId, Payload, Row, RowKey, Store};

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
pub use read::{ComparisonError, Difference, DigestTreeView, IntoScanRange, Leaf, MemberStream, Page, PageGetter, PageMisfit, ScanRange};
pub use write::{Applied, Created, Prepared, Transaction};

use rows::TreeRows;

/// Which way a read runs over the byte order of what it reads: a store's row
/// keys, or a tree's addresses.
pub use ankurah_core_types::ScanDirection;

/// A digest tree, held by its owner, who creates and changes it.
///
/// `K` is the type of the properties its key spec's parts name, from which
/// the tree makes each member's key; `I` is the member id type, `L` the
/// payload type, `G` the digest group, and `S` the store. A handle is cheap
/// to clone, and clones are the same tree. The tree holds no lock of its own:
/// its store keeps its commits and reads apart ([`Store`]).
pub struct DigestTree<K, I, L, G: DigestGroup, S> {
    inner: Arc<Inner<K, S>>,
    _members: MemberTypes<I, L, G>,
}

/// The member id, payload and group types a tree holds, which only its rows
/// mention. A function returning them keeps the tree `Send` and `Sync`
/// without owning any.
type MemberTypes<I, L, G> = PhantomData<fn() -> (I, L, G)>;

struct Inner<K, S> {
    key_spec: KeySpec<K>,
    store: S,
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
    /// [`Error::NoTree`] for a store that holds none, [`Error::WrongGroup`]
    /// for a tree summed in another digest group, and [`Error::Collation`]
    /// for a spec that names a collation.
    pub async fn open(store: S) -> Result<Self, Error> {
        let meta = TreeRows::new(store.begin_read().await?).meta().await?.ok_or(Error::NoTree)?;
        if meta.group != G::ROW_VERSION {
            return Err(Error::WrongGroup);
        }

        check_spec(&meta.key_spec)?;
        Ok(Self::holding(store, meta.key_spec))
    }

    /// One read of the tree's rows, which sees one committed state of them.
    async fn read(&self) -> Result<TreeRows<K, L, G, S::Read>, Error> { Ok(TreeRows::new(self.inner.store.begin_read().await?)) }

    fn holding(store: S, key_spec: KeySpec<K>) -> Self { Self { inner: Arc::new(Inner { key_spec, store }), _members: PhantomData } }

    /// The store the tree keeps its rows in.
    pub fn store(&self) -> &S { &self.inner.store }

    /// The spec the tree is keyed by.
    pub fn key_spec(&self) -> &KeySpec<K> { &self.inner.key_spec }
}
