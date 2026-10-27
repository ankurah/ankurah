//! Albums, the primer's sample members, keyed by the year each came out.

use std::borrow::Cow;

use ankurah_digest_tree::{DigestGroup, DigestTree, DigestTreeView, Member, MemoryStore, Payload, Row, RowKey};

use super::members::{key_of, key_part, spec, Id, NoValueFor, Order, Spec};

/// An album's title: the payload an album tree keeps and digests, so
/// retitling an album changes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Title(pub String);

impl Payload for Title {
    fn digest_bytes(&self) -> Cow<'_, [u8]> { Cow::Borrowed(self.0.as_bytes()) }
}

/// An album, the sample member of the top-level tests, keyed by the year it
/// came out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Album {
    id: Id,
    year: i64,
    title: Title,
}

/// An album's key is made of its year, its one value.
impl Member<Spec> for Album {
    type Id = Id;
    type Payload = Title;
    type KeyError = NoValueFor;

    fn id(&self) -> Id { self.id }

    fn key(&self, spec: &Spec) -> Result<Vec<u8>, NoValueFor> { key_of(&[Some(self.year)], spec) }

    fn payload(&self) -> Title { self.title.clone() }
}

/// Album `id`, out in `year`, titled `title`.
pub fn album(id: u64, year: i64, title: &str) -> Album { Album { id: Id(id), year, title: Title(title.to_string()) } }

/// The spec album trees are keyed by: the year each album came out,
/// ascending.
pub fn key_spec() -> Spec { spec(&[Order::Ascending]) }

/// The key of the albums that came out in `year`, as a bound of a key range.
pub fn year(year: i64) -> Vec<u8> { key_part(Some(year), Order::Ascending) }

/// A tree of albums, in memory.
pub type Tree<G> = DigestTree<Spec, Id, Title, G, MemoryStore<RowKey, Row<Spec, Title, G>>>;

/// A view of a tree of albums.
pub type View<G> = DigestTreeView<Spec, Id, Title, G, MemoryStore<RowKey, Row<Spec, Title, G>>>;

/// A tree of albums in memory, holding exactly `albums`.
pub async fn tree<G: DigestGroup>(albums: impl IntoIterator<Item = Album>) -> Tree<G> {
    DigestTree::build(MemoryStore::new(), key_spec(), albums).await.unwrap()
}
