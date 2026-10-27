//! Albums, the introduction's sample members, keyed by the year each came out.

use std::borrow::Cow;

use ankurah_digest_tree::{DigestGroup, DigestTree, DigestTreeView, Member, MemoryStore, Payload, Row, RowKey};

use super::members::{asc, Id, KeySpec, NoValueFor};

/// An album's title: the payload an album tree keeps and digests, so
/// retitling an album changes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Title(pub String);

impl Payload for Title {
    fn digest_bytes(&self) -> Cow<'_, [u8]> { Cow::Borrowed(self.0.as_bytes()) }
}

/// A title compares equal to its text, so a test can name titles plainly.
impl PartialEq<&str> for Title {
    fn eq(&self, text: &&str) -> bool { self.0 == *text }
}

/// An album, the sample member of the top-level tests, keyed by the year it
/// came out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Album {
    pub id: Id,
    pub year: i64,
    pub title: Title,
}

/// An album's key is the year it came out, its one value, collated by the
/// spec; a spec of more parts asks for values an album lacks.
impl Member<KeySpec> for Album {
    type Id = Id;
    type Payload = Title;
    type KeyError = NoValueFor;

    fn id(&self) -> Id { self.id }

    fn key(&self, spec: &KeySpec) -> Result<Vec<u8>, NoValueFor> {
        match spec.keyparts.len() {
            0 | 1 => Ok(spec.collate([self.year])),
            _ => Err(NoValueFor(1)),
        }
    }

    fn payload(&self) -> Title { self.title.clone() }
}

/// Album `id`, out in `year`, titled `title`: `album!(3, 2013, "Ice on the
/// Dune")`.
macro_rules! album {
    ($id:expr, $year:expr, $title:expr) => {
        $crate::common::Album { id: $crate::common::Id($id), year: $year, title: $crate::common::Title(($title).into()) }
    };
}

pub(crate) use album;

/// A tree of albums, in memory.
pub type Tree<G> = DigestTree<KeySpec, Id, Title, G, MemoryStore<RowKey, Row<KeySpec, Title, G>>>;

/// A view of a tree of albums.
pub type View<G> = DigestTreeView<KeySpec, Id, Title, G, MemoryStore<RowKey, Row<KeySpec, Title, G>>>;

/// A tree of albums in memory, keyed by the year each came out, holding
/// exactly `albums`.
pub async fn tree<G: DigestGroup>(albums: impl IntoIterator<Item = Album>) -> Tree<G> {
    DigestTree::build(MemoryStore::new(), KeySpec::new(vec![asc!("year")]), albums).await.unwrap()
}
