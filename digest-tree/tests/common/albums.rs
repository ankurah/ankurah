//! Albums, the introduction's sample members, keyed by the year each came out.

use std::borrow::Cow;

use ankurah_core_types::key_spec::IndexKeyPart;
use ankurah_core_types::Value;
use ankurah_digest_tree::{DigestTree, DigestTreeView, Member, MemoryStore, Payload, Row, RowKey};

use super::members::{Id, NoProperty, Property};

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

/// An album, the sample member of the top-level tests: an id, the year it
/// came out, and its title.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Album {
    pub id: Id,
    pub year: i32,
    pub title: Title,
}

/// An album holds its year, an `I32`, and no other property.
impl Member<Property> for Album {
    type Id = Id;
    type Payload = Title;
    type KeyError = NoProperty;

    fn id(&self) -> Id { self.id }

    fn value(&self, part: &IndexKeyPart<Property>) -> Result<Option<Value>, NoProperty> {
        match part.key {
            Property::Year => Ok(Some(Value::I32(self.year))),
            other => Err(NoProperty(other)),
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
pub type Tree<G> = DigestTree<Property, Id, Title, G, MemoryStore<RowKey, Row<Property, Title, G>>>;

/// A view of a tree of albums.
pub type View<G> = DigestTreeView<Property, Id, Title, G, MemoryStore<RowKey, Row<Property, Title, G>>>;
