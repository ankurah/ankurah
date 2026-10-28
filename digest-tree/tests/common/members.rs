//! The tests' members: numbered items, keyed by their values, and named
//! members, keyed by id alone; the properties they hold; and trees of them in
//! memory. The crate's own unit tests share this file.

use std::borrow::Cow;

use ankurah_core_types::key_spec::IndexKeyPart;
use ankurah_core_types::Value;
use ankurah_digest_tree::{DigestTree, DigestTreeView, Encode, Member, MemberId, MemoryStore, Payload, Ristretto255, Row, RowKey, StoreError};

/// An id: eight bytes, big-endian.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id(pub u64);

impl MemberId for Id {
    fn to_bytes(&self) -> Cow<'_, [u8]> { Cow::Owned(self.0.to_be_bytes().to_vec()) }

    fn from_bytes(bytes: &[u8]) -> Option<Self> { bytes.try_into().ok().map(u64::from_be_bytes).map(Id) }
}

/// A payload: bytes the tree keeps and digests as they are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tag(pub Vec<u8>);

impl Payload for Tag {
    fn digest_bytes(&self) -> Cow<'_, [u8]> { Cow::Borrowed(&self.0) }
}

impl Encode for Tag {
    fn encode(&self) -> Vec<u8> { self.0.clone() }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> { Ok(Tag(bytes.to_vec())) }
}

/// A property a key part names: the tests' stand-in for core's `PropertyId`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Property {
    /// The year an album came out.
    Year,

    /// An item's first value.
    A,

    /// An item's second value.
    B,
}

/// A property's encoding: its one byte.
impl Encode for Property {
    fn encode(&self) -> Vec<u8> {
        vec![match self {
            Property::Year => 0,
            Property::A => 1,
            Property::B => 2,
        }]
    }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        match bytes {
            [0] => Ok(Property::Year),
            [1] => Ok(Property::A),
            [2] => Ok(Property::B),
            _ => Err(StoreError::permanent("bytes that are no property")),
        }
    }
}

/// An ascending key part, as the planner's tests write one:
/// `asc!(Property::A, ValueType::I64)`.
macro_rules! asc {
    ($property:expr, $value_type:expr) => {
        ::ankurah_core_types::key_spec::IndexKeyPart::asc($property, $value_type)
    };
}

/// A descending key part: `desc!(Property::A, ValueType::I64)`.
macro_rules! desc {
    ($property:expr, $value_type:expr) => {
        ::ankurah_core_types::key_spec::IndexKeyPart::desc($property, $value_type)
    };
}

pub(crate) use {asc, desc};

/// A property a member doesn't have, which a key part named.
#[derive(Debug, thiserror::Error)]
#[error("no property {0:?}")]
pub struct NoProperty(pub Property);

/// A member: an id, values for properties `A` and then `B`, `None` where it
/// has none, and a payload. Its values are `I64`s.
#[derive(Clone, Debug)]
pub struct Item {
    pub id: Id,
    pub values: Vec<Option<i64>>,
    pub payload: Tag,
}

impl Item {
    pub fn new(id: u64, values: impl IntoIterator<Item = Option<i64>>, payload: &[u8]) -> Self {
        Self { id: Id(id), values: values.into_iter().collect(), payload: Tag(payload.to_vec()) }
    }
}

/// An item holds `A` and `B` as far as it has values for them, and no year.
impl Member<Property> for Item {
    type Id = Id;
    type Payload = Tag;
    type KeyError = NoProperty;

    fn id(&self) -> Id { self.id }

    fn value(&self, part: &IndexKeyPart<Property>) -> Result<Option<Value>, NoProperty> {
        let held = match part.key {
            Property::A => self.values.first(),
            Property::B => self.values.get(1),
            Property::Year => None,
        };

        held.map(|value| value.map(Value::I64)).ok_or(NoProperty(part.key))
    }

    fn payload(&self) -> Tag { self.payload.clone() }
}

/// An item with one value, or several in brackets, each an `i64` or `None`
/// or `Some` of one: `item!(1, 5, b"a")`, `item!(2, [Some(2), None], b"b")`.
macro_rules! item {
    ($id:expr, [$($value:expr),* $(,)?], $payload:expr) => {
        $crate::common::Item::new($id, [$(::std::option::Option::<i64>::from($value)),*], $payload)
    };

    ($id:expr, $value:expr, $payload:expr) => {
        $crate::common::Item::new($id, [::std::option::Option::<i64>::from($value)], $payload)
    };
}

pub(crate) use item;

/// A member id of any length: its bytes as they are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Name(pub Vec<u8>);

impl MemberId for Name {
    fn to_bytes(&self) -> Cow<'_, [u8]> { Cow::Borrowed(&self.0) }

    fn from_bytes(bytes: &[u8]) -> Option<Self> { Some(Name(bytes.to_vec())) }
}

/// A member known by a name, with a payload and no properties: for trees
/// keyed by id alone.
#[derive(Clone, Debug)]
pub struct Named(pub Vec<u8>, pub Tag);

impl Member<Property> for Named {
    type Id = Name;
    type Payload = Tag;
    type KeyError = NoProperty;

    fn id(&self) -> Name { Name(self.0.clone()) }

    fn value(&self, part: &IndexKeyPart<Property>) -> Result<Option<Value>, NoProperty> { Err(NoProperty(part.key)) }

    fn payload(&self) -> Tag { self.1.clone() }
}

/// The rows of a tree of items, in memory.
pub type ItemStore<G = Ristretto255> = MemoryStore<RowKey, Row<Property, Tag, G>>;

/// A tree of items, in memory.
pub type ItemTree<G = Ristretto255> = DigestTree<Property, Id, Tag, G, ItemStore<G>>;

/// A view of a tree of items.
pub type ItemView<G = Ristretto255> = DigestTreeView<Property, Id, Tag, G, ItemStore<G>>;

/// A tree of named members, in memory.
pub type NamedTree = DigestTree<Property, Name, Tag, Ristretto255, ItemStore>;

/// A tree in memory under a key spec, holding exactly these members:
/// `tree_of!(KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), [item!(1, 5, b"a")])`.
/// Its type names its digest group.
macro_rules! tree_of {
    ($key_spec:expr, $members:expr) => {
        ::ankurah_digest_tree::DigestTree::build(::ankurah_digest_tree::MemoryStore::new(), $key_spec, $members).await.unwrap()
    };
}

pub(crate) use tree_of;
