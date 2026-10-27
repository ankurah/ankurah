//! The tests' members: numbered items and named members, keyed by the tests'
//! stand-in for an owner's key spec and collation, and trees of them in
//! memory.

use std::borrow::Cow;
use std::convert::Infallible;

use ankurah_digest_tree::{
    DigestGroup, DigestTree, Encode, Member, MemberId, MemoryStore, Payload, Ristretto255, Row, RowKey, ScanRange, Span, Store, StoreError,
    Transaction,
};
use futures::TryStreamExt;

/// An id: eight bytes, big-endian.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

/// The direction of one key part's values: the tests' stand-in for core's
/// `IndexDirection`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexDirection {
    /// Smallest first.
    Asc,

    /// Largest first.
    Desc,
}

/// One part of a key spec: the tests' stand-in for core's `IndexKeyPart`, a
/// name and a direction. The stand-in collation takes integers alone, so a
/// part names no value type, and its name only says what the part holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexKeyPart {
    pub key: String,
    pub direction: IndexDirection,
}

impl IndexKeyPart {
    pub fn asc(key: impl Into<String>) -> Self { Self { key: key.into(), direction: IndexDirection::Asc } }

    pub fn desc(key: impl Into<String>) -> Self { Self { key: key.into(), direction: IndexDirection::Desc } }
}

/// An ascending key part, as the planner's tests write one: `asc!("year")`.
macro_rules! asc {
    ($key:expr) => {
        $crate::common::IndexKeyPart::asc($key)
    };
}

/// A descending key part: `desc!("year")`.
macro_rules! desc {
    ($key:expr) => {
        $crate::common::IndexKeyPart::desc($key)
    };
}

pub(crate) use {asc, desc};

/// The tests' stand-in for an owner's key spec, core's `KeySpec`: its parts,
/// the most significant first. Members make their keys with it; the tree
/// only stores it and hands it to them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeySpec {
    pub keyparts: Vec<IndexKeyPart>,
}

impl KeySpec {
    pub fn new(keyparts: Vec<IndexKeyPart>) -> Self { Self { keyparts } }

    /// The bytes of `values` under the spec, the tests' stand-in for core's
    /// `encode_tuple_values_with_key_spec`: each value collated in its
    /// part's direction, the first part's first, and `None` a missing value.
    /// Fewer values than parts give a key's leading parts, as a range's bound
    /// takes them.
    ///
    /// A present value is `0x01` and its big-endian bytes with the sign bit
    /// flipped, so values sort as numbers do; a missing value is `0x02`,
    /// after every present one, as PostgreSQL sorts NULL; and a descending
    /// part's bytes are inverted, so that it sorts the other way, missing
    /// values first.
    pub fn collate(&self, values: impl IntoIterator<Item = impl Into<Option<i64>>>) -> Vec<u8> {
        self.keyparts.iter().zip(values).flat_map(|(part, value)| collate(value.into(), part.direction)).collect()
    }
}

/// One value's bytes in `direction`, as [`KeySpec::collate`] describes them.
fn collate(value: Option<i64>, direction: IndexDirection) -> Vec<u8> {
    let mut bytes = match value {
        Some(value) => [&[0x01][..], &(value as u64 ^ 1 << 63).to_be_bytes()].concat(),
        None => vec![0x02],
    };

    if direction == IndexDirection::Desc {
        bytes.iter_mut().for_each(|byte| *byte = !*byte);
    }

    bytes
}

/// A spec's encoding: for each part, its direction's byte, `0` ascending and
/// `1` descending, then its name's length in a byte and the name.
impl Encode for KeySpec {
    fn encode(&self) -> Vec<u8> {
        let part = |part: &IndexKeyPart| {
            let length = u8::try_from(part.key.len()).expect("a key part's name is under 256 bytes");
            [&[u8::from(part.direction == IndexDirection::Desc), length][..], part.key.as_bytes()].concat()
        };

        self.keyparts.iter().flat_map(part).collect()
    }

    fn decode(mut bytes: &[u8]) -> Result<Self, StoreError> {
        let mut keyparts = Vec::new();
        while let [direction, length, rest @ ..] = bytes {
            let direction = match direction {
                0 => IndexDirection::Asc,
                1 => IndexDirection::Desc,
                _ => return Err(StoreError::permanent("a key part's direction is 0 or 1")),
            };

            let (key, after) =
                rest.split_at_checked(usize::from(*length)).ok_or(StoreError::permanent("a key part's name runs past the spec"))?;

            let key = String::from_utf8(key.to_vec()).map_err(|_| StoreError::permanent("a key part's name is not UTF-8"))?;

            keyparts.push(IndexKeyPart { key, direction });
            bytes = after;
        }

        match bytes {
            [] => Ok(KeySpec { keyparts }),
            _ => Err(StoreError::permanent("a key part is cut short")),
        }
    }
}

/// A key part a member gives no value for, not even a missing one: the
/// tests' stand-in for a value its owner can't encode.
#[derive(Debug, thiserror::Error)]
#[error("no value for key part {0}")]
pub struct NoValueFor(pub usize);

/// A member: an id, a value for each key part in turn, `None` where it has
/// none, and a payload.
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

impl Member<KeySpec> for Item {
    type Id = Id;
    type Payload = Tag;
    type KeyError = NoValueFor;

    fn id(&self) -> Id { self.id }

    fn key(&self, spec: &KeySpec) -> Result<Vec<u8>, NoValueFor> {
        match self.values.len() < spec.keyparts.len() {
            true => Err(NoValueFor(self.values.len())),
            false => Ok(spec.collate(self.values.iter().copied())),
        }
    }

    fn payload(&self) -> Tag { self.payload.clone() }
}

/// A spec of one ascending integer part.
pub fn integer_spec() -> KeySpec { KeySpec::new(vec![asc!("a")]) }

/// A member under the integer spec, its one value `key`, missing when `None`:
/// `item!(1, Some(5), b"a")`.
macro_rules! item {
    ($id:expr, $key:expr, $payload:expr) => {
        $crate::common::Item::new($id, [$key], $payload)
    };
}

pub(crate) use item;

/// The rows of a tree of items, in memory.
pub type ItemStore<G = Ristretto255> = MemoryStore<RowKey, Row<KeySpec, Tag, G>>;

/// A tree of items, in memory.
pub type ItemTree<G = Ristretto255> = DigestTree<KeySpec, Id, Tag, G, ItemStore<G>>;

/// A tree in memory holding exactly `members`.
pub async fn built<G: DigestGroup>(key_spec: KeySpec, members: impl IntoIterator<Item = Item>) -> ItemTree<G> {
    DigestTree::build(ItemStore::new(), key_spec, members).await.unwrap()
}

/// Items 0 to 9, each keyed by its own number.
pub fn ten_items() -> impl Iterator<Item = Item> { (0..10).map(|n| item!(n, Some(n as i64), b"")) }

/// A transaction on a tree of [`ten_items`] that removes item 3, adds item
/// 10, and moves item 5 to key 50.
pub fn remove_add_and_move<S: Store<RowKey, Row<KeySpec, Tag, Ristretto255>>>(
    tree: &DigestTree<KeySpec, Id, Tag, Ristretto255, S>,
) -> Transaction<KeySpec, Id, Tag, Ristretto255, S> {
    let mut trx = tree.transaction();
    trx.remove(item!(3, Some(3), b"")).unwrap();
    trx.insert(item!(10, Some(10), b"")).unwrap();
    trx.update(item!(5, Some(5), b""), item!(5, Some(50), b"")).unwrap();

    trx
}

/// The members of a tree of [`ten_items`] once [`remove_add_and_move`] has
/// committed.
pub fn removed_added_and_moved() -> impl Iterator<Item = Item> {
    (0..10).filter(|&n| n != 3 && n != 5).map(|n| item!(n, Some(n as i64), b"")).chain([item!(5, Some(50), b""), item!(10, Some(10), b"")])
}

/// A member id of any length: its bytes as they are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Name(pub Vec<u8>);

impl MemberId for Name {
    fn to_bytes(&self) -> Cow<'_, [u8]> { Cow::Borrowed(&self.0) }

    fn from_bytes(bytes: &[u8]) -> Option<Self> { Some(Name(bytes.to_vec())) }
}

/// A member known by a name, with a payload and an empty key: for trees
/// keyed by id alone.
#[derive(Clone, Debug)]
pub struct Named(pub Vec<u8>, pub Tag);

impl Member<KeySpec> for Named {
    type Id = Name;
    type Payload = Tag;
    type KeyError = Infallible;

    fn id(&self) -> Name { Name(self.0.clone()) }

    fn key(&self, _: &KeySpec) -> Result<Vec<u8>, Infallible> { Ok(Vec::new()) }

    fn payload(&self) -> Tag { self.1.clone() }
}

/// A tree of named members, in memory.
pub type NamedTree = DigestTree<KeySpec, Name, Tag, Ristretto255, MemoryStore<RowKey, Row<KeySpec, Tag, Ristretto255>>>;

/// Whether two trees hold the same members with the same digests, as far as
/// reads can tell: equal members, an equal digest over the whole tree, and a
/// clean audit of each.
pub async fn assert_same_tree<G: DigestGroup>(a: &ItemTree<G>, b: &ItemTree<G>) {
    let (a_view, b_view) = (a.view(), b.view());
    let a_members: Vec<_> = a_view.range(..).try_collect().await.unwrap();
    let b_members: Vec<_> = b_view.range(..).try_collect().await.unwrap();

    assert_eq!(a_members, b_members, "members");
    assert_eq!(a_view.span_digest(&Span::all()).await.unwrap().digest, b_view.span_digest(&Span::all()).await.unwrap().digest, "digest");

    a.audit().await.unwrap();
    b.audit().await.unwrap();
}
