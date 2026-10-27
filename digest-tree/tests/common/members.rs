//! The tests' members: numbered items and named members, keyed by the tests'
//! stand-in for an owner's key spec and collation, and trees of them in
//! memory.

use std::borrow::Cow;
use std::convert::Infallible;

use ankurah_digest_tree::{
    DigestGroup, DigestTree, Encode, Member, MemberId, MemoryStore, Payload, Ristretto255, Row, RowKey, Span, Store, StoreError,
    Transaction,
};

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

/// The order of one key part's values: the tests' stand-in for an index key
/// part's direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// Smallest first.
    Ascending,

    /// Largest first.
    Descending,
}

/// The tests' stand-in for an owner's key spec: the order of each part of a
/// key, the most significant first. Members make their keys from it; the
/// tree only stores it and hands it to them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Spec(pub Vec<Order>);

/// A spec's encoding: one byte per part, `0` for ascending and `1` for
/// descending.
impl Encode for Spec {
    fn encode(&self) -> Vec<u8> { self.0.iter().map(|order| u8::from(*order == Order::Descending)).collect() }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        let order = |byte: &u8| match byte {
            0 => Ok(Order::Ascending),
            1 => Ok(Order::Descending),
            _ => Err(StoreError::permanent("a key part's order is 0 or 1")),
        };

        bytes.iter().map(order).collect::<Result<_, _>>().map(Spec)
    }
}

/// A spec of these parts.
pub fn spec(orders: &[Order]) -> Spec { Spec(orders.to_vec()) }

/// One key part's bytes, the tests' stand-in for an owner's collation: a
/// present value is `0x01` and its big-endian bytes with the sign bit
/// flipped, so values sort as numbers do; a missing value is `0x02`, after
/// every present one, as PostgreSQL sorts NULL; and a descending part's bytes
/// are inverted, so that it sorts the other way, missing values first.
pub fn key_part(value: Option<i64>, order: Order) -> Vec<u8> {
    let mut bytes = match value {
        Some(value) => [&[0x01][..], &(value as u64 ^ 1 << 63).to_be_bytes()].concat(),
        None => vec![0x02],
    };

    if order == Order::Descending {
        bytes.iter_mut().for_each(|byte| *byte = !*byte);
    }

    bytes
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

impl Member<Spec> for Item {
    type Id = Id;
    type Payload = Tag;
    type KeyError = NoValueFor;

    fn id(&self) -> Id { self.id }

    fn key(&self, spec: &Spec) -> Result<Vec<u8>, NoValueFor> { key_of(&self.values, spec) }

    fn payload(&self) -> Tag { self.payload.clone() }
}

/// The key of a member with these values under `spec`: each value made into
/// its part's bytes, in the spec's order.
pub fn key_of(values: &[Option<i64>], spec: &Spec) -> Result<Vec<u8>, NoValueFor> {
    let parts = spec.0.iter().enumerate().map(|(part, &order)| match values.get(part) {
        Some(&value) => Ok(key_part(value, order)),
        None => Err(NoValueFor(part)),
    });

    Ok(parts.collect::<Result<Vec<_>, _>>()?.concat())
}

/// A spec of one ascending integer part.
pub fn integer_spec() -> Spec { spec(&[Order::Ascending]) }

/// The key of a member under the integer spec: `key` missing when `None`.
pub fn integer_key(key: Option<i64>) -> Vec<u8> { key_part(key, Order::Ascending) }

/// A member under the integer spec: `key` missing when `None`.
pub fn item(id: u64, key: Option<i64>, payload: &[u8]) -> Item { Item::new(id, [key], payload) }

/// The rows of a tree of items, in memory.
pub type ItemStore<G = Ristretto255> = MemoryStore<RowKey, Row<Spec, Tag, G>>;

/// A tree of items, in memory.
pub type ItemTree<G = Ristretto255> = DigestTree<Spec, Id, Tag, G, ItemStore<G>>;

/// A tree in memory holding exactly `members`.
pub async fn built<G: DigestGroup>(key_spec: Spec, members: impl IntoIterator<Item = Item>) -> ItemTree<G> {
    DigestTree::build(ItemStore::new(), key_spec, members).await.unwrap()
}

/// Items 0 to 9, each keyed by its own number.
pub fn ten_items() -> impl Iterator<Item = Item> { (0..10).map(|n| item(n, Some(n as i64), b"")) }

/// A transaction on a tree of [`ten_items`] that removes item 3, adds item
/// 10, and moves item 5 to key 50.
pub fn remove_add_and_move<S: Store<RowKey, Row<Spec, Tag, Ristretto255>>>(
    tree: &DigestTree<Spec, Id, Tag, Ristretto255, S>,
) -> Transaction<Spec, Id, Tag, Ristretto255, S> {
    let mut trx = tree.transaction();
    trx.remove(item(3, Some(3), b"")).unwrap();
    trx.insert(item(10, Some(10), b"")).unwrap();
    trx.update(item(5, Some(5), b""), item(5, Some(50), b"")).unwrap();

    trx
}

/// The members of a tree of [`ten_items`] once [`remove_add_and_move`] has
/// committed.
pub fn removed_added_and_moved() -> impl Iterator<Item = Item> {
    (0..10).filter(|&n| n != 3 && n != 5).map(|n| item(n, Some(n as i64), b"")).chain([item(5, Some(50), b""), item(10, Some(10), b"")])
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

impl Member<Spec> for Named {
    type Id = Name;
    type Payload = Tag;
    type KeyError = Infallible;

    fn id(&self) -> Name { Name(self.0.clone()) }

    fn key(&self, _: &Spec) -> Result<Vec<u8>, Infallible> { Ok(Vec::new()) }

    fn payload(&self) -> Tag { self.1.clone() }
}

/// A tree of named members, in memory.
pub type NamedTree = DigestTree<Spec, Name, Tag, Ristretto255, MemoryStore<RowKey, Row<Spec, Tag, Ristretto255>>>;

/// Whether two trees hold the same members with the same digests, as far as
/// reads can tell: equal members, an equal digest over the whole tree, and a
/// clean audit of each.
pub async fn assert_same_tree<G: DigestGroup>(a: &ItemTree<G>, b: &ItemTree<G>) {
    let (a_view, b_view) = (a.view(), b.view());
    assert_eq!(a_view.members(&Span::all(), usize::MAX).await.unwrap(), b_view.members(&Span::all(), usize::MAX).await.unwrap(), "members");
    assert_eq!(a_view.span_digest(&Span::all()).await.unwrap().digest, b_view.span_digest(&Span::all()).await.unwrap().digest, "digest");

    a.audit().await.unwrap();
    b.audit().await.unwrap();
}
