//! Members, ids, payloads and the key spec the crate's tests share.

use std::borrow::Cow;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use crate::digest::leaf_encoding;
use crate::{DigestGroup, Encode, Member, MemberId, Payload, StoreError};

/// A member id: four bytes, big-endian, so ids sort as numbers do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Id(pub(crate) u32);

impl MemberId for Id {
    fn to_bytes(&self) -> Cow<'_, [u8]> { Cow::Owned(self.0.to_be_bytes().to_vec()) }

    fn from_bytes(bytes: &[u8]) -> Option<Self> { bytes.try_into().ok().map(u32::from_be_bytes).map(Id) }
}

/// A payload: bytes the tree keeps and digests as they are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Tag(pub(crate) Vec<u8>);

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
pub(crate) enum Order {
    /// Smallest first.
    Ascending,

    /// Largest first.
    Descending,
}

/// The tests' stand-in for an owner's key spec: the order of each part of a
/// key, the most significant first. Members make their keys from it; the
/// tree only stores it and hands it to them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Spec(pub(crate) Vec<Order>);

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
pub(crate) fn spec(orders: &[Order]) -> Spec { Spec(orders.to_vec()) }

/// A member: an id, a value for each key part in turn, `None` where it has
/// none, and a payload.
#[derive(Clone, Debug)]
pub(crate) struct Item {
    pub(crate) id: Id,
    pub(crate) values: Vec<Option<i64>>,
    pub(crate) payload: Tag,
}

impl Item {
    pub(crate) fn new(id: u32, values: impl IntoIterator<Item = Option<i64>>, payload: &[u8]) -> Self {
        Self { id: Id(id), values: values.into_iter().collect(), payload: Tag(payload.to_vec()) }
    }
}

impl Member<Spec> for Item {
    type Id = Id;
    type Payload = Tag;
    type KeyError = NoValueFor;

    fn id(&self) -> Id { self.id }

    fn key(&self, spec: &Spec) -> Result<Vec<u8>, NoValueFor> {
        let parts = spec.0.iter().enumerate().map(|(part, &order)| match self.values.get(part) {
            Some(&value) => Ok(key_part(value, order)),
            None => Err(NoValueFor(part)),
        });

        Ok(parts.collect::<Result<Vec<_>, _>>()?.concat())
    }

    fn payload(&self) -> Tag { self.payload.clone() }
}

/// A key part an item gives no value for, not even a missing one: the tests'
/// stand-in for a value its owner can't encode.
#[derive(Debug, thiserror::Error)]
#[error("no value for key part {0}")]
pub(crate) struct NoValueFor(pub(crate) usize);

/// One key part's bytes, the tests' stand-in for an owner's collation: a
/// present value is `0x01` and its big-endian bytes with the sign bit
/// flipped, so values sort as numbers do; a missing value is `0x02`, after
/// every present one, as PostgreSQL sorts NULL; and a descending part's bytes
/// are inverted, so that it sorts the other way, missing values first.
pub(crate) fn key_part(value: Option<i64>, order: Order) -> Vec<u8> {
    let mut bytes = match value {
        Some(value) => [&[0x01][..], &(value as u64 ^ 1 << 63).to_be_bytes()].concat(),
        None => vec![0x02],
    };

    if order == Order::Descending {
        bytes.iter_mut().for_each(|byte| *byte = !*byte);
    }

    bytes
}

/// The element in `G` of the leaf of this key, id and payload.
pub(crate) fn leaf_element<G: DigestGroup>(key: &[u8], id: &[u8], payload: &[u8]) -> G::Element {
    G::hash_leaf(&leaf_encoding(key, id, payload))
}

/// Run `future` to completion on the calling thread, for property tests,
/// which aren't async. Over a memory store every poll makes progress: the
/// store never waits, an uncontended lock is taken at once, and a yield wakes
/// its task before returning.
pub(crate) fn block_on<T>(future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());

    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
    }
}

/// Bytes as lowercase hexadecimal.
pub(crate) fn to_hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }

/// The bytes these hexadecimal digits spell, spaces between them ignored.
pub(crate) fn from_hex(digits: &str) -> Vec<u8> {
    let digits: Vec<u8> = digits.bytes().filter(|digit| *digit != b' ').collect();
    digits.chunks(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}
