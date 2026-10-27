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

/// The direction of one key part's values: the tests' stand-in for core's
/// `IndexDirection`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexDirection {
    /// Smallest first.
    Asc,

    /// Largest first.
    Desc,
}

/// One part of a key spec: the tests' stand-in for core's `IndexKeyPart`, a
/// name and a direction. The stand-in collation takes integers alone, so a
/// part names no value type, and its name only says what the part holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexKeyPart {
    pub(crate) key: String,
    pub(crate) direction: IndexDirection,
}

impl IndexKeyPart {
    pub(crate) fn asc(key: impl Into<String>) -> Self { Self { key: key.into(), direction: IndexDirection::Asc } }

    pub(crate) fn desc(key: impl Into<String>) -> Self { Self { key: key.into(), direction: IndexDirection::Desc } }
}

/// An ascending key part, as the planner's tests write one: `asc!("year")`.
macro_rules! asc {
    ($key:expr) => {
        $crate::fixtures::IndexKeyPart::asc($key)
    };
}

/// A descending key part: `desc!("year")`.
macro_rules! desc {
    ($key:expr) => {
        $crate::fixtures::IndexKeyPart::desc($key)
    };
}

pub(crate) use {asc, desc};

/// The tests' stand-in for an owner's key spec, core's `KeySpec`: its parts,
/// the most significant first. Members make their keys with it; the tree
/// only stores it and hands it to them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeySpec {
    pub(crate) keyparts: Vec<IndexKeyPart>,
}

impl KeySpec {
    pub(crate) fn new(keyparts: Vec<IndexKeyPart>) -> Self { Self { keyparts } }

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
    pub(crate) fn collate(&self, values: impl IntoIterator<Item = impl Into<Option<i64>>>) -> Vec<u8> {
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

/// A key part an item gives no value for, not even a missing one: the tests'
/// stand-in for a value its owner can't encode.
#[derive(Debug, thiserror::Error)]
#[error("no value for key part {0}")]
pub(crate) struct NoValueFor(pub(crate) usize);

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
