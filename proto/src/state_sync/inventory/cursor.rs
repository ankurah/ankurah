//! Borrowed traversal of a packed inventory: a lending cursor and the head views it lends.

use bytes::Bytes;
use serde::{ser::SerializeSeq, Serialize, Serializer};

use super::{
    codec::{self, KeyEncoding, Reader},
    EntityHead, KeyedEntityHead,
};
use crate::{Clock, EntityId, EventId};

/// Sequential traversal with one reusable key buffer; no per-head allocations.
/// This intentionally is not Iterator: the next key may overwrite the current view.
pub struct HeadCursor {
    bytes: Bytes,
    offset: usize,
    remaining: usize,
    encoding: KeyEncoding,
    key: Vec<u8>,
}

impl HeadCursor {
    /// A cursor over a payload that `codec::write` produced or `codec::validate` accepted.
    pub(super) fn new(bytes: Bytes) -> Self {
        let mut reader = Reader::new(&bytes);
        let encoding = reader.key_encoding().expect("constructed or validated inventory");
        let remaining = reader.uint().expect("constructed or validated inventory");
        let offset = reader.offset;
        Self { bytes, offset, remaining, encoding, key: Vec::new() }
    }

    pub fn len(&self) -> usize { self.remaining }
    pub fn is_empty(&self) -> bool { self.remaining == 0 }
    #[allow(clippy::should_implement_trait)] // Lending result borrows the cursor's reusable key.
    pub fn next(&mut self) -> Option<KeyedEntityHeadRef<'_>> {
        if self.remaining == 0 {
            return None;
        }
        let mut reader = Reader { bytes: &self.bytes, offset: self.offset };
        let head = reader.head(self.encoding, &mut self.key).expect("constructed or validated inventory");
        self.offset = reader.offset;
        self.remaining -= 1;
        Some(KeyedEntityHeadRef::from_encoded(head))
    }
}

#[derive(Debug, Serialize)]
pub struct KeyedEntityHeadRef<'a> {
    pub key: &'a [u8],
    pub entity: EntityHeadRef<'a>,
}

impl<'a> KeyedEntityHeadRef<'a> {
    pub(super) fn from_encoded(head: codec::EncodedHead<'a, 'a>) -> Self {
        Self { key: head.key, entity: EntityHeadRef { entity_id: head.entity_id, head: ClockRef { bytes: head.clock, count: head.tips } } }
    }

    pub fn to_owned(&self) -> KeyedEntityHead { KeyedEntityHead { key: self.key.to_vec(), entity: self.entity.to_owned() } }
}

#[derive(Debug, Serialize)]
pub struct EntityHeadRef<'a> {
    pub entity_id: EntityId,
    pub head: ClockRef<'a>,
}

impl EntityHeadRef<'_> {
    pub fn to_owned(&self) -> EntityHead { EntityHead { entity_id: self.entity_id, head: self.head.to_owned() } }
}

#[derive(Debug, Clone, Copy)]
pub struct ClockRef<'a> {
    bytes: &'a [u8],
    count: usize,
}

impl ClockRef<'_> {
    pub fn entries(&self) -> impl Iterator<Item = (u32, EventId)> + '_ { codec::tips(self.bytes, self.count) }
    pub fn to_owned(&self) -> Clock { Clock::new(self.entries().collect::<Vec<_>>()).expect("validated clock") }
    pub fn equals(&self, clock: &Clock) -> bool {
        self.count == clock.len()
            && self.entries().zip(clock.entries()).all(|((g, id), (other_g, other_id))| g == *other_g && id == *other_id)
    }
}

impl Serialize for ClockRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.count))?;
        for entry in self.entries() {
            seq.serialize_element(&entry)?;
        }
        seq.end()
    }
}
