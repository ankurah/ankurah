//! Core's entity identities, heads and properties as digest-tree members.
//! The wrappers let core implement traits whose definitions and underlying
//! types both belong to other crates.

use std::borrow::Cow;
use std::convert::Infallible;

use ankurah_core_types::key_spec::IndexKeyPart;
use ankurah_core_types::{EntityId, PropertyId, Value};
use ankurah_digest_tree::{Encode, Member, MemberId, Payload, StoreError};
use ankurah_proto::Clock;

use crate::entity::TemporaryEntity;
use crate::selection::filter::Filterable;
use crate::value::ValueExt;

/// A property named by an entity tree's key specification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TreeProperty(pub PropertyId);

/// An entity's identity as a digest-tree member.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EntityMemberId(pub EntityId);

/// The entity version represented by a leaf. Stored clocks retain generations;
/// digests cover the event IDs, which already determine their generations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityHead(pub Clock);

pub(super) struct EntityMember<'a>(pub &'a TemporaryEntity);

impl Encode for TreeProperty {
    fn encode(&self) -> Vec<u8> { bincode::serialize(&self.0).expect("property identities serialize") }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> { bincode::deserialize(bytes).map(Self).map_err(StoreError::permanent) }
}

impl MemberId for EntityMemberId {
    fn to_bytes(&self) -> Cow<'_, [u8]> { Cow::Owned(self.0.to_bytes().to_vec()) }

    fn from_bytes(bytes: &[u8]) -> Option<Self> { Some(Self(EntityId::from_bytes(bytes.try_into().ok()?))) }
}

impl Encode for EntityHead {
    fn encode(&self) -> Vec<u8> { bincode::serialize(&self.0).expect("clocks serialize") }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> { bincode::deserialize(bytes).map(Self).map_err(StoreError::permanent) }
}

impl Payload for EntityHead {
    fn digest_bytes(&self) -> Cow<'_, [u8]> { Cow::Owned(self.0.ids().flat_map(|id| id.as_bytes().iter().copied()).collect()) }
}

impl Member<TreeProperty> for EntityMember<'_> {
    type Id = EntityMemberId;
    type Payload = EntityHead;
    type KeyError = Infallible;

    fn id(&self) -> EntityMemberId { EntityMemberId(self.0.id()) }

    fn value(&self, part: &IndexKeyPart<TreeProperty>) -> Result<Option<Value>, Infallible> {
        let value = self.0.value(&part.key.0);
        Ok(match &part.sub_path {
            None => value,
            // A nested JSON property can change scalar type between entities.
            // Preserve JSON's type tags rather than pretending the path has the
            // enclosing property's declared type (or the first value's type).
            Some(path) => value.and_then(|value| value.json_order_at_path(path)).map(Value::Json),
        })
    }

    fn payload(&self) -> EntityHead { EntityHead(self.0.head()) }
}
