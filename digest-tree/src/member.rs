//! What the tree reads from the things it holds.

use std::borrow::Cow;

use ankurah_core_types::key_spec::IndexKeyPart;
use ankurah_core_types::Value;

/// A member's identity: anything that converts to and from bytes, of any
/// length. Its bytes end the member's address, and its leaf digest covers
/// them.
pub trait MemberId: Clone + Eq + Send + Sync + 'static {
    /// The id's bytes.
    fn to_bytes(&self) -> Cow<'_, [u8]>;

    /// The id these bytes encode, if they encode one.
    fn from_bytes(bytes: &[u8]) -> Option<Self>;
}

/// What a tree keeps with each member, hands back in reads, and covers in the
/// member's leaf digest. Two trees whose members agree on ids, keys and
/// payloads hold equal digests. The tree attaches no meaning to it.
pub trait Payload: Clone + Send + Sync + 'static {
    /// The bytes the member's leaf digest covers. Payloads the caller regards
    /// as equal must give equal bytes.
    fn digest_bytes(&self) -> Cow<'_, [u8]>;
}

/// What the tree reads from something its owner tells it to hold, under a key
/// spec whose parts name properties of type `K`.
pub trait Member<K> {
    /// The member's id type.
    type Id: MemberId;

    /// The member's payload type.
    type Payload: Payload;

    /// Why the member couldn't give a value, as for a part naming a property
    /// the member doesn't have.
    type KeyError: std::error::Error + Send + Sync + 'static;

    /// The member's id.
    fn id(&self) -> Self::Id;

    /// The member's value for `part` of the tree's key, or `None` if it has
    /// none. The tree checks the value's type against the part's, and makes
    /// the member's key from its values for every part, in the spec's order.
    /// The member's leaf digest covers its key, so peers' trees compare equal
    /// only if every peer gives the same values for the same member.
    fn value(&self, part: &IndexKeyPart<K>) -> Result<Option<Value>, Self::KeyError>;

    /// The payload the tree keeps with the member.
    fn payload(&self) -> Self::Payload;
}

impl<K, M: Member<K> + ?Sized> Member<K> for &M {
    type Id = M::Id;
    type Payload = M::Payload;
    type KeyError = M::KeyError;

    fn id(&self) -> Self::Id { (**self).id() }

    fn value(&self, part: &IndexKeyPart<K>) -> Result<Option<Value>, Self::KeyError> { (**self).value(part) }

    fn payload(&self) -> Self::Payload { (**self).payload() }
}
