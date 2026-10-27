//! What the tree reads from the things it holds.

use std::borrow::Cow;

use crate::Error;

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

/// What the tree reads from something its owner tells it to hold, under the
/// owner's key spec `K`.
pub trait Member<K> {
    /// The member's id type.
    type Id: MemberId;

    /// The member's payload type.
    type Payload: Payload;

    /// Why the member couldn't make its key.
    type KeyError: std::error::Error + Send + Sync + 'static;

    /// The member's id.
    fn id(&self) -> Self::Id;

    /// The member's key under `spec`: bytes that sort as the owner orders its
    /// members, made with the owner's collation. The tree reads nothing in
    /// them but their order, and members with equal keys sort by id. The
    /// member's leaf digest covers them too, so peers' trees compare equal
    /// only if every peer makes the same key for the same member.
    fn key(&self, spec: &K) -> Result<Vec<u8>, Self::KeyError>;

    /// The payload the tree keeps with the member.
    fn payload(&self) -> Self::Payload;
}

impl<K, M: Member<K> + ?Sized> Member<K> for &M {
    type Id = M::Id;
    type Payload = M::Payload;
    type KeyError = M::KeyError;

    fn id(&self) -> Self::Id { (**self).id() }

    fn key(&self, spec: &K) -> Result<Vec<u8>, Self::KeyError> { (**self).key(spec) }

    fn payload(&self) -> Self::Payload { (**self).payload() }
}

/// `member`'s key under `spec`, or the error that names the member it
/// failed for.
pub(crate) fn member_key<K, M: Member<K>>(spec: &K, member: &M) -> Result<Vec<u8>, Error> {
    member.key(spec).map_err(|source| Error::Key { member: member.id().to_bytes().into_owned(), source: Box::new(source) })
}
