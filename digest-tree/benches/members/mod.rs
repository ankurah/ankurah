//! The members both benchmarks hold: albums with sixteen-byte ids, keyed by
//! the year each came out, each carrying the payload its benchmark gives it.

use std::borrow::Cow;
use std::convert::Infallible;

use ankurah_core_types::key_spec::IndexKeyPart;
use ankurah_core_types::Value;
use ankurah_digest_tree::{Encode, Member, MemberId, Payload, StoreError};

/// A sixteen-byte id.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Id(pub [u8; 16]);

impl MemberId for Id {
    fn to_bytes(&self) -> Cow<'_, [u8]> { Cow::Borrowed(&self.0) }

    fn from_bytes(bytes: &[u8]) -> Option<Self> { bytes.try_into().ok().map(Id) }
}

/// The property albums are keyed by: the year each came out.
#[derive(Clone)]
pub struct Year;

impl Encode for Year {
    fn encode(&self) -> Vec<u8> { Vec::new() }

    fn decode(_: &[u8]) -> Result<Self, StoreError> { Ok(Year) }
}

/// An album, keyed by the year it came out, with a payload of type `P`.
#[derive(Clone, Copy)]
pub struct Album<P> {
    pub id: Id,
    pub year: i64,
    pub payload: P,
}

impl<P: Payload> Member<Year> for Album<P> {
    type Id = Id;
    type Payload = P;
    type KeyError = Infallible;

    fn id(&self) -> Id { self.id }

    /// An album's one value: its year.
    fn value(&self, _: &IndexKeyPart<Year>) -> Result<Option<Value>, Infallible> { Ok(Some(Value::I64(self.year))) }

    fn payload(&self) -> P { self.payload.clone() }
}
