//! The encoding Ankurah carries, hashes and signs, as specs/protocol/bytes.md section 1 sets it
//! out. Every encode and every decode in proto goes through here.

use bincode::Options;
use serde::{de::DeserializeOwned, Serialize};

/// What bincode 1.3.3's default free functions write: fixed-width little-endian integers and no
/// size limit. Unlike those functions, a decode refuses bytes left over after the value.
fn options() -> impl Options { bincode::DefaultOptions::new().with_fixint_encoding() }

/// `value`'s bytes.
pub(crate) fn encode<T: Serialize + ?Sized>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_into(&mut bytes, value);
    bytes
}

/// `value`'s bytes, written after those already in `bytes`.
pub(crate) fn encode_into<T: Serialize + ?Sized>(bytes: &mut Vec<u8>, value: &T) {
    // Cannot fail: no size limit is set, writing to a vector cannot fail, every sequence and map
    // in proto's types has a known length, and no `Serialize` impl they reach returns an error.
    options().serialize_into(bytes, value).expect("proto values encode")
}

/// The value `bytes` hold; bytes left over after it make them undecodable.
pub(crate) fn decode_exact<T: DeserializeOwned>(bytes: &[u8]) -> bincode::Result<T> { options().deserialize(bytes) }
