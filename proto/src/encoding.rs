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

/// Bytes that do not hold the value one of proto's decoders read them as, or that hold bytes after it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("bytes that do not decode: {0}")]
pub struct UndecodableBytes(String);

impl UndecodableBytes {
    /// The bytes the serializer refused with `error`.
    pub(crate) fn of(error: bincode::Error) -> Self { Self(error.to_string()) }
}

/// Why bytes do not hold a value of an enum whose variants a later build may append.
pub(crate) enum UndecodableVariant {
    /// The bytes open with a variant index the enum does not have in this build.
    Unknown { index: u32 },
    /// The bytes do not hold a value of the variant they name, or hold bytes after it.
    Malformed(bincode::Error),
}

/// The value of enum `T` that `bytes` hold, telling bytes of a variant this build does not know
/// from malformed bytes.
pub(crate) fn decode_variant<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, UndecodableVariant> {
    decode_exact(bytes).map_err(|error| match unknown_variant::<T>(bytes) {
        Some(index) => UndecodableVariant::Unknown { index },
        None => UndecodableVariant::Malformed(error),
    })
}

/// The variant index `bytes` open with, when enum `T` has no such variant in this build.
fn unknown_variant<T: DeserializeOwned>(bytes: &[u8]) -> Option<u32> {
    let index = u32::from_le_bytes(*bytes.first_chunk()?);
    variant_count::<T>().is_some_and(|count| index >= count).then_some(index)
}

/// The variant index of enum `N` that `bytes` name right after variant `holding` of the enum they
/// open with, when `N` has no such variant in this build: a kind a later build added inside a
/// variant this build knows.
pub(crate) fn unknown_variant_within<N: DeserializeOwned>(bytes: &[u8], holding: u32) -> Option<u32> {
    let (index, held) = bytes.split_first_chunk()?;
    (u32::from_le_bytes(*index) == holding).then(|| unknown_variant::<N>(held)).flatten()
}

/// The number of variants the `Deserialize` of enum `T` knows, or `None` when `T` reads no enum.
/// An enum's `Deserialize` opens by naming its variants to the deserializer; this one counts them
/// and reads nothing.
fn variant_count<T: DeserializeOwned>() -> Option<u32> {
    match T::deserialize(VariantCounter) {
        Err(Counted(count)) => count,
        Ok(_) => None,
    }
}

/// A deserializer that refuses every read, with the number of variants when asked for an enum.
struct VariantCounter;

/// The refusal [`VariantCounter`] gives: the variant count of the enum it was asked for, if any.
#[derive(Debug)]
struct Counted(Option<u32>);

impl std::fmt::Display for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("a deserializer that only counts variants") }
}

impl std::error::Error for Counted {}

impl serde::de::Error for Counted {
    fn custom<M: std::fmt::Display>(_: M) -> Self { Self(None) }
}

impl<'de> serde::Deserializer<'de> for VariantCounter {
    type Error = Counted;

    fn deserialize_any<V: serde::de::Visitor<'de>>(self, _: V) -> Result<V::Value, Counted> { Err(Counted(None)) }

    fn deserialize_enum<V: serde::de::Visitor<'de>>(
        self,
        _: &'static str,
        variants: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Counted> {
        Err(Counted(u32::try_from(variants.len()).ok()))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option unit unit_struct
        newtype_struct seq tuple tuple_struct map struct identifier ignored_any
    }
}
