//! A span's digest, and the bytes it crosses the wire as.

use std::fmt;
use std::marker::PhantomData;
use std::ops::Bound;

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::Span;
use crate::bytes::{write_field, ByteReader};
use crate::digest::DecodeError;
use crate::{Digest, DigestGroup};

/// A span's digest: the span, and the sum and count of the members within
/// it, as a tree makes it and as it crosses the wire. Nothing of the tree's
/// shape: two trees holding the same members within a span make the same
/// span digest, whatever their nodes. It serializes through a byte encoding
/// that validates as it decodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanDigest<G: DigestGroup> {
    /// The span.
    pub bounds: Span,

    /// The sum and count of the members within the span.
    pub digest: Digest<G>,
}

/// The version byte that begins the encoding, so that another layout can
/// follow without being misread.
const VERSION: u8 = 1;

/// How the encoding marks each kind of bound.
const UNBOUNDED: u8 = 0;
const INCLUDED: u8 = 1;
const EXCLUDED: u8 = 2;

impl<G: DigestGroup> SpanDigest<G> {
    /// The bytes a peer receives: the version byte; the start and the end,
    /// each a byte saying which kind of bound it is and, unless unbounded, its
    /// byte string preceded by its length as an unsigned 32-bit big-endian
    /// integer; then the stored form of the digest, which begins with its
    /// group's version byte.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![VERSION];

        for bound in [&self.bounds.start, &self.bounds.end] {
            match bound {
                Bound::Unbounded => out.push(UNBOUNDED),

                Bound::Included(at) => {
                    out.push(INCLUDED);
                    write_field(&mut out, at);
                }

                Bound::Excluded(at) => {
                    out.push(EXCLUDED);
                    write_field(&mut out, at);
                }
            }
        }

        self.digest.encode_into(&mut out);
        out
    }

    /// Decode bytes a peer sent, checking the version, the kinds of the
    /// bounds, the digest's group and canonical element, and that nothing
    /// trails; or say why the bytes are no span digest.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        let mut reader = ByteReader::new(bytes);
        if reader.byte().ok_or(TRUNCATED)? != VERSION {
            return Err("an unknown version");
        }

        let bounds = Span::new(read_bound(&mut reader)?, read_bound(&mut reader)?);

        let digest = Digest::decode(reader.rest()).map_err(|error| match error {
            DecodeError::Version(_) => "a digest of another group",
            DecodeError::Length { expected, found } if found < expected => TRUNCATED,
            DecodeError::Length { .. } => "trailing bytes",
            DecodeError::Element => "an invalid element",
        })?;

        Ok(Self { bounds, digest })
    }
}

/// Bytes a peer sent that end before the span digest does.
const TRUNCATED: &str = "truncated bytes";

/// The next bound of a span digest's span.
fn read_bound(reader: &mut ByteReader<'_>) -> Result<Bound<Vec<u8>>, &'static str> {
    match reader.byte().ok_or(TRUNCATED)? {
        UNBOUNDED => Ok(Bound::Unbounded),
        INCLUDED => Ok(Bound::Included(reader.field().ok_or(TRUNCATED)?.to_vec())),
        EXCLUDED => Ok(Bound::Excluded(reader.field().ok_or(TRUNCATED)?.to_vec())),
        _ => Err("an unknown kind of bound"),
    }
}

impl<G: DigestGroup> Serialize for SpanDigest<G> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> { serializer.serialize_bytes(&self.to_bytes()) }
}

impl<'de, G: DigestGroup> Deserialize<'de> for SpanDigest<G> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(SpanDigestVisitor(PhantomData))
    }
}

/// Reads the encoding as bytes, or as a sequence of bytes from a format that
/// has no byte strings.
struct SpanDigestVisitor<G>(PhantomData<G>);

impl<'de, G: DigestGroup> Visitor<'de> for SpanDigestVisitor<G> {
    type Value = SpanDigest<G>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("the bytes of a span digest") }

    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
        SpanDigest::from_bytes(bytes).map_err(|reason| E::custom(format_args!("malformed span digest: {reason}")))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        // The hint comes from a peer, so it sizes no more than a typical span
        // digest up front.
        let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
        while let Some(byte) = seq.next_element()? {
            bytes.push(byte);
        }

        self.visit_bytes(&bytes)
    }
}

#[cfg(test)]
mod tests;
