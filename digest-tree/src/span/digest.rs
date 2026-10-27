//! A span's digest, and the bytes it crosses the wire as.

use std::fmt;
use std::marker::PhantomData;
use std::ops::Bound;

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{NodePrefix, Span};
use crate::bytes::{write_field, ByteReader};
use crate::digest::DecodeError;
use crate::{Digest, DigestGroup, Error};

/// A span's digest, named with the smallest subtree that contains the span,
/// where refinement continues. Only a tree makes them. It serializes through a
/// byte encoding that validates as it decodes.
///
/// A tree names a span with its own subtrees, whose shape depends on the
/// order its members came and went in, so trees holding the same members can
/// name one span differently. Peers compare span digests by their `digest`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanDigest<G: DigestGroup> {
    /// The smallest subtree that contains the span.
    pub prefix: NodePrefix,

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
    /// Check that the span lies within its prefix's subtree.
    pub fn validate(&self) -> Result<(), Error> {
        match self.bounds.is_subset_of(&self.prefix.span()) {
            true => Ok(()),
            false => Err(Error::MalformedSpanDigest("the span lies outside its prefix's subtree")),
        }
    }

    /// The bytes a peer receives: the version byte; the prefix's encoding, the
    /// start and the end, each byte string preceded by its length as an
    /// unsigned 32-bit big-endian integer and each bound by a byte saying
    /// which kind it is; then the stored form of the digest, which begins
    /// with its group's version byte.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![VERSION];
        write_field(&mut out, &self.prefix.to_bytes());

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

    /// Decode bytes a peer sent, checking the version, the prefix's encoding,
    /// the digest's group and canonical element, that nothing trails, and
    /// that the span lies within the prefix's subtree.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = ByteReader::new(bytes);
        if reader.byte().ok_or_else(truncated)? != VERSION {
            return Err(Error::MalformedSpanDigest("an unknown version"));
        }

        let prefix =
            NodePrefix::from_bytes(reader.field().ok_or_else(truncated)?).map_err(|_| Error::MalformedSpanDigest("an invalid prefix"))?;

        let bounds = Span::new(read_bound(&mut reader)?, read_bound(&mut reader)?);

        let digest = Digest::decode(reader.rest()).map_err(|error| {
            Error::MalformedSpanDigest(match error {
                DecodeError::Version(_) => "a digest of another group",
                DecodeError::Length { expected, found } if found < expected => "truncated bytes",
                DecodeError::Length { .. } => "trailing bytes",
                DecodeError::Element => "an invalid element",
            })
        })?;

        let span_digest = Self { prefix, bounds, digest };
        span_digest.validate()?;

        Ok(span_digest)
    }
}

/// Bytes a peer sent that end before the span digest does.
fn truncated() -> Error { Error::MalformedSpanDigest("truncated bytes") }

/// The next bound of a span digest's span.
fn read_bound(reader: &mut ByteReader<'_>) -> Result<Bound<Vec<u8>>, Error> {
    match reader.byte().ok_or_else(truncated)? {
        UNBOUNDED => Ok(Bound::Unbounded),
        INCLUDED => Ok(Bound::Included(reader.field().ok_or_else(truncated)?.to_vec())),
        EXCLUDED => Ok(Bound::Excluded(reader.field().ok_or_else(truncated)?.to_vec())),
        _ => Err(Error::MalformedSpanDigest("an unknown kind of bound")),
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

    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> { SpanDigest::from_bytes(bytes).map_err(E::custom) }

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
