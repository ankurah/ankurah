use super::*;
use crate::digest::leaf_encoding;
use crate::{LtHash16, Ristretto255};

/// A digest of one member in the group `G`.
fn one<G: DigestGroup>() -> Digest<G> { Digest::of_element(G::hash_leaf(&leaf_encoding(b"k", b"id", b"payload"))) }

/// A span digest of one member over a span bounded at both ends.
fn bounded<G: DigestGroup>() -> SpanDigest<G> {
    SpanDigest { bounds: Span::new(Bound::Excluded(vec![0x90]), Bound::Included(vec![0xA0, 0])), digest: one() }
}

/// The reason a decoding refused its bytes.
fn refusal<G: DigestGroup>(bytes: &[u8]) -> &'static str { SpanDigest::<G>::from_bytes(bytes).expect_err("a malformed span digest") }

fn the_encoding_round_trips<G: DigestGroup>() {
    for span_digest in [
        bounded::<G>(),
        SpanDigest { bounds: Span::all(), digest: Digest::empty() },
        SpanDigest { bounds: Span::new(Bound::Included(vec![0x60]), Bound::Unbounded), digest: one() },
    ] {
        assert_eq!(SpanDigest::<G>::from_bytes(&span_digest.to_bytes()).unwrap(), span_digest);
    }
}

#[test]
fn ristretto255_span_digests_round_trip() { the_encoding_round_trips::<Ristretto255>() }

#[test]
fn lthash16_span_digests_round_trip() { the_encoding_round_trips::<LtHash16>() }

#[test]
fn decoding_refuses_every_kind_of_malformed_bytes() {
    let valid = bounded::<Ristretto255>().to_bytes();
    let with = |at: usize, byte: u8| {
        let mut bytes = valid.clone();
        bytes[at] = byte;
        bytes
    };

    assert_eq!(refusal::<Ristretto255>(&with(0, 2)), "an unknown version");

    // The start's kind follows the version byte.
    assert_eq!(refusal::<Ristretto255>(&with(1, 3)), "an unknown kind of bound");

    // Cut inside the version, the start's kind, its length, its byte, the
    // end's length, and the digest.
    for cut in [0, 1, 4, 6, 9, valid.len() - 1] {
        assert_eq!(refusal::<Ristretto255>(&valid[..cut]), "truncated bytes", "cut to {cut} bytes");
    }

    assert_eq!(refusal::<Ristretto255>(&[&valid[..], &[0]].concat()), "trailing bytes");
    assert_eq!(refusal::<LtHash16>(&valid), "a digest of another group");

    let element_at = valid.len() - 8 - 32;
    assert_eq!(refusal::<Ristretto255>(&with(element_at + 31, 0xFF)), "an invalid element");
}

/// Formats with byte strings hand the encoding over as bytes, and formats
/// without them as a sequence; both decode, and both refuse what the
/// encoding refuses.
#[test]
fn serde_carries_the_validating_encoding() {
    let span_digest = bounded::<Ristretto255>();
    let bytes = bincode::serialize(&span_digest).unwrap();
    assert_eq!(bincode::deserialize::<SpanDigest<Ristretto255>>(&bytes).unwrap(), span_digest);

    let json = serde_json::to_string(&span_digest).unwrap();
    assert_eq!(serde_json::from_str::<SpanDigest<Ristretto255>>(&json).unwrap(), span_digest);

    let truncated = span_digest.to_bytes()[..9].to_vec();
    assert!(bincode::deserialize::<SpanDigest<Ristretto255>>(&bincode::serialize(&truncated).unwrap()).is_err());
    assert!(serde_json::from_str::<SpanDigest<Ristretto255>>(&serde_json::to_string(&truncated).unwrap()).is_err());
    assert!(serde_json::from_str::<SpanDigest<LtHash16>>(&json).is_err(), "a digest of another group");
}
