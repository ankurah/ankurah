use super::*;
use crate::digest::leaf_encoding;
use crate::{LtHash16, Ristretto255};

/// A digest of one member in the group `G`.
fn one<G: DigestGroup>() -> Digest<G> { Digest::of_element(G::hash_leaf(&leaf_encoding(b"k", b"id", b"payload"))) }

/// The prefix spelled by `text`, one character per bit.
fn bits(text: &str) -> NodePrefix { text.chars().fold(NodePrefix::root(), |prefix, bit| prefix.child(bit == '1')) }

/// A span digest beneath the prefix 1, over part of its subtree.
fn beneath_one<G: DigestGroup>() -> SpanDigest<G> {
    SpanDigest { prefix: bits("1"), bounds: Span::new(Bound::Excluded(vec![0x90]), Bound::Included(vec![0xA0, 0])), digest: one() }
}

/// The reason a decoding refused its bytes.
fn refusal<G: DigestGroup>(bytes: &[u8]) -> &'static str {
    match SpanDigest::<G>::from_bytes(bytes) {
        Err(Error::MalformedSpanDigest(reason)) => reason,
        other => panic!("expected a malformed span digest, got {other:?}"),
    }
}

#[test]
fn a_span_within_its_prefix_validates_and_one_outside_does_not() {
    beneath_one::<Ristretto255>().validate().unwrap();
    SpanDigest::<Ristretto255> { prefix: NodePrefix::root(), bounds: Span::all(), digest: Digest::empty() }.validate().unwrap();

    let outside = SpanDigest::<Ristretto255> { bounds: Span::new(Bound::Included(vec![0x70]), Bound::Unbounded), ..beneath_one() };
    assert!(matches!(outside.validate(), Err(Error::MalformedSpanDigest("the span lies outside its prefix's subtree"))));
}

fn the_encoding_round_trips<G: DigestGroup>() {
    for span_digest in [
        beneath_one::<G>(),
        SpanDigest { prefix: NodePrefix::root(), bounds: Span::all(), digest: Digest::empty() },
        SpanDigest { prefix: bits("0110"), bounds: bits("0110").span(), digest: one() },
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
    let valid = beneath_one::<Ristretto255>().to_bytes();
    let with = |at: usize, byte: u8| {
        let mut bytes = valid.clone();
        bytes[at] = byte;
        bytes
    };

    assert_eq!(refusal::<Ristretto255>(&with(0, 2)), "an unknown version");

    // The prefix 1 encodes as the single byte 127, after its four length bytes.
    assert_eq!(refusal::<Ristretto255>(&with(5, 254)), "an invalid prefix");

    // The start's kind follows the prefix.
    assert_eq!(refusal::<Ristretto255>(&with(6, 3)), "an unknown kind of bound");

    for cut in [0, 1, 4, 6, 9, valid.len() - 1] {
        assert_eq!(refusal::<Ristretto255>(&valid[..cut]), "truncated bytes", "cut to {cut} bytes");
    }

    assert_eq!(refusal::<Ristretto255>(&[&valid[..], &[0]].concat()), "trailing bytes");
    assert_eq!(refusal::<LtHash16>(&valid), "a digest of another group");

    let element_at = valid.len() - 8 - 32;
    assert_eq!(refusal::<Ristretto255>(&with(element_at + 31, 0xFF)), "an invalid element");

    let outside = SpanDigest::<Ristretto255> { bounds: Span::all(), ..beneath_one() };
    assert_eq!(refusal::<Ristretto255>(&outside.to_bytes()), "the span lies outside its prefix's subtree");
}

/// Formats with byte strings hand the encoding over as bytes, and formats
/// without them as a sequence; both decode, and both refuse what the
/// encoding refuses.
#[test]
fn serde_carries_the_validating_encoding() {
    let span_digest = beneath_one::<Ristretto255>();
    let bytes = bincode::serialize(&span_digest).unwrap();
    assert_eq!(bincode::deserialize::<SpanDigest<Ristretto255>>(&bytes).unwrap(), span_digest);

    let json = serde_json::to_string(&span_digest).unwrap();
    assert_eq!(serde_json::from_str::<SpanDigest<Ristretto255>>(&json).unwrap(), span_digest);

    let outside = SpanDigest::<Ristretto255> { bounds: Span::all(), ..beneath_one() };
    assert!(bincode::deserialize::<SpanDigest<Ristretto255>>(&bincode::serialize(&outside).unwrap()).is_err());
    assert!(serde_json::from_str::<SpanDigest<Ristretto255>>(&serde_json::to_string(&outside).unwrap()).is_err());
    assert!(serde_json::from_str::<SpanDigest<LtHash16>>(&json).is_err(), "a digest of another group");
}
