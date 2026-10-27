use super::*;
use crate::digest::leaf_encoding;
use crate::fixtures::{asc, desc, KeySpec, Tag};
use crate::{LtHash16, Ristretto255};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

/// Row keys of every kind, with short bodies so that ties and prefixes are
/// common.
fn row_key() -> impl Strategy<Value = RowKey> {
    let bytes = || proptest::collection::vec(prop_oneof![Just(0u8), Just(0xFF), any::<u8>()], 0..4);
    prop_oneof![
        Just(RowKey::Meta),
        (proptest::collection::vec(any::<bool>(), 0..20))
            .prop_map(|bits| RowKey::Node(bits.into_iter().fold(NodePrefix::root(), |prefix, bit| prefix.child(bit)))),
        bytes().prop_map(RowKey::Leaf),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2048, rng_seed: RngSeed::Fixed(0xe4c0_0001), ..ProptestConfig::default() })]

    #[test]
    fn row_keys_round_trip_and_their_encodings_sort_as_they_do(a in row_key(), b in row_key()) {
        prop_assert_eq!(RowKey::decode(&a.encode()).unwrap(), a.clone());
        prop_assert_eq!(a.encode().cmp(&b.encode()), a.cmp(&b), "{:?} against {:?}", a, b);
    }
}

/// The message of a decoding's failure, which must be permanent.
fn refusal<T: std::fmt::Debug>(decoded: Result<T, StoreError>) -> String {
    let failure = decoded.unwrap_err();
    assert!(!failure.is_retryable());
    failure.to_string()
}

#[test]
fn bytes_that_are_no_row_key_are_refused_permanently() {
    for (bytes, why) in [
        (vec![], "bytes that are no row key"),
        (vec![4], "bytes that are no row key"),
        (vec![META, 0], "bytes that are no row key"),
        (vec![NODE, 254], "a byte past the places of a seven-level walk"),
    ] {
        assert!(refusal(RowKey::decode(&bytes)).contains(why), "{bytes:?}: {why}");
    }
}

fn element<G: DigestGroup>() -> G::Element { G::hash_leaf(&leaf_encoding(b"k", b"id", b"p")) }

/// A row's encoding, decoded and encoded again: equal bytes mean the row
/// came back whole.
fn round_trip<G: DigestGroup>(row: &Row<KeySpec, Tag, G>) -> Vec<u8> { Row::<KeySpec, Tag, G>::decode(&row.encode()).unwrap().encode() }

fn rows_round_trip<G: DigestGroup>() {
    let element = element::<G>();
    let mut digest = Digest::empty();
    digest.add_element(&element).unwrap();

    let key_spec = KeySpec::new(vec![asc!("a"), desc!("b")]);
    let rows: [Row<KeySpec, Tag, G>; 4] = [
        Row::Meta(Meta { key_spec: key_spec.clone(), group: G::ROW_VERSION }),
        Row::Meta(Meta { key_spec: KeySpec::new(vec![]), group: G::ROW_VERSION }),
        Row::Node(digest),
        Row::Leaf(LeafRow { payload: Tag(b"payload".to_vec()), element }),
    ];

    for row in &rows {
        assert_eq!(round_trip(row), row.encode(), "{row:?}");
    }

    match Row::<KeySpec, Tag, G>::decode(&rows[0].encode()).unwrap() {
        Row::Meta(meta) => assert_eq!((meta.key_spec, meta.group), (key_spec, G::ROW_VERSION)),
        other => panic!("expected metadata, got {other:?}"),
    }

    match Row::<KeySpec, Tag, G>::decode(&rows[3].encode()).unwrap() {
        Row::Leaf(leaf) => assert_eq!((leaf.payload, leaf.element), (Tag(b"payload".to_vec()), element)),
        other => panic!("expected a leaf, got {other:?}"),
    }
}

#[test]
fn ristretto255_rows_round_trip() { rows_round_trip::<Ristretto255>() }

#[test]
fn lthash16_rows_round_trip() { rows_round_trip::<LtHash16>() }

#[test]
fn bytes_that_are_no_row_are_refused_permanently() {
    let leaf = Row::<KeySpec, Tag, LtHash16>::Leaf(LeafRow { payload: Tag(vec![]), element: element::<LtHash16>() }).encode();

    for (bytes, why) in [
        (vec![], "bytes that are no row"),
        (vec![4], "bytes that are no row"),
        (vec![META], "a truncated metadata row"),
        (vec![META, 1, 7, 0], "a key part's direction is 0 or 1"),
        (leaf, "unknown digest version 2"),
        (vec![NODE, 1], "expected 41 bytes, found 1"),
    ] {
        assert!(refusal(Row::<KeySpec, Tag, Ristretto255>::decode(&bytes)).contains(why), "{bytes:02x?}: {why}");
    }
}
