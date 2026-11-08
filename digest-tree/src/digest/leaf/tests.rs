use super::*;
use crate::digest::group::GroupOperations;
use crate::digest::{LtHash16, Ristretto255};
use crate::fixtures::{from_hex, to_hex};
use curve25519_dalek::ristretto::RistrettoPoint;
use proptest::prelude::*;
use proptest::test_runner::RngSeed;
use sha2::{Digest as _, Sha256};

/// The id every known answer uses but one: the bytes 0 to 15.
const ID: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

/// A key as an owner's collation might make it for one string part holding
/// "blue".
const BLUE: &[u8] = b"\x01blue\x00\x00";

/// A leaf a known answer covers: its name, then its key, id and payload.
type Case<'a> = (&'static str, &'a [u8], &'a [u8], &'a [u8]);

/// The leaves the known answers cover.
fn leaves() -> [Case<'static>; 6] {
    [
        ("a string key", BLUE, &ID, b"payload-1"),
        ("the empty key of a tree keyed by id alone", b"", &ID, b"payload-1"),
        ("a key of one missing value", b"\x02", &ID, b"payload-1"),
        ("a string key holding an escaped zero byte", b"\x01a\x00\xffb\x00\x00", &ID, b"payload-1"),
        ("an empty payload", BLUE, &ID, b""),
        ("a three-byte id", BLUE, b"abc", b"payload-1"),
    ]
}

/// Known answers for each leaf of [`leaves`], pinned at its encoding, the 64
/// uniform bytes RFC 9380's expand_message_xmd makes of it, and its
/// ristretto255 element's canonical encoding. Every value was computed by a
/// separate Python implementation of the leaf encoding, of RFC 9380 section
/// 5.3.1 on hashlib's SHA-512, and of RFC 9496's element derivation and
/// encoding, which reproduces the RFCs' published vectors. They are pinned so
/// that any other implementation of the leaf hash can be checked against them.
#[test]
fn leaf_encodings_and_ristretto255_elements_match_known_answers() {
    let answers = [
        (
            "000000000000000701626c756500000000000000000010000102030405060708090a0b0c0d0e0f00000000000000097061796c6f61642d31",
            "56651e4aa82d2546e7aee72d846a93f1e34139c0e7c328a143912cdc14a3a5b36cd0c29f7ddbdba677d38cb559898ce7e69776b18e18d89ae72c2bf1c2c14aa3",
            "52a48fce1b47b74e9b2ad6269c9a49daeab3e8f62cc27fc6de8e7a213601aa75",
        ),
        (
            "00000000000000000000000000000010000102030405060708090a0b0c0d0e0f00000000000000097061796c6f61642d31",
            "2e5b619e108a8ff4cec1b8513d9504d0240eadc32775c32edc7ee12278e4d93ba7e7108d315bafd6ba5d8e4b230675de7e86e70dacb72b65a6e67d99dd12ee4d",
            "5ab810eaedcee739a162f7f39a18c25511e187de2ad3b803864d2ff21cf2dd5f",
        ),
        (
            "0000000000000001020000000000000010000102030405060708090a0b0c0d0e0f00000000000000097061796c6f61642d31",
            "c875ffcaa54abb89f8bedf52a712c509e8d12abf2c28abd81b1202783f243e2e8cbed0f05c9a09e5b1e4c304786357493ada846c3e52cae57e8c3f28fe79777a",
            "2667e43fba07edd36fcdb476618f4ddf915d621482999fd120caed6656fd1155",
        ),
        (
            "0000000000000007016100ff6200000000000000000010000102030405060708090a0b0c0d0e0f00000000000000097061796c6f61642d31",
            "ba8d062f1f214f4adf2c3546f53b484a0f2a26c1387799d9f7d66215dbd32f0cc8a8970873456ab56c21985fa20acb56f8a03921b39185afd3809528d5f80bc0",
            "92018224b2c8f1b27395f51f32e843662b33ac54ff04982ad85256fad760d662",
        ),
        (
            "000000000000000701626c756500000000000000000010000102030405060708090a0b0c0d0e0f0000000000000000",
            "7aa985b5f12a3c97ab957dc44ac8e87f856de8bfcc23095ca585b72551a04bf8627e320d4f2d0468180d091068bb0c2cb2d2a803f34b1d60eeb2154d8fc8ef28",
            "da4a778b6bf994aa20305e2a8c15a989a5ee1ac5a56ee001038aa3b49635dd51",
        ),
        (
            "000000000000000701626c75650000000000000000000361626300000000000000097061796c6f61642d31",
            "c66d757887812001e66708503fa85aa89fc2c3da479d571fae11a5bc42c92d66bdbfe195851e2dc7420717228a6000bbd8d26ae3758dea9863777928384edcca",
            "a4a2646dc1cc3d4e2f03eb901bc82e20b43bd3653413fb1b2ba25a774656967b",
        ),
    ];

    for ((name, key, id, payload), (encoding, uniform_bytes, element)) in leaves().into_iter().zip(answers) {
        let encoded = leaf_encoding(key, id, payload);
        assert_eq!(to_hex(&encoded), encoding, "{name}");

        let uniform_bytes: [u8; 64] = from_hex(uniform_bytes).try_into().unwrap();
        let leaf_element = Ristretto255::hash_leaf(&encoded);

        assert_eq!(leaf_element.0, RistrettoPoint::from_uniform_bytes(&uniform_bytes), "{name}");
        assert_eq!(to_hex(leaf_element.0.compress().as_bytes()), element, "{name}");
    }
}

/// Keys of 255 and 256 bytes, counting up from zero: the eight length bytes
/// carry the whole length, which a single byte could not. The answers come
/// from the same Python implementation.
#[test]
fn keys_of_255_and_256_bytes_keep_their_whole_length() {
    let answers = [
        (
            255,
            "00000000000000ff",
            "3ef6263c4c3f94105788ad36a00ef26563fb9518a88536bce2a0294290fa23b5acc6b5e1d8a514326dbddd850957de981f5df0a24da5c67467845679e15da40e",
            "72fd08bc891c042ceda97e65320b8fd892e96bcae55d7e6547b982dd541f6a4b",
        ),
        (
            256,
            "0000000000000100",
            "d3f4586853628f3e7387b5f21002b7a52c9cd301de8ae46ed5c50e3dcdefdb9640517976cb60ea18c815d40868f3ee6017a3a2bdaa30bb45b9d30f3aae7397e1",
            "0a2106440a355ddae0b537d177473e0cc5b696ff92e65acdc211a09cc8f1783f",
        ),
    ];

    for (len, length_bytes, uniform_bytes, element) in answers {
        let key: Vec<u8> = (0..len).map(|i| i as u8).collect();
        let encoded = leaf_encoding(&key, &ID, b"payload-1");

        assert_eq!(to_hex(&encoded[..8]), length_bytes);
        assert_eq!(encoded[8..8 + len], key[..]);

        let uniform_bytes: [u8; 64] = from_hex(uniform_bytes).try_into().unwrap();
        let leaf_element = Ristretto255::hash_leaf(&encoded);

        assert_eq!(leaf_element.0, RistrettoPoint::from_uniform_bytes(&uniform_bytes));
        assert_eq!(to_hex(leaf_element.0.compress().as_bytes()), element);
    }
}

/// Known answers for the LtHash16 elements of the leaves of [`leaves`], and of
/// one under a key of 2,000 bytes counting up from zero and wrapping, whose
/// encoding BLAKE3 hashes as three chunks: each element's encoding, pinned by
/// its first 16 bytes and the SHA-256 of all 2,048. They come from a separate
/// Python implementation of BLAKE3, written from its specification, which
/// gives BLAKE3's published answer for the empty input and agrees with the
/// `blake3` crate across chunk boundaries.
#[test]
fn lthash16_leaf_elements_match_known_answers() {
    let long_key: Vec<u8> = (0..2000).map(|i| i as u8).collect();
    let mut cases: Vec<Case> = leaves().to_vec();
    cases.push(("a 2,000-byte key", &long_key, &ID, b"payload-1"));

    let answers = [
        ("a23262653fe0c711c3b33c4c6593f298", "a6134ed96596e166810a3079cada38be30f6f9881abbcb8b569508c518b92a27"),
        ("08680d3faea08e0ee120a76fa373a854", "3070bb6ed3a6f2147e4dbfb4e0fef06ab10769a5c129c05ef74381c288aaf1ac"),
        ("5f6c65f0136c00403f28296ac2f87cec", "5705bb757ac6492cb38456411ef0e0a1454ef7ac2f3317ad992ac59a629168bb"),
        ("8734fad5143630944f41f5f1e62c0176", "087dcfc8428bceb3bf70024b4b913689c25c07878f43d71701c80ff5152304c7"),
        ("ff4cb05a7d3ddbf6c9bdf7ce38759a85", "5418acd4bea94ded63113cbde92b25998eb5230c35b9157bca36ef1206cb1d29"),
        ("caa2029a41424ffcf6bdd4430c5dbeb3", "7bf9f163251e182bbf7e20599cf1fca1c772145360038677d3e26ba7921ca4f0"),
        ("5a2847e97bd9f19be2aab96733099643", "0b5358a150496175e40b6d17167573e4c3f0741eadfdb374dc93e841a8e2ed47"),
    ];

    assert_eq!(cases.len(), answers.len());

    for ((name, key, id, payload), (first_bytes, sha256)) in cases.into_iter().zip(answers) {
        let element = LtHash16::hash_leaf(&leaf_encoding(key, id, payload));
        let mut encoding = [0u8; 2048];
        LtHash16::encode(&element, &mut encoding);

        assert_eq!(to_hex(&encoding[..16]), first_bytes, "{name}");
        assert_eq!(to_hex(&Sha256::digest(encoding)), sha256, "{name}");
    }
}

/// The first field of a leaf encoding, read through the eight big-endian
/// bytes of its length, and the bytes after it.
fn split_field(bytes: &[u8]) -> (&[u8], &[u8]) {
    let (length, rest) = bytes.split_at(8);
    rest.split_at(u64::from_be_bytes(length.try_into().unwrap()) as usize)
}

/// Fields mostly short, some either side of 255 bytes, with zero bytes
/// common, since keys escape them.
fn field() -> impl Strategy<Value = Vec<u8>> {
    let byte = prop_oneof![Just(0u8), any::<u8>()];
    prop_oneof![3 => proptest::collection::vec(byte.clone(), 0..12), 1 => proptest::collection::vec(byte, 250..262)]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, rng_seed: RngSeed::Fixed(0x1eaf_0001), ..ProptestConfig::default() })]

    /// The encoding is injective: it parses back into exactly the key, id and
    /// payload it was made from, with nothing left over, so no two distinct
    /// leaves share an encoding.
    #[test]
    fn the_leaf_encoding_parses_back_into_its_fields(key in field(), id in field(), payload in field()) {
        let encoded = leaf_encoding(&key, &id, &payload);
        let (decoded_key, rest) = split_field(&encoded);
        let (decoded_id, rest) = split_field(rest);
        let (decoded_payload, rest) = split_field(rest);

        prop_assert_eq!(decoded_key, &key[..]);
        prop_assert_eq!(decoded_id, &id[..]);
        prop_assert_eq!(decoded_payload, &payload[..]);
        prop_assert!(rest.is_empty());
    }

    /// Moving bytes from the end of one field to the start of the next makes
    /// another leaf, with another element.
    #[test]
    fn shifting_bytes_between_fields_changes_the_element(key in field(), id in field(), cut in any::<proptest::sample::Index>()) {
        prop_assume!(!key.is_empty());
        let cut = cut.index(key.len());
        let shifted_id = [&key[cut..], &id[..]].concat();

        prop_assert_ne!(
            Ristretto255::hash_leaf(&leaf_encoding(&key, &id, b"p")),
            Ristretto255::hash_leaf(&leaf_encoding(&key[..cut], &shifted_id, b"p"))
        );
    }
}
