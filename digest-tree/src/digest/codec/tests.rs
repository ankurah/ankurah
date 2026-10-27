use super::*;
use crate::digest::group::GroupOperations;
use crate::digest::{leaf_encoding, LtHash16, Ristretto255};
use crate::fixtures::from_hex;

fn element<G: DigestGroup>(n: u8) -> G::Element { G::hash_leaf(&leaf_encoding(&[n], &[n; 16], &[n; 8])) }

fn digest_of<G: DigestGroup>(ns: &[u8]) -> Digest<G> {
    let mut digest = Digest::empty();
    for &n in ns {
        digest.add_element(&element::<G>(n)).unwrap();
    }

    digest
}

fn stored<G: DigestGroup>(digest: &Digest<G>) -> Vec<u8> {
    let mut bytes = Vec::new();
    digest.encode_into(&mut bytes);
    bytes
}

fn stored_element<G: DigestGroup>(element: &G::Element) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_stored_element::<G>(element, &mut bytes);
    bytes
}

#[test]
fn a_stored_digest_is_the_version_byte_then_the_canonical_element_then_the_count() {
    let digest = digest_of::<Ristretto255>(&[1, 2, 3]);
    let bytes = stored(&digest);

    assert_eq!(bytes.len(), Digest::<Ristretto255>::STORED_LEN);
    assert_eq!(bytes[0], Ristretto255::ROW_VERSION);
    assert_eq!(bytes[1..33], digest.element.0.compress().to_bytes());
    assert_eq!(bytes[33..], 3u64.to_be_bytes());

    let leaf = element::<Ristretto255>(1);
    let bytes = stored_element::<Ristretto255>(&leaf);

    assert_eq!((bytes[0], &bytes[1..]), (Ristretto255::ROW_VERSION, &leaf.0.compress().to_bytes()[..]));
}

#[test]
fn the_empty_digest_is_stored_as_the_version_byte_then_zeros() {
    // The canonical encoding of the identity is 32 zero bytes.
    let bytes = [&[Ristretto255::ROW_VERSION], &[0u8; 40][..]].concat();
    assert_eq!(stored(&Digest::<Ristretto255>::empty()), bytes);
    assert_eq!(Digest::<Ristretto255>::decode(&bytes), Ok(Digest::empty()));
}

/// Both groups' digests and elements come back from their stored forms, with
/// any count.
fn stored_forms_round_trip<G: DigestGroup>() {
    let digest = digest_of::<G>(&[4, 5]);
    assert_eq!(Digest::<G>::decode(&stored(&digest)), Ok(digest));

    for count in [0, 1, u64::MAX] {
        let digest = Digest::<G> { count, ..digest };
        assert_eq!(Digest::<G>::decode(&stored(&digest)), Ok(digest), "{count}");
    }

    let leaf = element::<G>(6);
    assert_eq!(decode_stored_element::<G>(&stored_element::<G>(&leaf)), Ok(leaf));
}

#[test]
fn ristretto255_stored_forms_round_trip() { stored_forms_round_trip::<Ristretto255>() }

#[test]
fn lthash16_stored_forms_round_trip() { stored_forms_round_trip::<LtHash16>() }

#[test]
fn every_decoder_refuses_short_and_trailing_bytes() {
    let digest = stored(&digest_of::<Ristretto255>(&[1]));
    let leaf = stored_element::<Ristretto255>(&element::<Ristretto255>(1));

    // A decoder, reduced to whether it accepts the bytes.
    type Decoder = fn(&[u8]) -> Result<(), DecodeError>;

    let layouts: [(&str, Vec<u8>, Decoder); 2] = [
        ("digest", digest, |bytes| Digest::<Ristretto255>::decode(bytes).map(drop)),
        ("leaf element", leaf, |bytes| decode_stored_element::<Ristretto255>(bytes).map(drop)),
    ];

    for (layout, bytes, decode) in layouts {
        let expected = bytes.len();
        assert_eq!(decode(&bytes), Ok(()), "{layout}");

        for found in [0, 1, expected - 1] {
            assert_eq!(decode(&bytes[..found]), Err(DecodeError::Length { expected, found }), "{layout} cut to {found} bytes");
        }

        let trailing = [&bytes[..], &[0]].concat();
        assert_eq!(decode(&trailing), Err(DecodeError::Length { expected, found: expected + 1 }), "{layout} and a trailing byte");
    }
}

#[test]
fn decoders_refuse_unknown_versions() {
    for version in [0, 3, 0xff] {
        let mut digest = stored(&digest_of::<Ristretto255>(&[1]));
        digest[0] = version;
        assert_eq!(Digest::<Ristretto255>::decode(&digest), Err(DecodeError::Version(version)));

        let mut leaf = stored_element::<Ristretto255>(&element::<Ristretto255>(1));
        leaf[0] = version;

        assert_eq!(decode_stored_element::<Ristretto255>(&leaf), Err(DecodeError::Version(version)));
    }
}

/// A row written for one group is an unknown version to the other, whose
/// rows have another length, so it is refused rather than misread.
#[test]
fn a_row_of_another_group_is_an_unknown_version() {
    let ristretto255 = DecodeError::Version(Ristretto255::ROW_VERSION);
    assert_eq!(Digest::<LtHash16>::decode(&stored(&digest_of::<Ristretto255>(&[1]))), Err(ristretto255.clone()));
    assert_eq!(decode_stored_element::<LtHash16>(&stored_element::<Ristretto255>(&element::<Ristretto255>(1))), Err(ristretto255));

    let lthash16 = DecodeError::Version(LtHash16::ROW_VERSION);
    assert_eq!(Digest::<Ristretto255>::decode(&stored(&digest_of::<LtHash16>(&[1]))), Err(lthash16.clone()));
    assert_eq!(decode_stored_element::<Ristretto255>(&stored_element::<LtHash16>(&element::<LtHash16>(1))), Err(lthash16));
}

/// RFC 9496 appendix A.2: invalid encodings, grouped by the check of the
/// section 4.3.1 decoding that refuses them.
const INVALID_ENCODINGS: [(&str, &[&str]); 5] = [
    (
        "non-canonical field encodings",
        &[
            "00ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
            "f3ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
            "edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
        ],
    ),
    (
        "negative field elements",
        &[
            "0100000000000000000000000000000000000000000000000000000000000000",
            "01ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
            "ed57ffd8c914fb201471d1c3d245ce3c746fcbe63a3679d51b6a516ebebe0e20",
            "c34c4e1826e5d403b78e246e88aa051c36ccf0aafebffe137d148a2bf9104562",
            "c940e5a4404157cfb1628b108db051a8d439e1a421394ec4ebccb9ec92a8ac78",
            "47cfc5497c53dc8e61c91d17fd626ffb1c49e2bca94eed052281b510b1117a24",
            "f1c6165d33367351b0da8f6e4511010c68174a03b6581212c71c0e1d026c3c72",
            "87260f7a2f12495118360f02c26a470f450dadf34a413d21042b43b9d93e1309",
        ],
    ),
    (
        "non-square x^2",
        &[
            "26948d35ca62e643e26a83177332e6b6afeb9d08e4268b650f1f5bbd8d81d371",
            "4eac077a713c57b4f4397629a4145982c661f48044dd3f96427d40b147d9742f",
            "de6a7b00deadc788eb6b6c8d20c0ae96c2f2019078fa604fee5b87d6e989ad7b",
            "bcab477be20861e01e4a0e295284146a510150d9817763caf1a6f4b422d67042",
            "2a292df7e32cababbd9de088d1d1abec9fc0440f637ed2fba145094dc14bea08",
            "f4a9e534fc0d216c44b218fa0c42d99635a0127ee2e53c712f70609649fdff22",
            "8268436f8c4126196cf64b3c7ddbda90746a378625f9813dd9b8457077256731",
            "2810e5cbc2cc4d4eece54f61c6f69758e289aa7ab440b3cbeaa21995c2f4232b",
        ],
    ),
    (
        "negative xy value",
        &[
            "3eb858e78f5a7254d8c9731174a94f76755fd3941c0ac93735c07ba14579630e",
            "a45fdc55c76448c049a1ab33f17023edfb2be3581e9c7aade8a6125215e04220",
            "d483fe813c6ba647ebbfd3ec41adca1c6130c2beeee9d9bf065c8d151c5f396e",
            "8a2e1d30050198c65a54483123960ccc38aef6848e1ec8f5f780e8523769ba32",
            "32888462f8b486c68ad7dd9610be5192bbeaf3b443951ac1a8118419d9fa097b",
            "227142501b9d4355ccba290404bde41575b037693cef1f438c47f8fbf35d1165",
            "5c37cc491da847cfeb9281d407efc41e15144c876e0170b499a96a22ed31e01e",
            "445425117cb8c90edcbc7c1cc0e74f747f2c1efa5630a967c64f287792a48a4b",
        ],
    ),
    ("s = -1, which causes y = 0", &["ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f"]),
];

#[test]
fn decoding_refuses_rfc_9496_invalid_encodings() {
    // An encoding that decodes, and the same bytes with the top bit set,
    // which makes them an integer of 2^255 or more and so not canonical.
    let valid = element::<Ristretto255>(1).0.compress().to_bytes();
    let mut high_bit_set = valid;
    high_bit_set[31] |= 0x80;

    let digest_with = |element: &[u8]| [&[Ristretto255::ROW_VERSION], element, &1u64.to_be_bytes()].concat();
    assert!(Digest::<Ristretto255>::decode(&digest_with(&valid)).is_ok());

    let rfc_encodings =
        INVALID_ENCODINGS.iter().flat_map(|(group, encodings)| encodings.iter().map(|encoding| (*group, from_hex(encoding))));

    for (group, element) in rfc_encodings.chain([("an otherwise valid encoding with its high bit set", high_bit_set.to_vec())]) {
        assert_eq!(Digest::<Ristretto255>::decode(&digest_with(&element)), Err(DecodeError::Element), "{group}: {element:02x?}");
        let leaf = [&[Ristretto255::ROW_VERSION], &element[..]].concat();

        assert_eq!(decode_stored_element::<Ristretto255>(&leaf), Err(DecodeError::Element), "{group}: {element:02x?}");
    }
}
