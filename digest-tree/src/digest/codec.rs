//! The row layouts of digests and leaf elements: the bytes a tree's maps
//! hold. The module documentation explains why rows hold the canonical
//! encoding.
//!
//! Each group's rows carry a version byte of its own
//! ([`DigestGroup::ROW_VERSION`]): 1 for ristretto255's canonical encoding and
//! 2 for LtHash16's lanes.

use super::group::DigestGroup;
use super::{Digest, LeafElement};

/// Why stored bytes are not a digest or a leaf element.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The bytes have the wrong length for the layout.
    #[error("expected {expected} bytes, found {found}")]
    Length {
        /// The layout's length.
        expected: usize,
        /// The length given.
        found: usize,
    },
    /// A row carries a codec version other than its group's: a layout this
    /// build cannot read, or a row written in another group. The tree it
    /// belongs to must be rebuilt from its leaves.
    #[error("unknown row codec version {0}")]
    Version(u8),
    /// The element bytes are not the canonical encoding of an element of the
    /// group. Only a group that can tell reports it; LtHash16 never does.
    #[error("not the canonical encoding of a group element")]
    Element,
}

const COUNT_LEN: usize = 8;

impl<G: DigestGroup> Digest<G> {
    /// Length of [`Digest::to_row_bytes`]: the version byte, the element, then
    /// the count.
    pub const ROW_LEN: usize = 1 + G::ELEMENT_LEN + COUNT_LEN;

    /// The row layout: the group's version byte, the element's canonical
    /// encoding, then the count as a signed 64-bit big-endian integer.
    pub fn to_row_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0u8; Self::ROW_LEN];
        bytes[0] = G::ROW_VERSION;
        let (element, count) = bytes[1..].split_at_mut(G::ELEMENT_LEN);
        G::encode(&self.element, element);
        count.copy_from_slice(&self.count.to_be_bytes());
        bytes
    }

    /// Decode [`Digest::to_row_bytes`], refusing a version byte other than
    /// the group's and any element bytes the group can tell are not a
    /// canonical encoding. The count may be any `i64`, so combine the decoded
    /// digest with others through the checked operations, such as
    /// [`Digest::checked_add`], which return an overflow as an error.
    pub fn from_row_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let (element, count) = versioned::<G>(bytes, Self::ROW_LEN)?.split_at(G::ELEMENT_LEN);
        let count: [u8; COUNT_LEN] = count.try_into().expect("a digest row ends with the eight count bytes");
        Ok(Self { element: G::decode(element)?, count: i64::from_be_bytes(count) })
    }
}

impl<G: DigestGroup> LeafElement<G> {
    /// Length of [`LeafElement::to_row_bytes`]: the version byte, then the
    /// element.
    pub const ROW_LEN: usize = 1 + G::ELEMENT_LEN;

    /// The row layout: the group's version byte, then the element's canonical
    /// encoding.
    pub fn to_row_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0u8; Self::ROW_LEN];
        bytes[0] = G::ROW_VERSION;
        G::encode(&self.0, &mut bytes[1..]);
        bytes
    }

    /// Decode [`LeafElement::to_row_bytes`], refusing a version byte other
    /// than the group's and any element bytes the group can tell are not a
    /// canonical encoding.
    pub fn from_row_bytes(bytes: &[u8]) -> Result<Self, DecodeError> { Ok(Self(G::decode(versioned::<G>(bytes, Self::ROW_LEN)?)?)) }
}

/// `bytes`, if they are exactly `len` long.
pub(crate) fn exact(bytes: &[u8], len: usize) -> Result<&[u8], DecodeError> {
    match bytes.len() == len {
        true => Ok(bytes),
        false => Err(DecodeError::Length { expected: len, found: bytes.len() }),
    }
}

/// The bytes after a row's version byte, when the row is `len` bytes long and
/// its version byte is `G`'s. The version is checked first, so a row of
/// another group, whose length differs, is refused as an unknown version.
fn versioned<G: DigestGroup>(row: &[u8], len: usize) -> Result<&[u8], DecodeError> {
    match row.first() {
        Some(&version) if version != G::ROW_VERSION => Err(DecodeError::Version(version)),
        _ => Ok(&exact(row, len)?[1..]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::{CountOverflow, HeadHash, Leaf, Ristretto255};
    use ankurah_core_types::EntityId;

    // These tests pin ristretto255's layouts.
    type Digest = crate::digest::Digest<Ristretto255>;
    type LeafElement = crate::digest::LeafElement<Ristretto255>;

    fn leaf_element(n: u8) -> LeafElement { Leaf::new(&[n], EntityId::from_bytes([n; 32]), HeadHash::of([[n; 32]])).element() }

    fn hex(s: &str) -> Vec<u8> { (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect() }

    /// A digest row of these element bytes and a count of one: a row whose
    /// only possible fault is its element.
    fn digest_row_with(element: &[u8]) -> Vec<u8> { [&[Ristretto255::ROW_VERSION], element, &1i64.to_be_bytes()].concat() }

    #[test]
    fn layouts_are_the_version_byte_then_the_canonical_element_then_the_count() {
        let element = leaf_element(1);
        let digest = Digest::from(element) + Digest::from(leaf_element(2)) + Digest::from(leaf_element(3));
        let row = digest.to_row_bytes();
        assert_eq!(row[0], Ristretto255::ROW_VERSION);
        assert_eq!(row[1..1 + Ristretto255::ELEMENT_LEN], digest.element.compress().to_bytes());
        assert_eq!(row[1 + Ristretto255::ELEMENT_LEN..], 3i64.to_be_bytes());
        let leaf_row = element.to_row_bytes();
        assert_eq!((leaf_row[0], &leaf_row[1..]), (Ristretto255::ROW_VERSION, &element.0.compress().to_bytes()[..]));
    }

    #[test]
    fn the_empty_digest_row_is_the_version_byte_then_zeros() {
        // The canonical encoding of the identity is 32 zero bytes.
        let row = [&[Ristretto255::ROW_VERSION], &[0u8; Digest::ROW_LEN - 1][..]].concat();
        assert_eq!(Digest::empty().to_row_bytes(), row);
        assert_eq!(Digest::from_row_bytes(&row), Ok(Digest::empty()));
    }

    #[test]
    fn every_decoder_refuses_short_and_trailing_bytes() {
        let element = leaf_element(1);
        let digest = Digest::from(element);
        // A public decoder, reduced to whether it accepts the bytes.
        type Decoder = fn(&[u8]) -> Result<(), DecodeError>;
        let layouts: [(&str, Vec<u8>, Decoder); 2] = [
            ("digest row", digest.to_row_bytes(), |bytes| Digest::from_row_bytes(bytes).map(drop)),
            ("leaf element row", element.to_row_bytes(), |bytes| LeafElement::from_row_bytes(bytes).map(drop)),
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
    fn row_decoders_refuse_unknown_versions() {
        let element = leaf_element(1);
        for version in [0, 2, 0xff] {
            let mut digest_row = Digest::from(element).to_row_bytes();
            digest_row[0] = version;
            assert_eq!(Digest::from_row_bytes(&digest_row), Err(DecodeError::Version(version)));
            let mut leaf_element_row = element.to_row_bytes();
            leaf_element_row[0] = version;
            assert_eq!(LeafElement::from_row_bytes(&leaf_element_row), Err(DecodeError::Version(version)));
        }
    }

    /// A row written for one group is an unknown version to the other, whose
    /// rows have another length, so its tree is rebuilt rather than misread.
    #[test]
    fn a_row_of_another_group_is_an_unknown_version() {
        use crate::digest::LtHash16;
        let leaf = Leaf::new(b"k", EntityId::from_bytes([1; 32]), HeadHash::of([[1; 32]]));
        let (ristretto255, lthash16) = (leaf.element::<Ristretto255>(), leaf.element::<LtHash16>());
        let from_ristretto255 = DecodeError::Version(Ristretto255::ROW_VERSION);
        let ristretto255_digest_row = Digest::from(ristretto255).to_row_bytes();
        assert_eq!(crate::digest::Digest::<LtHash16>::from_row_bytes(&ristretto255_digest_row), Err(from_ristretto255.clone()));
        assert_eq!(crate::digest::LeafElement::<LtHash16>::from_row_bytes(&ristretto255.to_row_bytes()), Err(from_ristretto255));
        let from_lthash16 = DecodeError::Version(LtHash16::ROW_VERSION);
        assert_eq!(Digest::from_row_bytes(&crate::digest::Digest::from(lthash16).to_row_bytes()), Err(from_lthash16.clone()));
        assert_eq!(LeafElement::from_row_bytes(&lthash16.to_row_bytes()), Err(from_lthash16));
    }

    /// RFC 9496 appendix A.2: invalid encodings, grouped by the check of the
    /// section 4.3.1 decoding that refuses them. A separate Python
    /// implementation of that decoding refuses each at the check its group
    /// names.
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
        let valid = leaf_element(1).0.compress().to_bytes();
        let mut high_bit_set = valid;
        high_bit_set[31] |= 0x80;
        assert!(Digest::from_row_bytes(&digest_row_with(&valid)).is_ok());

        let rfc_encodings =
            INVALID_ENCODINGS.iter().flat_map(|(group, encodings)| encodings.iter().map(|encoding| (*group, hex(encoding))));
        for (group, element) in rfc_encodings.chain([("an otherwise valid encoding with its high bit set", high_bit_set.to_vec())]) {
            assert_eq!(Digest::from_row_bytes(&digest_row_with(&element)), Err(DecodeError::Element), "{group}: {element:02x?}");
            assert_eq!(
                LeafElement::from_row_bytes(&[&[Ristretto255::ROW_VERSION], &element[..]].concat()),
                Err(DecodeError::Element),
                "{group}: {element:02x?}"
            );
        }
    }

    /// The count is a signed 64-bit big-endian integer, and the decoders take
    /// any value, so arithmetic on a decoded count goes through the checked
    /// operations.
    #[test]
    fn rows_carry_negative_and_extreme_counts_exactly() {
        let element = leaf_element(1);
        let row_with_count =
            |count_bytes: &str| [&[Ristretto255::ROW_VERSION], &element.0.compress().as_bytes()[..], &hex(count_bytes)].concat();
        for (count, count_bytes) in
            [(-1, "ffffffffffffffff"), (i64::MIN, "8000000000000000"), (i64::MIN + 1, "8000000000000001"), (i64::MAX, "7fffffffffffffff")]
        {
            let row = row_with_count(count_bytes);
            let digest = Digest::from_row_bytes(&row).expect("a canonical element and any count");
            assert_eq!(digest.count(), count);
            assert_eq!(digest.to_row_bytes()[..], row[..]);
        }

        let max = Digest::from_row_bytes(&row_with_count("7fffffffffffffff")).unwrap();
        let min = Digest::from_row_bytes(&row_with_count("8000000000000000")).unwrap();
        assert_eq!(max.checked_add(Digest::from(element)), Err(CountOverflow));
        assert_eq!(min.checked_sub(Digest::from(element)), Err(CountOverflow));
        assert_eq!(min.checked_neg(), Err(CountOverflow));
    }
}
