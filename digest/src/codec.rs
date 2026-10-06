//! Byte layouts for digests and leaf points: the canonical form for the wire
//! and for anything signed or fingerprinted, and the versioned row layouts
//! storage keeps. The crate documentation explains why rows hold the canonical
//! encoding.

use curve25519_dalek::ristretto::{CompressedRistretto, RistrettoPoint};

use crate::point::{Digest, LeafPoint};

/// Why stored or received bytes are not a digest or a leaf point.
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
    /// A row carries a codec version this build cannot read; the tree it
    /// belongs to must be rebuilt from its leaves.
    #[error("unknown row codec version {0}")]
    Version(u8),
    /// The 32 point bytes are not the canonical encoding of a ristretto255
    /// element.
    #[error("not the canonical encoding of a ristretto255 element")]
    Point,
}

/// Row codec version 1: the canonical encoding behind the version byte.
const ROW_V1: u8 = 1;

const POINT_LEN: usize = 32;
const COUNT_LEN: usize = 8;

/// Length of [`Digest::to_wire_bytes`]: the point, then the count.
pub const DIGEST_WIRE_LEN: usize = POINT_LEN + COUNT_LEN;

/// Length of [`Digest::to_row_bytes`]: the version byte, then the wire form.
pub const DIGEST_ROW_LEN: usize = 1 + DIGEST_WIRE_LEN;

/// Length of [`LeafPoint::to_row_bytes`]: the version byte, then the point.
pub const LEAF_POINT_ROW_LEN: usize = 1 + POINT_LEN;

impl Digest {
    /// The canonical form, for the wire and for anything signed or
    /// fingerprinted: the point's canonical 32-byte encoding, then the count
    /// as a signed 64-bit big-endian integer.
    pub fn to_wire_bytes(&self) -> [u8; DIGEST_WIRE_LEN] {
        let mut bytes = [0u8; DIGEST_WIRE_LEN];
        bytes[..POINT_LEN].copy_from_slice(self.point.compress().as_bytes());
        bytes[POINT_LEN..].copy_from_slice(&self.count.to_be_bytes());
        bytes
    }

    /// Decode [`Digest::to_wire_bytes`], refusing any point bytes that are not
    /// a canonical encoding. The count may be any `i64`, so combine the
    /// decoded digest with others through the checked operations, such as
    /// [`Digest::checked_add`], which return an overflow as an error.
    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let bytes = exact::<DIGEST_WIRE_LEN>(bytes)?;
        let (point, count) = bytes.split_at(POINT_LEN);
        let count: [u8; COUNT_LEN] = count.try_into().expect("the wire form ends with the eight count bytes");
        Ok(Self { point: decompress(point)?, count: i64::from_be_bytes(count) })
    }

    /// The row layout, codec version 1: the version byte, then
    /// [`Digest::to_wire_bytes`]. The crate documentation explains why rows
    /// hold the canonical encoding rather than uncompressed coordinates.
    pub fn to_row_bytes(&self) -> [u8; DIGEST_ROW_LEN] {
        let mut bytes = [0u8; DIGEST_ROW_LEN];
        bytes[0] = ROW_V1;
        bytes[1..].copy_from_slice(&self.to_wire_bytes());
        bytes
    }

    /// Decode [`Digest::to_row_bytes`], refusing an unknown version and any
    /// point bytes that are not a canonical encoding. As with
    /// [`Digest::from_wire_bytes`], the count may be any `i64`.
    pub fn from_row_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let bytes = exact::<DIGEST_ROW_LEN>(bytes)?;
        Self::from_wire_bytes(versioned(bytes)?)
    }
}

impl LeafPoint {
    /// The row layout, codec version 1: the version byte, then the point's
    /// canonical 32-byte encoding.
    pub fn to_row_bytes(&self) -> [u8; LEAF_POINT_ROW_LEN] {
        let mut bytes = [0u8; LEAF_POINT_ROW_LEN];
        bytes[0] = ROW_V1;
        bytes[1..].copy_from_slice(self.0.compress().as_bytes());
        bytes
    }

    /// Decode [`LeafPoint::to_row_bytes`], refusing an unknown version and any
    /// point bytes that are not a canonical encoding.
    pub fn from_row_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let bytes = exact::<LEAF_POINT_ROW_LEN>(bytes)?;
        Ok(Self(decompress(versioned(bytes)?)?))
    }
}

fn exact<const N: usize>(bytes: &[u8]) -> Result<&[u8; N], DecodeError> {
    bytes.try_into().map_err(|_| DecodeError::Length { expected: N, found: bytes.len() })
}

/// The bytes after a row's version byte, when this build reads that version.
fn versioned<const N: usize>(row: &[u8; N]) -> Result<&[u8], DecodeError> {
    match row[0] {
        ROW_V1 => Ok(&row[1..]),
        version => Err(DecodeError::Version(version)),
    }
}

fn decompress(bytes: &[u8]) -> Result<RistrettoPoint, DecodeError> {
    CompressedRistretto::from_slice(bytes).ok().and_then(|encoding| encoding.decompress()).ok_or(DecodeError::Point)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CountOverflow, HeadHash, Leaf};
    use ankurah_core_types::EntityId;

    fn leaf_point(n: u8) -> LeafPoint { Leaf::new(&[n], EntityId::from_bytes([n; 32]), HeadHash::of([[n; 32]])).point() }

    fn hex(s: &str) -> Vec<u8> { (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect() }

    /// The point bytes, then a count of one: a wire form whose only possible
    /// fault is its point.
    fn wire_with_point(point: &[u8]) -> Vec<u8> { [point, &1i64.to_be_bytes()].concat() }

    #[test]
    fn layouts_are_the_version_byte_then_the_canonical_point_then_the_count() {
        let point = leaf_point(1);
        let digest = Digest::from(point) + Digest::from(leaf_point(2)) + Digest::from(leaf_point(3));
        let wire = digest.to_wire_bytes();
        assert_eq!(wire[..POINT_LEN], digest.point.compress().to_bytes());
        assert_eq!(wire[POINT_LEN..], 3i64.to_be_bytes());
        let row = digest.to_row_bytes();
        assert_eq!((row[0], &row[1..]), (ROW_V1, &wire[..]));
        let leaf_row = point.to_row_bytes();
        assert_eq!((leaf_row[0], &leaf_row[1..]), (ROW_V1, &point.0.compress().to_bytes()[..]));
    }

    #[test]
    fn the_empty_digest_is_all_zeros_on_the_wire() {
        // The canonical encoding of the identity is 32 zero bytes.
        assert_eq!(Digest::empty().to_wire_bytes(), [0u8; DIGEST_WIRE_LEN]);
        assert_eq!(Digest::from_wire_bytes(&[0u8; DIGEST_WIRE_LEN]), Ok(Digest::empty()));
    }

    #[test]
    fn every_decoder_refuses_short_and_trailing_bytes() {
        let point = leaf_point(1);
        let digest = Digest::from(point);
        // A public decoder, reduced to whether it accepts the bytes.
        type Decoder = fn(&[u8]) -> Result<(), DecodeError>;
        let layouts: [(&str, Vec<u8>, Decoder); 3] = [
            ("digest wire form", digest.to_wire_bytes().to_vec(), |bytes| Digest::from_wire_bytes(bytes).map(drop)),
            ("digest row", digest.to_row_bytes().to_vec(), |bytes| Digest::from_row_bytes(bytes).map(drop)),
            ("leaf point row", point.to_row_bytes().to_vec(), |bytes| LeafPoint::from_row_bytes(bytes).map(drop)),
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
        let point = leaf_point(1);
        for version in [0, 2, 0xff] {
            let mut digest_row = Digest::from(point).to_row_bytes();
            digest_row[0] = version;
            assert_eq!(Digest::from_row_bytes(&digest_row), Err(DecodeError::Version(version)));
            let mut leaf_point_row = point.to_row_bytes();
            leaf_point_row[0] = version;
            assert_eq!(LeafPoint::from_row_bytes(&leaf_point_row), Err(DecodeError::Version(version)));
        }
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
        let valid = leaf_point(1).0.compress().to_bytes();
        let mut high_bit_set = valid;
        high_bit_set[31] |= 0x80;
        assert!(Digest::from_wire_bytes(&wire_with_point(&valid)).is_ok());

        let rfc_encodings =
            INVALID_ENCODINGS.iter().flat_map(|(group, encodings)| encodings.iter().map(|encoding| (*group, hex(encoding))));
        for (group, point) in rfc_encodings.chain([("an otherwise valid encoding with its high bit set", high_bit_set.to_vec())]) {
            let wire = wire_with_point(&point);
            assert_eq!(Digest::from_wire_bytes(&wire), Err(DecodeError::Point), "{group}: {point:02x?}");
            assert_eq!(Digest::from_row_bytes(&[&[ROW_V1], &wire[..]].concat()), Err(DecodeError::Point), "{group}: {point:02x?}");
            assert_eq!(LeafPoint::from_row_bytes(&[&[ROW_V1], &point[..]].concat()), Err(DecodeError::Point), "{group}: {point:02x?}");
        }
    }

    /// The count is a signed 64-bit big-endian integer, and the decoders take
    /// any value, so arithmetic on a decoded count goes through the checked
    /// operations.
    #[test]
    fn rows_carry_negative_and_extreme_counts_exactly() {
        let point = leaf_point(1);
        let row_with_count = |count_bytes: &str| [&[ROW_V1], &point.0.compress().as_bytes()[..], &hex(count_bytes)].concat();
        for (count, count_bytes) in
            [(-1, "ffffffffffffffff"), (i64::MIN, "8000000000000000"), (i64::MIN + 1, "8000000000000001"), (i64::MAX, "7fffffffffffffff")]
        {
            let row = row_with_count(count_bytes);
            let digest = Digest::from_row_bytes(&row).expect("a canonical point and any count");
            assert_eq!(digest.count(), count);
            assert_eq!(digest.to_row_bytes()[..], row[..]);
            assert_eq!(Digest::from_wire_bytes(&row[1..]), Ok(digest));
        }

        let max = Digest::from_row_bytes(&row_with_count("7fffffffffffffff")).unwrap();
        let min = Digest::from_row_bytes(&row_with_count("8000000000000000")).unwrap();
        assert_eq!(max.checked_add(Digest::from(point)), Err(CountOverflow));
        assert_eq!(min.checked_sub(Digest::from(point)), Err(CountOverflow));
        assert_eq!(min.checked_neg(), Err(CountOverflow));
    }
}
