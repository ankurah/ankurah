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
    /// a canonical encoding.
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
    /// point bytes that are not a canonical encoding.
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
    use crate::{HeadHash, Leaf};
    use ankurah_core_types::EntityId;

    fn leaf_point(n: u8) -> LeafPoint { Leaf::new(&[n], EntityId::from_bytes([n; 32]), HeadHash::of([[n; 32]])).point() }

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
    fn decoding_refuses_wrong_lengths_unknown_versions_and_non_canonical_points() {
        let row = Digest::from(leaf_point(1)).to_row_bytes();
        assert_eq!(Digest::from_row_bytes(&row[..40]), Err(DecodeError::Length { expected: DIGEST_ROW_LEN, found: 40 }));
        assert_eq!(Digest::from_wire_bytes(&row), Err(DecodeError::Length { expected: DIGEST_WIRE_LEN, found: 41 }));
        assert_eq!(LeafPoint::from_row_bytes(&[]), Err(DecodeError::Length { expected: LEAF_POINT_ROW_LEN, found: 0 }));

        let mut unknown = row;
        unknown[0] = 2;
        assert_eq!(Digest::from_row_bytes(&unknown), Err(DecodeError::Version(2)));

        // RFC 9496 section 4.3.1 refuses a field element of p or more, which is
        // not canonical, and a negative one, which is odd.
        let mut odd = [0u8; POINT_LEN];
        odd[0] = 1;
        for point in [[0xff; POINT_LEN], odd] {
            let mut wire = [0u8; DIGEST_WIRE_LEN];
            wire[..POINT_LEN].copy_from_slice(&point);
            assert_eq!(Digest::from_wire_bytes(&wire), Err(DecodeError::Point));
            let mut leaf_row = [ROW_V1; LEAF_POINT_ROW_LEN];
            leaf_row[1..].copy_from_slice(&point);
            assert_eq!(LeafPoint::from_row_bytes(&leaf_row), Err(DecodeError::Point));
        }
    }
}
