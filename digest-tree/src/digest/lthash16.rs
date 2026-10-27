//! LtHash16, the second group: 1,024 lanes of 16 bits, summed lane by lane.

use super::codec::{exact, DecodeError};
use super::group::GroupOperations;
use super::DigestGroup;

/// The BLAKE3 key derivation context LtHash16 leaf elements are derived under,
/// which keeps them apart from every other use of BLAKE3. Like ristretto255's
/// tag, it names the application, the purpose and a version, and the version
/// changes with any change to the leaf encoding or to how the hash's output
/// becomes lanes.
const LEAF_CONTEXT: &str = "org.ankurah.digest.lthash16.leaf.v0";

/// How many lanes an element holds.
const LANES: usize = 1024;

/// LtHash16, in the shape of Facebook's (Lewi, Kim, Maykov and Weis,
/// "Securing Update Propagation with Homomorphic Hashing", 2019): an element is
/// 1,024 lanes of 16 bits, and the group adds and subtracts lane by lane,
/// wrapping modulo 2^16, with every lane zero for the identity.
///
/// Adding one element 65,536 times therefore gives zero in every lane, while
/// a digest's count records 65,536 members. A tree's sums never meet this,
/// because a tree never repeats a leaf; the [module documentation](super)
/// explains why the guarantee is for sets.
///
/// A leaf's element is BLAKE3 in its key derivation mode, under the context
/// `org.ankurah.digest.lthash16.leaf.v0`, over the encoded leaf: 2,048 bytes
/// of its extendable output, read as little-endian lanes. An element encodes
/// as its lanes, little-endian, in 2,048 bytes.
///
/// It is here to compare with ristretto255 and to keep the tree generic over
/// its group, not as a security recommendation: a 2026 preprint (IACR ePrint
/// 2026/2083) estimates that collisions for these parameters cost far less
/// than the 200 bits of security claimed for them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LtHash16;

/// An element of LtHash16: a member's element, or the sum beneath a subtree,
/// as 1,024 lanes of 16 bits. It is opaque outside the crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LtHash16Element(pub(crate) [u16; LANES]);

impl DigestGroup for LtHash16 {
    type Element = LtHash16Element;
}

impl GroupOperations<LtHash16Element> for LtHash16 {
    const ROW_VERSION: u8 = 2;

    const ELEMENT_LEN: usize = 2 * LANES;

    fn zero() -> LtHash16Element { LtHash16Element([0; LANES]) }

    fn add(sum: &mut LtHash16Element, other: &LtHash16Element) {
        sum.0.iter_mut().zip(&other.0).for_each(|(lane, other)| *lane = lane.wrapping_add(*other))
    }

    fn sub(sum: &mut LtHash16Element, other: &LtHash16Element) {
        sum.0.iter_mut().zip(&other.0).for_each(|(lane, other)| *lane = lane.wrapping_sub(*other))
    }

    fn hash_leaf(encoded_leaf: &[u8]) -> LtHash16Element {
        let mut output = [0u8; 2 * LANES];
        blake3::Hasher::new_derive_key(LEAF_CONTEXT).update(encoded_leaf).finalize_xof().fill(&mut output);
        lanes(&output)
    }

    fn encode(element: &LtHash16Element, out: &mut [u8]) {
        assert_eq!(out.len(), Self::ELEMENT_LEN, "an LtHash16 element encodes in 2,048 bytes");

        for (bytes, lane) in out.as_chunks_mut::<2>().0.iter_mut().zip(&element.0) {
            *bytes = lane.to_le_bytes();
        }
    }

    /// Every 2,048 bytes are an element, so only the length is checked.
    fn decode(bytes: &[u8]) -> Result<LtHash16Element, DecodeError> { Ok(lanes(exact(bytes, Self::ELEMENT_LEN)?)) }
}

/// The element whose little-endian lanes are these bytes, two per lane.
fn lanes(bytes: &[u8]) -> LtHash16Element {
    let (pairs, _) = bytes.as_chunks::<2>();
    LtHash16Element(std::array::from_fn(|i| u16::from_le_bytes(pairs[i])))
}

#[cfg(test)]
mod tests;
