//! ristretto255, the default group: leaves hash to it with RFC 9380's
//! `hash_to_ristretto255`, and its elements encode as RFC 9496's canonical 32
//! bytes.

use curve25519_dalek::ristretto::{CompressedRistretto, RistrettoPoint};
use curve25519_dalek::traits::Identity;
use sha2::{Digest as _, Sha512};

use super::codec::{exact, DecodeError};
use super::group::GroupOperations;
use super::DigestGroup;

/// The domain separation tag ristretto255 leaf elements are hashed under (RFC
/// 9380 section 3.1), keeping them apart from every other use of the same
/// hash.
///
/// It names the application, the purpose and a version. The version changes
/// with any change to the leaf encoding or to the hash-to-curve suite
/// (`ristretto255_XMD:SHA-512_R255MAP_RO_`), so one tag never names two
/// constructions.
const LEAF_DST: &[u8] = b"org.ankurah.digest.leaf.v0";

/// The prime-order group ristretto255 (RFC 9496), the default
/// [`DigestGroup`]: a leaf hashes to it with RFC 9380's `hash_to_ristretto255`
/// under the tag `org.ankurah.digest.leaf.v0`, and an element encodes as its
/// canonical 32 bytes (RFC 9496 section 4.3.2), which decoding demands. The
/// [module documentation](super) states the security it gives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ristretto255;

/// An element of ristretto255: a member's element, or the sum beneath a
/// subtree. It is opaque outside the crate, so the curve library the crate
/// computes with is no part of its public API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ristretto255Element(pub(crate) RistrettoPoint);

impl DigestGroup for Ristretto255 {
    type Element = Ristretto255Element;
}

impl GroupOperations<Ristretto255Element> for Ristretto255 {
    const ROW_VERSION: u8 = 1;

    const ELEMENT_LEN: usize = 32;

    fn zero() -> Ristretto255Element { Ristretto255Element(RistrettoPoint::identity()) }

    fn add(sum: &mut Ristretto255Element, other: &Ristretto255Element) { sum.0 += other.0 }

    fn sub(sum: &mut Ristretto255Element, other: &Ristretto255Element) { sum.0 -= other.0 }

    fn hash_leaf(encoded_leaf: &[u8]) -> Ristretto255Element { Ristretto255Element(hash_to_ristretto255(encoded_leaf, LEAF_DST)) }

    fn encode(element: &Ristretto255Element, out: &mut [u8]) { out.copy_from_slice(element.0.compress().as_bytes()) }

    /// Decompression accepts only canonical encodings of group elements, so
    /// it refuses any other 32 bytes.
    fn decode(bytes: &[u8]) -> Result<Ristretto255Element, DecodeError> {
        let bytes = exact(bytes, Self::ELEMENT_LEN)?;
        CompressedRistretto::from_slice(bytes)
            .ok()
            .and_then(|encoding| encoding.decompress())
            .map(Ristretto255Element)
            .ok_or(DecodeError::Element)
    }
}

/// SHA-512's output size in bytes, `b_in_bytes` in RFC 9380 section 5.3.1.
const B_IN_BYTES: usize = 64;

/// SHA-512's input block size in bytes, `s_in_bytes` in RFC 9380 section 5.3.1.
const S_IN_BYTES: usize = 128;

/// Hash `msg` under the domain separation tag `dst` to a ristretto255 element
/// with RFC 9380's `hash_to_ristretto255` (appendix B), the random-oracle hash
/// leaf elements are made with: expand_message_xmd with SHA-512 (section
/// 5.3.1) stretches `msg` to 64 uniform bytes, and the RFC 9496 element
/// derivation (section 4.3.4, `RistrettoPoint::from_uniform_bytes`) maps them
/// to the group.
fn hash_to_ristretto255(msg: &[u8], dst: &[u8]) -> RistrettoPoint {
    let mut uniform_bytes = [0u8; 64];
    expand_message_xmd_sha512(msg, dst, &mut uniform_bytes);
    RistrettoPoint::from_uniform_bytes(&uniform_bytes)
}

/// RFC 9380 section 5.3.1 expand_message_xmd with SHA-512, filling `out`
/// (whose length is `len_in_bytes`).
///
/// # Panics
///
/// If `dst` is longer than 255 bytes, which RFC 9380 section 5.3.3 would hash
/// down first and no tag here needs, or if `out` is longer than the 255 blocks
/// the expansion can produce.
fn expand_message_xmd_sha512(msg: &[u8], dst: &[u8], out: &mut [u8]) {
    let dst_len = u8::try_from(dst.len()).expect("domain separation tags are at most 255 bytes");
    assert!(out.len().div_ceil(B_IN_BYTES) <= 255, "expand_message_xmd produces at most 255 blocks");

    // 255 blocks of 64 bytes fit the two bytes RFC 9380 gives the length.
    let len_in_bytes = out.len() as u16;

    // DST_prime = DST || I2OSP(len(DST), 1)
    let dst_prime = |hasher: &mut Sha512| {
        hasher.update(dst);
        hasher.update([dst_len]);
    };

    // b_0 = H(Z_pad || msg || l_i_b_str || I2OSP(0, 1) || DST_prime)
    let mut hasher = Sha512::new();
    hasher.update([0u8; S_IN_BYTES]);
    hasher.update(msg);
    hasher.update(len_in_bytes.to_be_bytes());
    hasher.update([0u8]);
    dst_prime(&mut hasher);
    let b_0: [u8; B_IN_BYTES] = hasher.finalize().into();

    // b_1 = H(b_0 || I2OSP(1, 1) || DST_prime), and for i > 1
    // b_i = H(strxor(b_0, b_(i-1)) || I2OSP(i, 1) || DST_prime);
    // the output is b_1 || b_2 || ..., truncated to len_in_bytes.
    let mut b_prev = [0u8; B_IN_BYTES];
    for (index, chunk) in out.chunks_mut(B_IN_BYTES).enumerate() {
        let mut hasher = Sha512::new();
        if index == 0 {
            hasher.update(b_0);
        } else {
            let mut xored = [0u8; B_IN_BYTES];
            for (x, (a, b)) in xored.iter_mut().zip(b_0.iter().zip(b_prev.iter())) {
                *x = a ^ b;
            }

            hasher.update(xored);
        }

        hasher.update([index as u8 + 1]);
        dst_prime(&mut hasher);
        b_prev = hasher.finalize().into();
        chunk.copy_from_slice(&b_prev[..chunk.len()]);
    }
}

#[cfg(test)]
mod tests;
