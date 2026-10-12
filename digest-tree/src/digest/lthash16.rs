//! LtHash16, the second group: 1,024 lanes of 16 bits, summed lane by lane.

use super::codec::{exact, DecodeError};
use super::group::DigestGroup;

/// The BLAKE3 key derivation context LtHash16 leaf elements are derived under,
/// which keeps them apart from every other use of BLAKE3.
///
/// Like [`LEAF_DST`](super::LEAF_DST), it names the application, the purpose
/// and a version, and the version changes with any change to
/// [`Leaf::encode`](super::Leaf::encode) or to how the hash's output becomes
/// lanes.
pub const LTHASH16_LEAF_CONTEXT: &str = "org.ankurah.digest.lthash16.leaf.v0";

/// How many lanes an element holds.
const LANES: usize = 1024;

/// LtHash16, in the shape of Facebook's (Lewi, Kim, Maykov and Weis,
/// "Securing Update Propagation with Homomorphic Hashing", 2019): an element is
/// 1,024 lanes of 16 bits, and the group adds, subtracts and negates lane by
/// lane, wrapping modulo 2^16, with every lane zero for the identity.
///
/// Adding one element 65,536 times therefore gives zero in every lane, while
/// a digest's count records 65,536 leaves: 65,536 copies of one leaf and
/// 65,536 copies of another have equal digests, zero lanes and equal counts.
/// A tree's sums never meet this, because a tree never repeats a leaf; the
/// crate documentation explains why the guarantee is for sets.
///
/// A leaf's element is BLAKE3 in its key derivation mode, under
/// [`LTHASH16_LEAF_CONTEXT`], over the encoded leaf: 2,048 bytes of its
/// extendable output, read as little-endian lanes. An element encodes as its
/// lanes, little-endian, in 2,048 bytes.
///
/// It is here to compare with ristretto255 and to keep the tree generic over
/// its group, not as a security recommendation: a 2026 preprint (IACR ePrint
/// 2026/2083) estimates that collisions for these parameters cost far less
/// than the 200 bits of security claimed for them.
///
/// Nothing validates an LtHash16 row. Every 2,048 bytes are an element, so
/// decoding checks the length alone, and only the audit, which recomputes a
/// tree from its leaves, catches a damaged row.
///
/// The type names the group as a type parameter, so it has no values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LtHash16 {}

impl DigestGroup for LtHash16 {
    type Element = [u16; LANES];

    /// Row codec version 2: the lanes behind the version byte.
    const ROW_VERSION: u8 = 2;

    const ELEMENT_LEN: usize = 2 * LANES;

    fn zero() -> [u16; LANES] { [0; LANES] }

    fn add(sum: &mut [u16; LANES], other: &[u16; LANES]) {
        sum.iter_mut().zip(other).for_each(|(lane, other)| *lane = lane.wrapping_add(*other))
    }

    fn sub(sum: &mut [u16; LANES], other: &[u16; LANES]) {
        sum.iter_mut().zip(other).for_each(|(lane, other)| *lane = lane.wrapping_sub(*other))
    }

    fn neg(element: &mut [u16; LANES]) { element.iter_mut().for_each(|lane| *lane = lane.wrapping_neg()) }

    fn hash_leaf(encoded_leaf: &[u8]) -> [u16; LANES] {
        let mut output = [0u8; 2 * LANES];
        blake3::Hasher::new_derive_key(LTHASH16_LEAF_CONTEXT).update(encoded_leaf).finalize_xof().fill(&mut output);
        lanes(&output)
    }

    fn encode(element: &[u16; LANES], out: &mut [u8]) {
        assert_eq!(out.len(), Self::ELEMENT_LEN, "an LtHash16 element encodes in 2,048 bytes");
        for (bytes, lane) in out.chunks_exact_mut(2).zip(element) {
            bytes.copy_from_slice(&lane.to_le_bytes());
        }
    }

    /// Every 2,048 bytes are an element, so only the length is checked.
    fn decode(bytes: &[u8]) -> Result<[u16; LANES], DecodeError> { Ok(lanes(exact(bytes, Self::ELEMENT_LEN)?)) }
}

/// The little-endian lanes of 2,048 bytes.
fn lanes(bytes: &[u8]) -> [u16; LANES] {
    let bytes: &[u8; 2 * LANES] = bytes.try_into().expect("2,048 bytes");
    std::array::from_fn(|i| u16::from_le_bytes([bytes[2 * i], bytes[2 * i + 1]]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::{CountOverflow, Digest, HeadHash, Leaf, LeafElement};
    use ankurah_core_types::EntityId;
    use proptest::prelude::*;
    use sha2::{Digest as _, Sha256};

    fn hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }

    /// Known answers for the leaves of `leaf_elements_match_known_answers` in
    /// leaf.rs, and for one under a key of 2,000 bytes counting up from zero
    /// and wrapping, whose encoding BLAKE3 hashes as three chunks: each
    /// element's encoding, pinned by its first 16 bytes and the SHA-256 of all
    /// 2,048. They come from a separate Python implementation of BLAKE3,
    /// written from its specification, which gives BLAKE3's published answer
    /// for the empty input.
    #[test]
    fn leaf_elements_match_known_answers() {
        let entity_id = EntityId::from_bytes(std::array::from_fn(|i| i as u8));
        let head = HeadHash::of([[0xaa; 32], [0x11; 32]]);
        let long_key: Vec<u8> = (0..2000).map(|i| i as u8).collect();
        let vectors: [(&[u8], HeadHash, &str, &str); 6] = [
            (b"blue\0", head, "35cf710bc3ed185478d32c10e7139084", "33f08f562d6ce039065b4f4199dcdbe1c1f336012e26ffd1959fee445c8b4f4e"),
            (b"", head, "b6f8535e16e583442a7846ef717971fc", "2a9781f0ce937a5f204c582db6cfc94bde087428e6d92890477cbaefeae3fa00"),
            (b"blue\0sky\0", head, "b54f9a78569e39eaff339c4e416a3b36", "5d4fe29ead98a52b30fc069c541024b385e4a6c22ab39765186b11f45d7b6f78"),
            (b"a\0\xffb\0", head, "b1fedef103b716476dd788aaec7c1b45", "6e6012246f2cf7651687e016d427b4f4fb761cf920bfdaea921d10de672ff803"),
            (
                b"blue\0",
                HeadHash::of([]),
                "76da592a9a7ee11187e7bd5ce4edbcf5",
                "ed7d6e9ef0730dd82c1762b60dcc31a853727f2f171835905c43c913d5742c84",
            ),
            (&long_key, head, "22fae8369028d094e67dc9c3993c313f", "21413812b240a5f369ce5368f9faa5aaadbf2a8d43c2f145bcc211bc92423928"),
        ];
        for (key, head, first_bytes, sha256) in vectors {
            let element: LeafElement<LtHash16> = Leaf::new(key, entity_id, head).element();
            let mut encoding = [0u8; 2 * LANES];
            LtHash16::encode(&element.0, &mut encoding);
            assert_eq!(hex(&encoding[..16]), first_bytes, "a key of {} bytes", key.len());
            assert_eq!(hex(&Sha256::digest(encoding)), sha256, "a key of {} bytes", key.len());
        }
    }

    /// Each lane adds, subtracts and negates modulo 2^16 on its own: nothing
    /// carries into the next lane.
    #[test]
    fn lanes_wrap_without_carrying() {
        let mut sum = LtHash16::zero();
        sum[..2].copy_from_slice(&[0xffff, 1]);
        let mut one = LtHash16::zero();
        one[0] = 1;
        LtHash16::add(&mut sum, &one);
        assert_eq!(sum[..3], [0, 1, 0]);
        LtHash16::sub(&mut sum, &one);
        assert_eq!(sum[..3], [0xffff, 1, 0]);
        LtHash16::neg(&mut sum);
        assert_eq!(sum[..3], [1, 0xffff, 0]);
    }

    /// Lanes are little-endian, and every 2,048 bytes decode, so a damaged
    /// row reads as another element.
    #[test]
    fn decoding_checks_the_length_alone() {
        assert_eq!(LtHash16::decode(&[1, 0].repeat(LANES)), Ok([1; LANES]));
        assert_eq!(LtHash16::decode(&[0xff; 2 * LANES]), Ok([0xffff; LANES]));
        assert_eq!(LtHash16::decode(&[0; 2 * LANES - 1]), Err(DecodeError::Length { expected: 2 * LANES, found: 2 * LANES - 1 }));

        let element: LeafElement<LtHash16> = Leaf::new(b"k", EntityId::from_bytes([1; 32]), HeadHash::of([[1; 32]])).element();
        let mut row = element.to_row_bytes();
        row[1] ^= 1;
        let damaged = LeafElement::<LtHash16>::from_row_bytes(&row).expect("any lanes decode");
        assert_ne!(damaged, element);
    }

    fn leaf_element(n: u8) -> LeafElement<LtHash16> { Leaf::new(&[n], EntityId::from_bytes([n; 32]), HeadHash::of([[n; 32]])).element() }

    /// Adding an element 65,536 times, here by doubling a leaf's digest sixteen
    /// times, gives zero in every lane while the count records every leaf, so
    /// 65,536 copies of one leaf and of another have equal digests.
    #[test]
    fn an_element_added_65_536_times_cancels_while_the_count_stays() {
        let repeated = |n: u8| {
            let mut digest = Digest::from(leaf_element(n));
            for _ in 0..16 {
                digest += digest;
            }
            digest
        };
        let (first, second) = (repeated(1), repeated(2));
        assert_eq!(first.element, LtHash16::zero());
        assert_eq!(first.count(), 65_536);
        assert_eq!(first, second, "two different leaves, each repeated 65,536 times");
    }

    /// Lanes anywhere in their range, with some at its edges.
    fn element() -> impl Strategy<Value = [u16; LANES]> {
        let edges = proptest::collection::vec((0..LANES, prop::sample::select(vec![0u16, 1, 0x7fff, 0x8000, 0xffff])), 0..64);
        (proptest::collection::vec(any::<u16>(), LANES), edges).prop_map(|(lanes, edges)| {
            let mut lanes: [u16; LANES] = lanes.try_into().expect("1,024 lanes");
            for (lane, edge) in edges {
                lanes[lane] = edge;
            }
            lanes
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Lane arithmetic matches a straightforward reference that works in
        /// 32 bits and reduces modulo 2^16 lane by lane.
        #[test]
        fn lane_arithmetic_matches_a_reference(a in element(), b in element()) {
            let reference = |lane: fn(u32, u32) -> u32| -> [u16; LANES] {
                std::array::from_fn(|i| (lane(u32::from(a[i]), u32::from(b[i])) % 65_536) as u16)
            };
            let mut sum = a;
            LtHash16::add(&mut sum, &b);
            prop_assert_eq!(sum, reference(|a, b| a + b));
            let mut difference = a;
            LtHash16::sub(&mut difference, &b);
            prop_assert_eq!(difference, reference(|a, b| a + 65_536 - b));
            let mut inverse = a;
            LtHash16::neg(&mut inverse);
            prop_assert_eq!(inverse, reference(|a, _| 65_536 - a));
        }
    }

    /// Rows carry any count exactly, beside lanes at the
    /// edges of their range, and a decoded count at a limit refuses to pass it.
    #[test]
    fn rows_carry_extreme_counts_exactly() {
        let lanes: [u16; LANES] = std::array::from_fn(|i| [0xffff, 0, 1, 0x8000][i % 4]);
        for count in [0, 1, -1, i64::MIN, i64::MIN + 1, i64::MAX] {
            let digest = Digest::<LtHash16> { element: lanes, count };
            let row = digest.to_row_bytes();
            assert_eq!((row.len(), row[0]), (Digest::<LtHash16>::ROW_LEN, LtHash16::ROW_VERSION));
            assert_eq!(row[1 + 2 * LANES..], count.to_be_bytes());
            assert_eq!(Digest::<LtHash16>::from_row_bytes(&row), Ok(digest), "{count}");
        }
        let one = Digest::from(LeafElement::<LtHash16>(lanes));
        let at = |count| Digest::<LtHash16>::from_row_bytes(&Digest::<LtHash16> { element: lanes, count }.to_row_bytes()).unwrap();
        assert_eq!(at(i64::MAX).checked_add(one), Err(CountOverflow));
        assert_eq!(at(i64::MIN).checked_sub(one), Err(CountOverflow));
        assert_eq!(at(i64::MIN).checked_neg(), Err(CountOverflow));
    }
}
