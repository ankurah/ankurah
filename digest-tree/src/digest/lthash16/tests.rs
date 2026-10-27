use super::*;
use crate::digest::codec::{decode_stored_element, encode_stored_element};
use crate::digest::{leaf_encoding, Digest};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

fn element(n: u8) -> LtHash16Element { LtHash16::hash_leaf(&leaf_encoding(&[n], &[n; 16], &[n; 8])) }

/// Each lane adds and subtracts modulo 2^16 on its own: nothing carries into
/// the next lane.
#[test]
fn lanes_wrap_without_carrying() {
    let mut sum = LtHash16::zero();
    sum.0[..2].copy_from_slice(&[0xffff, 1]);

    let mut one = LtHash16::zero();
    one.0[0] = 1;
    LtHash16::add(&mut sum, &one);

    assert_eq!(sum.0[..3], [0, 1, 0]);
    LtHash16::sub(&mut sum, &one);

    assert_eq!(sum.0[..3], [0xffff, 1, 0]);
}

/// Lanes are little-endian, and every 2,048 bytes decode, so a damaged
/// stored element reads as another element.
#[test]
fn decoding_checks_the_length_alone() {
    assert_eq!(LtHash16::decode(&[1, 0].repeat(LANES)), Ok(LtHash16Element([1; LANES])));
    assert_eq!(LtHash16::decode(&[0xff; 2 * LANES]), Ok(LtHash16Element([0xffff; LANES])));
    assert_eq!(LtHash16::decode(&[0; 2 * LANES - 1]), Err(DecodeError::Length { expected: 2 * LANES, found: 2 * LANES - 1 }));

    let element = element(1);
    let mut stored = Vec::new();
    encode_stored_element::<LtHash16>(&element, &mut stored);
    stored[1] ^= 1;

    let damaged = decode_stored_element::<LtHash16>(&stored).expect("any lanes decode");
    assert_ne!(damaged, element);
}

/// Adding an element 65,536 times, here by doubling a leaf's digest sixteen
/// times, gives zero in every lane while the count records every member, so
/// 65,536 copies of one leaf and of another have equal digests.
#[test]
fn an_element_added_65_536_times_cancels_while_the_count_stays() {
    let repeated = |n: u8| {
        let mut digest = Digest::<LtHash16>::of_element(element(n));
        for _ in 0..16 {
            let copy = digest;
            digest.add(&copy).unwrap();
        }

        digest
    };

    let (first, second) = (repeated(1), repeated(2));
    assert_eq!(first, Digest { element: LtHash16::zero(), count: 65_536 });
    assert_eq!(first, second, "two different leaves, each repeated 65,536 times");
}

/// Lanes anywhere in their range, with some at its edges.
fn lanes_strategy() -> impl Strategy<Value = [u16; LANES]> {
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
    #![proptest_config(ProptestConfig { cases: 64, rng_seed: RngSeed::Fixed(0x1a4e_0016), ..ProptestConfig::default() })]

    /// Lane arithmetic matches a straightforward reference that works in 32
    /// bits and reduces modulo 2^16 lane by lane.
    #[test]
    fn lane_arithmetic_matches_a_reference(a in lanes_strategy(), b in lanes_strategy()) {
        let reference = |lane: fn(u32, u32) -> u32| -> [u16; LANES] {
            std::array::from_fn(|i| (lane(u32::from(a[i]), u32::from(b[i])) % 65_536) as u16)
        };

        let mut sum = LtHash16Element(a);
        LtHash16::add(&mut sum, &LtHash16Element(b));
        prop_assert_eq!(sum.0, reference(|a, b| a + b));

        let mut difference = LtHash16Element(a);
        LtHash16::sub(&mut difference, &LtHash16Element(b));

        prop_assert_eq!(difference.0, reference(|a, b| a + 65_536 - b));
    }
}
