use super::*;
use rand::{rngs::SmallRng, Rng, SeedableRng};

fn random_prefix(rng: &mut SmallRng) -> NodePrefix {
    // Short lengths make shared prefixes, and so the interesting
    // comparisons, common.
    let len = rng.gen_range(0..=40);
    let address: Vec<u8> = (0..5).map(|_| rng.gen::<u8>() & 0xC3).collect();
    NodePrefix::of(&address, len)
}

#[test]
fn byte_order_is_prefix_order() {
    let mut rng = SmallRng::seed_from_u64(7);
    let prefixes: Vec<NodePrefix> = (0..400).map(|_| random_prefix(&mut rng)).collect();
    for a in &prefixes {
        assert_eq!(NodePrefix::from_bytes(&a.to_bytes()).as_ref(), Ok(a), "round trip");
        for b in &prefixes {
            assert_eq!(a.cmp(b), a.to_bytes().cmp(&b.to_bytes()), "{a:?} against {b:?}");
        }
    }
}

#[test]
fn a_prefix_precedes_its_subtree_and_the_subtree_is_contiguous() {
    let mut rng = SmallRng::seed_from_u64(11);
    let prefixes: Vec<NodePrefix> = (0..400).map(|_| random_prefix(&mut rng)).collect();
    for a in &prefixes {
        let after = a.after_subtree();
        for b in &prefixes {
            let inside = *a <= *b && after.as_ref().is_none_or(|after| b < after);
            assert_eq!(inside, a.contains(b), "{a:?} against {b:?}");
        }
    }
}

/// A subtree's key end lies past every string that begins with the
/// encoding of the prefix or of one beneath it, before any string that
/// begins with an ancestor's encoding and the byte 255 and follows the
/// subtree, and no later than the next prefix's encoding.
#[test]
fn the_subtree_key_end_follows_the_strings_the_subtree_begins() {
    let mut rng = SmallRng::seed_from_u64(29);
    for _ in 0..1000 {
        // Runs of ones make prefixes that end an ancestor's subtree common.
        let address: Vec<u8> = (0..12).map(|_| [0x00, 0xff, rng.gen()][rng.gen_range(0..3)]).collect();
        let ancestor_len = rng.gen_range(0..=60);
        let prefix_len = rng.gen_range(ancestor_len + 1..=80);
        let descendant_len = rng.gen_range(prefix_len + 1..=96);
        let (ancestor, prefix) = (NodePrefix::of(&address, ancestor_len), NodePrefix::of(&address, prefix_len));
        let descendant = NodePrefix::of(&address, descendant_len);
        let tail: Vec<u8> = (0..rng.gen_range(0..4)).map(|_| rng.gen()).collect();
        let end = prefix.subtree_key_end();
        let before_end = |bytes: &[u8]| end.as_ref().is_none_or(|end| bytes < end.as_slice());
        for begins in [prefix.to_bytes(), descendant.to_bytes()] {
            for string in [begins.clone(), [&begins[..], &[255], &tail].concat(), [&begins[..], &tail].concat()] {
                assert!(string >= prefix.to_bytes() && before_end(&string), "{prefix:?} over {descendant:?}");
            }
        }
        let past_ancestor = [&ancestor.to_bytes()[..], &[255], &tail].concat();
        assert!(past_ancestor < prefix.to_bytes() || !before_end(&past_ancestor), "{ancestor:?} above {prefix:?}");
        if let Some(after) = prefix.after_subtree() {
            assert!(end.is_some_and(|end| end <= after.to_bytes()), "{prefix:?}");
        }
    }
    assert_eq!(NodePrefix::root().subtree_key_end(), None);
}

/// Whether `address` lies within the bounds of `prefix`.
fn admits(prefix: &NodePrefix, address: &[u8]) -> bool {
    let (start, end) = prefix.address_bounds();
    address >= start.as_slice() && end.is_none_or(|end| address < end.as_slice())
}

#[test]
fn address_bounds_hold_exactly_the_contained_addresses() {
    let mut rng = SmallRng::seed_from_u64(13);
    for _ in 0..200 {
        let prefix = random_prefix(&mut rng);
        for _ in 0..50 {
            let len = rng.gen_range(0..=6);
            let address: Vec<u8> = (0..len).map(|_| rng.gen::<u8>() & 0xC3).collect();
            assert_eq!(admits(&prefix, &address), prefix.contains_address(&address), "{prefix:?} against {address:?}");
        }
    }
}

/// Every prefix of up to sixteen bits against every address of up to two
/// bytes: a prefix's bounds hold exactly the addresses that begin with its
/// bits, judged by a bit test of its own rather than by `NodePrefix`.
#[test]
fn address_bounds_are_exact_through_sixteen_bits() {
    let mut addresses: Vec<Vec<u8>> = vec![Vec::new()];
    addresses.extend((0..=u8::MAX).map(|byte| vec![byte]));
    addresses.extend((0..=u16::MAX).map(|bytes| bytes.to_be_bytes().to_vec()));
    addresses.sort();
    let bits_of = |address: &[u8]| (8 * address.len() as u32, address.iter().fold(0u32, |value, &byte| value << 8 | u32::from(byte)));
    // The d-bit prefix p sits at 2^d - 1 + p, so every prefix through
    // sixteen bits has a slot.
    let slot = |depth: u32, value: u32| (1usize << depth) - 1 + value as usize;
    let mut beginning_with = vec![0usize; slot(17, 0)];
    for address in &addresses {
        let (len, value) = bits_of(address);
        for depth in 0..=len {
            beginning_with[slot(depth, value >> (len - depth))] += 1;
        }
    }
    for depth in 0..=16 {
        for value in 0..1u32 << depth {
            let prefix = NodePrefix::of(&((value << (16 - depth)) as u16).to_be_bytes(), depth);
            let (start, end) = prefix.address_bounds();
            let from = addresses.partition_point(|address| *address < start);
            let to = end.map_or(addresses.len(), |end| addresses.partition_point(|address| *address < end));
            // The bounds hold a run of the sorted addresses. Each one in it
            // begins with the prefix, and the run is as long as the count
            // of such addresses, so no address outside it does.
            for address in &addresses[from..to] {
                let (len, bits) = bits_of(address);
                assert!(len >= depth && bits >> (len - depth) == value, "{prefix:?} admits {address:02x?}");
            }
            assert_eq!(to - from, beginning_with[slot(depth, value)], "{prefix:?} misses an address beneath it");
        }
    }
}

/// Bounds where the exhaustive check does not reach: a carry across whole
/// bytes, prefixes of all ones, chunk edges, and addresses as long as real
/// leaves (a key, then a 32-byte entity id).
#[test]
fn address_bounds_at_the_edges() {
    let bits = |text: &str| text.chars().filter(|c| *c != ' ').fold(NodePrefix::root(), |prefix, bit| prefix.child(bit == '1'));
    let hex = |text: &str| text.split(' ').map(|byte| u8::from_str_radix(byte, 16).unwrap()).collect::<Vec<u8>>();
    let check = check_bounds;
    let zeros = |n: usize| vec![0u8; n];

    check(NodePrefix::root(), (vec![], None), &[vec![], hex("ff"), vec![0xFF; 33]], &[]);
    // The review's counterexamples: a carry used to leave a zero byte in
    // the end bound, admitting shorter addresses.
    check(
        bits("0000 0000 1"),
        (hex("00 80"), Some(hex("01"))),
        &[hex("00 80"), hex("00 ff ff"), hex("00 80 00 00 00 00")],
        &[hex("01"), hex("00"), hex("00 7f ff"), hex("01 00")],
    );
    let deep = NodePrefix::of(&[zeros(33), hex("80")].concat(), 265);
    check(
        deep,
        ([zeros(33), hex("80")].concat(), Some([zeros(32), hex("01")].concat())),
        &[[zeros(33), hex("80")].concat(), [zeros(33), vec![0xFF; 32]].concat()],
        // The empty key's encoding 00, then the entity id 00 x 31 01.
        &[[zeros(32), hex("01")].concat(), [zeros(33), hex("7f")].concat()],
    );
    // All ones: nothing follows the subtree.
    check(bits("1"), (hex("80"), None), &[hex("80"), hex("ff ff")], &[hex("7f ff"), vec![]]);
    check(bits("1111 111"), (hex("fe"), None), &[hex("fe"), hex("ff")], &[hex("fd ff")]);
    check(bits("1111 1111"), (hex("ff"), None), &[hex("ff"), vec![0xFF; 6]], &[hex("fe ff")]);
    check(bits("1111 1111 1111 1111"), (hex("ff ff"), None), &[hex("ff ff"), vec![0xFF; 33]], &[hex("ff"), hex("ff fe ff")]);
    // Around one encoded chunk (seven bits) and one byte.
    check(bits("0000 000"), (hex("00"), Some(hex("02"))), &[hex("00"), hex("01 ff")], &[hex("02"), vec![]]);
    check(bits("0000 001"), (hex("02"), Some(hex("04"))), &[hex("03 ff")], &[hex("01 ff"), hex("04")]);
    check(bits("0000 0001"), (hex("01"), Some(hex("02"))), &[hex("01"), hex("01 00")], &[hex("00 ff"), hex("02")]);
    check(bits("0000 0001 1"), (hex("01 80"), Some(hex("02"))), &[hex("01 80"), hex("01 ff ff")], &[hex("01"), hex("01 7f"), hex("02")]);
    // Around two chunks (fourteen bits) and two bytes.
    check(
        bits("0000 0000 1111 11"),
        (hex("00 fc"), Some(hex("01"))),
        &[hex("00 fc"), hex("00 ff ff")],
        &[hex("00"), hex("00 fb ff"), hex("01")],
    );
    check(bits("0000 0000 1111 111"), (hex("00 fe"), Some(hex("01"))), &[hex("00 fe 00")], &[hex("00 fd"), hex("01")]);
    check(
        bits("0000 0000 1111 1111"),
        (hex("00 ff"), Some(hex("01"))),
        &[hex("00 ff"), hex("00 ff 00")],
        &[hex("00"), hex("00 fe ff"), hex("01")],
    );
    check(
        bits("0000 0001 1111 1111 1"),
        (hex("01 ff 80"), Some(hex("02"))),
        &[hex("01 ff 80"), hex("01 ff ff ff ff ff")],
        &[hex("01 ff"), hex("01 ff 7f ff ff ff"), hex("02")],
    );
}

/// Check a prefix's bounds, and that exactly the addresses `inside` lie
/// within them, judged by the bounds and by `contains_address` alike.
fn check_bounds(prefix: NodePrefix, bounds: (Vec<u8>, Option<Vec<u8>>), inside: &[Vec<u8>], outside: &[Vec<u8>]) {
    assert_eq!(prefix.address_bounds(), bounds, "{prefix:?}");
    for address in inside {
        assert!(admits(&prefix, address) && prefix.contains_address(address), "{prefix:?} holds {address:02x?}");
    }
    for address in outside {
        assert!(!admits(&prefix, address) && !prefix.contains_address(address), "{prefix:?} excludes {address:02x?}");
    }
}

#[test]
fn encodings_name_the_seven_level_walk() {
    let bits = |text: &str| text.chars().fold(NodePrefix::root(), |prefix, bit| prefix.child(bit == '1'));
    assert_eq!(bits("0").to_bytes(), [0]);
    assert_eq!(bits("00").to_bytes(), [1]);
    assert_eq!(bits("0000001").to_bytes(), [7]);
    assert_eq!(bits("1").to_bytes(), [127]);
    assert_eq!(bits("1111111").to_bytes(), [253]);
    assert_eq!(bits("11111110").to_bytes(), [253, 0]);
    assert_eq!(NodePrefix::from_bytes(&[254]), Err(NodePrefixError::InvalidByte(254)));
    assert_eq!(NodePrefix::from_bytes(&[0, 0]), Err(NodePrefixError::ShortChunkBeforeEnd));
}

/// A subtree's key end, made from the prefix's encoding alone, is one past
/// the encoding of the last prefix beneath it within its last chunk: the
/// prefix extended with ones to the chunk's end, its last byte raised by one.
#[test]
fn the_subtree_key_end_is_one_past_the_last_prefix_in_the_chunk() {
    let mut prefixes = vec![NodePrefix::root()];
    let mut next = 0;
    while let Some(prefix) = prefixes.get(next).cloned() {
        next += 1;
        if prefix.bit_len() < 15 {
            prefixes.extend([prefix.child(false), prefix.child(true)]);
        }
    }
    for prefix in &prefixes[1..] {
        let mut last = prefix.clone();
        while !last.bit_len().is_multiple_of(CHUNK) {
            last = last.child(true);
        }
        let mut expected = last.to_bytes();
        *expected.last_mut().unwrap() += 1;
        assert_eq!(prefix.subtree_key_end(), Some(expected), "{prefix:?}");
    }
}
