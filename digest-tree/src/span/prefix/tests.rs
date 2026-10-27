use super::*;
use crate::fixtures::from_hex;
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

/// Prefixes of up to 40 bits of addresses whose bytes keep only their top
/// and bottom two bits, so that shared prefixes, and the comparisons that
/// matter, are common.
fn prefix() -> impl Strategy<Value = NodePrefix> {
    (proptest::collection::vec(any::<u8>().prop_map(|byte| byte & 0xC3), 5), 0..=40u32)
        .prop_map(|(address, bits)| NodePrefix::of(&address, bits))
}

/// Addresses of up to six such bytes.
fn address() -> impl Strategy<Value = Vec<u8>> { proptest::collection::vec(any::<u8>().prop_map(|byte| byte & 0xC3), 0..=6) }

/// The prefix spelled by `text`, one character per bit, spaces ignored.
fn bits(text: &str) -> NodePrefix { text.chars().filter(|c| *c != ' ').fold(NodePrefix::root(), |prefix, bit| prefix.child(bit == '1')) }

proptest! {
    #![proptest_config(ProptestConfig { cases: 2048, rng_seed: RngSeed::Fixed(0x9e1f_0001), ..ProptestConfig::default() })]

    #[test]
    fn byte_order_is_prefix_order(a in prefix(), b in prefix()) {
        prop_assert_eq!(NodePrefix::from_bytes(&a.to_bytes()), Ok(a.clone()), "round trip");
        prop_assert_eq!(a.cmp(&b), a.to_bytes().cmp(&b.to_bytes()), "{:?} against {:?}", a, b);
    }

    /// A prefix comes before every prefix it contains, and they all come
    /// before the first prefix after its subtree.
    #[test]
    fn a_prefix_precedes_its_subtree_and_the_subtree_is_contiguous(a in prefix(), b in prefix()) {
        let after = a.after_subtree();
        let inside = a <= b && after.as_ref().is_none_or(|after| b < *after);

        prop_assert_eq!(inside, a.contains(&b), "{:?} against {:?}", a, b);
    }

    #[test]
    fn the_span_of_a_prefix_holds_exactly_the_addresses_it_contains(prefix in prefix(), address in address()) {
        prop_assert_eq!(prefix.span().contains(&address), prefix.contains_address(&address), "{:?} against {:02x?}", prefix, address);
    }

    /// Siblings differ in their last bit alone and share their parent's span.
    #[test]
    fn a_sibling_differs_in_its_last_bit(prefix in prefix()) {
        match prefix.sibling() {
            None => prop_assert_eq!(prefix.bit_len(), 0),

            Some(sibling) => {
                let last = prefix.bit_len() - 1;
                prop_assert_eq!(sibling.bit(last), !prefix.bit(last));
                prop_assert_eq!(sibling.sibling(), Some(prefix.clone()));

                let parent = NodePrefix::of(&prefix.bytes, last);
                prop_assert!(parent.contains(&prefix) && parent.contains(&sibling));
            }
        }
    }
}

/// Every prefix of up to sixteen bits against every address of up to two
/// bytes: a prefix's span holds exactly the addresses that begin with its
/// bits, judged by a bit test of its own rather than by `NodePrefix`.
#[test]
fn spans_are_exact_through_sixteen_bits() {
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

    // Where each bound falls among the sorted addresses: the first address a
    // start admits, and the first an end refuses.
    let first_admitted = |start: &Bound<Vec<u8>>| match start {
        Bound::Unbounded => 0,
        Bound::Included(at) => addresses.partition_point(|address| address < at),
        Bound::Excluded(at) => addresses.partition_point(|address| address <= at),
    };

    let first_refused = |end: &Bound<Vec<u8>>| match end {
        Bound::Unbounded => addresses.len(),
        Bound::Included(at) => addresses.partition_point(|address| address <= at),
        Bound::Excluded(at) => addresses.partition_point(|address| address < at),
    };

    for depth in 0..=16 {
        for value in 0..1u32 << depth {
            let prefix = NodePrefix::of(&((value << (16 - depth)) as u16).to_be_bytes(), depth);
            let span = prefix.span();
            let (from, to) = (first_admitted(&span.start), first_refused(&span.end));

            // The span holds a run of the sorted addresses. Each one in it
            // begins with the prefix, and the run is as long as the count of
            // such addresses, so no address outside it does.
            for address in &addresses[from..to] {
                let (len, bits) = bits_of(address);
                assert!(len >= depth && bits >> (len - depth) == value, "{prefix:?} admits {address:02x?}");
            }

            assert_eq!(to - from, beginning_with[slot(depth, value)], "{prefix:?} misses an address beneath it");
        }
    }
}

/// Check a prefix's span, and that exactly the addresses `inside` lie within
/// it, judged by the span and by `contains_address` alike.
fn check_span(prefix: NodePrefix, span: (&str, Option<&str>), inside: &[Vec<u8>], outside: &[Vec<u8>]) {
    let expected = Span::new(Bound::Included(from_hex(span.0)), span.1.map_or(Bound::Unbounded, |end| Bound::Excluded(from_hex(end))));
    assert_eq!(prefix.span(), expected, "{prefix:?}");

    for address in inside {
        assert!(prefix.span().contains(address) && prefix.contains_address(address), "{prefix:?} holds {address:02x?}");
    }

    for address in outside {
        assert!(!prefix.span().contains(address) && !prefix.contains_address(address), "{prefix:?} excludes {address:02x?}");
    }
}

/// Spans where the exhaustive check does not reach: a carry across whole
/// bytes, prefixes of all ones, chunk edges, and addresses as long as real
/// leaves.
#[test]
fn spans_at_the_edges() {
    let zeros = |n: usize| vec![0u8; n];
    assert_eq!(NodePrefix::root().span(), Span::all(), "the root holds every address");

    // A carry must not leave a zero byte at the end of the bound, which would
    // admit shorter addresses.
    check_span(
        bits("0000 0000 1"),
        ("00 80", Some("01")),
        &[from_hex("00 80"), from_hex("00 ff ff"), from_hex("00 80 00 00 00 00")],
        &[from_hex("01"), from_hex("00"), from_hex("00 7f ff"), from_hex("01 00")],
    );

    let deep = NodePrefix::of(&[zeros(33), from_hex("80")].concat(), 265);
    assert_eq!(
        deep.span(),
        Span::new(Bound::Included([zeros(33), from_hex("80")].concat()), Bound::Excluded([zeros(32), from_hex("01")].concat()))
    );

    // All ones: nothing follows the subtree.
    check_span(bits("1"), ("80", None), &[from_hex("80"), from_hex("ff ff")], &[from_hex("7f ff"), vec![]]);
    check_span(bits("1111 111"), ("fe", None), &[from_hex("fe"), from_hex("ff")], &[from_hex("fd ff")]);
    check_span(bits("1111 1111"), ("ff", None), &[from_hex("ff"), vec![0xFF; 6]], &[from_hex("fe ff")]);
    check_span(bits("1111 1111 1111 1111"), ("ff ff", None), &[from_hex("ff ff"), vec![0xFF; 33]], &[from_hex("ff"), from_hex("ff fe ff")]);

    // Around one encoded chunk (seven bits) and one byte.
    check_span(bits("0000 000"), ("00", Some("02")), &[from_hex("00"), from_hex("01 ff")], &[from_hex("02"), vec![]]);
    check_span(bits("0000 001"), ("02", Some("04")), &[from_hex("03 ff")], &[from_hex("01 ff"), from_hex("04")]);
    check_span(bits("0000 0001"), ("01", Some("02")), &[from_hex("01"), from_hex("01 00")], &[from_hex("00 ff"), from_hex("02")]);

    check_span(
        bits("0000 0001 1"),
        ("01 80", Some("02")),
        &[from_hex("01 80"), from_hex("01 ff ff")],
        &[from_hex("01"), from_hex("01 7f"), from_hex("02")],
    );

    // Around two chunks (fourteen bits) and two bytes.
    check_span(
        bits("0000 0000 1111 11"),
        ("00 fc", Some("01")),
        &[from_hex("00 fc"), from_hex("00 ff ff")],
        &[from_hex("00"), from_hex("00 fb ff"), from_hex("01")],
    );

    check_span(
        bits("0000 0000 1111 1111"),
        ("00 ff", Some("01")),
        &[from_hex("00 ff"), from_hex("00 ff 00")],
        &[from_hex("00"), from_hex("00 fe ff"), from_hex("01")],
    );
}

#[test]
fn encodings_name_the_seven_level_walk() {
    assert_eq!(bits("0").to_bytes(), [0]);
    assert_eq!(bits("00").to_bytes(), [1]);
    assert_eq!(bits("0000001").to_bytes(), [7]);
    assert_eq!(bits("1").to_bytes(), [127]);
    assert_eq!(bits("1111111").to_bytes(), [253]);
    assert_eq!(bits("11111110").to_bytes(), [253, 0]);
    assert_eq!(NodePrefix::root().to_bytes(), Vec::<u8>::new());
}

#[test]
fn decoding_refuses_bytes_that_name_no_prefix() {
    assert_eq!(NodePrefix::from_bytes(&[254]), Err("a byte past the places of a seven-level walk"));
    assert_eq!(NodePrefix::from_bytes(&[0, 0]), Err("a chunk shorter than seven bits before the last"));
}

#[test]
fn a_prefix_prints_its_bits() { assert_eq!(format!("{:?}", bits("0110")), "NodePrefix(0110)") }
