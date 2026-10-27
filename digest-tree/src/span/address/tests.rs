use super::*;
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

/// Fields of up to four bytes drawn from zero, one and `0xFF`, the bytes the
/// escaping treats specially, so that fields beginning one another are
/// common.
fn field() -> impl Strategy<Value = Vec<u8>> { proptest::collection::vec(prop_oneof![Just(0x00u8), Just(0x01), Just(0xFF)], 0..=4) }

#[test]
fn an_address_is_the_key_then_the_id_each_escaped_and_ended() {
    assert_eq!(address_of(&[0x6b, 0x00], &[0x00, 0x09]), [0x6b, 0x00, 0xFF, 0x00, 0x00, 0x00, 0xFF, 0x09, 0x00, 0x00]);
    assert_eq!(address_of(&[], &[0x09]), [0x00, 0x00, 0x09, 0x00, 0x00], "a tree keyed by id alone has empty keys");
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 4096, rng_seed: RngSeed::Fixed(0xadd7_0001), ..ProptestConfig::default() })]

    #[test]
    fn an_address_splits_back_into_its_key_and_id(key in field(), id in field()) {
        prop_assert_eq!(split_address(&address_of(&key, &id)).unwrap(), (key, id));
    }

    /// Addresses sort by key and then by id, and none begins another, even
    /// where one key or id begins another.
    #[test]
    fn addresses_sort_by_key_then_id_and_none_begins_another(a in (field(), field()), b in (field(), field())) {
        let (first, second) = (address_of(&a.0, &a.1), address_of(&b.0, &b.1));

        prop_assert_eq!(first.cmp(&second), a.cmp(&b));
        prop_assert!(first == second || !second.starts_with(&first), "{:02x?} begins {:02x?}", first, second);
    }

    #[test]
    fn an_address_begins_with_a_key_prefix_exactly_when_its_key_does(prefix in field(), key in field(), id in field()) {
        prop_assert_eq!(address_of(&key, &id).starts_with(&key_prefix(&prefix)), key.starts_with(&prefix));
    }
}

#[test]
fn bytes_that_are_not_a_key_and_an_id_are_damage() {
    let damaged = [&[0x01][..], &[0x01, 0x00, 0x00], &[0x00, 0x01, 0x00, 0x00, 0x00, 0x00], &[0x00, 0x00, 0x00, 0x00, 0x07]];
    for bytes in damaged {
        assert!(matches!(split_address(bytes), Err(Error::Inconsistent(_))), "{bytes:02x?}");
    }
}
