use std::cmp::Ordering;
use std::ops::Bound;

use ankurah_core_types::key_spec::{IndexDirection, IndexKeyPart, KeySpec, NullsOrder};
use ankurah_core_types::{Value, ValueType};
use proptest::prelude::*;

use super::{check_spec, lookup_key, member_key, span_for, KeyRange};
use crate::common::{asc, desc, item, Id, Item, Property};
use crate::fixtures::from_hex;
use crate::span::address_of;
use crate::{Error, MemberId};

/// A spec of one ascending `I64` part, property `A`.
fn ascending() -> KeySpec<Property> { KeySpec::new(vec![asc!(Property::A, ValueType::I64)]) }

/// The keys these values make, one value per part of `spec`.
fn key_of(spec: &KeySpec<Property>, values: &[Option<i64>]) -> Vec<u8> {
    member_key(spec, &Item::new(1, values.iter().copied(), b"")).unwrap()
}

#[test]
fn an_ascending_integer_is_its_tag_then_its_eight_bytes_with_the_sign_bit_flipped() {
    assert_eq!(key_of(&ascending(), &[Some(2013)]), from_hex("01 80000000000007dd"));
    assert_eq!(key_of(&ascending(), &[Some(-1)]), from_hex("01 7fffffffffffffff"));
}

#[test]
fn a_missing_value_is_one_tag_after_every_value_ascending_and_before_them_descending() {
    let descending = KeySpec::new(vec![desc!(Property::A, ValueType::I64)]);

    assert_eq!(key_of(&ascending(), &[None]), [0x02]);
    assert_eq!(key_of(&descending, &[None]), [!0x02]);
    assert_eq!(key_of(&descending, &[Some(2013)]), from_hex("fe 7ffffffffffff822"));
}

#[test]
fn a_part_s_nulls_order_overrides_postgresql_s() {
    let mut first = asc!(Property::A, ValueType::I64);
    first.nulls = Some(NullsOrder::First);
    let mut last = desc!(Property::A, ValueType::I64);
    last.nulls = Some(NullsOrder::Last);

    assert_eq!(key_of(&KeySpec::new(vec![first]), &[None]), [0x00]);
    assert_eq!(key_of(&KeySpec::new(vec![last]), &[None]), [0xff]);
}

#[test]
fn a_string_is_escaped_and_ended_so_that_it_begins_no_other() {
    let names: KeySpec<Property> = KeySpec::new(vec![asc!(Property::A, ValueType::String)]);
    let key = |name: &str| lookup_key(&names, &[Value::String(name.into())]).unwrap();

    assert_eq!(key("Ab"), from_hex("01 4162 0000"));
    assert_eq!(key("Ab\0"), from_hex("01 4162 00ff 0000"));
    assert!(key("Ab") < key("Ab\0") && key("Ab\0") < key("Abc"));
}

#[test]
fn every_part_s_encoding_follows_the_one_before() {
    let spec = KeySpec::new(vec![asc!(Property::A, ValueType::I64), desc!(Property::B, ValueType::I64)]);

    assert_eq!(key_of(&spec, &[Some(1), Some(1)]), [key_of(&ascending(), &[Some(1)]), from_hex("fe 7ffffffffffffffe")].concat());
}

#[test]
fn a_value_of_another_type_than_its_part_s_is_refused() {
    let years = KeySpec::new(vec![asc!(Property::A, ValueType::I32)]);

    match member_key(&years, &item!(7, 2013, b"")) {
        Err(Error::WrongValueType { member, part: 0, expected: ValueType::I32, found: ValueType::I64 }) => {
            assert_eq!(member, Id(7).to_bytes().into_owned());
        }
        other => panic!("expected a wrong value type, got {other:?}"),
    }

    assert!(matches!(
        lookup_key(&years, &[Value::I64(2013)]),
        Err(Error::KeyValueType { part: 0, expected: ValueType::I32, found: ValueType::I64 })
    ));
}

#[test]
fn a_member_without_a_property_its_part_names_has_no_key() {
    let years = KeySpec::new(vec![asc!(Property::Year, ValueType::I32)]);

    assert!(matches!(member_key(&years, &item!(7, 2013, b"")), Err(Error::Key { .. })));
}

#[test]
fn a_lookup_names_one_value_for_every_part() {
    assert!(matches!(lookup_key(&ascending(), &[]), Err(Error::KeyLength { parts: 1, given: 0 })));
    assert!(matches!(lookup_key(&ascending(), &[Value::I64(1), Value::I64(2)]), Err(Error::KeyLength { parts: 1, given: 2 })));
}

#[test]
fn a_key_range_names_no_more_parts_than_the_key_has() {
    let range = KeyRange::prefix([1i64]).and(2i64..);

    assert!(matches!(span_for(&ascending(), &range), Err(Error::KeyLength { parts: 1, given: 2 })));
}

#[test]
fn a_spec_that_names_a_collation_is_refused() {
    let mut named = asc!(Property::A, ValueType::String);
    named.collation = Some("unicode".into());

    assert!(matches!(check_spec(&KeySpec::new(vec![ascending().keyparts[0].clone(), named])), Err(Error::Collation { part: 1 })));
}

/// A part of `property` holding `I64`s, in `direction`, with `nulls`.
fn part(property: Property, direction: IndexDirection, nulls: Option<NullsOrder>) -> IndexKeyPart<Property> {
    IndexKeyPart { key: property, sub_path: None, direction, value_type: ValueType::I64, nulls, collation: None }
}

/// Any two-part spec over `A` and `B`.
fn specs() -> impl Strategy<Value = KeySpec<Property>> {
    let direction = prop_oneof![Just(IndexDirection::Asc), Just(IndexDirection::Desc)];
    let nulls = prop_oneof![Just(None), Just(Some(NullsOrder::First)), Just(Some(NullsOrder::Last))];

    (direction.clone(), nulls.clone(), direction, nulls)
        .prop_map(|(a, a_nulls, b, b_nulls)| KeySpec::new(vec![part(Property::A, a, a_nulls), part(Property::B, b, b_nulls)]))
}

/// A value from a small range, so that members tie, or none.
fn values() -> impl Strategy<Value = Option<i64>> { prop_oneof![1 => Just(None), 4 => (-3i64..3).prop_map(Some)] }

/// Where a member without a value goes among values, in `part`'s own order:
/// first or last.
fn sorts_first(part: &IndexKeyPart<Property>) -> bool {
    match part.nulls {
        Some(order) => order == NullsOrder::First,
        None => part.direction == IndexDirection::Desc,
    }
}

/// The order of two values in `part`'s order, missing values placed as the
/// part places them.
fn compare(part: &IndexKeyPart<Property>, a: Option<i64>, b: Option<i64>) -> Ordering {
    let first = sorts_first(part);
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => if first { Ordering::Less } else { Ordering::Greater },
        (Some(_), None) => if first { Ordering::Greater } else { Ordering::Less },

        (Some(a), Some(b)) => match part.direction {
            IndexDirection::Asc => a.cmp(&b),
            IndexDirection::Desc => b.cmp(&a),
        },
    }
}

/// Whether `value` lies within the bounds, as a comparison judges: a missing
/// value lies within none.
fn within(value: Option<i64>, (low, high): &(Bound<i64>, Bound<i64>)) -> bool {
    let Some(value) = value else { return false };
    let above = match low {
        Bound::Included(low) => value >= *low,
        Bound::Excluded(low) => value > *low,
        Bound::Unbounded => true,
    };

    let below = match high {
        Bound::Included(high) => value <= *high,
        Bound::Excluded(high) => value < *high,
        Bound::Unbounded => true,
    };

    above && below
}

fn bounds() -> impl Strategy<Value = Bound<i64>> {
    prop_oneof![Just(Bound::Unbounded), (-3i64..3).prop_map(Bound::Included), (-3i64..3).prop_map(Bound::Excluded)]
}

proptest! {
    /// Keys sort as their values do, part by part, each part in its own
    /// direction with missing values where its nulls order puts them, and no
    /// key begins another.
    #[test]
    fn keys_sort_as_their_values_do(spec in specs(), values in prop::collection::vec((values(), values()), 2..12)) {
        let keyed: Vec<((Option<i64>, Option<i64>), Vec<u8>)> =
            values.iter().map(|&(a, b)| ((a, b), key_of(&spec, &[a, b]))).collect();

        for (a_values, a_key) in &keyed {
            for (b_values, b_key) in &keyed {
                let expected = compare(&spec.keyparts[0], a_values.0, b_values.0)
                    .then_with(|| compare(&spec.keyparts[1], a_values.1, b_values.1));

                prop_assert_eq!(a_key.cmp(b_key), expected, "{:?} against {:?}", a_values, b_values);
                prop_assert!(a_key == b_key || !b_key.starts_with(a_key), "{:?} begins {:?}", a_values, b_values);
            }
        }
    }

    /// A key range's span holds exactly the members whose values it holds:
    /// equal values for the prefix, a missing one for `None`, and for the
    /// ranged part a value within the range, never a missing one.
    #[test]
    fn a_key_range_s_span_holds_the_members_it_names(
        spec in specs(),
        members in prop::collection::vec((values(), values()), 0..16),
        prefix in prop::option::of(values()),
        range in prop::option::of((bounds(), bounds())),
    ) {
        let key_range = KeyRange::new(
            prefix.into_iter().map(|value| value.map(Value::I64)).collect(),
            range.map(|(low, high)| (low.map(Value::I64), high.map(Value::I64))),
        );
        let span = span_for(&spec, &key_range).unwrap();

        for (index, &(a, b)) in members.iter().enumerate() {
            let address = address_of(&key_of(&spec, &[a, b]), &(index as u64).to_be_bytes());
            let (ranged, rest) = match prefix {
                Some(prefix) => (b, prefix == a),
                None => (a, true),
            };

            let unbounded = matches!(range, None | Some((Bound::Unbounded, Bound::Unbounded)));
            let expected = rest && (unbounded || within(ranged, range.as_ref().unwrap()));

            prop_assert_eq!(span.contains(&address), expected, "{:?} with prefix {:?} and range {:?}", (a, b), prefix, range);
        }
    }
}
