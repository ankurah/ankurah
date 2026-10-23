use super::*;
use ankql::ast::{ComparisonOperator, Expr};
use ankurah_core_types::{EntityId, IndexDirection, IndexKeyPart, NullsOrder, PropertyPath, Value, ValueType};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};

fn property(n: u8) -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([n; 32])) }

/// The key spec ascending on property 1 alone, a string.
fn by_name() -> KeySpec<PropertyId> { KeySpec::new(vec![IndexKeyPart::asc(property(1), ValueType::String)]) }

/// The predicate `rating > bound`, with `rating` the property 3.
fn rating_above(bound: Value) -> Predicate<Resolved> {
    Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::registered(EntityId::from_bytes([3; 32]), "rating", vec![]))),
        operator: ComparisonOperator::GreaterThan,
        right: Box::new(Expr::Literal(bound)),
    }
}

fn hash_of(spec: &TreeSpec) -> u64 {
    let mut hasher = DefaultHasher::new();
    spec.hash(&mut hasher);
    hasher.finish()
}

fn hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }

#[test]
fn display_labels_do_not_change_identity_or_replace_diagnostic_names() {
    let spec = |label: &str, id: u8, subpath: Vec<String>| {
        let path = PropertyPath::registered(EntityId::from_bytes([id; 32]), label, subpath);
        let predicate = Predicate::Not(Box::new(Predicate::IsNull(Box::new(Expr::Path(path)))));
        TreeSpec::new(by_name(), predicate)
    };
    let original = spec("written_name", 3, vec!["city".into()]);
    let renamed = spec("renamed", 3, vec!["city".into()]);
    assert_eq!(original, renamed);
    assert_eq!(hash_of(&original), hash_of(&renamed));
    let trees = HashMap::from([(original.clone(), "one tree")]);
    assert_eq!(trees.get(&renamed), Some(&"one tree"));
    assert_ne!(bincode::serialize(&original).unwrap(), bincode::serialize(&renamed).unwrap(), "labels survive for diagnostics");
    assert_ne!(original, spec("written_name", 4, vec!["city".into()]), "property identity matters");
    assert_ne!(original, spec("written_name", 3, vec!["zip".into()]), "the subpath matters");
}

#[test]
fn specs_that_differ_in_any_field_differ() {
    let part = |n: u8, value_type| IndexKeyPart::asc(property(n), value_type);
    let specs = [
        TreeSpec::new(KeySpec::new(vec![]), Predicate::True),
        TreeSpec::new(by_name(), Predicate::True),
        TreeSpec::new(KeySpec::new(vec![part(1, ValueType::I64)]), Predicate::True),
        TreeSpec::new(KeySpec::new(vec![part(2, ValueType::String)]), Predicate::True),
        TreeSpec::new(
            KeySpec::new(vec![IndexKeyPart { sub_path: Some(vec!["city".into()]), ..part(1, ValueType::String) }]),
            Predicate::True,
        ),
        TreeSpec::new(KeySpec::new(vec![IndexKeyPart { collation: Some("en-US".into()), ..part(1, ValueType::String) }]), Predicate::True),
        TreeSpec::new(by_name(), rating_above(Value::I64(3))),
        TreeSpec::new(by_name(), rating_above(Value::I64(4))),
    ];
    let distinct: HashSet<TreeSpec> = specs.iter().cloned().collect();
    assert_eq!(distinct.len(), specs.len());
    for spec in &specs {
        assert_eq!(spec.clone(), *spec, "a spec built again");
        assert_eq!(hash_of(&spec.clone()), hash_of(spec));
    }
}

/// Every order over the same parts names one tree: the key specs of
/// `ORDER BY a ASC, b DESC`, its reverse, and the index on (a, b) in any
/// directions and nulls placements give equal specs with equal hashes, and
/// the spec reports the parts ascending with no nulls placement, their
/// sub-paths, types and collations kept. Another part order, or another
/// type for a part, is another tree.
#[test]
fn every_order_over_the_same_parts_is_one_tree() {
    use IndexDirection::{Asc, Desc};
    use NullsOrder::{First, Last};
    let part = |n: u8, direction, nulls| IndexKeyPart {
        direction,
        nulls,
        sub_path: Some(vec!["address".into()]),
        collation: Some("en-US".into()),
        ..IndexKeyPart::asc(property(n), ValueType::String)
    };
    let orders = [
        KeySpec::new(vec![part(1, Asc, None), part(2, Desc, None)]),
        KeySpec::new(vec![part(1, Desc, None), part(2, Asc, None)]),
        KeySpec::new(vec![part(1, Asc, Some(First)), part(2, Asc, Some(Last))]),
        KeySpec::new(vec![part(1, Desc, Some(Last)), part(2, Desc, Some(First))]),
    ];
    let expected = TreeSpec::new(KeySpec::new(vec![part(1, Asc, None), part(2, Asc, None)]), rating_above(Value::I64(3)));
    for order in &orders {
        let spec = TreeSpec::new(order.clone(), rating_above(Value::I64(3)));
        assert_eq!(spec, expected, "{order:?}");
        assert_eq!(hash_of(&spec), hash_of(&expected));
        for kept in &spec.key_spec().keyparts {
            assert!(!kept.direction.is_desc() && kept.nulls.is_none(), "ascending, no nulls placement");
            assert_eq!((kept.sub_path.as_deref(), kept.collation.as_deref()), (Some(&["address".to_string()][..]), Some("en-US")));
        }
    }
    let swapped = TreeSpec::new(KeySpec::new(vec![part(2, Asc, None), part(1, Asc, None)]), rating_above(Value::I64(3)));
    assert_ne!(swapped, expected, "the parts in another order");
    let typed = KeySpec::new(vec![part(1, Asc, None), IndexKeyPart { value_type: ValueType::I64, ..part(2, Asc, None) }]);
    assert_ne!(TreeSpec::new(typed, rating_above(Value::I64(3))), expected, "a part of another type");
}

/// The entity-id tree's identity bytes, pinned: the empty key spec
/// serializes as its eight-byte length and the predicate `true` as its
/// variant's index in four little-endian bytes. A change to the layout,
/// serde's or bincode's included, shows up here.
#[test]
fn the_entity_id_tree_serializes_canonically() {
    let spec = TreeSpec::new(KeySpec::new(vec![]), Predicate::True);
    assert_eq!(hex(&spec.identity_bytes()), "000000000000000005000000");
}

/// Specs with a key and a float in the predicate, pinned at their identity
/// bytes, so that any build, native or wasm32, that serializes a spec
/// differently shows up here. A literal enters by its bits, its last eight
/// bytes here: 0.0 and -0.0 make two specs, and a NaN makes the spec of
/// its bit pattern and equals itself, while a NaN of another payload is
/// another spec. The bytes were computed once from this serialization.
#[test]
fn specs_with_float_literals_compare_by_their_bits() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let other_nan = f64::from_bits(0x7ff8_0000_0000_0002);
    let two_parts = KeySpec::new(vec![IndexKeyPart::asc(property(1), ValueType::String), IndexKeyPart::desc(property(2), ValueType::F64)]);
    let vectors = [(by_name(), 0.0, ZERO), (by_name(), -0.0, NEGATIVE_ZERO), (by_name(), nan, NAN), (two_parts, 2.5, TWO_PARTS)];
    let specs: Vec<TreeSpec> = vectors
        .into_iter()
        .map(|(key_spec, bound, bytes)| {
            let spec = TreeSpec::new(key_spec, rating_above(Value::F64(bound)));
            assert_eq!(hex(&spec.identity_bytes()), bytes, "{bound:?}");
            spec
        })
        .collect();
    assert_ne!(specs[0], specs[1], "0.0 and -0.0");
    assert_eq!(specs[2], specs[2].clone(), "a NaN equals itself");
    assert_eq!(hash_of(&specs[2]), hash_of(&specs[2].clone()), "and hashes alike");
    assert_ne!(specs[2], TreeSpec::new(by_name(), rating_above(Value::F64(other_nan))), "a NaN of another payload");
}

const ZERO: &str =
    "01000000000000000100000001010101010101010101010101010101010101010101010101010101010101010000000000050000000000000000000100000001\
    00000003030303030303030303030303030303030303030303030303030303030303032b0000000000000041774d4441774d4441774d4441774d4441774d4441\
    774d4441774d4441774d4441774d4441774d4441774d00000000000000000200000000000000030000000000000000000000";
const NEGATIVE_ZERO: &str =
    "01000000000000000100000001010101010101010101010101010101010101010101010101010101010101010000000000050000000000000000000100000001\
    00000003030303030303030303030303030303030303030303030303030303030303032b0000000000000041774d4441774d4441774d4441774d4441774d4441\
    774d4441774d4441774d4441774d4441774d4441774d00000000000000000200000000000000030000000000000000000080";
const NAN: &str =
    "01000000000000000100000001010101010101010101010101010101010101010101010101010101010101010000000000050000000000000000000100000001\
    00000003030303030303030303030303030303030303030303030303030303030303032b0000000000000041774d4441774d4441774d4441774d4441774d4441\
    774d4441774d4441774d4441774d4441774d4441774d0000000000000000020000000000000003000000010000000000f87f";
const TWO_PARTS: &str =
    "02000000000000000100000001010101010101010101010101010101010101010101010101010101010101010000000000050000000000010000000202020202\
    02020202020202020202020202020202020202020202020202020200000000000300000000000000000001000000010000000303030303030303030303030303\
    0303030303030303030303030303030303032b0000000000000041774d4441774d4441774d4441774d4441774d4441774d4441774d4441774d4441774d444177\
    4d4441774d00000000000000000200000000000000030000000000000000000440";

/// Equal specs find one tree in a map keyed by spec; a spec that differs
/// in its predicate finds none.
#[test]
fn equal_specs_find_one_tree_in_a_map() {
    let index_tree = || TreeSpec::new(by_name(), Predicate::True);
    let tree_of_its_own = || TreeSpec::new(by_name(), rating_above(Value::I64(3)));
    let trees: HashMap<TreeSpec, &str> = [(index_tree(), "the index tree"), (tree_of_its_own(), "a tree of its own")].into_iter().collect();
    assert_eq!(trees.get(&index_tree()), Some(&"the index tree"));
    assert_eq!(trees.get(&tree_of_its_own()), Some(&"a tree of its own"));
    let another = TreeSpec::new(by_name(), rating_above(Value::I64(4)));
    assert_eq!(trees.get(&another), None, "the predicate is part of the spec");
}
