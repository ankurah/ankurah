//! The tree spec: what defines a tree.

use std::hash::{Hash, Hasher};

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core_types::{IndexDirection, KeySpec, PropertyId, PropertyPath};
use serde::{Deserialize, Serialize};

/// What defines a tree: the leaves it admits and the order it keeps them in.
///
/// A tree spec is the resolved, canonical description of a tree, so that two
/// trees can be checked to use the same admission and addressing rules before
/// their digests are compared. Spec equality does not establish equal content,
/// and a compatible scope can reuse a tree with a different admission predicate.
/// One spec stands behind every use of a tree: an index's tree, a partial
/// replica's scope, a live query's result (where the tree is the matched set,
/// each entity with its head), a durable subscription, a fetch. What a use
/// compares is its scope, which the use keeps and the spec does not carry: the
/// spans of the tree it reads, as a list of span digests with tree node prefixes with key
/// bounds where a span cuts a tree node, the order it walks them in, and the
/// count it takes under a limit.
///
/// Its two fields divide into ordering and admission.
///
/// The key spec is the ordering. It fixes the address of every leaf, the
/// canonical key bytes followed by the entity id, and with it the order the
/// leaves are kept in, the prefixes the tree nodes are named by, and the spans
/// a scope can name. Every part of it ascends and none places nulls:
/// [`TreeSpec::new`] drops the directions and nulls placements of the key spec
/// it is handed, since the tree encodes every part ascending, with nulls at
/// one fixed end, and leaves the direction to whoever walks it. So every order
/// over the same parts names one tree: the index on (a, b) in any directions,
/// `ORDER BY a ASC, b DESC` and its reverse all read the one tree keyed
/// (a, b). Which way a use walks each part, and where nulls fall for it, is
/// the use's order, a [`KeySpec`] with directions over the same parts that the
/// use keeps with its scope. A digest is a sum, so the order changes no
/// digest, only which leaves fall under a limit's cut, and that cut is a few
/// spans of the one tree: the tiers before the cut whole, then within the
/// cut's tier the leaves past the cut value in the walk's direction, then
/// within the cut's own value the entity ids up to it. The default key for a
/// new Selection tree uses equality-pinned parts in canonical order, followed
/// by nonconstant ORDER BY parts under a limit (entity-id order when omitted).
/// Without a limit, it adds one constrained range part if available, using a
/// matching ORDER BY part to choose among several, then a canonical tie-break.
/// This is a creation policy; reuse considers the existing tree's layout.
/// Thus `project = 'acme' AND foo > 7` uses (project, foo), with or without
/// `ORDER BY foo`. The range predicate supplies foo; ordering is not needed
/// to make narrower foo ranges addressable. The entity-id tree has the
/// empty key spec, so its address is the entity id alone.
///
/// The predicate is the admission. It chooses which entities file a leaf,
/// never where they file: the Selection's whole predicate, resolved to
/// property ids, membership in a component included as a term of it.
/// Missing key parts require an explicit encoding or admission policy: a key
/// choice must not silently exclude entities the predicate admits.
/// A filtered tree can also serve a narrower
/// Selection when it admits every entity the Selection requires and the
/// remaining restriction is exactly expressed by bounds in that tree's key
/// order. Stored bucket boundaries need not coincide with those bounds: a
/// partial tree node must be descended or its qualifying leaves read, never
/// included with its whole digest. For example,
/// the tree admitting `project = 'acme' AND foo > 7`, keyed by (project, foo),
/// serves `project = 'acme' AND foo > 10` as a span. The requesting predicate
/// need not equal the tree's admission predicate. Both uses read the same
/// leaves at the same addresses; reuse creates no second tree.
///
/// Whoever feeds the tree decides admission against entity state and derives
/// keys with the canonical codec. The reuse planner analyzes predicates to
/// prove coverage and exact bounds, but does not load or evaluate entity state.
/// Whether a tree is an index tree is a fact about its owner, not its spec.
#[derive(Debug, Clone, Serialize)]
pub struct TreeSpec {
    key_spec: KeySpec<PropertyId>,
    predicate: Predicate<Resolved>,
}

impl<'de> Deserialize<'de> for TreeSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            key_spec: KeySpec<PropertyId>,
            predicate: Predicate<Resolved>,
        }
        let fields = Fields::deserialize(deserializer)?;
        Ok(Self::new(fields.key_spec, fields.predicate))
    }
}

impl TreeSpec {
    /// The spec of the tree that serves `key_spec` and `predicate`: the key
    /// spec with its directions and nulls placements dropped, every part
    /// ascending, and the whole predicate. `for_selection` uses this same
    /// constructor, so a spec never carries a direction.
    pub fn new(mut key_spec: KeySpec<PropertyId>, predicate: Predicate<Resolved>) -> Self {
        for part in &mut key_spec.keyparts {
            part.direction = IndexDirection::Asc;
            part.nulls = None;
        }
        Self { key_spec, predicate }
    }

    /// The ordering: the parts that address the leaves, every one ascending.
    /// Empty for the entity-id tree. A predicate's range part can be present
    /// without an ORDER BY, allowing narrower selections to use key bounds.
    pub fn key_spec(&self) -> &KeySpec<PropertyId> { &self.key_spec }

    /// The admission: the Selection's whole predicate, resolved.
    /// Key availability must not silently add another membership restriction.
    pub fn predicate(&self) -> &Predicate<Resolved> { &self.predicate }

    /// Identity ignores display labels but preserves predicate structure and
    /// literal bits. The stored predicate retains its labels for diagnostics.
    fn identity_bytes(&self) -> Vec<u8> {
        let predicate = ankql::selection::map_references::<Resolved, Resolved>(
            &Selection::from(self.predicate.clone()),
            &|path| {
                let mut reference = PropertyPath::from(path.property_id());
                reference.subpath = path.subpath.clone();
                reference
            },
            &|model| *model,
        )
        .predicate;
        bincode::serialize(&(&self.key_spec, predicate)).expect("a tree spec serializes")
    }
}

/// Two specs are equal when their identity bytes agree. Display labels do not
/// participate. A float literal compares by
/// its bits, so 0.0 and -0.0, or two NaNs with different payloads, make
/// different specs, and a NaN equals itself: canonicalizing literals, so that
/// equal Selections share a tree, is the caller's job, done before it builds
/// the spec. So is the order of a conjunction's terms, which the bytes keep.
impl PartialEq for TreeSpec {
    fn eq(&self, other: &Self) -> bool { self.identity_bytes() == other.identity_bytes() }
}

/// Equality compares bytes, which compare totally, float literals included.
impl Eq for TreeSpec {}

/// Hashes the bytes equality compares, so equal specs hash alike.
impl Hash for TreeSpec {
    fn hash<H: Hasher>(&self, state: &mut H) { self.identity_bytes().hash(state) }
}

#[cfg(test)]
mod tests {
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
            TreeSpec::new(
                KeySpec::new(vec![IndexKeyPart { collation: Some("en-US".into()), ..part(1, ValueType::String) }]),
                Predicate::True,
            ),
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
        let two_parts =
            KeySpec::new(vec![IndexKeyPart::asc(property(1), ValueType::String), IndexKeyPart::desc(property(2), ValueType::F64)]);
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
        let trees: HashMap<TreeSpec, &str> =
            [(index_tree(), "the index tree"), (tree_of_its_own(), "a tree of its own")].into_iter().collect();
        assert_eq!(trees.get(&index_tree()), Some(&"the index tree"));
        assert_eq!(trees.get(&tree_of_its_own()), Some(&"a tree of its own"));
        let another = TreeSpec::new(by_name(), rating_above(Value::I64(4)));
        assert_eq!(trees.get(&another), None, "the predicate is part of the spec");
    }
}
