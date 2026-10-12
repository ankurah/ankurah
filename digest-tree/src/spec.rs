//! The tree spec: what defines a tree, and the key spec a tree files its
//! leaves by.

use std::hash::{Hash, Hasher};

use ankql::ast::{Predicate, Resolved};
use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyId, ValueType};
use serde::Serialize;

/// What defines a tree: the leaves it admits and the order it keeps them in.
///
/// A tree spec is the resolved, canonical description of a tree, so that two
/// trees can be checked to hold the same leaf set before their digests are
/// compared, and so that the tree a Selection needs can be found by its spec.
/// One spec stands behind every use of a tree: an index's tree, a partial
/// replica's scope, a live query's result (where the tree is the matched set,
/// each entity with its head), a durable subscription, a fetch. What a use
/// compares is its scope, which the use keeps and the spec does not carry: the
/// spans of the tree it reads, as a head array of tree node prefixes with key
/// bounds where a span cuts a tree node, the order it walks them in, and the
/// count it takes under a limit.
///
/// Its two fields divide into ordering and admission.
///
/// The key spec is the ordering. It fixes the address of every leaf, the
/// canonical key bytes followed by the entity id, and with it the order the
/// leaves are kept in, the prefixes the tree nodes are named by, and the spans
/// a scope can name. It has no directions: every part is encoded ascending,
/// with nulls at one fixed end, so the tree is the same whatever order a
/// Selection asks for. Which way a use walks each part, and where nulls fall
/// for it, is the use's order, a [`KeySpec`] over the same parts that the use
/// keeps with its scope. A digest is a sum, so the order changes no digest,
/// only which leaves fall under a limit's cut, and that cut is a few spans of
/// the one tree: the tiers before the one the cut falls in, then within that
/// tier the leaves past the cut value in the walk's direction, then within the
/// cut's own value the entity ids up to it. Which parts make the key is
/// derived from the Selection alone, by one rule applied wherever the tree is
/// built, never from the indexes at hand: the parts the predicate pins by
/// equality, then the ORDER BY parts when the Selection has a limit, or the
/// single range part when it has none; a limit-less ORDER BY never enters a
/// spec, since it changes no membership. The entity-id tree has the empty key
/// spec, so its address is the entity id alone. An index tree's key spec is
/// the index's [`KeySpec`] with its directions dropped.
///
/// The predicate is the admission. It chooses which entities file a leaf,
/// never where they file: the Selection's whole predicate, resolved to
/// property ids, membership in a component included as a term of it;
/// [`Predicate::True`] admits every entity that has all the key's parts, which
/// is what an index tree holds. A tree with another predicate has the prefix
/// structure of the index tree over the same key spec with fewer leaves, and
/// its leaves are a filtered copy of that index tree's. A Selection whose
/// predicate is nothing but equalities and a range on a key needs no tree of
/// its own: it is a scope over the index tree.
///
/// The tree reads the spec only to compare and hash it: it evaluates no
/// predicate and encodes no key; whoever feeds the tree derives the keys and
/// decides admission. Whether a tree is an index tree is a fact about its
/// owner, not a property of the spec. Every field is public, so a spec is
/// built with struct literal syntax.
#[derive(Debug, Clone, Serialize)]
pub struct TreeSpec {
    /// The ordering: the parts that address the leaves, each encoded
    /// ascending. The parts the predicate pins by equality, then the ORDER BY
    /// parts under a limit or the range part without one; empty for the
    /// entity-id tree.
    pub key_spec: TreeKeySpec,
    /// The admission: the Selection's whole predicate, resolved.
    /// [`Predicate::True`] admits every entity that has all the key's parts,
    /// which is what an index tree and the entity-id tree hold.
    pub predicate: Predicate<Resolved>,
}

/// The key spec a tree files its leaves by: its parts in order, without
/// directions. A [`KeySpec`] says which way each part sorts and where its
/// nulls fall; a tree needs neither, since it encodes every part ascending
/// with nulls at one fixed end and leaves the direction to whoever walks it.
/// So every order over the same parts names one tree, and an index's key spec
/// becomes its tree's by dropping its directions: [`TreeKeySpec::from`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TreeKeySpec {
    /// The parts, most significant first.
    pub parts: Vec<TreeKeyPart>,
}

impl TreeKeySpec {
    /// The key spec of these parts, most significant first; empty for the
    /// entity-id tree.
    pub fn new(parts: Vec<TreeKeyPart>) -> Self { Self { parts } }
}

/// One part of a tree's key spec: the property it files by and how its value
/// is encoded, with no direction.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TreeKeyPart {
    /// The property, by its durable id.
    pub property: PropertyId,
    /// The path below the property, for a value inside a document.
    pub sub_path: Option<Vec<String>>,
    /// The type the value is encoded as.
    pub value_type: ValueType,
    /// The collation that orders a string, when it is not the plain byte
    /// order.
    pub collation: Option<String>,
}

impl TreeKeyPart {
    /// A part on `property` with no sub-path or collation.
    pub fn new(property: PropertyId, value_type: ValueType) -> Self { Self { property, sub_path: None, value_type, collation: None } }
}

/// The part without its direction and nulls placement.
impl From<&IndexKeyPart<PropertyId>> for TreeKeyPart {
    fn from(part: &IndexKeyPart<PropertyId>) -> Self {
        Self { property: part.key, sub_path: part.sub_path.clone(), value_type: part.value_type, collation: part.collation.clone() }
    }
}

/// The key spec without its directions and nulls placements: the key spec of
/// the tree that serves every order over these parts.
impl From<&KeySpec<PropertyId>> for TreeKeySpec {
    fn from(key_spec: &KeySpec<PropertyId>) -> Self { Self { parts: key_spec.keyparts.iter().map(TreeKeyPart::from).collect() } }
}

impl TreeSpec {
    /// The bytes specs are compared and hashed by: the bincode serialization,
    /// the workspace's canonical binary form, which writes a float literal as
    /// its bits.
    fn canonical_bytes(&self) -> Vec<u8> { bincode::serialize(self).expect("a tree spec serializes") }
}

/// Two specs are equal when they serialize alike. A float literal compares by
/// its bits, so 0.0 and -0.0, or two NaNs with different payloads, make
/// different specs, and a NaN equals itself: canonicalizing literals, so that
/// equal Selections share a tree, is the caller's job, done before it builds
/// the spec. So is the order of a conjunction's terms, which the bytes keep.
impl PartialEq for TreeSpec {
    fn eq(&self, other: &Self) -> bool { self.canonical_bytes() == other.canonical_bytes() }
}

/// Equality compares bytes, which compare totally, float literals included.
impl Eq for TreeSpec {}

/// Hashes the bytes equality compares, so equal specs hash alike.
impl Hash for TreeSpec {
    fn hash<H: Hasher>(&self, state: &mut H) { self.canonical_bytes().hash(state) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::{ComparisonOperator, Expr};
    use ankurah_core_types::{EntityId, IndexDirection, NullsOrder, PropertyPath, Value};
    use std::collections::hash_map::DefaultHasher;
    use std::collections::{HashMap, HashSet};

    fn property(n: u8) -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([n; 32])) }

    /// The key spec on property 1 alone, a string.
    fn by_name() -> TreeKeySpec { TreeKeySpec::new(vec![TreeKeyPart::new(property(1), ValueType::String)]) }

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
    fn specs_that_differ_in_any_field_differ() {
        let specs = [
            TreeSpec { key_spec: TreeKeySpec::new(vec![]), predicate: Predicate::True },
            TreeSpec { key_spec: by_name(), predicate: Predicate::True },
            TreeSpec { key_spec: TreeKeySpec::new(vec![TreeKeyPart::new(property(1), ValueType::I64)]), predicate: Predicate::True },
            TreeSpec { key_spec: TreeKeySpec::new(vec![TreeKeyPart::new(property(2), ValueType::String)]), predicate: Predicate::True },
            TreeSpec {
                key_spec: TreeKeySpec::new(vec![TreeKeyPart { sub_path: Some(vec!["city".into()]), ..TreeKeyPart::new(property(1), ValueType::String) }]),
                predicate: Predicate::True,
            },
            TreeSpec { key_spec: by_name(), predicate: rating_above(Value::I64(3)) },
            TreeSpec { key_spec: by_name(), predicate: rating_above(Value::I64(4)) },
        ];
        let distinct: HashSet<TreeSpec> = specs.iter().cloned().collect();
        assert_eq!(distinct.len(), specs.len());
        for spec in &specs {
            assert_eq!(spec.clone(), *spec, "a spec built again");
            assert_eq!(hash_of(&spec.clone()), hash_of(spec));
        }
    }

    /// Every order over the same parts names one tree: a key spec's
    /// directions and nulls placements drop away, so the key specs of
    /// `ORDER BY a ASC, b DESC`, its reverse, and the index on (a, b) in any
    /// directions are one tree key spec. Another part order, or another type
    /// for a part, is another tree.
    #[test]
    fn every_order_over_the_same_parts_is_one_key_spec() {
        use IndexDirection::{Asc, Desc};
        use NullsOrder::{First, Last};
        let part = |n: u8, direction, nulls| IndexKeyPart { direction, nulls, ..IndexKeyPart::asc(property(n), ValueType::String) };
        let orders = [
            KeySpec::new(vec![part(1, Asc, None), part(2, Desc, None)]),
            KeySpec::new(vec![part(1, Desc, None), part(2, Asc, None)]),
            KeySpec::new(vec![part(1, Asc, Some(First)), part(2, Asc, Some(Last))]),
            KeySpec::new(vec![part(1, Desc, Some(Last)), part(2, Desc, Some(First))]),
        ];
        let expected = TreeKeySpec::new(vec![TreeKeyPart::new(property(1), ValueType::String), TreeKeyPart::new(property(2), ValueType::String)]);
        for order in &orders {
            assert_eq!(TreeKeySpec::from(order), expected, "{order:?}");
        }
        assert_ne!(TreeKeySpec::from(&KeySpec::new(vec![part(2, Asc, None), part(1, Asc, None)])), expected, "the parts in another order");
        let typed = KeySpec::new(vec![part(1, Asc, None), IndexKeyPart::asc(property(2), ValueType::I64)]);
        assert_ne!(TreeKeySpec::from(&typed), expected, "a part of another type");
        assert_eq!(TreeKeySpec::from(&KeySpec::new(vec![])), TreeKeySpec::new(vec![]), "the entity-id tree");

        // A part's sub-path, type and collation come through the conversion;
        // only its direction and nulls placement are dropped.
        let full = IndexKeyPart {
            sub_path: Some(vec!["address".into(), "city".into()]),
            direction: Desc,
            nulls: Some(First),
            collation: Some("en-US".into()),
            ..IndexKeyPart::asc(property(4), ValueType::I64)
        };
        assert_eq!(
            TreeKeyPart::from(&full),
            TreeKeyPart {
                property: property(4),
                sub_path: Some(vec!["address".into(), "city".into()]),
                value_type: ValueType::I64,
                collation: Some("en-US".into()),
            }
        );
        let bare = TreeKeyPart::new(property(4), ValueType::I64);
        assert_ne!(TreeKeyPart::from(&full), bare, "the sub-path and the collation are part of the key");
    }

    /// The entity-id tree's canonical bytes, pinned: the empty key spec
    /// serializes as its eight-byte length and the predicate `true` as its
    /// variant's index in four little-endian bytes. A change to the layout,
    /// serde's or bincode's included, shows up here.
    #[test]
    fn the_entity_id_tree_serializes_canonically() {
        let spec = TreeSpec { key_spec: TreeKeySpec::new(vec![]), predicate: Predicate::True };
        assert_eq!(hex(&spec.canonical_bytes()), "000000000000000005000000");
    }

    /// Specs with a key and a float in the predicate, pinned at their canonical
    /// bytes, so that any build, native or wasm32, that serializes a spec
    /// differently shows up here. A literal enters by its bits, its last eight
    /// bytes here: 0.0 and -0.0 make two specs, and a NaN makes the spec of
    /// its bit pattern and equals itself, while a NaN of another payload is
    /// another spec. The bytes were computed once from this serialization.
    #[test]
    fn specs_with_float_literals_compare_by_their_bits() {
        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let other_nan = f64::from_bits(0x7ff8_0000_0000_0002);
        let two_parts = TreeKeySpec::new(vec![TreeKeyPart::new(property(1), ValueType::String), TreeKeyPart::new(property(2), ValueType::F64)]);
        let vectors = [(by_name(), 0.0, ZERO), (by_name(), -0.0, NEGATIVE_ZERO), (by_name(), nan, NAN), (two_parts, 2.5, TWO_PARTS)];
        let specs: Vec<TreeSpec> = vectors
            .into_iter()
            .map(|(key_spec, bound, bytes)| {
                let spec = TreeSpec { key_spec, predicate: rating_above(Value::F64(bound)) };
                assert_eq!(hex(&spec.canonical_bytes()), bytes, "{bound:?}");
                spec
            })
            .collect();
        assert_ne!(specs[0], specs[1], "0.0 and -0.0");
        assert_eq!(specs[2], specs[2].clone(), "a NaN equals itself");
        assert_eq!(hash_of(&specs[2]), hash_of(&specs[2].clone()), "and hashes alike");
        assert_ne!(specs[2], TreeSpec { key_spec: by_name(), predicate: rating_above(Value::F64(other_nan)) }, "a NaN of another payload");
    }

    const ZERO: &str = "01000000000000000100000001010101010101010101010101010101010101010101010101010101010101010005000000000000000001000000010000000303\
                          0303030303030303030303030303030303030303030303030303030303030600000000000000726174696e670000000000000000020000000000000003000000\
                          0000000000000000";
    const NEGATIVE_ZERO: &str = "01000000000000000100000001010101010101010101010101010101010101010101010101010101010101010005000000000000000001000000010000000303\
                          0303030303030303030303030303030303030303030303030303030303030600000000000000726174696e670000000000000000020000000000000003000000\
                          0000000000000080";
    const NAN: &str = "01000000000000000100000001010101010101010101010101010101010101010101010101010101010101010005000000000000000001000000010000000303\
                          0303030303030303030303030303030303030303030303030303030303030600000000000000726174696e670000000000000000020000000000000003000000\
                          010000000000f87f";
    const TWO_PARTS: &str = "02000000000000000100000001010101010101010101010101010101010101010101010101010101010101010005000000000100000002020202020202020202\
                          02020202020202020202020202020202020202020202000300000000000000000100000001000000030303030303030303030303030303030303030303030303\
                          03030303030303030600000000000000726174696e6700000000000000000200000000000000030000000000000000000440";

    /// Equal specs find one tree in a map keyed by spec; a spec that differs
    /// in its predicate finds none.
    #[test]
    fn equal_specs_find_one_tree_in_a_map() {
        let index_tree = || TreeSpec { key_spec: by_name(), predicate: Predicate::True };
        let tree_of_its_own = || TreeSpec { key_spec: by_name(), predicate: rating_above(Value::I64(3)) };
        let trees: HashMap<TreeSpec, &str> =
            [(index_tree(), "the index tree"), (tree_of_its_own(), "a tree of its own")].into_iter().collect();
        assert_eq!(trees.get(&index_tree()), Some(&"the index tree"));
        assert_eq!(trees.get(&tree_of_its_own()), Some(&"a tree of its own"));
        let another = TreeSpec { key_spec: by_name(), predicate: rating_above(Value::I64(4)) };
        assert_eq!(trees.get(&another), None, "the predicate is part of the spec");
    }
}
