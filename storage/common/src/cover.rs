//! Whether a tree a member already keeps serves a Selection, and with which cover.
//!
//! An interest is any Selection. A tree kept for an index serves it, through the cover of a
//! range of that index's keys, only when the Selection's result has an exact,
//! data-independent representation as that range. This decision recognizes one when the
//! Selection names the component the index files, by one membership every match must have,
//! or names none for the entity-id index of every entity; the planner pushes the rest of the
//! predicate into an index whose KeySpec the tree's matches, exactly or as a prefix, with no
//! residual; the tree keeps every key part in the value type the schema declares for its
//! property, and each matched part in the null order and collation the Selection compares it
//! by; the Selection compares each matched part only with values of a type that encodes alike
//! with the declared one; the Selection has no limit; the tree files no entity twice within the
//! range; and the tree leaves out no entity the range names. An order alone never matters,
//! because ordering does not change the set. Any other Selection is refused with its reason,
//! and a temporary digest over its result serves it instead.
//!
//! The declared types are checked here because neither side is guaranteed to agree with them:
//! a tree's registration is not yet validated against the schema, and a resolved Selection can
//! be built or deserialized without the resolver, which casts each compared value to its
//! property's declared type. Unchecked, `w <= 3` with the integer 3, over a float property `w`
//! and a tree registered with an integer part for it, would reuse the cover of the keys up to
//! 3, where the tree files the row of 3.5 as 3, though Selection evaluation compares 3.5 with
//! 3.0 and rejects it.
//!
//! The decision is conservative, not complete: some Selections it refuses do have such a
//! representation, since it reads them through the planner as the planner stands. The planner
//! keys equality conjuncts in the order they are written, so `b = 2 AND a = 1` does not match
//! a tree keyed by a then b; KeySpec::matches accepts the same directions or all of them
//! inverted, so `a = 1 AND b >= 2` does not match a tree keyed by a descending then b
//! ascending, though with a fixed the range of b is contiguous; and the planner keeps
//! comparisons of the entity id as a residual, so no Selection reaches a range of the entity
//! id, though the cover can tile one.

use std::ops::Bound;

use ankql::ast::{Predicate, Resolved, Selection};
use ankql::selection::map_references;
use ankurah_core::indexing::{Cover, IndexKeyPart, KeyRange, KeySpec, RangeError, encodes_alike};
use ankurah_core::storage::tree::HashedIndex;
use ankurah_core::value::{Value, ValueType};
use ankurah_proto::{ModelId, PropertyId};

use crate::materialization_plan::MaterializationPlan;
use crate::{ColumnPath, Endpoint, EngineColumns, KeyBoundComponent, KeyBounds, KeyDatum, Plan, Planner, PlannerConfig};

/// A tree a member keeps, as the reuse decision needs to know it.
#[derive(Debug, Clone, PartialEq)]
pub struct RegisteredTree {
    pub index: HashedIndex,
    /// The key parts, by position, under which one entity can be filed with several values,
    /// such as a group part that files an entity once per group it belongs to. Every other
    /// part holds one value per entity.
    pub multi_valued_parts: Vec<usize>,
}

/// Why no tree a member keeps serves a Selection.
#[derive(Debug, Clone, PartialEq)]
pub enum NotReusable {
    /// A limit cuts the result at an edge that moves with the data.
    Limit(u64),
    /// The planner evaluates this part of the predicate on fetched rows, so the result is a
    /// subset of the range it scans, chosen by the data.
    Residual(Predicate<EngineColumns>),
    /// The predicate can never match, so there is nothing to compare.
    Unsatisfiable,
    /// The planner pushes the whole predicate into an index with this KeySpec, whose keys are
    /// canonical property ids, and no registered tree of the component (of every entity when
    /// `None`) matches it.
    NoMatchingTree { component: Option<ModelId>, key_spec: KeySpec<String> },
    /// The schema declares no value type for the property of this key part, so neither the
    /// tree's part nor the Selection's values for it can be checked against one.
    UndeclaredProperty { index: HashedIndex, part: usize },
    /// The tree keeps this key part in another value type than the schema declares for its
    /// property, so it files rows by their values cast to a type the Selection does not
    /// compare them in.
    MisregisteredPart { index: HashedIndex, part: usize },
    /// The Selection compares this key part with a value whose type does not encode alike with
    /// the type the schema declares for its property, integer widths counting as one. The
    /// resolver casts every compared value to its property's declared type, so the Selection
    /// was built or changed without it.
    NonCanonicalSelection { index: HashedIndex, part: usize },
    /// The tree orders this key part's nulls or collates it otherwise than the Selection
    /// compares it, so the tree's keys need not order as the Selection compares values.
    /// KeySpec::matches, which finds the tree, compares only properties, subpaths and
    /// directions.
    KeyPartMismatch { index: HashedIndex, part: usize },
    /// The tree can file one entity under several values of this key part within the range.
    FilesEntityTwice { index: HashedIndex, part: usize },
    /// The tree leaves out entities that lack this key part, which the range leaves open.
    OmitsEntities { index: HashedIndex, part: usize },
    /// The range has no exact cover in the tree.
    Range { index: HashedIndex, error: RangeError },
    /// The planner's bounds take a shape this decision does not read: a bound after the first
    /// key part that is not fixed to one value, or an infinite endpoint. The planner builds
    /// neither today.
    UnreadableBounds(KeyBounds),
}

/// The cover of `selection` over the first of `trees` that serves it, or the reason none does.
/// `declared_type` gives the value type the schema declares for a property, the entity id's
/// included, or `None` for a property the schema does not know, as the catalog's
/// `registered_value_type` answers it for the resolver.
pub fn cover_selection(
    selection: &Selection<Resolved>,
    trees: &[RegisteredTree],
    declared_type: impl Fn(&PropertyId) -> Option<ValueType>,
) -> Result<Cover, NotReusable> {
    if let Some(limit) = selection.limit {
        return Err(NotReusable::Limit(limit));
    }
    // A membership every match must have names the component whose trees may serve the rest
    // of the predicate; any other membership condition stays in it, as a residual.
    let (component, predicate) = match MaterializationPlan::new(selection).indexed_materialization() {
        Some((component, rest)) => (Some(component), rest.predicate),
        None => (None, selection.predicate.clone()),
    };
    let unordered = Selection { predicate, order_by: None, limit: None };
    let lowered =
        map_references(&unordered, &|path| ColumnPath::new(path.property_id().to_string(), path.subpath.clone()), &|model| *model);
    let plans = Planner::new(PlannerConfig::full_support()).plan(&lowered, &PropertyId::Id.to_string());
    // A plan that pushes the predicate down whole says more about why it failed than a residual.
    let (mut specific, mut residual) = (None, None);
    for plan in plans {
        match serve(plan, component, trees, &declared_type) {
            Ok(cover) => return Ok(cover),
            Err(refusal @ NotReusable::Residual(_)) => residual = residual.or(Some(refusal)),
            Err(refusal) => specific = specific.or(Some(refusal)),
        }
    }
    Err(specific.or(residual).unwrap_or(NotReusable::Residual(lowered.predicate)))
}

/// The cover of one plan over the first tree of `component` that matches and serves it.
fn serve(
    plan: Plan,
    component: Option<ModelId>,
    trees: &[RegisteredTree],
    declared_type: &impl Fn(&PropertyId) -> Option<ValueType>,
) -> Result<Cover, NotReusable> {
    let (key_spec, bounds) = match plan {
        Plan::EmptyScan => return Err(NotReusable::Unsatisfiable),
        // The residual of a table scan keeps every conjunct, comparisons of the entity id
        // included, so an empty one means the predicate is always true: every entity of the
        // component, which an entity-id index holds whole.
        Plan::TableScan { remaining_predicate: Predicate::True, .. } => (KeySpec::new(Vec::new()), KeyBounds::empty()),
        Plan::Index { index_spec, bounds, remaining_predicate: Predicate::True, .. } => (index_spec, bounds),
        Plan::TableScan { remaining_predicate, .. } | Plan::Index { remaining_predicate, .. } => {
            return Err(NotReusable::Residual(remaining_predicate));
        }
    };
    let mut refusal = None;
    for tree in trees.iter().filter(|tree| tree.matches(component, &key_spec)) {
        match tree.cover(&key_spec, &bounds, declared_type) {
            Ok(cover) => return Ok(cover),
            Err(error) => refusal = refusal.or(Some(error)),
        }
    }
    Err(refusal.unwrap_or(NotReusable::NoMatchingTree { component, key_spec }))
}

impl RegisteredTree {
    /// Whether this tree files the members of `component` (every entity when `None`) under a
    /// key the planner's `key_spec` matches, exactly or as a prefix.
    fn matches(&self, component: Option<ModelId>, key_spec: &KeySpec<String>) -> bool {
        let files = match &self.index {
            HashedIndex::EntityId => None,
            HashedIndex::Component { component, .. } => Some(*component),
        };
        files == component && key_spec.matches(&engine_key_spec(key_parts(&self.index))).is_some()
    }

    /// The cover of the planner's `bounds` on this tree's leading key parts, unless the tree or
    /// the bounds' values contradict the types the schema declares, the tree keeps those parts
    /// otherwise than the planner's `key_spec` compares them, or it does not hold exactly the
    /// entities the bounds name.
    fn cover(
        &self,
        key_spec: &KeySpec<String>,
        bounds: &KeyBounds,
        declared_type: &impl Fn(&PropertyId) -> Option<ValueType>,
    ) -> Result<Cover, NotReusable> {
        if let Some(part) = self.mismatched_part(key_spec) {
            return Err(NotReusable::KeyPartMismatch { index: self.index.clone(), part });
        }
        let range = key_range(bounds).ok_or_else(|| NotReusable::UnreadableBounds(bounds.clone()))?;
        let declared = self.declared_types(declared_type)?;
        if let Some(part) = noncanonical_part(&range, &declared) {
            return Err(NotReusable::NonCanonicalSelection { index: self.index.clone(), part });
        }
        let fixed = range.prefix.len();
        if let Some(&part) = self.multi_valued_parts.iter().find(|&&part| part >= fixed) {
            return Err(NotReusable::FilesEntityTwice { index: self.index.clone(), part });
        }
        let bounded = !matches!((&range.lower, &range.upper), (Bound::Unbounded, Bound::Unbounded));
        // The entity id is the one key part every entity has; an index files no entity that
        // lacks one of the others.
        let mut open_parts = key_parts(&self.index).iter().enumerate().skip(fixed + usize::from(bounded));
        if let Some((part, _)) = open_parts.find(|(_, part)| part.key != PropertyId::Id) {
            return Err(NotReusable::OmitsEntities { index: self.index.clone(), part });
        }
        let cover = Cover::new(self.index.clone(), range).map_err(|error| NotReusable::Range { index: self.index.clone(), error })?;
        // An empty range is a predicate that can never match, which the planner does not always detect.
        if cover.blocks().is_empty() {
            return Err(NotReusable::Unsatisfiable);
        }
        Ok(cover)
    }

    /// The first of the planner's key parts whose nulls this tree orders, or which it collates,
    /// otherwise than the planner's part. The value types are judged against the declared
    /// ones instead, since the planner takes a part's type from the first value the Selection
    /// compares it with.
    fn mismatched_part(&self, key_spec: &KeySpec<String>) -> Option<usize> {
        key_spec
            .keyparts
            .iter()
            .zip(key_parts(&self.index))
            .position(|(compared, kept)| compared.nulls != kept.nulls || compared.collation != kept.collation)
    }

    /// The value type the schema declares for the property of each of this tree's key parts,
    /// unless the schema declares none for one, or the tree keeps one in another type.
    fn declared_types(&self, declared_type: &impl Fn(&PropertyId) -> Option<ValueType>) -> Result<Vec<ValueType>, NotReusable> {
        key_parts(&self.index)
            .iter()
            .enumerate()
            .map(|(part, kept)| match declared_type(&kept.key) {
                None => Err(NotReusable::UndeclaredProperty { index: self.index.clone(), part }),
                Some(declared) if declared != kept.value_type => Err(NotReusable::MisregisteredPart { index: self.index.clone(), part }),
                Some(declared) => Ok(declared),
            })
            .collect()
    }
}

/// The first key part for which `range` holds a value whose type does not encode alike with the
/// part's `declared` type: a value the resolver would have cast to that type. A bound past the
/// last key part is on the entity id, whose type the cover itself checks.
fn noncanonical_part(range: &KeyRange, declared: &[ValueType]) -> Option<usize> {
    let bounds = [&range.lower, &range.upper].into_iter().filter_map(|bound| match bound {
        Bound::Included(value) | Bound::Excluded(value) => Some((range.prefix.len(), value)),
        Bound::Unbounded => None,
    });
    let mut values = range.prefix.iter().enumerate().chain(bounds);
    values
        .find(|&(part, value)| declared.get(part).is_some_and(|&declared| !encodes_alike(ValueType::of(value), declared)))
        .map(|(part, _)| part)
}

/// The key parts under which `index` files an entity, before its entity id.
fn key_parts(index: &HashedIndex) -> &[IndexKeyPart<PropertyId>] {
    match index {
        HashedIndex::EntityId => &[],
        HashedIndex::Component { key_spec, .. } => &key_spec.keyparts,
    }
}

/// The key range of the planner's bounds: the key parts fixed to one value, then at most one
/// part bounded on either side. `None` for any other shape.
fn key_range(bounds: &KeyBounds) -> Option<KeyRange> {
    let prefix: Vec<Value> = bounds.keyparts.iter().map_while(fixed_value).cloned().collect();
    match &bounds.keyparts[prefix.len()..] {
        [] => Some(KeyRange::prefix(prefix)),
        [bounded] => Some(KeyRange { prefix, lower: bound(&bounded.low, true)?, upper: bound(&bounded.high, false)? }),
        _ => None,
    }
}

/// The one value a component of the planner's bounds fixes its key part to, if it fixes one.
fn fixed_value(component: &KeyBoundComponent) -> Option<&Value> {
    match (&component.low, &component.high) {
        (
            Endpoint::Value { datum: KeyDatum::Val(low), inclusive: true },
            Endpoint::Value { datum: KeyDatum::Val(high), inclusive: true },
        ) if low == high => Some(low),
        _ => None,
    }
}

/// One side of the bounded key part, or `None` for an endpoint the planner does not build on
/// that side.
fn bound(endpoint: &Endpoint, is_lower: bool) -> Option<Bound<Value>> {
    match endpoint {
        Endpoint::Value { datum: KeyDatum::Val(value), inclusive: true } => Some(Bound::Included(value.clone())),
        Endpoint::Value { datum: KeyDatum::Val(value), inclusive: false } => Some(Bound::Excluded(value.clone())),
        Endpoint::UnboundedLow(_) if is_lower => Some(Bound::Unbounded),
        Endpoint::UnboundedHigh(_) if !is_lower => Some(Bound::Unbounded),
        _ => None,
    }
}

/// A tree's key parts as the planner names columns here: each property by its canonical id.
fn engine_key_spec(parts: &[IndexKeyPart<PropertyId>]) -> KeySpec<String> {
    let parts = parts.iter().map(|part| IndexKeyPart {
        key: part.key.to_string(),
        sub_path: part.sub_path.clone(),
        direction: part.direction,
        value_type: part.value_type,
        nulls: part.nulls,
        collation: part.collation.clone(),
    });
    KeySpec::new(parts.collect())
}

#[cfg(test)]
mod tests {
    use ankql::ast::{ComparisonOperator, Expr, Parsed, PropertyPath};
    use ankurah_core::indexing::{Block, IndexDirection, NullsOrder, encode_component_typed};
    use ankurah_core::schema::resolver::{ModelResolutionError, ModelResolver, ResolvedProperty, resolve_selection};
    use ankurah_core::selection::filter::{self, Filterable, evaluate_predicate};
    use ankurah_core::value::ValueType;
    use ankurah_proto::EntityId;

    use super::*;

    const PROPERTIES: [(&str, ValueType); 7] = [
        ("score", ValueType::I64),
        ("rank", ValueType::I64),
        ("name", ValueType::String),
        ("team", ValueType::EntityId),
        ("owner", ValueType::EntityId),
        ("done", ValueType::Bool),
        ("weight", ValueType::F64),
    ];

    fn property(name: &str) -> PropertyId {
        let mut bytes = [0u8; 32];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        PropertyId::EntityId(EntityId::from_bytes(bytes))
    }

    /// The component the Selections are written for.
    fn albums() -> ModelId { ModelId::EntityId(EntityId::from_bytes([7; 32])) }

    fn other_component() -> ModelId { ModelId::EntityId(EntityId::from_bytes([8; 32])) }

    struct Properties;

    impl ModelResolver for Properties {
        fn resolve_property(&self, _model: &ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
            Ok(PROPERTIES
                .iter()
                .find(|(known, _)| *known == name)
                .map(|&(name, value_type)| ResolvedProperty { id: property(name), value_type }))
        }
    }

    /// The value types the schema declares, by property, the entity id's included.
    fn declared_type(id: &PropertyId) -> Option<ValueType> {
        if *id == PropertyId::Id {
            return Some(ValueType::EntityId);
        }
        PROPERTIES.iter().find(|&&(name, _)| property(name) == *id).map(|&(_, value_type)| value_type)
    }

    /// Parse and resolve a Selection for the albums component, as a query does: its literals
    /// take their properties' types, and every match must be a member of the component.
    fn selection(text: &str) -> Selection<Resolved> {
        let parsed: Selection<Parsed> = ankql::parser::parse_selection(text).unwrap();
        resolve_selection(&albums(), &Properties, parsed).unwrap().and_member_of(albums())
    }

    /// The albums matching `predicate`, as a Selection built without the resolver: its values
    /// keep the types they are given.
    fn albums_where(predicate: Predicate<Resolved>) -> Selection<Resolved> {
        Selection { predicate, order_by: None, limit: None }.and_member_of(albums())
    }

    /// The comparison of property `name` with `value`.
    fn comparison(name: &str, operator: ComparisonOperator, value: Value) -> Predicate<Resolved> {
        Predicate::Comparison {
            left: Box::new(Expr::Path(PropertyPath::from(property(name)))),
            operator,
            right: Box::new(Expr::Literal(value)),
        }
    }

    fn everything() -> Selection<Resolved> { Selection { predicate: Predicate::True, order_by: None, limit: None } }

    fn part(name: &str, value_type: ValueType, direction: IndexDirection) -> IndexKeyPart<PropertyId> {
        let key = if name == "id" { PropertyId::Id } else { property(name) };
        match direction {
            IndexDirection::Asc => IndexKeyPart::asc(key, value_type),
            IndexDirection::Desc => IndexKeyPart::desc(key, value_type),
        }
    }

    /// A tree over an index of the albums component.
    fn tree(parts: Vec<IndexKeyPart<PropertyId>>) -> RegisteredTree {
        RegisteredTree {
            index: HashedIndex::Component { component: albums(), key_spec: KeySpec::new(parts) },
            multi_valued_parts: Vec::new(),
        }
    }

    fn entity_id_tree() -> RegisteredTree { RegisteredTree { index: HashedIndex::EntityId, multi_valued_parts: Vec::new() } }

    fn scores(direction: IndexDirection) -> RegisteredTree { tree(vec![part("score", ValueType::I64, direction)]) }

    /// Teams, then the entity id: every entity has the last part.
    fn teams() -> RegisteredTree {
        tree(vec![part("team", ValueType::EntityId, IndexDirection::Asc), part("id", ValueType::EntityId, IndexDirection::Asc)])
    }

    /// Teams, then owners, then the entity id.
    fn teams_and_owners() -> RegisteredTree {
        tree(vec![
            part("team", ValueType::EntityId, IndexDirection::Asc),
            part("owner", ValueType::EntityId, IndexDirection::Asc),
            part("id", ValueType::EntityId, IndexDirection::Asc),
        ])
    }

    fn entity(byte: u8) -> EntityId { EntityId::from_bytes([byte; 32]) }

    fn expected(tree: &RegisteredTree, prefix: Vec<Value>, lower: Bound<Value>, upper: Bound<Value>) -> Cover {
        Cover::new(tree.index.clone(), KeyRange { prefix, lower, upper }).unwrap()
    }

    /// An album as Selection evaluation sees it: a member of the albums component with a value
    /// for one property.
    struct Album {
        entity: EntityId,
        property: &'static str,
        value: Value,
    }

    impl Album {
        fn weighing(weight: f64, entity: EntityId) -> Self { Self { entity, property: "weight", value: Value::F64(weight) } }
    }

    impl Filterable for Album {
        fn value(&self, id: &PropertyId) -> Option<Value> {
            if *id == PropertyId::Id {
                Some(Value::EntityId(self.entity))
            } else {
                (*id == property(self.property)).then(|| self.value.clone())
            }
        }

        fn is_member_of(&self, model: &ModelId) -> Result<bool, filter::Error> { Ok(*model == albums()) }
    }

    /// Whether `cover` holds the address under which `tree` files `album`: its values for the
    /// tree's key parts, each encoded in the part's type and direction, then its entity id.
    fn holds(cover: &Cover, tree: &RegisteredTree, album: &Album) -> bool {
        let mut address = Vec::new();
        for part in key_parts(&tree.index) {
            let value = album.value(&part.key).expect("the album has a value for every key part");
            address.extend(encode_component_typed(&value, part.value_type, part.direction.is_desc()).unwrap());
        }
        address.extend(album.entity.to_bytes());
        cover.blocks().iter().any(|block| block.contains(&address))
    }

    #[test]
    fn the_full_replica_reuses_the_root_of_the_entity_id_tree() {
        let members = tree(Vec::new());
        assert_eq!(cover_selection(&everything(), &[members.clone(), entity_id_tree()], declared_type), Ok(Cover::root()));
        // A tree of one component never holds every entity.
        assert_eq!(
            cover_selection(&everything(), &[members], declared_type),
            Err(NotReusable::NoMatchingTree { component: None, key_spec: KeySpec::new(Vec::new()) })
        );
    }

    #[test]
    fn a_bare_membership_reuses_the_root_of_the_component_entity_id_tree() {
        let members = tree(Vec::new());
        let every_album = everything().and_member_of(albums());
        let cover =
            cover_selection(&every_album, &[entity_id_tree(), scores(IndexDirection::Asc), members.clone()], declared_type).unwrap();
        assert_eq!(cover, expected(&members, Vec::new(), Bound::Unbounded, Bound::Unbounded));
        assert_eq!(cover.blocks(), [Block::root()]);
        // The full replica's tree holds every entity, not only the component's members.
        assert_eq!(
            cover_selection(&every_album, &[entity_id_tree()], declared_type),
            Err(NotReusable::NoMatchingTree { component: Some(albums()), key_spec: KeySpec::new(Vec::new()) })
        );
    }

    #[test]
    fn a_range_on_an_integer_reuses_its_tree_in_either_direction() {
        for tree in [scores(IndexDirection::Asc), scores(IndexDirection::Desc)] {
            for (text, lower, upper) in [
                ("score >= 10 AND score < 20", Bound::Included(10), Bound::Excluded(20)),
                ("score > 10 AND score <= 20", Bound::Excluded(10), Bound::Included(20)),
                ("score > 10", Bound::Excluded(10), Bound::Unbounded),
                ("score <= 20", Bound::Unbounded, Bound::Included(20)),
            ] {
                let cover = cover_selection(&selection(text), std::slice::from_ref(&tree), declared_type).unwrap();
                assert_eq!(cover, expected(&tree, Vec::new(), lower.map(Value::I64), upper.map(Value::I64)), "{text}");
            }
            let cover = cover_selection(&selection("score = 15"), std::slice::from_ref(&tree), declared_type).unwrap();
            assert_eq!(cover, expected(&tree, vec![Value::I64(15)], Bound::Unbounded, Bound::Unbounded));
            assert_eq!(cover.blocks().len(), 1, "an equality on the whole key is one block");
        }
    }

    #[test]
    fn a_tree_of_another_component_does_not_serve() {
        let elsewhere = RegisteredTree {
            index: HashedIndex::Component {
                component: other_component(),
                key_spec: KeySpec::new(vec![part("score", ValueType::I64, IndexDirection::Asc)]),
            },
            multi_valued_parts: Vec::new(),
        };
        let refusal = cover_selection(&selection("score > 10"), &[elsewhere, entity_id_tree()], declared_type);
        assert!(
            matches!(refusal, Err(NotReusable::NoMatchingTree { component: Some(component), .. }) if component == albums()),
            "{refusal:?}"
        );
    }

    #[test]
    fn a_prefix_of_a_multi_part_tree_is_one_block() {
        let (team, owner) = (entity(0x10), entity(0x20));
        let tree = teams_and_owners();
        let both = selection(&format!("team = '{}' AND owner = '{}'", team.to_base64(), owner.to_base64()));
        let cover = cover_selection(&both, std::slice::from_ref(&tree), declared_type).unwrap();
        let key = [team.to_bytes(), owner.to_bytes()].concat();
        assert_eq!(cover.range(), &KeyRange::prefix(vec![Value::EntityId(team), Value::EntityId(owner)]));
        assert_eq!(cover.blocks(), [Block::new(&key, 512)]);
        let one = selection(&format!("team = '{}'", team.to_base64()));
        assert_eq!(cover_selection(&one, &[teams()], declared_type).unwrap().blocks(), [Block::new(&team.to_bytes(), 256)]);
    }

    #[test]
    fn an_order_never_prevents_reuse_and_a_limit_always_does() {
        let tree = scores(IndexDirection::Asc);
        let unordered = cover_selection(&selection("score >= 10"), std::slice::from_ref(&tree), declared_type).unwrap();
        for text in ["score >= 10 ORDER BY rank DESC", "score >= 10 ORDER BY score"] {
            assert_eq!(cover_selection(&selection(text), std::slice::from_ref(&tree), declared_type), Ok(unordered.clone()), "{text}");
        }
        for text in ["score >= 10 LIMIT 5", "score >= 10 ORDER BY score LIMIT 5"] {
            assert_eq!(cover_selection(&selection(text), std::slice::from_ref(&tree), declared_type), Err(NotReusable::Limit(5)), "{text}");
        }
    }

    #[test]
    fn a_residual_prevents_reuse() {
        let trees = [scores(IndexDirection::Asc), tree(vec![part("rank", ValueType::I64, IndexDirection::Asc)]), tree(Vec::new())];
        for text in ["score > 5 AND rank > 3", "score > 5 OR score < 2", "score != 5", "score IN (1, 2)"] {
            let refusal = cover_selection(&selection(text), &trees, declared_type);
            assert!(matches!(refusal, Err(NotReusable::Residual(_))), "{text}: {refusal:?}");
        }
        // Only the one membership every match must have names the component; another stays.
        let both_components = selection("score >= 10").and_member_of(other_component());
        let refusal = cover_selection(&both_components, &trees, declared_type);
        assert!(matches!(refusal, Err(NotReusable::Residual(Predicate::MemberOf(_)))), "{refusal:?}");
        let either = Selection {
            predicate: Predicate::Or(Box::new(Predicate::MemberOf(albums())), Box::new(Predicate::MemberOf(other_component()))),
            order_by: None,
            limit: None,
        };
        assert!(matches!(cover_selection(&either, &[tree(Vec::new()), entity_id_tree()], declared_type), Err(NotReusable::Residual(_))));
    }

    #[test]
    fn a_tree_must_match_the_index_the_planner_chooses() {
        let refusal = cover_selection(&selection("rank = 3"), &[scores(IndexDirection::Asc), tree(Vec::new())], declared_type);
        let key_spec = KeySpec::new(vec![IndexKeyPart::asc(property("rank").to_string(), ValueType::I64)]);
        assert_eq!(refusal, Err(NotReusable::NoMatchingTree { component: Some(albums()), key_spec }));
    }

    /// KeySpec::matches finds a tree by its properties, subpaths and directions alone; the tree
    /// must also order each matched part's nulls and collate it as the Selection compares it,
    /// and the refusal names the first part that differs.
    #[test]
    fn a_tree_keeping_a_part_otherwise_than_the_selection_compares_it_is_refused() {
        let team = entity(0x10);
        let both = selection(&format!("team = '{}' AND score > 3", team.to_base64()));
        let teams = part("team", ValueType::EntityId, IndexDirection::Asc);
        let score = part("score", ValueType::I64, IndexDirection::Asc);
        let nulls_first = IndexKeyPart { nulls: Some(NullsOrder::First), ..score.clone() };
        let collated = IndexKeyPart { collation: Some("und-u-ks-level2".to_owned()), ..score };
        for parts in [vec![teams.clone(), nulls_first], vec![teams, collated]] {
            let tree = tree(parts);
            let refusal = cover_selection(&both, std::slice::from_ref(&tree), declared_type);
            assert_eq!(refusal, Err(NotReusable::KeyPartMismatch { index: tree.index.clone(), part: 1 }));
        }
    }

    /// The tree must keep every key part in the type the schema declares for its property, the
    /// parts the Selection leaves open included, and in exactly that width: an I32 part cannot
    /// file a score past the I32 limits, which the I64 property can hold.
    #[test]
    fn a_tree_keeping_a_part_in_another_type_than_its_property_is_declared_with_is_refused() {
        let team = entity(0x10);
        let both = selection(&format!("team = '{}' AND score > 3", team.to_base64()));
        let teams = part("team", ValueType::EntityId, IndexDirection::Asc);
        let score = |value_type| part("score", value_type, IndexDirection::Asc);
        for (parts, part) in [
            (vec![part("team", ValueType::String, IndexDirection::Asc), score(ValueType::I64)], 0),
            (vec![teams.clone(), score(ValueType::F64)], 1),
            (vec![teams.clone(), score(ValueType::String)], 1),
            (vec![teams.clone(), score(ValueType::I32)], 1),
            (vec![teams.clone(), score(ValueType::I64), part("id", ValueType::String, IndexDirection::Asc)], 2),
        ] {
            let tree = tree(parts);
            let refusal = cover_selection(&both, std::slice::from_ref(&tree), declared_type);
            assert_eq!(refusal, Err(NotReusable::MisregisteredPart { index: tree.index.clone(), part }));
        }
        // Integer widths encode alike, yet the I32 part cannot file an album whose score is
        // i64::MAX, which the Selection admits.
        assert!(encode_component_typed(&Value::I64(i64::MAX), ValueType::I32, false).is_err());
        // A part whose property the schema does not know cannot be checked.
        let unknown = tree(vec![teams, score(ValueType::I64), part("label", ValueType::String, IndexDirection::Asc)]);
        assert_eq!(
            cover_selection(&both, std::slice::from_ref(&unknown), declared_type),
            Err(NotReusable::UndeclaredProperty { index: unknown.index.clone(), part: 2 })
        );
    }

    /// weight <= 3 with the integer 3, which the resolver would have cast to 3.0, over a tree
    /// registered with an I32 weight: that tree files the album weighing 3.5 as 3, inside the
    /// cover of the bound, though Selection evaluation compares 3.5 with 3.0 and rejects it.
    /// The tree and the Selection each contradict the declared F64, and each is refused for it
    /// on its own.
    #[test]
    fn a_tree_or_selection_contradicting_the_declared_types_is_refused() {
        let integer_bound = albums_where(comparison("weight", ComparisonOperator::LessThanOrEqual, Value::I64(3)));
        let float_bound = albums_where(comparison("weight", ComparisonOperator::LessThanOrEqual, Value::F64(3.0)));
        let integers = tree(vec![part("weight", ValueType::I32, IndexDirection::Asc)]);
        let floats = tree(vec![part("weight", ValueType::F64, IndexDirection::Asc)]);
        // Unchecked, the integer tree's cover of the bound holds the album the Selection rejects.
        let album = Album::weighing(3.5, entity(0x00));
        let unchecked = expected(&integers, Vec::new(), Bound::Unbounded, Bound::Included(Value::I64(3)));
        assert!(holds(&unchecked, &integers, &album));
        assert!(!evaluate_predicate(&album, &integer_bound.predicate).unwrap());

        let misregistered = Err(NotReusable::MisregisteredPart { index: integers.index.clone(), part: 0 });
        assert_eq!(cover_selection(&integer_bound, std::slice::from_ref(&integers), declared_type), misregistered);
        assert_eq!(cover_selection(&float_bound, std::slice::from_ref(&integers), declared_type), misregistered);
        assert_eq!(
            cover_selection(&integer_bound, std::slice::from_ref(&floats), declared_type),
            Err(NotReusable::NonCanonicalSelection { index: floats.index.clone(), part: 0 })
        );
        // The Selection as the resolver leaves it, over the tree as the schema declares it, holds
        // exactly the albums evaluation admits.
        let cover = cover_selection(&float_bound, std::slice::from_ref(&floats), declared_type).unwrap();
        for weight in [f64::NAN, f64::NEG_INFINITY, 2.5, 3.0, 3.0f64.next_up(), 3.5, f64::INFINITY] {
            for album in [entity(0x00), entity(0xFF)].map(|entity| Album::weighing(weight, entity)) {
                let admitted = evaluate_predicate(&album, &float_bound.predicate).unwrap();
                assert_eq!(holds(&cover, &floats, &album), admitted, "weight {weight}");
            }
        }
    }

    /// Integer widths encode alike, so a Selection built by hand may compare the I64 score with
    /// an I32 or I16 value: the tree, keeping the declared width, holds exactly the albums
    /// evaluation admits, scores past the I32 limits included.
    #[test]
    fn a_value_of_another_integer_width_than_its_property_is_canonical() {
        let between = albums_where(Predicate::And(
            Box::new(comparison("score", ComparisonOperator::GreaterThan, Value::I32(-3))),
            Box::new(comparison("score", ComparisonOperator::LessThanOrEqual, Value::I16(7))),
        ));
        let scores_past_i32 = [i64::MIN, i32::MIN as i64 - 1, -3, -2, 7, 8, i32::MAX as i64 + 1, i64::MAX];
        for direction in [IndexDirection::Asc, IndexDirection::Desc] {
            let tree = scores(direction);
            let cover = cover_selection(&between, std::slice::from_ref(&tree), declared_type).unwrap();
            for score in scores_past_i32 {
                let album = Album { entity: entity(0x00), property: "score", value: Value::I64(score) };
                let admitted = evaluate_predicate(&album, &between.predicate).unwrap();
                assert_eq!(holds(&cover, &tree, &album), admitted, "score {score} over {direction:?}");
            }
        }
    }

    /// The decision is conservative: each of these Selections is a range of the tree beside it,
    /// but the planner as it stands does not find that range, so the Selection is refused.
    #[test]
    fn ranges_the_planner_does_not_find_are_refused() {
        let key_spec =
            |parts: [&str; 2]| KeySpec::new(parts.map(|name| IndexKeyPart::asc(property(name).to_string(), ValueType::I64)).to_vec());
        // The planner keys equality conjuncts in the order they are written.
        let scores_then_ranks =
            tree(vec![part("score", ValueType::I64, IndexDirection::Asc), part("rank", ValueType::I64, IndexDirection::Asc)]);
        assert_eq!(
            cover_selection(&selection("rank = 2 AND score = 1"), std::slice::from_ref(&scores_then_ranks), declared_type),
            Err(NotReusable::NoMatchingTree { component: Some(albums()), key_spec: key_spec(["rank", "score"]) })
        );
        // KeySpec::matches accepts the same directions or all of them inverted, though with the
        // score fixed the ranks at least 2 are contiguous in this tree too.
        let mixed = tree(vec![part("score", ValueType::I64, IndexDirection::Desc), part("rank", ValueType::I64, IndexDirection::Asc)]);
        assert_eq!(
            cover_selection(&selection("score = 1 AND rank >= 2"), std::slice::from_ref(&mixed), declared_type),
            Err(NotReusable::NoMatchingTree { component: Some(albums()), key_spec: key_spec(["score", "rank"]) })
        );
        // The planner keeps comparisons of the entity id as a residual, for the members of a
        // component and for every entity alike.
        let after = format!("id > '{}'", entity(0x80).to_base64());
        let refusal = cover_selection(&selection(&after), &[tree(Vec::new())], declared_type);
        assert!(matches!(refusal, Err(NotReusable::Residual(_))), "{refusal:?}");
        let every_entity = resolve_selection(&albums(), &Properties, ankql::parser::parse_selection(&after).unwrap()).unwrap();
        let refusal = cover_selection(&every_entity, &[entity_id_tree()], declared_type);
        assert!(matches!(refusal, Err(NotReusable::Residual(_))), "{refusal:?}");
    }

    #[test]
    fn a_tree_that_leaves_out_entities_the_range_names_is_refused() {
        // An entity of the team without a score is in the result but not in the tree.
        let team = entity(0x10);
        let tree = tree(vec![part("team", ValueType::EntityId, IndexDirection::Asc), part("score", ValueType::I64, IndexDirection::Asc)]);
        let refusal = cover_selection(&selection(&format!("team = '{}'", team.to_base64())), std::slice::from_ref(&tree), declared_type);
        assert_eq!(refusal, Err(NotReusable::OmitsEntities { index: tree.index.clone(), part: 1 }));
        // Bounding the score as well names only entities the tree files.
        let both = selection(&format!("team = '{}' AND score > 3", team.to_base64()));
        assert_eq!(
            cover_selection(&both, std::slice::from_ref(&tree), declared_type),
            Ok(expected(&tree, vec![Value::EntityId(team)], Bound::Excluded(Value::I64(3)), Bound::Unbounded))
        );
    }

    #[test]
    fn a_tree_that_can_file_an_entity_twice_within_the_range_is_refused() {
        let team = entity(0x10);
        let tree = RegisteredTree { multi_valued_parts: vec![0], ..teams() };
        let across_teams = selection(&format!("team > '{}'", team.to_base64()));
        assert_eq!(
            cover_selection(&across_teams, std::slice::from_ref(&tree), declared_type),
            Err(NotReusable::FilesEntityTwice { index: tree.index.clone(), part: 0 })
        );
        // Within one team the tree files each entity once.
        let one_team = selection(&format!("team = '{}'", team.to_base64()));
        assert_eq!(
            cover_selection(&one_team, std::slice::from_ref(&tree), declared_type).unwrap().blocks(),
            [Block::new(&team.to_bytes(), 256)]
        );
    }

    #[test]
    fn the_first_tree_that_serves_the_selection_is_used() {
        let team = entity(0x10);
        let open_score =
            tree(vec![part("team", ValueType::EntityId, IndexDirection::Asc), part("score", ValueType::I64, IndexDirection::Asc)]);
        let one_team = selection(&format!("team = '{}'", team.to_base64()));
        let cover = cover_selection(&one_team, &[open_score, teams_and_owners(), teams()], declared_type).unwrap();
        assert_eq!(cover.index(), &teams().index);
    }

    #[test]
    fn a_range_on_an_ascending_string_is_refused_until_its_encoding_is_prefix_free() {
        let ascending = tree(vec![part("name", ValueType::String, IndexDirection::Asc)]);
        let refusal = cover_selection(&selection("name = 'jazz'"), std::slice::from_ref(&ascending), declared_type);
        let error = RangeError::AmbiguousEncoding { part: 0, value_type: ValueType::String, direction: IndexDirection::Asc };
        assert_eq!(refusal, Err(NotReusable::Range { index: ascending.index.clone(), error }));
        let descending = tree(vec![part("name", ValueType::String, IndexDirection::Desc)]);
        let cover =
            cover_selection(&selection("name >= 'blues' AND name < 'jazz'"), std::slice::from_ref(&descending), declared_type).unwrap();
        let text = |text: &str| Value::String(text.to_owned());
        assert_eq!(cover, expected(&descending, Vec::new(), Bound::Included(text("blues")), Bound::Excluded(text("jazz"))));
    }

    #[test]
    fn a_predicate_that_can_never_match_is_refused() {
        for text in ["score > 5 AND score < 3", "score >= 5 AND score < 5"] {
            assert_eq!(
                cover_selection(&selection(text), &[scores(IndexDirection::Asc)], declared_type),
                Err(NotReusable::Unsatisfiable),
                "{text}"
            );
        }
    }

    /// The review's counterexample, judged row by row by Selection evaluation: weight >= 0.0
    /// reused the block 80/1 of an ascending tree, and 00/1 of a descending one, each holding
    /// the rows of NaN weights, which the Selection rejects.
    #[test]
    fn a_float_range_reuses_no_block_holding_nan() {
        let weights = [f64::NAN, f64::NEG_INFINITY, -1.5, -0.0, 0.0, 0.5, f64::INFINITY];
        for direction in [IndexDirection::Asc, IndexDirection::Desc] {
            let tree = tree(vec![part("weight", ValueType::F64, direction)]);
            for text in ["weight >= 0.0", "weight > 0.5", "weight <= 0.5", "weight >= 0.0 AND weight < 1.0", "weight = 0.0"] {
                let selection = selection(text);
                let cover = cover_selection(&selection, std::slice::from_ref(&tree), declared_type).unwrap();
                for album in
                    weights.into_iter().flat_map(|weight| [entity(0x00), entity(0xFF)].map(|entity| Album::weighing(weight, entity)))
                {
                    let admitted = evaluate_predicate(&album, &selection.predicate).unwrap();
                    assert_eq!(holds(&cover, &tree, &album), admitted, "{text} over {direction:?} weights: weight {}", album.value);
                }
            }
        }
    }

    /// An equality or ordered comparison with NaN matches nothing. The planner's bounds never
    /// imply one, so it stays residual and the Selection is refused; bounds holding NaN that do
    /// reach a tree name no key and are refused as unsatisfiable.
    #[test]
    fn a_comparison_with_nan_is_refused() {
        let tree = tree(vec![part("weight", ValueType::F64, IndexDirection::Asc)]);
        let nan = || Value::F64(f64::NAN);
        for operator in [ComparisonOperator::GreaterThanOrEqual, ComparisonOperator::LessThan, ComparisonOperator::Equal] {
            let comparison = Predicate::Comparison {
                left: Box::new(Expr::Path(PropertyPath::from(property("weight")))),
                operator,
                right: Box::new(Expr::Literal(nan())),
            };
            let selection = Selection { predicate: comparison, order_by: None, limit: None }.and_member_of(albums());
            let refusal = cover_selection(&selection, std::slice::from_ref(&tree), declared_type);
            assert!(matches!(refusal, Err(NotReusable::Residual(_))), "{refusal:?}");
        }
        let column = property("weight").to_string();
        for (low, high) in
            [(Endpoint::incl(nan()), Endpoint::UnboundedHigh(ValueType::F64)), (Endpoint::incl(nan()), Endpoint::incl(nan()))]
        {
            let bounds = KeyBounds::new(vec![KeyBoundComponent { column: column.clone(), low, high }]);
            assert_eq!(tree.cover(&engine_key_spec(key_parts(&tree.index)), &bounds, &declared_type), Err(NotReusable::Unsatisfiable));
        }
    }

    /// A comparison with NaN is not always empty: NaN != x matches every album with a weight,
    /// NaN's included, and so does the negation of an equality or ordered comparison with NaN.
    /// The planner's bounds imply no comparison with NaN, alone, negated or combined with a
    /// range of weights, so each stays residual and the Selection is refused, over a tree in
    /// either direction.
    #[test]
    fn a_negated_or_unequal_comparison_with_nan_is_refused() {
        use ComparisonOperator::{Equal, GreaterThanOrEqual, LessThan, NotEqual};
        let weight = |operator, value| comparison("weight", operator, Value::F64(value));
        let not = |predicate| Predicate::Not(Box::new(predicate));
        let and = |left, right| Predicate::And(Box::new(left), Box::new(right));
        let or = |left, right| Predicate::Or(Box::new(left), Box::new(right));
        let nan = f64::NAN;
        let predicates = [
            weight(NotEqual, nan),
            not(weight(Equal, nan)),
            not(weight(GreaterThanOrEqual, nan)),
            not(weight(NotEqual, nan)),
            and(weight(GreaterThanOrEqual, 0.0), weight(NotEqual, nan)),
            and(not(weight(LessThan, nan)), weight(LessThan, 1.0)),
            or(weight(NotEqual, nan), weight(GreaterThanOrEqual, 0.0)),
            or(not(weight(Equal, nan)), weight(Equal, 0.0)),
            not(and(weight(GreaterThanOrEqual, 0.0), weight(Equal, nan))),
        ];
        for direction in [IndexDirection::Asc, IndexDirection::Desc] {
            let tree = tree(vec![part("weight", ValueType::F64, direction)]);
            for predicate in &predicates {
                let refusal = cover_selection(&albums_where(predicate.clone()), std::slice::from_ref(&tree), declared_type);
                assert!(matches!(refusal, Err(NotReusable::Residual(_))), "{predicate:?} over {direction:?}: {refusal:?}");
            }
        }
        for album in [f64::NAN, f64::NEG_INFINITY, 0.0, f64::INFINITY].map(|weight| Album::weighing(weight, entity(0x00))) {
            for predicate in [weight(NotEqual, nan), not(weight(GreaterThanOrEqual, nan))] {
                assert!(evaluate_predicate(&album, &predicate).unwrap(), "{predicate:?} at weight {}", album.value);
            }
        }
    }
}
