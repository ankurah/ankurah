use std::collections::{BTreeMap, BTreeSet};

use ankql::ast::{Predicate, Resolved, Selection};
use ankql::selection::map_references;
use ankurah_proto::{EntityId, ModelId, PropertyId};

use crate::{ColumnPath, Plan, Planner, PlannerConfig};

/// The materializations participating in a selection. Membership checks retain
/// their Boolean meaning; property comparisons remain for the engine to evaluate.
pub struct MaterializationPlan<'a> {
    pub models: Vec<ModelId>,
    selection: &'a Selection<Resolved>,
}

impl<'a> MaterializationPlan<'a> {
    pub fn new(selection: &'a Selection<Resolved>) -> Self {
        Self { models: selection.predicate.referenced_models().into_iter().collect(), selection }
    }

    /// Can a match exist outside all referenced materializations?
    /// For example, NOT MemberOf(A) requires scanning beyond A's materialization.
    pub fn needs_all_entities(&self) -> bool { self.may_match_memberships(|_| false) }

    /// Whether membership permits a match; property predicates still need evaluation.
    /// Unknown property values must not reject a candidate before its state is loaded.
    pub fn may_match_memberships(&self, member: impl Fn(&ModelId) -> bool) -> bool {
        membership_match(&self.selection.predicate, &member) != Some(false)
    }

    /// Combine materialization keys before hydrating or sorting their matching entities.
    pub fn candidate_ids(
        &self,
        materializations: &BTreeMap<ModelId, BTreeSet<EntityId>>,
        all_entities: impl IntoIterator<Item = EntityId>,
    ) -> Vec<EntityId> {
        let candidates: BTreeSet<_> =
            all_entities.into_iter().chain(materializations.values().flat_map(|ids| ids.iter().copied())).collect();
        candidates.into_iter().filter(|id| self.may_match_memberships(|model| materializations[model].contains(id))).collect()
    }

    /// Choose an indexed input from a mandatory membership conjunct. Other
    /// memberships remain in the predicate and must be checked before LIMIT.
    /// An OR such as MemberOf(A) OR MemberOf(B) cannot use A alone: it would miss B-only entities.
    pub fn indexed_materialization(&self) -> Option<(ModelId, Selection<Resolved>)> {
        let conjuncts = crate::predicate::ConjunctFinder::find(&self.selection.predicate);
        let model = conjuncts
            .iter()
            .find_map(|predicate| match predicate {
                Predicate::MemberOf(model) => Some(model),
                _ => None,
            })
            .or_else(|| match self.models.as_slice() {
                [model] if !self.needs_all_entities() => Some(model),
                _ => None,
            })?;
        let mut selection = self.selection.clone();
        selection.predicate = in_materialization(&selection.predicate, model);
        Some((*model, selection))
    }
}

/// Plan how one model's materialization table serves `selection`, the
/// selection [`MaterializationPlan::indexed_materialization`] hands that
/// model: the shared planner's first plan for it, lowered to the table's
/// columns, so that a SQL engine asks for the same indexes sled and IndexedDB
/// build for the same query. `columns` is the table's column for each
/// referenced property that has one; `config` states what the engine's
/// indexes can express. None when the predicate names a property without a
/// column: the engine answers such a predicate from stored states, outside
/// any index. A property without a column that only ORDER BY names is folded
/// to NULL and its ORDER BY key dropped, as sled folds it.
pub fn plan_on_materialization(
    selection: &Selection<Resolved>,
    columns: &BTreeMap<PropertyId, String>,
    config: PlannerConfig,
) -> Option<Plan> {
    let column = |property: &PropertyId| if *property == PropertyId::Id { Some("id") } else { columns.get(property).map(String::as_str) };
    let absent: Vec<_> = selection.referenced_properties().into_iter().filter(|property| column(property).is_none()).collect();
    if selection.predicate.referenced_properties().iter().any(|property| absent.contains(property)) {
        return None;
    }
    let lowered = map_references(
        &selection.assume_null(&absent),
        &|path| ColumnPath::new(column(&path.property_id()).expect("every property left has a column"), path.subpath.clone()),
        &|model| *model,
    );
    Planner::new(config).plan(&lowered, "id").into_iter().next()
}

// None means property values are needed. Negation preserves that uncertainty.
fn membership_match(predicate: &Predicate<Resolved>, member: &impl Fn(&ModelId) -> bool) -> Option<bool> {
    match predicate {
        Predicate::MemberOf(model) => Some(member(model)),
        Predicate::True => Some(true),
        Predicate::False => Some(false),
        Predicate::Not(inner) => membership_match(inner, member).map(|value| !value),
        Predicate::And(left, right) => match (membership_match(left, member), membership_match(right, member)) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        Predicate::Or(left, right) => match (membership_match(left, member), membership_match(right, member)) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn in_materialization(predicate: &Predicate<Resolved>, model: &ModelId) -> Predicate<Resolved> {
    match predicate {
        Predicate::MemberOf(member) if member == model => Predicate::True,
        Predicate::And(left, right) => match (in_materialization(left, model), in_materialization(right, model)) {
            (Predicate::True, right) => right,
            (left, Predicate::True) => left,
            (left, right) => Predicate::And(Box::new(left), Box::new(right)),
        },
        Predicate::Or(left, right) => match (in_materialization(left, model), in_materialization(right, model)) {
            (Predicate::True, _) | (_, Predicate::True) => Predicate::True,
            (left, right) => Predicate::Or(Box::new(left), Box::new(right)),
        },
        Predicate::Not(inner) => match in_materialization(inner, model) {
            Predicate::True => Predicate::False,
            Predicate::False => Predicate::True,
            inner => Predicate::Not(Box::new(inner)),
        },
        predicate => predicate.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::PropertyPath;

    /// The id a catalog would give the property named `name`.
    fn property(name: &str) -> EntityId {
        let mut bytes = [0; EntityId::BYTE_LEN];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        EntityId::from_bytes(bytes)
    }

    fn resolved(selection: &str) -> Selection<Resolved> {
        map_references(
            &ankql::parser::parse_selection(selection).expect("the fixture parses"),
            &|path| match path.first() {
                "id" => PropertyPath::id(),
                name => PropertyPath::registered(property(name), name, path.steps[1..].to_vec()),
            },
            &|model| *model.as_id().expect("no model in the fixture"),
        )
    }

    fn index_paths(plan: Option<Plan>) -> Vec<String> {
        match plan {
            Some(Plan::Index { index_spec, .. }) => index_spec.keyparts.iter().map(|part| part.full_path()).collect(),
            plan => panic!("expected an index plan, got {plan:?}"),
        }
    }

    #[test]
    fn a_selection_is_planned_on_the_table_s_own_columns() {
        let columns = BTreeMap::from([
            (PropertyId::EntityId(property("recipient")), "recipient_2k4q".to_owned()),
            (PropertyId::EntityId(property("status")), "status".to_owned()),
            (PropertyId::EntityId(property("detail")), "detail".to_owned()),
        ]);
        let plan = |selection| plan_on_materialization(&resolved(selection), &columns, PlannerConfig::full_support());
        assert_eq!(index_paths(plan("status = 'unread' AND recipient = 'a'")), ["status", "recipient_2k4q"]);
        assert_eq!(index_paths(plan("detail.kind = 'mention' ORDER BY id")), ["detail.kind", "id"]);

        // An ORDER BY on a property without a column is dropped; a predicate on one plans nothing.
        assert_eq!(index_paths(plan("recipient = 'a' ORDER BY anchor")), ["recipient_2k4q"]);
        assert!(plan("recipient = 'a' AND anchor = 'x'").is_none());
        assert!(matches!(plan("status != 'dismissed'"), Some(Plan::TableScan { .. })));
    }
}
