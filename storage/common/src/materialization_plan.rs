use std::collections::{BTreeMap, BTreeSet};

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_proto::{EntityId, ModelId};

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
        let candidates: BTreeSet<_> = all_entities.into_iter()
            .chain(materializations.values().flat_map(|ids| ids.iter().copied())).collect();
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
