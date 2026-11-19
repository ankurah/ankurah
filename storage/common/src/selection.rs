//! Canonical filtering and ordering after engine candidate retrieval.

use std::{cmp::Ordering, collections::BinaryHeap, sync::Arc};

use ankql::ast::{OrderByItem, Resolved, Selection};
use ankurah_core::{
    entity::TemporaryEntity,
    error::RetrievalError,
    selection::filter::{ValueLookup, evaluate_predicate},
    selection::ordering::OrderBoundary,
};
use ankurah_core_types::Value;
use ankurah_proto::{Attested, EntityState};

/// Streaming canonical selection. A limited query retains at most LIMIT states,
/// even when an engine must inspect every candidate to establish their order.
/// Engine projections may select candidates, but cannot change these values.
pub struct StateSelection<'a> {
    selection: &'a Selection<Resolved>,
    order: Arc<Vec<OrderByItem<Resolved>>>,
    candidates: BinaryHeap<Candidate>,
    seen: usize,
    after: Option<OrderBoundary>,
}

impl<'a> StateSelection<'a> {
    pub fn new(selection: &'a Selection<Resolved>) -> Self {
        Self {
            selection,
            order: Arc::new(selection.order_by.clone().unwrap_or_default()),
            candidates: BinaryHeap::new(),
            seen: 0,
            after: None,
        }
    }

    /// Keep only candidates strictly after this boundary, before applying LIMIT.
    pub fn after(mut self, boundary: Option<&OrderBoundary>) -> Result<Self, RetrievalError> {
        if let Some(boundary) = boundary {
            if !boundary.matches_order(self.selection) {
                return Err(RetrievalError::InvalidState("refill boundary does not match selection order".into()));
            }
            self.order = Arc::new(boundary.order().to_vec());
            self.after = Some(boundary.clone());
        }
        Ok(self)
    }

    pub fn push(&mut self, state: Attested<EntityState>) -> Result<(), RetrievalError> {
        if self.selection.limit == Some(0) {
            return Ok(());
        }
        let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
        if !evaluate_predicate(&entity, &self.selection.predicate).unwrap_or(false) {
            return Ok(());
        }
        if self.after.as_ref().is_some_and(|boundary| !boundary.precedes(&entity)) {
            return Ok(());
        }
        let values = self.order.iter().map(|order| entity.order_value_at(&order.path)).collect();
        let candidate = Candidate { state, values, order: self.order.clone(), position: self.seen };
        self.seen += 1;
        if self.selection.limit.is_none_or(|limit| (self.candidates.len() as u64) < limit) {
            self.candidates.push(candidate);
        } else if self.candidates.peek().is_some_and(|worst| candidate < *worst) {
            self.candidates.pop();
            self.candidates.push(candidate);
        }
        Ok(())
    }

    pub fn finish(self) -> Vec<Attested<EntityState>> {
        self.candidates.into_sorted_vec().into_iter().map(|candidate| candidate.state).collect()
    }
}

struct Candidate {
    state: Attested<EntityState>,
    values: Vec<Option<Value>>,
    order: Arc<Vec<OrderByItem<Resolved>>>,
    position: usize,
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // A SQL engine may have already established order and ask only for
        // residual filtering. No ORDER BY here means preserve candidate order.
        if self.order.is_empty() {
            return self.position.cmp(&other.position);
        }
        for ((a, b), order) in self.values.iter().zip(&other.values).zip(&*self.order) {
            let cmp = crate::sorting::compare_nullable(a.as_ref(), b.as_ref(), &order.direction);
            if !cmp.is_eq() {
                return cmp;
            }
        }
        self.state.payload.entity_id.cmp(&other.state.payload.entity_id)
    }
}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() }
}
impl Eq for Candidate {}

/// Finish candidates already collected by an engine. Filtering precedes ordering
/// and LIMIT, including when candidates came from several materializations.
pub fn select_states(
    states: Vec<Attested<EntityState>>,
    selection: &Selection<Resolved>,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    let mut selected = StateSelection::new(selection);
    for state in states {
        selected.push(state)?;
    }
    Ok(selected.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::{OrderDirection, Predicate, PropertyPath};
    use ankurah_proto::{EntityId, State};

    fn state(byte: u8) -> Attested<EntityState> {
        Attested::opt(EntityState { entity_id: EntityId::from_bytes([byte; 32]), state: State::default() }, None)
    }

    #[test]
    fn residual_filtering_preserves_native_candidate_order() {
        let selection = Selection { predicate: Predicate::True, order_by: None, limit: Some(2) };
        let actual = select_states(vec![state(3), state(1), state(2)], &selection).unwrap();
        assert_eq!(
            actual.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(),
            vec![state(3).payload.entity_id, state(1).payload.entity_id]
        );
    }

    #[test]
    fn canonical_limit_bounds_retained_states() {
        let selection = Selection {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Asc }]),
            limit: Some(2),
        };
        let mut selected = StateSelection::new(&selection);
        for byte in (0..=255).rev() {
            selected.push(state(byte)).unwrap();
            assert!(selected.candidates.len() <= 2);
        }
        assert_eq!(
            selected.finish().iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(),
            vec![state(0).payload.entity_id, state(1).payload.entity_id]
        );
    }

    #[test]
    fn refill_filters_before_limit_and_rejects_another_order() {
        let selection = Selection { predicate: Predicate::True, order_by: None, limit: Some(1) };
        let at = state(2);
        let boundary = OrderBoundary::from_item(&selection, &TemporaryEntity::new(at.payload.entity_id, &at.payload.state).unwrap());
        let mut selected = StateSelection::new(&selection).after(Some(&boundary)).unwrap();
        for byte in (0..=5).rev() {
            selected.push(state(byte)).unwrap();
            assert!(selected.candidates.len() <= 1);
        }
        assert_eq!(selected.finish()[0].payload.entity_id, state(3).payload.entity_id);

        let reversed =
            Selection { order_by: Some(vec![OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Desc }]), ..selection };
        assert!(StateSelection::new(&reversed).after(Some(&boundary)).is_err());
    }
}
