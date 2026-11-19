use std::borrow::Cow;

use ankql::ast::{OrderByItem, OrderDirection, Predicate, PropertyPath, Resolved, Selection};
use ankurah_core::{entity::TemporaryEntity, error::RetrievalError, selection::filter::evaluate_predicate};
use ankurah_proto::{Attested, EntityState, PropertyId};

/// LIMIT membership has a total order: use ascending entity ID when no order
/// is supplied, and as the final tie-breaker otherwise. An explicit ID order
/// already makes the selection deterministic, including descending ID order.
pub fn deterministic_limit_order(selection: &Selection<Resolved>) -> Cow<'_, Selection<Resolved>> {
    if selection.limit.is_none()
        || selection
            .order_by
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|order| order.path.property_id() == PropertyId::Id && order.path.subpath.is_empty())
    {
        return Cow::Borrowed(selection);
    }
    let mut selection = selection.clone();
    selection.order_by.get_or_insert_with(Vec::new).push(OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Asc });
    Cow::Owned(selection)
}

/// Finish a storage selection after candidate retrieval. Filtering precedes
/// global ordering and LIMIT, including when candidates came from several materializations.
pub fn select_states(
    mut states: Vec<Attested<EntityState>>,
    selection: &Selection<Resolved>,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    if !matches!(selection.predicate, Predicate::True) || selection.order_by.is_some() {
        let mut matches = Vec::new();
        for state in states {
            let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
            if evaluate_predicate(&entity, &selection.predicate).unwrap_or(false) {
                matches.push((state, entity));
            }
        }
        if let Some(order_by) = &selection.order_by {
            matches.sort_by(|(_, a), (_, b)| crate::sorting::compare_items(a, b, order_by));
        }
        states = matches.into_iter().map(|(state, _)| state).collect();
    }
    if let Some(limit) = selection.limit {
        states.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    }
    Ok(states)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_order_supplies_an_id_tie_breaker_and_preserves_explicit_direction() {
        let mut selection = Selection { predicate: Predicate::True, order_by: None, limit: Some(1) };
        let ordered = deterministic_limit_order(&selection);
        assert_eq!(ordered.order_by.as_ref().unwrap(), &[OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Asc }]);
        selection.order_by = Some(vec![OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Desc }]);
        assert!(matches!(deterministic_limit_order(&selection), Cow::Borrowed(_)));
        selection.order_by = None;
        selection.limit = None;
        assert!(matches!(deterministic_limit_order(&selection), Cow::Borrowed(_)));
    }
}
