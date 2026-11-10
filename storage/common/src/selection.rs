use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core::{entity::TemporaryEntity, error::RetrievalError, selection::filter::evaluate_predicate};
use ankurah_proto::EntityState;

/// Finish a storage selection after candidate retrieval. Filtering precedes
/// global ordering and LIMIT, including when candidates came from several materializations.
pub fn select_states(mut states: Vec<EntityState>, selection: &Selection<Resolved>) -> Result<Vec<EntityState>, RetrievalError> {
    if !matches!(selection.predicate, Predicate::True) || selection.order_by.is_some() {
        let mut matches = Vec::new();
        for state in states {
            let entity = TemporaryEntity::new(state.entity_id, &state.state)?;
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
