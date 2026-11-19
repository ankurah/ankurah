use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core::{entity::TemporaryEntity, error::RetrievalError, selection::filter::evaluate_predicate};
use ankurah_proto::{Attested, EntityState};

/// Select the first N predicate matches while scanning canonical states in any
/// order. Retains at most N states using query value extraction and sortable
/// value bytes, PostgreSQL's default null placement, then ascending entity ID.
///
/// This is the snapshot fallback when an engine cannot run its query planner
/// inside the snapshot. It bounds memory, but still examines the full source.
pub struct LimitedSelection<'a> {
    selection: &'a Selection<Resolved>,
    limit: usize,
    selected: std::collections::BTreeMap<(Vec<Vec<u8>>, ankurah_proto::EntityId), Attested<EntityState>>,
}

impl<'a> LimitedSelection<'a> {
    pub fn new(selection: &'a Selection<Resolved>) -> Result<Self, RetrievalError> {
        let limit = selection.limit.ok_or_else(|| RetrievalError::Other("a limited snapshot selection requires LIMIT".into()))?;
        let limit =
            usize::try_from(limit).map_err(|_| RetrievalError::Other("selection limit exceeds this target's address space".into()))?;
        Ok(Self { selection, limit, selected: std::collections::BTreeMap::new() })
    }

    pub fn consider(&mut self, state: Attested<EntityState>) -> Result<(), RetrievalError> {
        use ankurah_core::selection::filter::{Error, ValueLookup};
        use ankurah_core_types::collation::Collatable;

        if self.limit == 0 {
            return Ok(());
        }
        let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
        match evaluate_predicate(&entity, &self.selection.predicate) {
            Ok(true) => {}
            Ok(false) | Err(Error::PropertyNotFound(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
        }

        let mut key = Vec::new();
        for order in self.selection.order_by.as_deref().unwrap_or_default() {
            let mut part = match entity.value_at(&order.path) {
                None => vec![2],
                Some(value) => {
                    let mut part = vec![1];
                    // Terminate the value without making its encoding a prefix
                    // of another. Inverting a descending part must reverse the
                    // order of strings such as "a" and "aa" as well.
                    for byte in Collatable::to_bytes(&value) {
                        part.push(byte);
                        if byte == 0 {
                            part.push(255);
                        }
                    }
                    part.extend([0, 0]);
                    part
                }
            };
            if order.direction == ankql::ast::OrderDirection::Desc {
                part.iter_mut().for_each(|byte| *byte = !*byte);
            }
            key.push(part);
        }
        let key = (key, state.payload.entity_id);
        if self.selected.len() == self.limit && self.selected.last_key_value().is_some_and(|(last, _)| key >= *last) {
            return Ok(());
        }
        self.selected.insert(key, state);
        if self.selected.len() > self.limit {
            self.selected.pop_last();
        }
        Ok(())
    }

    pub fn finish(self) -> Vec<Attested<EntityState>> { self.selected.into_values().collect() }
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
mod limited_tests {
    use super::*;
    use ankql::ast::{OrderByItem, OrderDirection};
    use ankurah_core::property::backend::{LWWBackend, PropertyBackend};
    use ankurah_core_types::{EntityId, PropertyId, Value};
    use ankurah_proto::{Clock, State, StateBuffers};
    use std::collections::BTreeMap;

    fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([99; 32])) }

    fn state(id: u8, value: Option<Value>) -> Attested<EntityState> {
        let backend = LWWBackend::new();
        if let Some(value) = value {
            backend.set(property(), Some(value));
        }
        let event = ankurah_core::test_helpers::commit_backend(&backend, id).unwrap();
        Attested::opt(
            EntityState {
                entity_id: EntityId::from_bytes([id; 32]),
                state: State {
                    state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer().unwrap())])),
                    memberships: Default::default(),
                    head: Clock::singleton(&event),
                },
            },
            None,
        )
    }

    fn select(selection: &Selection<Resolved>, states: Vec<Attested<EntityState>>) -> Vec<u8> {
        let mut selected = LimitedSelection::new(selection).unwrap();
        for state in states {
            selected.consider(state).unwrap();
            assert!(selected.selected.len() <= selection.limit.unwrap() as usize);
        }
        selected.finish().into_iter().map(|state| state.payload.entity_id.to_bytes()[0]).collect()
    }

    #[test]
    fn bounded_selection_orders_prefix_strings_nulls_and_id_ties() {
        let mut selection = Selection {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem { path: property().into(), direction: OrderDirection::Asc }]),
            limit: Some(4),
        };
        let values = || {
            vec![
                state(4, None),
                state(3, Some(Value::String("aa".into()))),
                state(2, Some(Value::String("a".into()))),
                state(1, Some(Value::String("a".into()))),
            ]
        };
        assert_eq!(select(&selection, values()), [1, 2, 3, 4]);
        selection.order_by.as_mut().unwrap()[0].direction = OrderDirection::Desc;
        assert_eq!(select(&selection, values()), [4, 3, 1, 2]);
        selection.limit = Some(2);
        assert_eq!(select(&selection, values()), [4, 3]);
        selection.order_by = None;
        assert_eq!(select(&selection, values()), [1, 2]);
        selection.limit = Some(0);
        assert!(select(&selection, values()).is_empty());
        selection.limit = None;
        assert!(LimitedSelection::new(&selection).is_err());
    }

    #[test]
    fn nested_json_scalar_membership_uses_query_numeric_order() {
        let mut path = ankql::ast::PropertyPath::from(property());
        path.subpath.push("score".into());
        let selection = Selection {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem { path, direction: OrderDirection::Asc }]),
            limit: Some(2),
        };
        let values = vec![
            state(1, Some(Value::String(r#"{"score":10}"#.into()))),
            state(2, Some(Value::String(r#"{"score":2}"#.into()))),
            state(3, Some(Value::String(r#"{"score":-1}"#.into()))),
            state(4, Some(Value::String(r#"{}"#.into()))),
        ];
        assert_eq!(select(&selection, values), [3, 2]);
    }
}
