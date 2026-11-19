//! Canonical selection ordering and the declared types needed to preserve it.

use ankql::ast::{OrderByItem, Resolved, Selection};
use ankurah_proto::{ModelId, PropertyId, SystemModel, SystemProperty};

use crate::{
    entity::TemporaryEntity,
    error::RetrievalError,
    selection::filter::{Filterable, ValueLookup},
    storage::StorageEngine,
    value::{Value, ValueType},
};

/// A strict continuation point in a selection's total order. Values are copied
/// from one entity version, so later changes cannot move this boundary.
#[derive(Clone, Debug)]
pub struct OrderBoundary {
    order: Vec<OrderByItem<Resolved>>,
    values: Vec<Option<Value>>,
}

impl OrderBoundary {
    /// Capture the effective ORDER BY, including the entity-ID tie-breaker.
    /// The selection must be ordered or limited to define a continuation point.
    pub fn from_item(selection: &Selection<Resolved>, item: &impl ValueLookup<Resolved>) -> Self {
        let order = ankql::selection::deterministic_order(selection).order_by.clone().unwrap_or_default();
        let values = order.iter().map(|part| item.order_value_at(&part.path)).collect();
        Self { order, values }
    }

    pub fn order(&self) -> &[OrderByItem<Resolved>] { &self.order }

    pub fn values(&self) -> &[Option<Value>] { &self.values }

    /// Reject use with another order, or an unordered unlimited selection.
    pub fn matches_order(&self, selection: &Selection<Resolved>) -> bool {
        !self.order.is_empty() && ankql::selection::deterministic_order(selection).order_by.as_deref() == Some(self.order.as_slice())
    }

    /// Whether the candidate lies strictly after this boundary.
    pub fn precedes(&self, item: &impl ValueLookup<Resolved>) -> bool {
        for (part, value) in self.order.iter().zip(&self.values) {
            let comparison = compare_nullable(value.as_ref(), item.order_value_at(&part.path).as_ref(), &part.direction);
            if !comparison.is_eq() {
                return comparison.is_lt();
            }
        }
        false
    }
}

/// Compare selection keys using PostgreSQL's value order and NULL placement.
/// Callers supply the effective order, including any entity-ID tie-breaker.
pub fn compare_items<S: ankql::ast::Stage, T: crate::selection::filter::ValueLookup<S>>(
    left: &T,
    right: &T,
    order: &[ankql::ast::OrderByItem<S>],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for part in order {
        let comparison =
            compare_nullable(left.order_value_at(&part.path).as_ref(), right.order_value_at(&part.path).as_ref(), &part.direction);
        if comparison != Ordering::Equal {
            return comparison;
        }
    }
    Ordering::Equal
}

pub fn compare_nullable(left: Option<&Value>, right: Option<&Value>, direction: &ankql::ast::OrderDirection) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let comparison = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(left), Some(right)) => ankurah_core_types::ordering::compare_values(left, right),
    };
    match direction {
        ankql::ast::OrderDirection::Asc => comparison,
        ankql::ast::OrderDirection::Desc => comparison.reverse(),
    }
}

/// A property's logical type, when its canonical definition is available.
/// An engine's physical column type cannot distinguish, for example, an
/// entity reference stored as text from an ordinary string.
pub async fn property_value_type(storage: &impl StorageEngine, property: PropertyId) -> Result<Option<ValueType>, RetrievalError> {
    match property {
        PropertyId::Id => Ok(Some(ValueType::EntityId)),
        PropertyId::System(property) => Ok(Some(crate::schema::resolver::system_property_value_type(property))),
        PropertyId::EntityId(id) => {
            let state = match storage.get_state(id).await {
                Ok(state) => state,
                Err(RetrievalError::EntityNotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            let model = ModelId::System(SystemModel::Property);
            if !state.payload.state.memberships.contains(&model) {
                return Err(RetrievalError::MissingComponent { entity_id: id, model_id: model });
            }
            let entity = TemporaryEntity::new(id, &state.payload.state)?;
            let Some(Value::String(name)) = entity.value(&PropertyId::System(SystemProperty::ValueType)) else {
                return Err(RetrievalError::InvalidState(format!("property {id} has no valid value_type")));
            };
            ValueType::from_property_str(&name)
                .map(Some)
                .ok_or_else(|| RetrievalError::InvalidState(format!("property {id} has no valid value_type")))
        }
    }
}
