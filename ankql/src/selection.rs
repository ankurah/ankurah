mod map;
pub mod sql;
pub use map::map_references;

/// Give ordered or limited selections a total order. Entity IDs break ties
/// ascending unless an explicit ID part already determines the order.
pub fn deterministic_order(
    selection: &crate::ast::Selection<crate::ast::Resolved>,
) -> std::borrow::Cow<'_, crate::ast::Selection<crate::ast::Resolved>> {
    use crate::ast::{OrderByItem, OrderDirection, PropertyId, PropertyPath};
    use std::borrow::Cow;
    let order = selection.order_by.as_deref().unwrap_or_default();
    if (selection.limit.is_none() && order.is_empty())
        || order.iter().any(|part| part.path.property_id() == PropertyId::Id && part.path.subpath.is_empty())
    {
        return Cow::Borrowed(selection);
    }
    let mut selection = selection.clone();
    selection.order_by.get_or_insert_with(Vec::new).push(OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Asc });
    Cow::Owned(selection)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{OrderByItem, OrderDirection, Predicate, PropertyPath, Selection};

    #[test]
    fn limit_without_order_uses_ids_but_an_unlimited_unordered_query_stays_unordered() {
        let mut selection = Selection { predicate: Predicate::True, order_by: None, limit: None };
        assert!(deterministic_order(&selection).order_by.is_none());
        selection.limit = Some(0);
        assert_eq!(
            deterministic_order(&selection).order_by,
            Some(vec![OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Asc }])
        );
    }

    #[test]
    fn an_explicit_id_direction_already_defines_total_order() {
        let selection = Selection {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem { path: PropertyPath::id(), direction: OrderDirection::Desc }]),
            limit: Some(2),
        };
        assert_eq!(deterministic_order(&selection).as_ref(), &selection);
    }
}
