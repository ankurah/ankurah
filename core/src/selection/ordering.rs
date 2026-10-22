//! Ordering shared by storage result selection and captured query views.
use std::cmp::Ordering;

use super::filter::ValueLookup;

/// Compare ORDER BY values, with missing values before present values in either
/// direction. Equal values compare equal; callers choose a stable tie order.
pub fn compare_items<S: ankql::ast::Stage, T: ValueLookup<S>>(a: &T, b: &T, order_by: &[ankql::ast::OrderByItem<S>]) -> Ordering {
    for order_item in order_by {
        let a_val = a.value_at(&order_item.path);
        let b_val = b.value_at(&order_item.path);

        // Handle None values: None sorts before Some
        let cmp = match (a_val, b_val, &order_item.direction) {
            (None, None, _) => Ordering::Equal,
            (None, Some(_), _) => Ordering::Less,
            (Some(_), None, _) => Ordering::Greater,
            (Some(a), Some(b), ankql::ast::OrderDirection::Asc) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            (Some(a), Some(b), ankql::ast::OrderDirection::Desc) => b.partial_cmp(&a).unwrap_or(Ordering::Equal),
        };

        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}
