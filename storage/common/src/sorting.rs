use ankql::selection::filter::Filterable;
use ankql::{self, ast as ankql_ast};
use ankurah_core::error::RetrievalError;
use ankurah_proto::{Attested, CollectionId, EntityState};

/// Apply in-memory sort for ORDER BY spill using TemporaryEntity collation
pub fn apply_order_by_sort(
    results: &mut Vec<Attested<EntityState>>,
    order_by: &Option<Vec<ankql_ast::OrderByItem>>,
    collection_id: &CollectionId,
) -> Result<(), RetrievalError> {
    let Some(order_items) = order_by else { return Ok(()) };

    results.sort_by(|a, b| {
        for order_item in order_items {
            if let ankql_ast::Identifier::Property(field_name) = &order_item.identifier {
                let entity_a =
                    ankurah_core::entity::TemporaryEntity::new(a.payload.entity_id, collection_id.clone(), &a.payload.state).ok();
                let entity_b =
                    ankurah_core::entity::TemporaryEntity::new(b.payload.entity_id, collection_id.clone(), &b.payload.state).ok();

                if let (Some(ent_a), Some(ent_b)) = (entity_a, entity_b) {
                    let val_a = ent_a.value(field_name);
                    let val_b = ent_b.value(field_name);

                    use std::cmp::Ordering;
                    let cmp = match (val_a, val_b) {
                        (Some(a_str), Some(b_str)) => {
                            if let (Ok(a_int), Ok(b_int)) = (a_str.parse::<i64>(), b_str.parse::<i64>()) {
                                a_int.cmp(&b_int)
                            } else if let (Ok(a_float), Ok(b_float)) = (a_str.parse::<f64>(), b_str.parse::<f64>()) {
                                a_float.partial_cmp(&b_float).unwrap_or(Ordering::Equal)
                            } else {
                                a_str.cmp(&b_str)
                            }
                        }
                        (Some(_), None) => Ordering::Greater,
                        (None, Some(_)) => Ordering::Less,
                        (None, None) => Ordering::Equal,
                    };

                    let final_cmp = match order_item.direction {
                        ankql_ast::OrderDirection::Asc => cmp,
                        ankql_ast::OrderDirection::Desc => cmp.reverse(),
                    };

                    if final_cmp != Ordering::Equal {
                        return final_cmp;
                    }
                }
            }
        }
        a.payload.entity_id.cmp(&b.payload.entity_id)
    });

    Ok(())
}
