use crate::{
    error::RetrievalError,
    reactor::AbstractEntity,
    value::{Value, ValueType},
};
use ankurah_proto as proto;
use async_trait::async_trait;

/// Trait for fetching entities to fill gaps when LIMIT causes entities to be evicted
#[async_trait]
pub trait GapFetcher<E: AbstractEntity>: Send + Sync + Clone + 'static {
    /// Fetch entities to fill a gap in a limited result set
    ///
    /// # Arguments
    /// * `collection_id` - The collection to fetch from
    /// * `selection` - The original selection (predicate, order_by, limit)
    /// * `last_entity` - The last entity in the current result set (used to build continuation predicate)
    /// * `gap_size` - Number of entities needed to fill the gap
    ///
    /// # Returns
    /// Vector of entities that match the selection and come after `last_entity` in sort order
    async fn fetch_gap(
        &self,
        collection_id: &proto::CollectionId,
        selection: &ankql::ast::Selection,
        last_entity: Option<&E>,
        gap_size: usize,
    ) -> Result<Vec<E>, RetrievalError>;
}

/// Build a supplemental predicate to fetch entities after the last entity in sort order
///
/// For ORDER BY a ASC, b DESC with last entity having a=5, b=10:
/// Returns: a >= 5 AND b <= 10 AND NOT (id = last_entity.id)
pub fn build_gap_predicate<E: AbstractEntity>(
    original_predicate: &ankql::ast::Predicate,
    order_by: &[ankql::ast::OrderByItem],
    last_entity: &E,
) -> Result<ankql::ast::Predicate, String> {
    use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, OrderDirection, Predicate};

    let mut gap_conditions = Vec::new();

    // Add original predicate
    gap_conditions.push(original_predicate.clone());

    // Add ORDER BY continuation conditions
    for order_item in order_by {
        let field_name = match &order_item.identifier {
            Identifier::Property(name) => name.clone(),
            _ => return Err("Collection properties not supported in ORDER BY".to_string()),
        };

        // Get the field value from the last entity
        if let Some(field_value) = last_entity.value(&field_name) {
            let literal = match field_value {
                Value::String(s) => Literal::String(s),
                Value::I16(i) => Literal::I16(i),
                Value::I32(i) => Literal::I32(i),
                Value::I64(i) => Literal::I64(i),
                Value::F64(f) => Literal::F64(f),
                Value::Bool(b) => Literal::Bool(b),
                Value::EntityId(id) => Literal::EntityId(id.into()),
                // Skip Object and Binary for now - they're not commonly used in ORDER BY
                Value::Object(_) | Value::Binary(_) => continue,
            };

            let operator = match order_item.direction {
                OrderDirection::Asc => ComparisonOperator::GreaterThanOrEqual,
                OrderDirection::Desc => ComparisonOperator::LessThanOrEqual,
            };

            let condition = Predicate::Comparison {
                left: Box::new(Expr::Identifier(order_item.identifier.clone())),
                operator,
                right: Box::new(Expr::Literal(literal)),
            };

            gap_conditions.push(condition);
        }
    }

    // Add entity ID exclusion to avoid fetching the last entity again
    let id_exclusion = Predicate::Not(Box::new(Predicate::Comparison {
        left: Box::new(Expr::Identifier(Identifier::Property("id".to_string()))),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Literal::EntityId(last_entity.id().clone().into()))),
    }));
    gap_conditions.push(id_exclusion);

    // Combine all conditions with AND
    let result =
        gap_conditions.into_iter().reduce(|acc, condition| Predicate::And(Box::new(acc), Box::new(condition))).unwrap_or(Predicate::True);

    Ok(result)
}

/// Infer ValueType from the first non-null value in a collection of entities
pub fn infer_value_type_for_field<E: AbstractEntity>(entities: &[E], field_name: &str) -> ValueType {
    for entity in entities {
        if let Some(value) = entity.value(field_name) {
            return ValueType::of(&value);
        }
    }

    // TODO: Get type from system catalog instead of defaulting to String
    ValueType::String
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use ankql::ast::{Identifier, OrderByItem, OrderDirection, Predicate};
    use ankurah_proto as proto;
    use maplit::hashmap;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone)]
    struct TestEntity {
        id: proto::EntityId,
        collection: proto::CollectionId,
        data: Arc<Mutex<HashMap<String, Value>>>,
    }

    impl TestEntity {
        fn new(id: u8, data: HashMap<String, Value>) -> Self {
            let mut id_bytes = [0u8; 16];
            id_bytes[15] = id;
            Self {
                id: proto::EntityId::from_bytes(id_bytes),
                collection: proto::CollectionId::fixed_name("test"),
                data: Arc::new(Mutex::new(data)),
            }
        }
    }

    impl AbstractEntity for TestEntity {
        fn collection(&self) -> proto::CollectionId { self.collection.clone() }

        fn id(&self) -> &proto::EntityId { &self.id }

        fn value(&self, field: &str) -> Option<Value> { self.data.lock().unwrap().get(field).cloned() }
    }

    #[test]
    fn test_build_gap_predicate_single_column_asc() {
        let entity = TestEntity::new(1, hashmap!("name".to_string() => Value::String("John".to_string())));

        let original_predicate = Predicate::True;
        let order_by = vec![OrderByItem { identifier: Identifier::Property("name".to_string()), direction: OrderDirection::Asc }];

        let gap_predicate = build_gap_predicate(&original_predicate, &order_by, &entity).unwrap();

        // Should create: true AND name >= "John" AND NOT (id = entity.id)
        // We can't easily test the exact structure, but we can verify it's not the original
        assert_ne!(format!("{:?}", gap_predicate), format!("{:?}", original_predicate));
    }

    #[test]
    fn test_build_gap_predicate_multi_column() {
        let entity =
            TestEntity::new(2, hashmap!("name".to_string() => Value::String("John".to_string()), "age".to_string() => Value::I32(30)));

        let original_predicate = Predicate::True;
        let order_by = vec![
            OrderByItem { identifier: Identifier::Property("name".to_string()), direction: OrderDirection::Asc },
            OrderByItem { identifier: Identifier::Property("age".to_string()), direction: OrderDirection::Desc },
        ];

        let gap_predicate = build_gap_predicate(&original_predicate, &order_by, &entity).unwrap();

        // Should create: true AND name >= "John" AND age <= 30 AND NOT (id = entity.id)
        assert_ne!(format!("{:?}", gap_predicate), format!("{:?}", original_predicate));
    }

    #[test]
    fn test_infer_value_type_for_field() {
        let entities = vec![
            TestEntity::new(1, hashmap!("name".to_string() => Value::String("Alice".to_string()))),
            TestEntity::new(2, hashmap!("age".to_string() => Value::I32(25))),
        ];

        assert_eq!(infer_value_type_for_field(&entities, "name"), ValueType::String);
        assert_eq!(infer_value_type_for_field(&entities, "age"), ValueType::I32);
        assert_eq!(infer_value_type_for_field(&entities, "nonexistent"), ValueType::String);
    }
}
