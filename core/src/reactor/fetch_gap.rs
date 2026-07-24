use crate::{
    context::NodeAndContext,
    error::RetrievalError,
    node::{MatchArgs, Node, NodeInner},
    policy::PolicyAgent,
    reactor::{AbstractEntity, ExtractValue},
    storage::StorageEngine,
    value::ValueType,
};
use async_trait::async_trait;
use std::sync::{Arc, Weak};

/// Trait for fetching entities to fill gaps when LIMIT causes entities to be evicted
#[async_trait]
pub trait GapFetcher<E: AbstractEntity>: Send + Sync + 'static {
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
        collection_id: &crate::ModelId,
        selection: &ankql::ast::Selection,
        last_entity: Option<&E>,
        gap_size: usize,
    ) -> Result<Vec<E>, RetrievalError>;
}

/// Concrete implementation of GapFetcher using a WeakNode and typed ContextData
#[derive(Clone)]
pub struct QueryGapFetcher<SE, PA>
where
    SE: StorageEngine,
    PA: PolicyAgent,
{
    weak_node: Weak<NodeInner<SE, PA>>,
    cdata: PA::ContextData,
}

impl<SE, PA> QueryGapFetcher<SE, PA>
where
    SE: StorageEngine,
    PA: PolicyAgent,
{
    pub fn new(node: &Node<SE, PA>, cdata: PA::ContextData) -> Self { Self { weak_node: Arc::downgrade(&node.0), cdata } }
}

#[async_trait]
impl<SE, PA> GapFetcher<crate::entity::Entity> for QueryGapFetcher<SE, PA>
where
    SE: StorageEngine + 'static,
    PA: PolicyAgent + 'static,
{
    async fn fetch_gap(
        &self,
        collection_id: &crate::ModelId,
        selection: &ankql::ast::Selection,
        last_entity: Option<&crate::entity::Entity>,
        gap_size: usize,
    ) -> Result<Vec<crate::entity::Entity>, RetrievalError> {
        // Try to upgrade the weak reference to the node
        let node_inner = self
            .weak_node
            .upgrade()
            .ok_or_else(|| RetrievalError::storage(std::io::Error::other("Node has been dropped, cannot fill gap")))?;

        // Create a Node wrapper and NodeAndContext
        let node = Node(node_inner);
        let node_context = NodeAndContext { node, cdata: self.cdata.clone() };

        // Build gap predicate if we have a last entity
        let gap_selection = if let Some(last) = last_entity {
            let gap_predicate = if let Some(ref order_by) = selection.order_by {
                build_continuation_predicate(&selection.predicate, order_by, last)
                    .map_err(|e| RetrievalError::storage(std::io::Error::other(e)))?
            } else {
                selection.predicate.clone()
            };

            ankql::ast::Selection { predicate: gap_predicate, order_by: selection.order_by.clone(), limit: Some(gap_size as u64) }
        } else {
            // No last entity, just use original selection with gap_size limit
            ankql::ast::Selection {
                predicate: selection.predicate.clone(),
                order_by: selection.order_by.clone(),
                limit: Some(gap_size as u64),
            }
        };

        let match_args = MatchArgs { selection: gap_selection, cached: false };

        node_context.fetch_entities(collection_id, match_args).await
    }
}

/// Build a supplemental predicate to fetch entities after the last entity in sort order
///
/// For ORDER BY a ASC, b DESC with last entity having a=5, b=10:
/// Returns: a >= 5 AND b <= 10 AND NOT (id = last_entity.id)
pub fn build_continuation_predicate<E: AbstractEntity>(
    original_predicate: &ankql::ast::Predicate,
    order_by: &[ankql::ast::OrderByItem],
    last_entity: &E,
) -> Result<ankql::ast::Predicate, String> {
    use ankql::ast::{ComparisonOperator, Expr, OrderDirection, OrderKey, PathExpr, Predicate, PropertyId, Value};

    let mut gap_conditions = Vec::new();

    // Add original predicate
    gap_conditions.push(original_predicate.clone());

    // Add ORDER BY continuation conditions
    for order_item in order_by {
        // Read the last entity's value for this sort key (sub-path aware, so a
        // JSON-keyed ordering reads the same component the sort encoded), and
        // keep the key expression to reuse as the continuation comparison's
        // left side. Sub-path values arrive as Value::Json and are skipped by
        // the literal conversion below, like every other Json sort value.
        let (field_value, key_expr) = match &order_item.key {
            OrderKey::Property(identifier) => (identifier.extract_value(last_entity), Expr::PropertyPath(identifier.clone())),
            OrderKey::Path(path) => (
                ankql::ast::SystemProperty::from_name(path.first()).and_then(|property| {
                    crate::reactor::extract_property_value(&PropertyId::System(property), &path.steps[1..], last_entity)
                }),
                Expr::Path(path.clone()),
            ),
        };
        if let Some(field_value) = field_value {
            let literal = match field_value {
                Value::String(s) => Value::String(s),
                Value::I16(i) => Value::I16(i),
                Value::I32(i) => Value::I32(i),
                Value::I64(i) => Value::I64(i),
                Value::F64(f) => Value::F64(f),
                Value::Bool(b) => Value::Bool(b),
                Value::EntityId(id) => Value::EntityId(id.into()),
                // Skip Object, Binary, and Json for now - they're not commonly used in ORDER BY
                Value::Object(_) | Value::Binary(_) | Value::Json(_) => continue,
            };

            let operator = match order_item.direction {
                OrderDirection::Asc => ComparisonOperator::GreaterThanOrEqual,
                OrderDirection::Desc => ComparisonOperator::LessThanOrEqual,
            };

            let condition = Predicate::Comparison { left: Box::new(key_expr), operator, right: Box::new(Expr::Literal(literal)) };

            gap_conditions.push(condition);
        }
    }

    // Add entity ID exclusion to avoid fetching the last entity again
    let id_exclusion = Predicate::Comparison {
        left: Box::new(Expr::Path(PathExpr::simple("id"))),
        operator: ComparisonOperator::NotEqual,
        right: Box::new(Expr::Literal(Value::EntityId((*last_entity.id()).into()))),
    };
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
    use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, OrderKey, PathExpr, Predicate, PropertyPath};
    use ankurah_proto as proto;
    use maplit::hashmap;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone)]
    struct TestEntity {
        id: proto::EntityId,
        collection: crate::ModelId,
        data: Arc<Mutex<HashMap<String, Value>>>,
        registered_data: Arc<Mutex<HashMap<proto::EntityId, Value>>>,
    }

    impl TestEntity {
        fn new(id: u8, data: HashMap<String, Value>) -> Self {
            let mut id_bytes = [0u8; 16];
            id_bytes[15] = id;
            Self {
                id: proto::EntityId::from_bytes(id_bytes),
                // Ordinary catalog entity identity used only by this fixture.
                collection: crate::ModelId::EntityId(proto::EntityId::from_bytes([0x71; 16])),
                data: Arc::new(Mutex::new(data)),
                registered_data: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn with_registered(mut self, data: HashMap<proto::EntityId, Value>) -> Self {
            self.registered_data = Arc::new(Mutex::new(data));
            self
        }
    }

    impl AbstractEntity for TestEntity {
        fn collection(&self) -> crate::ModelId { self.collection.clone() }

        fn id(&self) -> &proto::EntityId { &self.id }

        fn value(&self, field: &str) -> Option<Value> { self.data.lock().unwrap().get(field).cloned() }

        fn value_by_id(&self, property_id: proto::EntityId) -> Option<Value> {
            self.registered_data.lock().unwrap().get(&property_id).cloned()
        }
    }

    fn test_property_id(byte: u8) -> proto::EntityId {
        let mut bytes = [0u8; 16];
        bytes[15] = byte;
        proto::EntityId::from_bytes(bytes)
    }

    fn comparison(left: Expr, operator: ComparisonOperator, right: Value) -> Predicate {
        Predicate::Comparison { left: Box::new(left), operator, right: Box::new(Expr::Literal(right)) }
    }

    fn conjunction(conditions: Vec<Predicate>) -> Predicate {
        conditions.into_iter().reduce(|left, right| Predicate::And(Box::new(left), Box::new(right))).unwrap()
    }

    #[test]
    fn test_build_gap_predicate_single_key_asc() {
        let name_id = test_property_id(1);
        let name = PropertyPath::registered(name_id, "name", vec![]);
        let entity = TestEntity::new(1, HashMap::new()).with_registered(hashmap!(name_id => Value::String("John".to_string())));

        let original_predicate = Predicate::True;
        let order_by = vec![OrderByItem { key: OrderKey::Property(name.clone()), direction: OrderDirection::Asc }];

        let gap_predicate = build_continuation_predicate(&original_predicate, &order_by, &entity).unwrap();
        let expected = conjunction(vec![
            Predicate::True,
            comparison(Expr::PropertyPath(name), ComparisonOperator::GreaterThanOrEqual, Value::String("John".to_string())),
            comparison(Expr::Path(PathExpr::simple("id")), ComparisonOperator::NotEqual, Value::EntityId(*entity.id())),
        ]);

        assert_eq!(gap_predicate, expected);
    }

    #[test]
    fn test_build_gap_predicate_multi_key() {
        // This regression covers resolved identity preservation only. #402
        // tracks the preexisting lexicographic-continuation bug.
        let name_id = test_property_id(1);
        let age_id = test_property_id(2);
        let name = PropertyPath::registered(name_id, "name", vec![]);
        let age = PropertyPath::registered(age_id, "age", vec![]);
        let entity = TestEntity::new(2, HashMap::new())
            .with_registered(hashmap!(name_id => Value::String("John".to_string()), age_id => Value::I32(30)));

        let original_predicate = Predicate::True;
        let order_by = vec![
            OrderByItem { key: OrderKey::Property(name.clone()), direction: OrderDirection::Asc },
            OrderByItem { key: OrderKey::Property(age.clone()), direction: OrderDirection::Desc },
        ];

        let gap_predicate = build_continuation_predicate(&original_predicate, &order_by, &entity).unwrap();
        let expected = conjunction(vec![
            Predicate::True,
            comparison(Expr::PropertyPath(name), ComparisonOperator::GreaterThanOrEqual, Value::String("John".to_string())),
            comparison(Expr::PropertyPath(age), ComparisonOperator::LessThanOrEqual, Value::I32(30)),
            comparison(Expr::Path(PathExpr::simple("id")), ComparisonOperator::NotEqual, Value::EntityId(*entity.id())),
        ]);

        assert_eq!(gap_predicate, expected);
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
