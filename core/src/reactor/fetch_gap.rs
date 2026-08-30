use crate::context::NodeAndContext;
use crate::internal::prelude::*;
use crate::node::NodeInner;
use crate::reactor::AbstractEntity;
use crate::value::Value;
use ankql::ast::Resolved;
use ankurah_proto as proto;
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
        collection_id: &proto::CollectionId,
        selection: &ankql::ast::Selection<Resolved>,
        last_entity: Option<&E>,
        gap_size: usize,
    ) -> Result<Vec<E>, RetrievalError>;
}

/// Concrete implementation of GapFetcher using a WeakNode and a live
/// credential source, read at fetch time so a refreshed credential is
/// used without rebuilding the fetcher.
pub struct QueryGapFetcher<SE, PA>
where
    SE: StorageEngine,
    PA: PolicyAgent,
{
    weak_node: Weak<NodeInner<SE, PA>>,
    /// A context's set on the client side; on the server side a private
    /// set owning the per-query session the peer subscription server
    /// writes on each re-validated subscribe.
    sessions: crate::session::SessionSet<PA::ContextData>,
}

impl<SE, PA> QueryGapFetcher<SE, PA>
where
    SE: StorageEngine,
    PA: PolicyAgent,
{
    pub fn new(node: &Node<SE, PA>, sessions: crate::session::SessionSet<PA::ContextData>) -> Self {
        Self { weak_node: Arc::downgrade(&node.0), sessions }
    }
}

#[async_trait]
impl<SE, PA> GapFetcher<crate::entity::Entity> for QueryGapFetcher<SE, PA>
where
    SE: StorageEngine + 'static,
    PA: PolicyAgent + 'static,
{
    async fn fetch_gap(
        &self,
        collection_id: &proto::CollectionId,
        selection: &ankql::ast::Selection<Resolved>,
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
        let node_context = NodeAndContext {
            node: crate::node::NodeType::Strong(node),
            auth: crate::context::ContextAuth::Sessions(self.sessions.clone()),
        };

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
    original_predicate: &ankql::ast::Predicate<Resolved>,
    order_by: &[ankql::ast::OrderByItem<Resolved>],
    last_entity: &E,
) -> Result<ankql::ast::Predicate<Resolved>, String> {
    use ankql::ast::{ComparisonOperator, Expr, OrderDirection, Predicate};

    let mut gap_conditions = Vec::new();

    // Add original predicate
    gap_conditions.push(original_predicate.clone());

    // Add ORDER BY continuation conditions
    for order_item in order_by {
        let identifier = &order_item.path;

        // Get the field value from the last entity
        if let Some(field_value) = last_entity.value(&identifier.property_id()) {
            let literal = match field_value {
                // Skip Object, Binary, and Json for now - they're not commonly used in ORDER BY
                Value::Object(_) | Value::Binary(_) | Value::Json(_) => continue,
                literal => literal,
            };

            let operator = match order_item.direction {
                OrderDirection::Asc => ComparisonOperator::GreaterThanOrEqual,
                OrderDirection::Desc => ComparisonOperator::LessThanOrEqual,
            };

            let condition =
                Predicate::Comparison { left: Box::new(Expr::Path(identifier.clone())), operator, right: Box::new(Expr::Literal(literal)) };

            gap_conditions.push(condition);
        }
    }

    // Add entity ID exclusion to avoid fetching the last entity again
    let id_exclusion = Predicate::Comparison {
        left: Box::new(Expr::Path(ankql::ast::PropertyPath::id())),
        operator: ComparisonOperator::NotEqual,
        right: Box::new(Expr::Literal(Value::EntityId(*last_entity.id()))),
    };
    gap_conditions.push(id_exclusion);

    // Combine all conditions with AND
    let result =
        gap_conditions.into_iter().reduce(|acc, condition| Predicate::And(Box::new(acc), Box::new(condition))).unwrap_or(Predicate::True);

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use ankql::ast::{OrderByItem, OrderDirection, Parsed, Predicate, PropertyId, Resolved};
    use ankurah_derive::selection;
    use ankurah_proto as proto;
    use maplit::hashmap;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// A deterministic durable identity for a fixture field name.
    fn prop(name: &str) -> PropertyId {
        let mut bytes = [0u8; 32];
        let n = name.as_bytes();
        let len = n.len().min(32);
        bytes[..len].copy_from_slice(&n[..len]);
        PropertyId::EntityId(proto::EntityId::from_bytes(bytes))
    }

    /// A resolved sort key over a fixture field.
    fn key(name: &str, direction: OrderDirection) -> OrderByItem<Resolved> {
        use ankql::ast::PropertyIdExt;
        OrderByItem { path: prop(name).path(&[]), direction }
    }

    /// Bind a `selection!` literal's names to the fixture identities, so the
    /// expected predicate compares equal to the resolved one the builder
    /// emits (PropertyPath equality is identity + sub-path; labels differ
    /// freely).
    fn resolve(selection: ankql::ast::Selection<Parsed>) -> ankql::ast::Selection<Resolved> {
        use crate::schema::resolver::{resolve_selection, ModelResolutionError, ModelResolver, ResolvedProperty};
        struct FixtureResolver;
        impl ModelResolver for FixtureResolver {
            fn resolve_property(&self, _model: &proto::ModelId, name: &str) -> Result<Option<ResolvedProperty>, ModelResolutionError> {
                let id = prop(name);
                let value_type = if id == prop("age") { crate::value::ValueType::I32 } else { crate::value::ValueType::String };
                Ok(Some(ResolvedProperty { id, value_type }))
            }
        }
        let model = proto::ModelId::EntityId(proto::EntityId::from_bytes([0x77; 32]));
        resolve_selection(&model, &FixtureResolver, selection).unwrap()
    }

    #[derive(Debug, Clone)]
    struct TestEntity {
        id: proto::EntityId,
        collection: proto::CollectionId,
        data: Arc<Mutex<HashMap<PropertyId, Value>>>,
    }

    impl TestEntity {
        fn new(id: u8, data: HashMap<PropertyId, Value>) -> Self {
            let mut id_bytes = [0u8; 32];
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

        fn value(&self, property: &PropertyId) -> Option<Value> { self.data.lock().unwrap().get(property).cloned() }
    }

    #[test]
    fn test_build_gap_predicate_single_column_asc() {
        let entity = TestEntity::new(1, hashmap!(prop("name") => Value::String("John".to_string())));

        let original_predicate = Predicate::True;
        let order_by = vec![key("name", OrderDirection::Asc)];

        let gap_predicate = build_continuation_predicate(&original_predicate, &order_by, &entity).unwrap();
        let expected = resolve(ankurah_derive::selection!("true AND name >= 'John' AND id != {}", entity.id())).predicate;

        assert_eq!(gap_predicate, expected);
    }

    #[test]
    fn test_build_gap_predicate_multi_column() {
        let entity = TestEntity::new(2, hashmap!(prop("name") => Value::String("John".to_string()), prop("age") => Value::I32(30)));

        let original_predicate = Predicate::True;
        let order_by = vec![key("name", OrderDirection::Asc), key("age", OrderDirection::Desc)];

        let gap_predicate = build_continuation_predicate(&original_predicate, &order_by, &entity).unwrap();
        let expected = resolve(selection!("true AND name >= 'John' AND age <= 30 AND id != {}", entity.id())).predicate;

        assert_eq!(gap_predicate, expected);
    }
}
