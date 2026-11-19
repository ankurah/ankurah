use crate::internal::prelude::*;
use crate::reactor::AbstractEntity;
use crate::selection::{filter::ValueLookup, ordering::OrderBoundary};
use ankql::ast::{Resolved, Selection};
use async_trait::async_trait;

/// Fetch matching entities strictly after a retained result-set prefix.
#[async_trait]
pub trait GapFetcher<E: AbstractEntity>: Send + Sync + 'static {
    /// `selection` is the original limited query. `last_entity` ends its
    /// retained prefix; `None` requests candidates from the beginning.
    async fn fetch_gap(&self, selection: &Selection<Resolved>, last_entity: Option<&E>, gap_size: usize) -> Result<Vec<E>, RetrievalError>;
}

/// Fills query gaps through the same node and live credential source as the query.
pub struct QueryGapFetcher {
    context: crate::context::Context,
}

impl QueryGapFetcher {
    pub fn new(context: crate::context::Context) -> Self { Self { context } }
}

#[async_trait]
impl GapFetcher<crate::entity::Entity> for QueryGapFetcher {
    async fn fetch_gap(
        &self,
        selection: &Selection<Resolved>,
        last_entity: Option<&crate::entity::Entity>,
        gap_size: usize,
    ) -> Result<Vec<crate::entity::Entity>, RetrievalError> {
        fetch_gap_from(selection, last_entity, gap_size, |selection| {
            self.context.0.fetch_entities(MatchArgs { selection, cache_policy: CachePolicy::Durable })
        })
        .await
    }
}

async fn fetch_gap_from<E: ValueLookup<Resolved>, F: std::future::Future<Output = Result<Vec<E>, RetrievalError>>>(
    selection: &Selection<Resolved>,
    last_entity: Option<&E>,
    gap_size: usize,
    fetch: impl FnOnce(Selection<Resolved>) -> F,
) -> Result<Vec<E>, RetrievalError> {
    if gap_size == 0 {
        return Ok(Vec::new());
    }
    if selection.limit.is_none() {
        return Err(RetrievalError::Other("gap filling requires a limited selection".into()));
    }
    // Capture values before awaiting: an Entity handle can change during the
    // fetch. Keep the original Selection so policy and managed-tree identity
    // stay the same. Its top N bounds this fallback's candidate set.
    let boundary = last_entity.map(|last| OrderBoundary::from_item(selection, last));
    let candidates = fetch(selection.clone()).await?;
    // ORDER BY comparisons are not interchangeable with predicate comparisons
    // (notably for JSON, missing values and collations).
    Ok(candidates
        .into_iter()
        .filter(|candidate| boundary.as_ref().is_none_or(|boundary| boundary.precedes(candidate)))
        .take(gap_size)
        .collect())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use ankql::ast::{OrderByItem, OrderDirection, Predicate, PropertyId};
    use ankurah_proto::EntityId;

    use super::*;
    use crate::{selection::filter::Filterable, value::Value};

    fn property(byte: u8) -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([byte; 32])) }
    fn id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 32]) }

    #[derive(Clone)]
    struct Item {
        id: EntityId,
        values: Arc<Mutex<BTreeMap<PropertyId, Value>>>,
    }

    impl Item {
        fn new(byte: u8, values: impl IntoIterator<Item = (PropertyId, Value)>) -> Self {
            Self { id: id(byte), values: Arc::new(Mutex::new(values.into_iter().collect())) }
        }
    }

    impl Filterable for Item {
        fn value(&self, property: &PropertyId) -> Option<Value> {
            if *property == PropertyId::Id {
                Some(Value::EntityId(self.id))
            } else {
                self.values.lock().unwrap().get(property).cloned()
            }
        }
    }

    fn selection(order_by: Option<Vec<OrderByItem<Resolved>>>) -> Selection<Resolved> {
        Selection { predicate: Predicate::True, order_by, limit: Some(5) }
    }

    fn order(byte: u8, direction: OrderDirection) -> OrderByItem<Resolved> { OrderByItem { path: property(byte).into(), direction } }
    fn ids(items: &[Item]) -> Vec<EntityId> { items.iter().map(|item| item.id).collect() }

    #[tokio::test]
    async fn mixed_order_refill_preserves_selection_and_uses_a_strict_boundary() -> anyhow::Result<()> {
        let selection = selection(Some(vec![order(10, OrderDirection::Asc), order(11, OrderDirection::Desc)]));
        let items: Vec<_> = [(1, "a", 7), (2, "a", 5), (3, "a", 3), (4, "b", 99)]
            .into_iter()
            .map(|(id, label, number)| Item::new(id, [(property(10), Value::String(label.into())), (property(11), Value::I64(number))]))
            .collect();
        let boundary = items[1].clone();
        let expected_selection = selection.clone();
        let actual = fetch_gap_from(&selection, Some(&boundary), 2, |requested| async move {
            assert_eq!(requested, expected_selection, "a gap must not create a different maintained Selection");
            Ok(items)
        })
        .await?;
        assert_eq!(ids(&actual), [id(3), id(4)]);
        Ok(())
    }

    #[tokio::test]
    async fn implicit_id_order_excludes_the_prefix_before_taking_the_gap() -> anyhow::Result<()> {
        let selection = selection(None);
        let items: Vec<_> = [1, 2, 3, 4].map(|id| Item::new(id, [])).into();
        let boundary = items[1].clone();
        let actual = fetch_gap_from(&selection, Some(&boundary), 1, |_| async { Ok(items) }).await?;
        assert_eq!(ids(&actual), [id(3)]);
        Ok(())
    }

    #[tokio::test]
    async fn boundary_values_are_captured_before_the_fetch() -> anyhow::Result<()> {
        let selection = selection(Some(vec![order(10, OrderDirection::Asc)]));
        let boundary = Item::new(2, [(property(10), Value::I64(5))]);
        let actual = fetch_gap_from(&selection, Some(&boundary), 1, |_| async {
            boundary.values.lock().unwrap().insert(property(10), Value::I64(100));
            Ok(vec![Item::new(1, [(property(10), Value::I64(1))]), Item::new(3, [(property(10), Value::I64(6))])])
        })
        .await?;
        assert_eq!(ids(&actual), [id(3)]);
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_prefix_starts_at_the_beginning_and_zero_gap_skips_the_fetch() -> anyhow::Result<()> {
        let selection = selection(None);
        let actual = fetch_gap_from(&selection, None, 1, |_| async { Ok(vec![Item::new(1, []), Item::new(2, [])]) }).await?;
        assert_eq!(ids(&actual), [id(1)]);
        let actual = fetch_gap_from::<Item, _>(&selection, None, 0, |_| async { panic!("zero gap must not fetch") }).await?;
        assert!(actual.is_empty());
        Ok(())
    }
}
