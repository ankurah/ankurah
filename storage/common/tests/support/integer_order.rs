use std::collections::{BTreeMap, BTreeSet, HashMap};

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Resolved, Selection};
use ankurah_core::{
    property::backend::{LWWBackend, PropertyBackend},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, PropertyId, State, StateBuffers};

/// The stored integers in numeric order, spanning zero and both ends of `i64`.
const ASCENDING: [i64; 9] = [i64::MIN, -1_000_000_000_000, -5, -1, 0, 3, 10, 1_000_000_000_000, i64::MAX];

/// Shared black-box cases: each comparison returns exactly the stored integers
/// it matches numerically, and LIMIT keeps only distinct matches.
pub async fn check_comparisons(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let values = write_values(engine).await?;
    for (query, predicate, matches) in cases() {
        let expected: Vec<i64> = ASCENDING.into_iter().filter(|&n| matches(n)).collect();
        let mut all = fetch(engine, &values, &predicate, None, None).await?;
        all.sort();
        assert_eq!(all, expected, "{query}");
        let limited = fetch(engine, &values, &predicate, None, Some(2)).await?;
        let distinct: std::collections::BTreeSet<_> = limited.iter().collect();
        assert!(
            limited.len() == expected.len().min(2) && distinct.len() == limited.len() && limited.iter().all(|n| expected.contains(n)),
            "{query} LIMIT 2: {limited:?}"
        );
    }
    Ok(())
}

/// Shared black-box cases: ORDER BY returns the matching integers in numeric
/// order in both directions, and LIMIT keeps the leading ones.
pub async fn check_ordering(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let values = write_values(engine).await?;
    for (query, predicate, matches) in cases() {
        let ascending: Vec<i64> = ASCENDING.into_iter().filter(|&n| matches(n)).collect();
        let descending: Vec<i64> = ascending.iter().rev().copied().collect();
        for (direction, expected) in [(OrderDirection::Asc, ascending), (OrderDirection::Desc, descending)] {
            let ordered = fetch(engine, &values, &predicate, Some(direction.clone()), None).await?;
            assert_eq!(ordered, expected, "{query} ORDER BY value {direction:?}");
            let limited = fetch(engine, &values, &predicate, Some(direction.clone()), Some(2)).await?;
            assert_eq!(limited, expected[..expected.len().min(2)], "{query} ORDER BY value {direction:?} LIMIT 2");
        }
    }
    Ok(())
}

fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([101; 32])) }

fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([201; 32])) }

/// Each case's query text, its predicate on `property()`, and which integers it matches.
fn cases() -> [(&'static str, Predicate<Resolved>, fn(i64) -> bool); 5] {
    use ComparisonOperator::*;
    let compare = |operator, n| Predicate::Comparison {
        left: Box::new(Expr::Path(property().into())),
        operator,
        right: Box::new(Expr::Literal(Value::I64(n))),
    };
    let between = Predicate::And(Box::new(compare(GreaterThanOrEqual, -5)), Box::new(compare(LessThanOrEqual, 3)));
    [
        ("value > -2", compare(GreaterThan, -2), |n| n > -2),
        ("value < 0", compare(LessThan, 0), |n| n < 0),
        ("value >= -5 AND value <= 3", between, |n| (-5..=3).contains(&n)),
        ("value = -5", compare(Equal, -5), |n| n == -5),
        ("true", Predicate::True, |_| true),
    ]
}

/// The integers of the entities `engine` returns for `predicate` on `model()`, in returned order.
async fn fetch(
    engine: &impl StorageEngine,
    values: &HashMap<EntityId, i64>,
    predicate: &Predicate<Resolved>,
    direction: Option<OrderDirection>,
    limit: Option<u64>,
) -> anyhow::Result<Vec<i64>> {
    let order_by = direction.map(|direction| vec![OrderByItem { path: property().into(), direction }]);
    let selection = Selection { predicate: predicate.clone(), order_by, limit }.and_member_of(model());
    Ok(engine.fetch_states(&selection).await?.iter().map(|state| values[&state.payload.entity_id]).collect())
}

/// Store each integer of `ASCENDING` as one LWW property of its own `model()` entity.
async fn write_values(engine: &impl StorageEngine) -> anyhow::Result<HashMap<EntityId, i64>> {
    let mut values = HashMap::new();
    for (byte, value) in (1..).zip(ASCENDING) {
        let backend = LWWBackend::new();
        backend.set(property(), Some(Value::I64(value)));
        let event = EventId::from_bytes([byte; 32]);
        if let Some(operations) = backend.to_operations()? {
            backend.apply_operations_with_event(&operations, event.clone())?;
        }
        let entity_id = EntityId::from_bytes([byte; 32]);
        let state = Attested::opt(
            EntityState {
                entity_id,
                state: State {
                    state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                    memberships: BTreeSet::from([model()]),
                    head: Clock::genesis(event),
                },
            },
            None,
        );
        let mut transaction = engine.transaction();
        transaction.set_state(&Clock::default(), &state).await?;
        assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
        values.insert(entity_id, value);
    }
    Ok(values)
}
