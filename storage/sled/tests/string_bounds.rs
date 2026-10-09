//! A name extended through a NUL byte sorts after the shorter name; an index
//! scan must neither admit it where the shorter name is wanted nor skip it
//! where every greater name is wanted.

use std::collections::{BTreeMap, BTreeSet};

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Selection};
use ankurah_core::{
    property::backend::{LWWBackend, PropertyBackend},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, PropertyId, State, StateBuffers};
use ankurah_storage_sled::SledStorageEngine;

/// The stored names, each under the entity id of its position.
const NAMES: [&str; 4] = ["a", "a\0", "a\0b", "b"];

#[tokio::test]
async fn equality_does_not_admit_the_nul_extended_name() -> anyhow::Result<()> { check(ComparisonOperator::Equal, "a", ["a"]).await }

#[tokio::test]
async fn exclusive_lower_bound_returns_the_nul_extended_name() -> anyhow::Result<()> {
    check(ComparisonOperator::GreaterThan, "a", ["a\0", "a\0b", "b"]).await
}

#[tokio::test]
async fn inclusive_upper_bound_stops_before_the_nul_extended_name() -> anyhow::Result<()> {
    check(ComparisonOperator::LessThanOrEqual, "a", ["a"]).await
}

/// Store `NAMES`, then fetch `name <operator> literal` expecting exactly
/// `expected`, once through an index with an ascending `name` part and once
/// through one with a descending part (a fresh engine each time, since an
/// existing ascending index would otherwise serve the descending order as a
/// reverse scan).
async fn check<const N: usize>(operator: ComparisonOperator, literal: &str, expected: [&str; N]) -> anyhow::Result<()> {
    let model = ModelId::EntityId(EntityId::from_bytes([101; 32]));
    let name = PropertyId::EntityId(EntityId::from_bytes([201; 32]));
    let expected: BTreeSet<&str> = expected.into_iter().collect();
    for direction in [OrderDirection::Asc, OrderDirection::Desc] {
        let engine = SledStorageEngine::new_test()?;
        for (position, value) in NAMES.iter().enumerate() {
            insert(&engine, model, name, position as u8, value).await?;
        }
        let predicate = Predicate::Comparison {
            left: Box::new(Expr::Path(name.into())),
            operator: operator.clone(),
            right: Box::new(Expr::Literal(Value::String(literal.into()))),
        };
        let order_by = Some(vec![OrderByItem { path: name.into(), direction: direction.clone() }]);
        let selection = Selection { predicate, order_by, limit: None }.and_member_of(model);
        let states = engine.fetch_states(&selection).await?;
        let found: BTreeSet<&str> = states.iter().map(|state| NAMES[state.payload.entity_id.to_bytes()[0] as usize]).collect();
        assert_eq!(found, expected, "{selection:?}");
        assert_eq!(states.len(), expected.len(), "each matching entity appears once: {selection:?}");

        // An inequality must have been scanned through a `name` part of this
        // direction; an equality pins the part, so its direction is immaterial.
        if operator != ComparisonOperator::Equal {
            let database = engine.database.lock().unwrap().clone();
            let index_names: Vec<String> =
                database.index_manager.indexes.read().unwrap().values().map(|index| index.name().to_owned()).collect();
            let wanted = match direction {
                OrderDirection::Asc => " asc",
                OrderDirection::Desc => " desc",
            };
            assert!(index_names.iter().all(|name| name.ends_with(wanted)), "the fetch must scan a {wanted} name part: {index_names:?}");
        }
    }
    Ok(())
}

async fn insert(engine: &SledStorageEngine, model: ModelId, property: PropertyId, byte: u8, value: &str) -> anyhow::Result<()> {
    let backend = LWWBackend::new();
    backend.set(property, Some(Value::String(value.into())));
    let event = EventId::from_bytes([byte; 32]);
    if let Some(operations) = backend.to_operations()? {
        backend.apply_operations_with_event(&operations, event.clone())?;
    }
    let state = Attested::opt(
        EntityState {
            entity_id: EntityId::from_bytes([byte; 32]),
            state: State {
                state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                memberships: BTreeSet::from([model]),
                head: Clock::genesis(event),
            },
        },
        None,
    );
    let mut transaction = engine.transaction();
    transaction.set_state(&Clock::default(), &state).await?;
    assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
    Ok(())
}
