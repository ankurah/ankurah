//! Values the engine cannot bound never reach an index range: a predicate on
//! a property this engine never materialized takes the fail-closed path
//! without an index, and an IS NULL on a materialized property stays out of
//! the index spec the equality beside it creates.

use std::collections::{BTreeMap, BTreeSet};

use ankql::ast::{ComparisonOperator, Expr, Predicate, Resolved, Selection};
use ankurah_core::{
    property::backend::{LWWBackend, PropertyBackend},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, PropertyId, State, StateBuffers};
use ankurah_storage_sled::SledStorageEngine;

fn equals(property: PropertyId, literal: &str) -> Predicate<Resolved> {
    Predicate::Comparison {
        left: Box::new(Expr::Path(property.into())),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::String(literal.into()))),
    }
}

#[tokio::test]
async fn absent_and_null_properties_never_reach_an_index_range() -> anyhow::Result<()> {
    let engine = SledStorageEngine::new_test()?;
    let model = ModelId::EntityId(EntityId::from_bytes([101; 32]));
    let name = PropertyId::EntityId(EntityId::from_bytes([201; 32]));
    let status = PropertyId::EntityId(EntityId::from_bytes([202; 32]));
    let never_materialized = PropertyId::EntityId(EntityId::from_bytes([203; 32]));
    insert(&engine, model, 1, vec![(name, "a"), (status, "open")]).await?;
    insert(&engine, model, 2, vec![(name, "a")]).await?;
    let index_specs = || {
        let database = engine.database.lock().unwrap().clone();
        let specs: Vec<_> = database.index_manager.indexes.read().unwrap().values().map(|index| index.spec().clone()).collect();
        specs
    };

    // Absent: fail-closed, and no index is created for it.
    let selection = Selection { predicate: equals(never_materialized, "x"), order_by: None, limit: None }.and_member_of(model);
    assert!(engine.fetch_states(&selection).await?.is_empty());
    assert!(index_specs().is_empty(), "a predicate on an absent property must not create an index");

    // IS NULL beside an equality: the index covers the equality only.
    let predicate = Predicate::And(Box::new(equals(name, "a")), Box::new(Predicate::IsNull(Box::new(Expr::Path(status.into())))));
    let selection = Selection { predicate, order_by: None, limit: None }.and_member_of(model);
    engine.fetch_states(&selection).await?;
    let specs = index_specs();
    assert_eq!(specs.len(), 1, "{specs:?}");
    assert_eq!(specs[0].keyparts.len(), 2, "the materialization part and the name part, nothing for status: {specs:?}");

    // The same index serves the equality alone, for both entities.
    let selection = Selection { predicate: equals(name, "a"), order_by: None, limit: None }.and_member_of(model);
    let ids: BTreeSet<u8> = engine.fetch_states(&selection).await?.iter().map(|state| state.payload.entity_id.to_bytes()[0]).collect();
    assert_eq!(ids, BTreeSet::from([1, 2]));
    assert_eq!(index_specs().len(), 1);
    Ok(())
}

async fn insert(engine: &SledStorageEngine, model: ModelId, byte: u8, values: Vec<(PropertyId, &str)>) -> anyhow::Result<()> {
    let backend = LWWBackend::new();
    for (property, value) in values {
        backend.set(property, Some(Value::String(value.into())));
    }
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
