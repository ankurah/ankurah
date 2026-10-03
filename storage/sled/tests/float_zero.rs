use std::collections::{BTreeMap, BTreeSet};

use ankql::ast::{ComparisonOperator, Expr, Predicate, Selection};
use ankurah_core::{
    property::backend::{LWWBackend, PropertyBackend},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, PropertyId, State, StateBuffers};
use ankurah_storage_sled::SledStorageEngine;

#[tokio::test]
async fn signed_zeros_share_equality_and_range_matches() -> anyhow::Result<()> {
    let engine = SledStorageEngine::new_test()?;
    let model = ModelId::EntityId(EntityId::from_bytes([101; 32]));
    let property = PropertyId::EntityId(EntityId::from_bytes([201; 32]));
    let compare = |operator, value| Predicate::Comparison {
        left: Box::new(Expr::Path(property.into())),
        operator,
        right: Box::new(Expr::Literal(Value::F64(value))),
    };
    let query = |predicate| Selection { predicate, order_by: None, limit: None }.and_member_of(model);

    for (byte, value) in [(1, -1.0), (2, -0.0), (3, 1.0)] {
        insert(&engine, model, property, byte, value).await?;
    }
    // The first fetch builds the float index from existing rows. Then insert
    // positive zero through index maintenance and check both representations.
    let mut expected = BTreeSet::from([EntityId::from_bytes([2; 32])]);
    for after_insert in [false, true] {
        if after_insert {
            insert(&engine, model, property, 4, 0.0).await?;
            expected.insert(EntityId::from_bytes([4; 32]));
        }
        for zero in [-0.0, 0.0] {
            use ComparisonOperator::*;
            let predicates = [
                compare(Equal, zero),
                Predicate::And(Box::new(compare(GreaterThan, -1.0)), Box::new(compare(LessThan, 1.0))),
                Predicate::And(Box::new(compare(GreaterThanOrEqual, zero)), Box::new(compare(LessThanOrEqual, zero))),
            ];
            for predicate in predicates {
                let selection = query(predicate);
                let states = engine.fetch_states(&selection).await?;
                let ids: BTreeSet<_> = states.iter().map(|state| state.payload.entity_id).collect();
                assert_eq!(ids, expected, "{selection:?}, after_insert={after_insert}");
                assert_eq!(states.len(), expected.len(), "each matching entity appears once");
            }
        }
    }
    Ok(())
}

async fn insert(engine: &SledStorageEngine, model: ModelId, property: PropertyId, byte: u8, value: f64) -> anyhow::Result<()> {
    let backend = LWWBackend::new();
    backend.set(property, Some(Value::F64(value)));
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
                head: Clock::from(vec![event]),
            },
        },
        None,
    );
    let mut transaction = engine.transaction();
    transaction.set_state(&Clock::default(), &state).await?;
    assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
    Ok(())
}
