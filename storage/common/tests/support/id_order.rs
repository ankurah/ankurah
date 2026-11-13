use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Bound, RangeBounds};

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, PropertyPath, Resolved, Selection};
use ankurah_core::{
    property::backend::{LWWBackend, PropertyBackend},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, ModelId, State, StateBuffers};

/// First bytes of ids beginning with `A`, `Z`, `a`, `z`, `0`, `9`, `-`, and `_` in base64.
/// These cover the URL-safe base64 character groups in byte order. Each range comparison
/// against one of these ids matches different ids in text order than in byte order.
const FIRST_BYTES: [u8; 8] = [0x00, 0x64, 0x68, 0xCC, 0xD0, 0xF4, 0xF8, 0xFC];

/// Check id comparisons, ordering, and limits through the storage engine API.
/// Each comparison must return exactly the entities matching in byte order, restricted to the
/// component identified by `model()` only when requested. ORDER BY id must use byte order in
/// both directions, and LIMIT must retain the first results.
pub async fn check(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let stored = write_entities(engine).await?;
    let x = *stored.keys().nth(stored.len() / 2).expect("a stored id");
    for (query, predicate, range) in cases(x) {
        for of_model in [false, true] {
            let ascending: Vec<EntityId> =
                stored.iter().filter(|(id, in_model)| range.contains(*id) && (**in_model || !of_model)).map(|(id, _)| *id).collect();
            let descending: Vec<EntityId> = ascending.iter().rev().copied().collect();
            for (direction, expected) in [(OrderDirection::Asc, ascending), (OrderDirection::Desc, descending)] {
                let case = format!("{query}, of the model: {of_model}, ORDER BY id {direction:?}");
                let ordered = fetch(engine, &predicate, of_model, direction.clone(), None).await?;
                assert_eq!(ordered, expected, "{case}");
                let limited = fetch(engine, &predicate, of_model, direction, Some(3)).await?;
                assert_eq!(limited, expected[..expected.len().min(3)], "{case} LIMIT 3");
            }
        }
    }
    Ok(())
}

fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([101; 32])) }

fn other_model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([102; 32])) }

/// Each case's query text, its predicate on `id`, and the range of ids it matches.
fn cases(x: EntityId) -> [(&'static str, Predicate<Resolved>, (Bound<EntityId>, Bound<EntityId>)); 4] {
    use ComparisonOperator::*;
    let compare = |operator| Predicate::Comparison {
        left: Box::new(Expr::Path(PropertyPath::id())),
        operator,
        right: Box::new(Expr::Literal(Value::EntityId(x))),
    };
    [
        ("id > X", compare(GreaterThan), (Bound::Excluded(x), Bound::Unbounded)),
        ("id >= X", compare(GreaterThanOrEqual), (Bound::Included(x), Bound::Unbounded)),
        ("id < X", compare(LessThan), (Bound::Unbounded, Bound::Excluded(x))),
        ("id <= X", compare(LessThanOrEqual), (Bound::Unbounded, Bound::Included(x))),
    ]
}

/// Fetch ids matching `predicate`, ordered by id in `direction`, preserving the engine's result order.
/// Restrict results to the component identified by `model()` when `of_model` is true.
async fn fetch(
    engine: &impl StorageEngine,
    predicate: &Predicate<Resolved>,
    of_model: bool,
    direction: OrderDirection,
    limit: Option<u64>,
) -> anyhow::Result<Vec<EntityId>> {
    let order_by = Some(vec![OrderByItem { path: PropertyPath::id(), direction }]);
    let selection = Selection { predicate: predicate.clone(), order_by, limit };
    let selection = if of_model { selection.and_member_of(model()) } else { selection };
    Ok(engine.fetch_states(&selection).await?.iter().map(|state| state.payload.entity_id).collect())
}

/// Store two entities per `FIRST_BYTES` entry, one a member of `model()` and one of `other_model()`.
/// Return each id and whether its entity is a member of `model()`.
async fn write_entities(engine: &impl StorageEngine) -> anyhow::Result<BTreeMap<EntityId, bool>> {
    let mut stored = BTreeMap::new();
    for (index, (first, second)) in FIRST_BYTES.into_iter().flat_map(|first| [(first, 0x10), (first, 0xA0)]).enumerate() {
        let mut bytes = [0x5A; 32];
        (bytes[0], bytes[1]) = (first, second);
        let entity_id = EntityId::from_bytes(bytes);
        let in_model = index % 2 == 0;
        let backend = LWWBackend::new();
        let event = ankurah_core::test_helpers::commit_backend(&backend, index as u8 + 1)?;
        let state = Attested::opt(
            EntityState {
                entity_id,
                state: State {
                    state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                    memberships: BTreeSet::from([if in_model { model() } else { other_model() }]),
                    head: Clock::singleton(&event),
                },
            },
            None,
        );
        let mut transaction = engine.transaction();
        transaction.set_state(&Clock::default(), &state).await?;
        assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
        stored.insert(entity_id, in_model);
    }
    Ok(stored)
}
