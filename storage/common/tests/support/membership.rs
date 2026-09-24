use std::{collections::BTreeMap, sync::Arc};

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Resolved, Selection};
use ankurah_core::{
    property::backend::{LWWBackend, PropertyBackend},
    schema::CatalogResolver,
    storage::{GetStateResult, StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, PropertyId, State, StateBuffers};

struct Labels;

#[async_trait::async_trait]
impl CatalogResolver for Labels {
    async fn get_model_label(&self, _: &ModelId) -> Option<String> { Some("membership".into()) }
    async fn get_property_label(&self, _: &PropertyId) -> Option<String> { Some("value".into()) }
}

fn id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 32]) }

/// Shared black-box cases: one entity can match several materializations,
/// while Boolean membership, property filtering, ordering, and LIMIT compose.
pub async fn check(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let labels: Arc<dyn CatalogResolver> = Arc::new(Labels);
    engine.set_catalog_resolver(Arc::downgrade(&labels));
    let unknown = Predicate::MemberOf(ModelId::EntityId(id(123)));
    let empty = Predicate::And(Box::new(unknown), Box::new(Predicate::False));
    assert!(engine.fetch_states(&empty.into()).await?.is_empty());
    assert!(engine.list_materializations().await?.is_empty(), "a cold read must not create a materialization");
    let [a, b, c] = [101, 102, 103].map(|byte| ModelId::EntityId(id(byte)));
    let [p, q] = [201, 202].map(|byte| PropertyId::EntityId(id(byte)));
    // Seed colliding physical labels in different orders in A and B.
    write(engine, 1, &[a], &[(p, "z")]).await?;
    write(engine, 2, &[b], &[(q, "take")]).await?;
    write(engine, 2, &[b], &[(p, "b"), (q, "take")]).await?;
    write(engine, 3, &[a, b], &[(p, "c"), (q, "skip")]).await?;
    write(engine, 4, &[c], &[(p, "a")]).await?;
    write(engine, 5, &[], &[]).await?;
    let events: Vec<_> = (1..=5).map(|byte| Attested::opt(ankurah_proto::Event::update(
        id(byte), Clock::default(), ankurah_proto::AuthorId::Unknown, Default::default(),
    ), None)).collect();
    let mut transaction = engine.transaction();
    transaction.add_events(&events).await?;
    transaction.commit().await?.committed()?;
    let event_ids: Vec<_> = events.iter().map(|event| event.payload.id()).collect();

    let and = |left, right| Predicate::And(Box::new(left), Box::new(right));
    let or = |left, right| Predicate::Or(Box::new(left), Box::new(right));
    let member = Predicate::MemberOf;
    let not_a = Predicate::Not(Box::new(member(a)));
    let union = or(member(a), member(b));
    for (predicate, expected) in [
        (and(member(a), member(b)), vec![3]),
        (union.clone(), vec![1, 2, 3]),
        (and(member(a), Predicate::Not(Box::new(member(b)))), vec![1]),
        (not_a, vec![2, 4, 5]),
        (or(and(member(a), equals(p, "c")), and(member(b), equals(q, "take"))), vec![2, 3]),
        (equals(p, "a"), vec![4]),
    ] {
        let states = engine.fetch_states(&predicate.clone().into()).await?;
        let mut actual: Vec<_> = states.iter().map(|state| state.payload.entity_id).collect();
        actual.sort();
        let expected: Vec<_> = expected.into_iter().map(id).collect();
        assert_eq!(actual, expected, "{predicate:?}");
        // The event-authorization path must use the same predicate semantics.
        let mut allowed = engine.filter_entity_ids(&[id(1), id(2), id(3), id(4), id(5), id(99)], &predicate).await?;
        allowed.sort();
        assert_eq!(allowed, expected, "identity filtering: {predicate:?}");
        let mut event_entities: Vec<_> = engine.get_events(event_ids.clone(), &predicate).await?
            .iter().map(|event| event.payload.entity_id).collect();
        event_entities.sort();
        assert_eq!(event_entities, expected, "event retrieval: {predicate:?}");
    }
    assert!(engine.get_events(event_ids, &Predicate::False).await?.is_empty());
    assert!(matches!(engine.get_states(vec![id(1), id(99), id(1)], &Predicate::False).await?.as_slice(),
        [GetStateResult::PredicateMismatch(first), GetStateResult::NotFound(absent), GetStateResult::PredicateMismatch(last)]
        if *first == id(1) && *absent == id(99) && *last == id(1)));
    let ordered = Selection {
        predicate: union,
        order_by: Some(vec![OrderByItem { path: p.into(), direction: OrderDirection::Asc }]),
        limit: Some(2),
    };
    assert_eq!(engine.fetch_states(&ordered).await?.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(), vec![id(2), id(3)]);
    let limited = Selection { predicate: and(member(a), member(b)), order_by: None, limit: Some(1) };
    assert_eq!(engine.fetch_states(&limited).await?.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(), vec![id(3)]);
    assert!(engine.filter_entity_ids(&[], &Predicate::True).await?.is_empty());
    let missing = PropertyId::EntityId(id(203));
    let denied = and(member(a), Predicate::Not(Box::new(equals(missing, "blocked"))));
    assert!(engine.filter_entity_ids(&[id(1), id(3)], &denied).await?.is_empty(),
        "a missing property cannot become a grant through negation");
    assert!(engine.fetch_states(&Selection { predicate: denied.clone(), order_by: None, limit: Some(1) }).await?.is_empty(),
        "single-materialization retrieval must preserve missing-property semantics");
    assert!(engine.fetch_states(&Selection { predicate: and(denied, member(b)), order_by: None, limit: Some(1) }).await?.is_empty(),
        "indexed retrieval must preserve the same missing-property semantics");
    let before = engine.list_materializations().await?;
    let unknown = member(ModelId::EntityId(id(123)));
    for (predicate, expected) in [
        (and(unknown.clone(), Predicate::False), vec![]),
        (or(member(a), unknown.clone()), vec![1, 3]),
        (and(member(a), Predicate::Not(Box::new(unknown.clone()))), vec![1, 3]),
        (Predicate::Not(Box::new(unknown)), vec![1, 2, 3, 4, 5]),
    ] {
        let mut actual: Vec<_> = engine.fetch_states(&predicate.clone().into()).await?.iter().map(|state| state.payload.entity_id).collect();
        actual.sort();
        assert_eq!(actual, expected.into_iter().map(id).collect::<Vec<_>>(), "{predicate:?}");
    }
    let mut after = engine.list_materializations().await?;
    after.sort();
    let mut before = before;
    before.sort();
    assert_eq!(after, before, "reads must not create unknown materializations");

    // Successive states for one entity belong to one transaction, not separate core write batches.
    let original = engine.get_state(id(5)).await?;
    let mut first = original.clone();
    first.payload.state.head = EventId::from_bytes([40; 32]).into();
    first.payload.state.memberships.insert(a);
    let mut last = first.clone();
    last.payload.state.head = EventId::from_bytes([41; 32]).into();
    last.payload.state.memberships.insert(b);
    let mut transaction = engine.transaction();
    transaction.set_state(&original.payload.state.head, &first).await?;
    transaction.set_state(&first.payload.state.head, &last).await?;
    assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
    assert_eq!(engine.get_state(id(5)).await?.payload.state, last.payload.state);
    assert_eq!(engine.filter_entity_ids(&[id(5)], &and(member(a), member(b))).await?, vec![id(5)]);
    Ok(())
}

pub async fn check_id_order(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let [a, b] = [101, 102].map(|byte| ModelId::EntityId(id(byte)));
    for byte in [1, 208] { write(engine, byte, &[a, b], &[]).await?; }
    for (direction, expected) in [(OrderDirection::Asc, 1), (OrderDirection::Desc, 208)] {
        let selection = Selection {
            predicate: Predicate::And(Box::new(Predicate::MemberOf(a)), Box::new(Predicate::MemberOf(b))),
            order_by: Some(vec![OrderByItem { path: PropertyId::Id.into(), direction }]),
            limit: Some(1),
        };
        assert_eq!(engine.fetch_states(&selection).await?[0].payload.entity_id, id(expected));
    }
    Ok(())
}

fn equals(property: PropertyId, value: &str) -> Predicate<Resolved> {
    Predicate::Comparison {
        left: Box::new(Expr::Path(property.into())),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::String(value.into()))),
    }
}

/// Exercise property-index candidates with membership filtering before LIMIT,
/// including backfill, reuse from either membership, and subsequent writes.
pub async fn check_indexed(engine: &impl StorageEngine) -> anyhow::Result<()> {
    let labels: Arc<dyn CatalogResolver> = Arc::new(Labels);
    engine.set_catalog_resolver(Arc::downgrade(&labels));
    let [a, b] = [101, 102].map(|byte| ModelId::EntityId(id(byte)));
    let p = PropertyId::EntityId(id(201));
    write(engine, 1, &[a], &[(p, "a")]).await?;
    write(engine, 2, &[b], &[(p, "b")]).await?;
    write(engine, 3, &[a, b], &[(p, "c")]).await?;
    write(engine, 4, &[a, b], &[(p, "d")]).await?;
    for expected in [3, 2] {
        for (input, other) in [(a, b), (b, a)] {
            let selection = Selection {
                predicate: Predicate::And(
                    Box::new(Predicate::MemberOf(input)),
                    Box::new(Predicate::And(
                        Box::new(Predicate::MemberOf(other)),
                        Box::new(Predicate::Comparison {
                            left: Box::new(Expr::Path(p.into())),
                            operator: ComparisonOperator::GreaterThanOrEqual,
                            right: Box::new(Expr::Literal(Value::String("a".into()))),
                        }),
                    )),
                ),
                order_by: Some(vec![OrderByItem { path: p.into(), direction: OrderDirection::Asc }]),
                limit: Some(1),
            };
            let states = engine.fetch_states(&selection).await?;
            assert_eq!(states.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(), vec![id(expected)]);
        }
        if expected == 3 {
            write(engine, 2, &[a, b], &[(p, "bb")]).await?;
        }
    }
    Ok(())
}

async fn write(engine: &impl StorageEngine, byte: u8, models: &[ModelId], values: &[(PropertyId, &str)]) -> anyhow::Result<()> {
    let backend = LWWBackend::new();
    for (property, value) in values { backend.set(*property, Some(Value::String((*value).into()))); }
    let event = EventId::from_bytes([byte.wrapping_add(values.len() as u8); 32]);
    if let Some(operations) = backend.to_operations()? { backend.apply_operations_with_event(&operations, event.clone())?; }
    let state = Attested::opt(EntityState {
        entity_id: id(byte),
        state: State {
            state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
            memberships: models.iter().copied().collect(),
            head: Clock::from(vec![event]),
        },
    }, None);
    let expected = match engine.get_state(id(byte)).await {
        Ok(state) => state.payload.state.head,
        Err(ankurah_core::error::RetrievalError::EntityNotFound(_)) => Clock::default(),
        Err(error) => return Err(error.into()),
    };
    let mut transaction = engine.transaction();
    transaction.set_state(&expected, &state).await?;
    assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
    Ok(())
}
