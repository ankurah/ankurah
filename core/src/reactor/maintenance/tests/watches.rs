//! Which committed entities reach a reader: its bootstrap watch, its
//! predicate's candidates, and its registration.

use super::*;

#[tokio::test]
async fn finishing_bootstrap_cannot_drop_a_commit_crossing_the_scan() -> anyhow::Result<()> {
    let node = node().await?;
    let (start, mut cursor) = subscribe(&node).await?;
    let event = genesis(41);
    apply(&node, &event).await?;
    assert!(matches!(cursor.finish_bootstrap(Predicate::False, [], start).await, Err(SourceError::Changed)));
    let window = cursor.fence().await?;
    assert_eq!(window.batch.as_ref().unwrap().changes[0].head, proto::Clock::singleton(&event));
    cursor.finish_bootstrap(Predicate::False, [event.entity_id], window.revision).await?;

    // Even a predicate with no entry candidates must retain the explicitly
    // installed membership watch until the projector observes a departure.
    let later = Event::update(event.entity_id, proto::Clock::singleton(&event), AuthorId::Unknown, OperationSet::default());
    apply(&node, &later).await?;
    assert_eq!(cursor.next().await?.changes[0].head, proto::Clock::singleton(&later));
    Ok(())
}

#[tokio::test]
async fn maintenance_routes_model_candidates_and_removes_departed_entity_watches() -> anyhow::Result<()> {
    let node = node().await?;
    let model = ModelId::EntityId(EntityId::from_bytes([7; 32]));
    let property = proto::PropertyId::EntityId(EntityId::from_bytes([31; 32]));
    let predicate = Predicate::And(
        Box::new(Predicate::MemberOf(model)),
        Box::new(Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Path(ankql::ast::PropertyPath::from(property))),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(Value::String("inside".into()))),
        }),
    );
    let project_predicate = predicate.clone();
    let (revision, mut cursor) = node
        .reactor
        .subscribe_maintenance(move |entities| {
            entities
                .iter()
                .map(|entity| {
                    let mut projected = project_entity(entity)?;
                    projected.member =
                        entity.with_read(|read| crate::selection::filter::evaluate_predicate(read, &project_predicate))??;
                    Ok(projected)
                })
                .collect()
        })
        .await?;
    cursor.finish_bootstrap(predicate, [], revision).await?;

    let unrelated = Event::genesis(
        None,
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(ModelId::EntityId(EntityId::from_bytes([8; 32]))))]),
    );
    apply(&node, &unrelated).await?;
    assert!(cursor.fence().await?.batch.is_none(), "other models are not projected");

    let set = |value: &str| -> anyhow::Result<Operation> {
        let backend = LWWBackend::new();
        backend.set(property, Some(Value::String(value.into())));
        Ok(Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() })
    };
    let entered =
        Event::genesis(None, AuthorId::Unknown, OperationSet(vec![Operation::Membership(Membership::Add(model)), set("inside")?]));
    apply(&node, &entered).await?;
    assert_eq!(cursor.next().await?.changes[0].key.as_deref(), Some("inside"));
    assert!(cursor.maintenance.0.watchers.lock().unwrap().debug_data().2.contains_key(&entered.entity_id));
    let departed =
        Event::update(entered.entity_id, proto::Clock::singleton(&entered), AuthorId::Unknown, OperationSet(vec![set("outside")?]));
    apply(&node, &departed).await?;
    assert_eq!(cursor.next().await?.changes[0].key.as_deref(), Some("outside"));
    assert!(!cursor.maintenance.0.watchers.lock().unwrap().debug_data().2.contains_key(&entered.entity_id));
    Ok(())
}

#[tokio::test]
async fn dropping_maintenance_unregisters_its_watchers_and_projector() -> anyhow::Result<()> {
    let node = node().await?;
    let owned = Arc::new(());
    let weak = Arc::downgrade(&owned);
    let (revision, mut cursor) = node
        .reactor
        .subscribe_maintenance(move |entities| {
            let _ = &owned;
            project(entities)
        })
        .await?;
    let maintenance = cursor.maintenance.clone();
    let id = cursor.reader.id;
    cursor.finish_bootstrap(Predicate::True, [EntityId::from_bytes([32; 32])], revision).await?;
    drop(cursor);
    assert!(weak.upgrade().is_none(), "the subscription owns its projection closure");
    assert!(!maintenance.0.state.lock().unwrap().readers.contains_key(&id));
    let watchers = maintenance.0.watchers.lock().unwrap();
    let (_, wildcard, entities) = watchers.debug_data();
    assert!(!wildcard.iter().any(|(subscription, _)| *subscription == id));
    assert!(entities.is_empty());
    Ok(())
}

#[tokio::test]
async fn catalog_changes_reach_maintenance_without_any_predicate_candidate() -> anyhow::Result<()> {
    let node = node().await?;
    let (revision, mut cursor) = subscribe(&node).await?;
    cursor.finish_bootstrap(Predicate::False, [], revision).await?;
    let event = Event::genesis(
        None,
        AuthorId::Unknown,
        OperationSet(vec![Operation::Membership(Membership::Add(crate::schema::CATALOG_MODELS[0]))]),
    );
    let state = proto::State {
        memberships: [crate::schema::CATALOG_MODELS[0]].into_iter().collect(),
        head: proto::Clock::singleton(&event),
        ..Default::default()
    };
    let (_, entity) = node
        .entities
        .with_state(
            &LocalStateGetter::new(node.storage.clone()),
            &LocalEventGetter::new(node.storage.clone(), false),
            event.entity_id,
            state,
        )
        .await?;
    node.reactor.begin_commit().await.publish([&entity]);
    let window = cursor.fence().await?;
    assert!(window.batch.is_some());
    assert!(window.batch.as_ref().unwrap().catalog_changed);
    assert!(window.batch.as_ref().unwrap().changes.is_empty());
    Ok(())
}
