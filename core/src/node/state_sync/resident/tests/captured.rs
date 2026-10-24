use super::*;

#[tokio::test]
async fn maintenance_and_captures_retain_only_keys_and_cloned_heads() -> anyhow::Result<()> {
    let node = node().await?;
    let original = genesis("01")?;
    apply(&node, &original).await?;
    let selected = selection(None);
    let spec = spec_for(&selected, &node.catalog);
    let mut actor = complete_resident(&node, spec, vec![DEFAULT_CONTEXT]).await?;
    let id = original.entity_id;
    let frozen = view(&actor, &selected, &node.catalog).await?;
    assert_eq!(frozen.entries[&id].head, proto::Clock::singleton(&original));
    assert!(node.entities.get(&id).is_none(), "a resident tree must not pin an entity");

    let moved = update(&original, "02")?;
    apply(&node, &moved).await?;
    let batch = actor.cursor.next().await?;
    let published = batch.changes.iter().find(|change| change.entity_id == id).unwrap();
    assert_eq!(published.entry.as_ref().unwrap().head, proto::Clock::singleton(&moved));
    assert_eq!(published.events, vec![moved.id()], "maintenance queues event IDs, not event bodies");
    actor.apply(batch.changes, true).await?;
    let current = actor.tree.snapshot();
    assert_eq!(current.entries[&id].head, proto::Clock::singleton(&moved));
    assert_eq!(frozen.entries[&id].head, proto::Clock::singleton(&original));
    assert!(node.entities.get(&id).is_none(), "maintenance must not retain an entity handle");

    // Payload retrieval goes back to the node even while an older metadata read view lives.
    let now = current_state(&node, id).await?.unwrap();
    assert_eq!(now.state.head, proto::Clock::singleton(&moved));
    assert_eq!(TemporaryEntity::new(id, &now.state)?.value(&property()), Some(Value::String("02".into())));
    Ok(())
}
