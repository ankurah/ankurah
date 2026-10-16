//! Live membership changes must preserve the events observed by query subscribers.
mod common;
use ankurah::Mutable;
use anyhow::Result;
use common::*;
use std::{collections::HashSet, sync::Arc, time::Duration};

#[tokio::test]
async fn predicate_exit_preserves_the_live_removal_events() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let pet = trx.create(&Pet { name: "Rex".into(), age: "1".into() }).await?.read()?;
    trx.commit().await?;
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let query = reader.query_wait::<PetView>(nocache("age = '1'")?).await?;
    query.wait_durable_answered().await?;
    let retained = query.peek()[0].clone();
    let watcher = TestWatcher::changeset_with_event_ids();
    let _guard = query.subscribe(&watcher);
    let trx = writer.begin();
    pet.edit(&trx)?.age()?.replace("0")?;
    trx.commit().await?;
    let expected: HashSet<_> = pet.entity().head().ids().cloned().collect();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !query.ids().is_empty() || retained.age().as_deref() != Ok("0") {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let changes = watcher.quiesce_drain().await;
    let removals: Vec<_> = changes.iter().flatten().filter(|(id, kind, _)| *id == pet.id() && *kind == ChangeKind::Remove).collect();
    assert_eq!(removals.len(), 1, "one live predicate exit emits one removal: {changes:?}");
    let received: HashSet<_> = removals.into_iter().flat_map(|(_, _, events)| events.iter().cloned()).collect();
    assert_eq!(received, expected, "the removal must carry the events that caused the predicate exit");
    Ok(())
}

#[tokio::test]
async fn live_limit_replacement_publishes_one_membership_batch_with_both_event_sets() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let exiting = trx.create(&Pet { name: "A".into(), age: "1".into() }).await?.read()?;
    let entering = trx.create(&Pet { name: "B".into(), age: "0".into() }).await?.read()?;
    trx.commit().await?;
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let query = reader.query_wait::<PetView>(nocache("age = '1' ORDER BY name ASC LIMIT 1")?).await?;
    query.wait_durable_answered().await?;
    assert_eq!(query.ids(), [exiting.id()]);
    let watcher = TestWatcher::changeset_with_event_ids();
    let _guard = query.subscribe(&watcher);
    let trx = writer.begin();
    exiting.edit(&trx)?.age()?.replace("0")?;
    entering.edit(&trx)?.age()?.replace("1")?;
    trx.commit().await?;
    let batch = watcher.take_one_with_timeout(Duration::from_secs(10)).await;
    assert_eq!(batch.len(), 2, "replacement membership and its events belong to one publication: {batch:?}");
    for (pet, kind) in [(&exiting, ChangeKind::Remove), (&entering, ChangeKind::Add)] {
        let change = batch.iter().find(|(id, _, _)| *id == pet.id()).expect("both changed identities are present");
        assert_eq!(change.1, kind);
        assert_eq!(
            change.2.iter().cloned().collect::<HashSet<_>>(),
            pet.entity().head().ids().cloned().collect::<HashSet<_>>(),
            "each membership change carries its applied events"
        );
    }
    assert_eq!(query.ids(), [entering.id()]);
    assert_eq!(watcher.quiesce().await, 0, "the publication must not also emit separate duplicate changes");
    Ok(())
}
