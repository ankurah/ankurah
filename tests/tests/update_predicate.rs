mod common;
use crate::common::TestWatcher;
use ankurah::changes::ChangeKind;
use ankurah::signals::Subscribe;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::*;
use std::sync::Arc;

#[tokio::test]
async fn test_predicate_update() -> Result<()> {
    let storage_engine = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());

    // Initialize the node's system catalog
    node.system.create().await?;

    // Get context after system is ready
    let context = node.context_async(c).await;

    // Create some test albums
    let trx = context.begin();
    let a_id = trx.create(&Album { name: "Alpha".to_owned(), year: "2020".to_owned() }).await?.id();
    let b_id = trx.create(&Album { name: "Bravo".to_owned(), year: "2021".to_owned() }).await?.id();
    let c_id = trx.create(&Album { name: "Charlie".to_owned(), year: "2022".to_owned() }).await?.id();
    trx.commit().await?;

    let albums = context.query_wait::<AlbumView>("year > 2020").await?;

    let watcher = TestWatcher::changeset();
    let _guard = albums.subscribe(&watcher);

    // Should have Bravo, Charlie (sort for deterministic order - update_selection has to fetch and ulids created in the same ms)
    assert_eq!(albums.ids_sorted(), sorted![b_id, c_id]);
    assert_eq!(watcher.quiesce().await, 0); // no changes yet

    // Update the predicate to be more restrictive: year > 2021 - Should remove Bravo
    albums.update_selection_wait("year > 2021").await?;

    assert_eq!(albums.ids(), vec![c_id]); // Should now have only 1 album (Charlie)
    assert_eq!(watcher.take_one().await, vec![(b_id, ChangeKind::Remove)]);

    // Update predicate to be less restrictive: year >= "2020"
    albums.update_selection_wait("year >= 2020").await?;

    // Should now have all 3 albums
    assert_eq!(albums.ids_sorted(), sorted![a_id, b_id, c_id]);
    assert_eq!(watcher.drain_sorted(), vec![sortby_t0![(a_id, ChangeKind::Initial), (b_id, ChangeKind::Initial)]]);

    // should have no more changes
    assert_eq!(watcher.quiesce().await, 0);

    Ok(())
}

#[tokio::test]
async fn test_predicate_update_inter_node() -> Result<()> {
    use common::*;

    // Create server (durable) and client (ephemeral) nodes
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());

    // Connect the nodes
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(c)?;
    let client_ctx = client.context(c)?;

    // Create some test albums on the server
    let (a_id, b_id, c_id) = {
        let trx = server_ctx.begin();
        let ids = (
            trx.create(&Album { name: "Alpha".to_owned(), year: "2020".to_owned() }).await?.id(),
            trx.create(&Album { name: "Bravo".to_owned(), year: "2021".to_owned() }).await?.id(),
            trx.create(&Album { name: "Charlie".to_owned(), year: "2022".to_owned() }).await?.id(),
        );

        trx.commit().await?;
        ids
    };

    // Create LiveQuery on client with initial predicate
    let albums = client_ctx.query_wait::<AlbumView>("year > 2020").await?;

    let watcher = TestWatcher::changeset();
    let _guard = albums.subscribe(&watcher);

    // Should have Bravo, Charlie (sort for deterministic order - update_selection has to fetch and ulids created in the same ms)
    assert_eq!(albums.ids_sorted(), sorted![b_id, c_id]);
    assert_eq!(watcher.quiesce().await, 0); // no changes yet

    // Update the predicate to be more restrictive: year > 2021 - Should remove Bravo
    albums.update_selection_wait("year > 2021").await?;

    assert_eq!(albums.ids(), vec![c_id]); // Should now have only 1 album (Charlie)
    assert_eq!(watcher.take_one().await, vec![(b_id, ChangeKind::Remove)]);

    // Update predicate to be less restrictive: year >= "2020"
    albums.update_selection_wait("year >= 2020").await?;

    // Should now have all 3 albums (sort for deterministic order - update_selection has to fetch and ulids created in the same ms)
    assert_eq!(albums.ids_sorted(), sorted![a_id, b_id, c_id]);
    assert_eq!(watcher.drain_sorted(), vec![sortby_t0![(a_id, ChangeKind::Initial), (b_id, ChangeKind::Initial)]]);

    // should have no more changes
    assert_eq!(watcher.quiesce().await, 0);

    Ok(())
}
