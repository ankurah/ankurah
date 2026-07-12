mod common;
use ankurah::error::RetrievalError;
use common::*;
use std::collections::BTreeMap;

/// context.get() with a nonexistent entity ID returns an error.
#[tokio::test]
async fn get_nonexistent_entity_errors() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    let result = ctx.get::<AlbumView>(EntityId::new()).await;
    assert!(matches!(result, Err(RetrievalError::EntityNotFound(_))));
    Ok(())
}

/// Local node rejects phantom entity commits.
#[tokio::test]
async fn local_rejects_phantom_commit() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    let phantom = AlbumView::from_entity(node.conjure_evil_phantom(EntityId::new(), Album::collection()));
    let trx = ctx.begin();
    phantom.edit(&trx)?.name().replace("inside your mind")?;

    assert!(trx.commit().await.is_err());
    Ok(())
}

/// Server rejects update events for nonexistent entities.
#[tokio::test]
async fn server_rejects_update_for_nonexistent() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn: LocalProcessConnection<SledStorageEngine, PermissiveAgent, SledStorageEngine, PermissiveAgent> =
        LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    // #330: CommitTransaction ingress resolves the event's model id to a local
    // collection and rejects an unknown one. Register Album on the server (a
    // throwaway create only warms the catalog, mirroring epoch_flip.rs) so the
    // forged event carries Album's real model id and this test keeps exercising
    // the nonexistent-entity rejection rather than an unknown-model rejection.
    {
        let ctx = server.context(DEFAULT_CONTEXT)?;
        let trx = ctx.begin();
        trx.create(&Album { name: "warm".into(), year: "2024".into() }).await?;
        trx.commit().await?;
    }
    let album_model = server.catalog.model_id_for(Album::collection().as_str()).expect("Album registered by the warming create");

    let fake_update = proto::Event {
        model: album_model,
        entity_id: EntityId::new(),
        operations: proto::OperationSet(BTreeMap::new()),
        parent: proto::Clock::new([proto::EventId::from_bytes([1u8; 32])]),
    };

    let resp = client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::CommitTransaction { id: proto::TransactionId::new(), events: vec![fake_update.into()] },
        )
        .await?;

    assert!(matches!(resp, proto::NodeResponseBody::Error(_)));
    Ok(())
}

/// Server rejects create events for entities that already exist.
#[tokio::test]
async fn server_rejects_create_for_existing() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    // Create an entity on the server first
    let ctx = server.context(DEFAULT_CONTEXT)?;
    let trx = ctx.begin();
    let album = trx.create(&Album { name: "Existing".into(), year: "2024".into() }).await?;
    let existing_id = album.id();
    trx.commit().await?;

    // Try to send a create event for the same entity
    // Arguably this is a "collision" but collisions really should not happen
    let fake_create = proto::Event {
        // #330: Album was registered by the create above, so stamp the real
        // model id and let the duplicate-genesis guard (not model resolution)
        // reject it.
        model: server.catalog.model_id_for(Album::collection().as_str()).expect("Album registered by the create above"),
        entity_id: existing_id,
        operations: proto::OperationSet(BTreeMap::new()),
        parent: proto::Clock::new([]),
    };

    let resp = client
        .request(
            server.id,
            &DEFAULT_CONTEXT,
            proto::NodeRequestBody::CommitTransaction { id: proto::TransactionId::new(), events: vec![fake_create.into()] },
        )
        .await?;

    assert!(matches!(resp, proto::NodeResponseBody::Error(_)));
    Ok(())
}
