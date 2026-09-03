mod common;
use ankurah::error::RetrievalError;
use common::*;
use std::collections::BTreeMap;

/// context.get() with a nonexistent entity ID returns an error.
#[tokio::test]
async fn get_nonexistent_entity_errors() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    let result = ctx.get::<AlbumView>(EntityId::random()).await;
    assert!(matches!(result, Err(RetrievalError::EntityNotFound(_))));
    Ok(())
}

/// Local node rejects phantom entity commits.
#[tokio::test]
async fn local_rejects_phantom_commit() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;
    // Register Album so the commit reaches the phantom-baseline check.
    ctx.register_model::<Album>().await?;

    let phantom = AlbumView::from_entity(node.conjure_evil_phantom(EntityId::random(), Album::collection()));
    let trx = ctx.begin();
    phantom.edit(&trx)?.name()?.replace("inside your mind")?;

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

    let fake_update = proto::Event::update(
        Album::collection(),
        EntityId::random(),
        proto::Clock::new([proto::EventId::from_bytes([1u8; 32])]),
        proto::AuthorId::Unknown,
        proto::OperationSet::from_backends(BTreeMap::new()),
    );

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

/// Server refuses a genesis whose content derives an id other than the one the
/// event names (`EventStructureError::GenesisIdMismatch`).
///
/// The already-exists collision this test used to describe is unrepresentable
/// under derived ids: a genesis names whatever entity its own content derives,
/// so naming a foreign entity is a structural refusal that lands before any
/// existence check runs. Using a real existing id here is what makes that
/// concrete -- even an id the server certainly knows does not buy the event
/// admission.
#[tokio::test]
async fn server_refuses_a_genesis_whose_content_derives_a_different_id() -> anyhow::Result<()> {
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

    // Overwrite the derived id with the existing entity's, so the event's own
    // content no longer derives the id it names.
    let mut fake_create =
        proto::Event::genesis(Album::collection(), None, proto::AuthorId::Unknown, proto::OperationSet::from_backends(BTreeMap::new()));
    fake_create.entity_id = existing_id;

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
