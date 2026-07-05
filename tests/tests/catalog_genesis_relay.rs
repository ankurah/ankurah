//! RFC section 4: catalog genesis events arriving over the relay are
//! self-certifying. A receiver validates them by recomputation (entity id
//! against the derivation, event id against the frozen encoding) and
//! rejects anything that does not certify, regardless of the channel.

mod common;
use ankurah::core::schema::genesis;
use common::*;

fn update_with(server_id: EntityId, client_id: EntityId, event: proto::Event) -> proto::NodeMessage {
    let fragment =
        proto::EventFragment { operations: event.operations.clone(), parent: event.parent.clone(), attestations: Default::default() };
    proto::NodeMessage::Update(proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from: server_id,
        to: client_id,
        body: proto::NodeUpdateBody::SubscriptionUpdate {
            items: vec![proto::SubscriptionUpdateItem {
                entity_id: event.entity_id,
                collection: event.collection.clone(),
                content: proto::UpdateContent::EventOnly(vec![fragment]),
                predicate_relevance: vec![],
            }],
        },
    })
}

/// A legitimate catalog genesis (frozen encoding, derived id) applies; a
/// tampered one (claimed under a different entity id) is rejected and
/// nothing is persisted.
#[tokio::test]
async fn relayed_catalog_genesis_is_self_certified() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    // Updates without a registered subscription context are refused
    // wholesale (apply_updates), so stand up the catalog subscription the
    // way a real node does; catalog updates then arrive on it.
    let _ctx = client.context_async(DEFAULT_CONTEXT).await;
    client.catalog.wait_catalog_ready().await;

    let root = client.system.root().unwrap().payload.entity_id;

    // NOTE: handle_message converts apply errors into UpdateAcks (the wire
    // behavior), so acceptance is asserted through the event store and the
    // catalog map, not the call result.

    // Legitimate: produced by the frozen encoder against the shared root.
    let genuine = genesis::model_genesis(&root, "album");
    let genuine_event_id = genuine.id();
    client.handle_message(update_with(server.id, client.id, genuine)).await?;
    let storage = client.collections.get(&"_ankurah_model".into()).await?;
    assert_eq!(storage.get_events(vec![genuine_event_id]).await?.len(), 1, "certified genesis must be accepted and stored");
    assert!(client.catalog.model_by_collection("album").is_some(), "certified genesis reaches the catalog map");

    // Tampered: same payload, claimed under a different (undeserved) id.
    let mut forged = genesis::model_genesis(&root, "playlist");
    forged.entity_id = EntityId::new();
    let forged_event_id = forged.id();
    client.handle_message(update_with(server.id, client.id, forged)).await?;
    assert_eq!(storage.get_events(vec![forged_event_id]).await?.len(), 0, "forged genesis must not be stored");

    // Tampered payload: field value swapped so the derivation no longer
    // matches the claimed id.
    let mut swapped = genesis::model_genesis(&root, "playlist");
    swapped.operations = genesis::model_genesis(&root, "different").operations;
    let swapped_event_id = swapped.id();
    client.handle_message(update_with(server.id, client.id, swapped)).await?;
    assert_eq!(storage.get_events(vec![swapped_event_id]).await?.len(), 0, "payload-swapped genesis must not be stored");
    assert!(client.catalog.model_by_collection("playlist").is_none(), "rejected geneses never reach the catalog map");

    Ok(())
}
