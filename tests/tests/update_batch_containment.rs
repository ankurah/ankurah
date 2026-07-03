mod common;

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::value::Value;
use ankurah::proto::{self, Attested};
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent, View};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

use common::{Record, RecordView};

/// Forge a Record LWW event setting `title`, parented on the given clock.
fn forge_title_event(entity_id: proto::EntityId, parent: proto::Clock, title: &str) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set("title".into(), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    proto::Event {
        entity_id,
        collection: Record::collection(),
        operations: proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])),
        parent,
    }
}

fn event_only_item(event: proto::Event) -> proto::SubscriptionUpdateItem {
    proto::SubscriptionUpdateItem {
        entity_id: event.entity_id,
        collection: event.collection.clone(),
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(event, None).into()]),
        predicate_relevance: vec![],
    }
}

/// V6: one bad item in a subscription update batch must not poison the rest.
/// The middle item is an EventOnly (non-creation) for an entity the client
/// has never seen: the empty-head guard rejects it. Items one and three must
/// still apply, and the speculatively materialized empty-head resident for
/// the unknown entity must be evicted rather than left as a phantom.
#[tokio::test]
async fn test_event_only_unknown_entity_does_not_poison_batch() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;

    // A live subscription (to anything) establishes the relay context that
    // apply_updates requires for this peer. Held for the test's duration.
    let _relay_context = ctx_c.query_wait::<RecordView>("title = 'no-such-title'").await?;

    // Two records created on the server; the client materializes them at
    // their creation heads and holds them resident.
    let (a_id, b_id) = {
        let trx = ctx_s.begin();
        let a = trx.create(&Record { title: "a0".to_owned(), artist: "artist-a".to_owned() }).await?;
        let b = trx.create(&Record { title: "b0".to_owned(), artist: "artist-b".to_owned() }).await?;
        let ids = (a.id(), b.id());
        trx.commit().await?;
        ids
    };
    let view_a = ctx_c.get::<RecordView>(a_id).await?;
    let view_b = ctx_c.get::<RecordView>(b_id).await?;
    assert_eq!(view_a.title().unwrap(), "a0");
    assert_eq!(view_b.title().unwrap(), "b0");

    // Forge the batch: valid events for A and B (parented on their current
    // heads), and a non-creation event for an entity the client knows nothing
    // about in the middle.
    let ev_a = forge_title_event(a_id, view_a.entity().head().clone(), "a1");
    let ev_b = forge_title_event(b_id, view_b.entity().head().clone(), "b1");
    let unknown_id = proto::EntityId::new();
    let ev_unknown = forge_title_event(unknown_id, proto::Clock::from(vec![proto::EventId::from_bytes([7u8; 32])]), "ghost");
    let (id_ev_a, id_ev_b) = (ev_a.id(), ev_b.id());

    let update = proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from: server.id,
        to: client.id,
        body: proto::NodeUpdateBody::SubscriptionUpdate {
            items: vec![event_only_item(ev_a), event_only_item(ev_unknown), event_only_item(ev_b)],
        },
    };

    // handle_message reports per-update failures to the sender via
    // NodeUpdateAck::Error rather than its own return value; the aggregate
    // ApplyError::Items surfaces there. What this test pins is the state
    // outcome: containment, not batch poisoning.
    client.handle_message(proto::NodeMessage::Update(update)).await?;

    // Items one and three applied despite the failure in the middle.
    assert_eq!(view_a.title().unwrap(), "a1", "item 1 must apply");
    assert_eq!(view_b.title().unwrap(), "b1", "item 3 must apply");

    // Their events are durable on the client; nothing exists for the unknown.
    let collection = ctx_c.collection(&Record::collection()).await?;
    let a_events: Vec<_> = collection.dump_entity_events(a_id).await?.iter().map(|e| e.payload.id()).collect();
    let b_events: Vec<_> = collection.dump_entity_events(b_id).await?.iter().map(|e| e.payload.id()).collect();
    assert!(a_events.contains(&id_ev_a), "A's forged event must be committed on the client");
    assert!(b_events.contains(&id_ev_b), "B's forged event must be committed on the client");
    assert!(collection.dump_entity_events(unknown_id).await?.is_empty(), "no event may be durable for the unknown entity");

    // The speculative empty-head resident for the unknown entity was evicted:
    // a phantom resident would satisfy get() with an empty-state view, while
    // eviction forces a retrieval attempt that fails (server never had it).
    let phantom = ctx_c.get::<RecordView>(unknown_id).await;
    assert!(phantom.is_err(), "phantom empty-head resident must be evicted, got {phantom:?}");

    Ok(())
}
