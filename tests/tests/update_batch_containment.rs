mod common;

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::property::PropertyKey;
use ankurah::core::value::Value;
use ankurah::proto::{self, Attested};
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent, View};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

use common::{Record, RecordView};

/// Forge a Record LWW event setting `title`, parented on the given parent
/// EVENTS (generation stamped from their payloads; the registry ban).
fn forge_title_event(entity_id: proto::EntityId, parents: &[&proto::Event], title: &str) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(title_prop), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    ankurah_tests::forge::event_with_parents(
        entity_id,
        Record::collection(),
        proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])),
        parents,
    )
}

fn event_only_item(event: proto::Event, source_query: proto::QueryId) -> proto::SubscriptionUpdateItem {
    proto::SubscriptionUpdateItem {
        entity_id: event.entity_id,
        model: event.model,
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(event, None).into()]),
        predicate_relevance: vec![],
        source_queries: vec![source_query],
    }
}

/// A multi-event EventOnly item delivered child-first must apply
/// parent-first: wire order is untrusted for every multi-event shape, not
/// just bridges. Without the sort, the child gap-jumps the head (its parents
/// resolve via staging) and the parent's operations are silently dropped as
/// already-history.
#[tokio::test]
async fn test_event_only_multi_event_wire_order_is_untrusted() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;

    let relay_context = ctx_c.query_wait::<RecordView>("true").await?;

    let (rec_id, genesis) = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");
    assert_eq!(view.entity().head(), proto::Clock::from(vec![genesis.id()]));

    // Forge parent (writes artist) then child (writes title), and put the
    // CHILD first on the wire.
    let ev_parent = {
        use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
        use ankurah::core::property::PropertyKey;
        use ankurah::core::value::Value;
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Id(record_artist), Some(Value::String("artist-p1".to_owned())));
        let ops = backend.to_operations().unwrap().expect("ops");
        ankurah_tests::forge::event_with_parents(
            rec_id,
            Record::collection(),
            proto::OperationSet(std::collections::BTreeMap::from([("lww".to_owned(), ops)])),
            &[&genesis],
        )
    };
    let ev_child = forge_title_event(rec_id, &[&ev_parent], "t-child");
    let (id_parent, id_child) = (ev_parent.id(), ev_child.id());

    let item = proto::SubscriptionUpdateItem {
        entity_id: rec_id,
        model: record_model,
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(ev_child, None).into(), Attested::opt(ev_parent, None).into()]),
        predicate_relevance: vec![],
        source_queries: vec![relay_context.query_id()],
    };
    let update = proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from: server.id,
        to: client.id,
        body: proto::NodeUpdateBody::SubscriptionUpdate { items: vec![item] },
        schema: vec![],
    };
    client.handle_message(proto::NodeMessage::Update(update)).await?;

    // Both events applied in causal order: the parent's write survives.
    assert_eq!(view.title().unwrap(), "t-child", "child's write applied");
    assert_eq!(view.artist().unwrap(), "artist-p1", "parent's write must not be dropped by wire order");

    let collection = ctx_c.collection(&Record::collection()).await?;
    let ids: std::collections::HashSet<_> = collection.dump_entity_events(rec_id).await?.iter().map(|e| e.payload.id()).collect();
    assert!(ids.contains(&id_parent) && ids.contains(&id_child), "both events durable on the client");

    Ok(())
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
    let relay_context = ctx_c.query_wait::<RecordView>("true").await?;

    // Two records created on the server; the client materializes them at
    // their creation heads and holds them resident.
    let (a_id, b_id, genesis_a, genesis_b) = {
        let trx = ctx_s.begin();
        let a = trx.create(&Record { title: "a0".to_owned(), artist: "artist-a".to_owned() }).await?;
        let b = trx.create(&Record { title: "b0".to_owned(), artist: "artist-b".to_owned() }).await?;
        let ids = (a.id(), b.id());
        let events = trx.commit_and_return_events().await?;
        let ga = events.iter().find(|e| e.entity_id == ids.0).expect("a's creation event").clone();
        let gb = events.iter().find(|e| e.entity_id == ids.1).expect("b's creation event").clone();
        (ids.0, ids.1, ga, gb)
    };
    let view_a = ctx_c.get::<RecordView>(a_id).await?;
    let view_b = ctx_c.get::<RecordView>(b_id).await?;
    assert_eq!(view_a.title().unwrap(), "a0");
    assert_eq!(view_b.title().unwrap(), "b0");
    // #330: forged events carry Record's model id, registered by the creates above.
    let record_model = server.catalog.model_id_for(Record::collection().as_str()).expect("record registered by the creates");
    let record_title = server.catalog.resolve(Record::collection().as_str(), "title").expect("title registered with Record");

    // Forge the batch: valid events for A and B (parented on their current
    // heads), and a non-creation event for an entity the client knows nothing
    // about in the middle.
    let ev_a = forge_title_event(a_id, &[&genesis_a], "a1");
    let ev_b = forge_title_event(b_id, &[&genesis_b], "b1");
    let unknown_id = proto::EntityId::new();
    // The parent id is fabricated (no true generation exists); the explicit
    // claim of 2 is plausible and admission can never verify it.
    let ev_unknown = {
        let backend = LWWBackend::new();
        backend.set("title".into(), Some(Value::String("ghost".to_owned())));
        let ops = backend.to_operations().unwrap().expect("ops");
        ankurah_tests::forge::event_claiming(
            unknown_id,
            Record::collection(),
            proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])),
            proto::Clock::from(vec![proto::EventId::from_bytes([7u8; 32])]),
            2,
        )
    };
    let (id_ev_a, id_ev_b) = (ev_a.id(), ev_b.id());

    let update = proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from: server.id,
        to: client.id,
        body: proto::NodeUpdateBody::SubscriptionUpdate {
            items: vec![
                event_only_item(ev_a, relay_context.query_id()),
                event_only_item(ev_unknown, relay_context.query_id()),
                event_only_item(ev_b, relay_context.query_id()),
            ],
        },
        schema: vec![],
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
