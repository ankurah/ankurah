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
fn forge_title_event(title_property: proto::EntityId, entity_id: proto::EntityId, parents: &[&proto::Event], title: &str) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(title_property), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    ankurah_tests::forge::event_with_parents(
        entity_id,
        parents.first().expect("the forged update has a creation parent").model,
        proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])),
        parents,
    )
}

/// R9 (D1 plan section 5): state-buffer parity on the EventOnly arm.
///
/// After an EventOnly apply reaches quiescence, the persisted state buffer
/// must rest at the same head as the resident entity (uniform state
/// persistence, plan section 2.3), and a cold rehydration of the evicted
/// entity must yield the same materialized state. The resident is never
/// ahead of the durable event log at rest even today; the buffer is what
/// rehydration reads, so a lagging buffer serves stale state after eviction.
#[tokio::test]
async fn test_event_only_state_buffer_parity_and_cold_rehydration() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;

    // A live subscription establishes the relay context that apply_updates
    // requires for this peer. Held for the delivery's duration.
    let _relay_context = ctx_c.query_wait::<RecordView>("title = 'no-such-title'").await?;

    let (rec_id, genesis) = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");
    assert_eq!(view.entity().head(), proto::Clock::from(vec![genesis.id()]), "the client head is the creation event");

    // Forge a linear descendant of the client's current head and deliver it
    // through the streaming EventOnly arm.
    let title_property = server.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");
    let ev = forge_title_event(title_property, rec_id, &[&genesis], "t1");
    let ev_id = ev.id();
    let item = proto::SubscriptionUpdateItem {
        entity_id: rec_id,
        model: ev.model,
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(ev, None).into()]),
        predicate_relevance: vec![],
    };
    let update = proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from: server.id,
        to: client.id,
        body: proto::NodeUpdateBody::SubscriptionUpdate { items: vec![item] },
        schema: vec![],
    };
    client.handle_message(proto::NodeMessage::Update(update)).await?;

    // The resident advanced to the forged event's head.
    assert_eq!(view.title().unwrap(), "t1");
    let resident_head = view.entity().head().clone();
    assert_eq!(resident_head, proto::Clock::from(vec![ev_id]));

    // Parity: the persisted state buffer rests at the resident head. The
    // event itself is durable either way; the buffer is the rehydration
    // source, and it must not lag once the apply has quiesced.
    let collection = ctx_c.collection(&Record::collection()).await?;
    let buffered = collection.get_state(rec_id).await?;
    assert_eq!(buffered.payload.state.head, resident_head, "persisted state buffer head must equal the resident head at quiescence");

    // Cold rehydration: evict the resident and re-materialize purely from
    // local storage. The connection is dropped first so the cached get
    // cannot mask a stale buffer with a peer fetch.
    drop(view);
    drop(conn);
    assert!(client.get_resident_entity(rec_id).is_none(), "entity must not be resident for the rehydration read");
    let rehydrated = ctx_c.get_cached::<RecordView>(rec_id).await?;
    assert_eq!(rehydrated.title().unwrap(), "t1", "cold rehydration must yield the state the EventOnly apply produced");

    Ok(())
}
