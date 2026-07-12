mod common;

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::property::PropertyKey;
use ankurah::core::value::Value;
use ankurah::proto::{self, Attested};
use ankurah::signals::Subscribe;
use ankurah::{changes::ChangeKind, policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent, View};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use common::{Record, RecordView, TestWatcher};

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

/// Forge a full Record LWW state (title and artist) whose head is the given
/// event id. Post-0.9 LWW buffers carry per-property event provenance, so
/// the state is built by applying operations attributed to that event,
/// exactly as a real apply would.
fn forge_state(
    title_property: proto::EntityId,
    artist_property: proto::EntityId,
    title: &str,
    artist: &str,
    head_event: proto::EventId,
) -> proto::State {
    let writer = LWWBackend::new();
    writer.set(PropertyKey::Id(title_property), Some(Value::String(title.to_owned())));
    writer.set(PropertyKey::Id(artist_property), Some(Value::String(artist.to_owned())));
    let ops = writer.to_operations().unwrap().expect("LWW backend with writes produces operations");

    let backend = LWWBackend::new();
    backend.apply_operations_with_event(&ops, head_event.clone()).expect("forged operations apply");
    let buf = backend.to_state_buffer().expect("LWW state buffer serializes");
    proto::State {
        state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_owned(), buf)])),
        head: proto::Clock::from(vec![head_event.clone()]),
        // The head event is fabricated (no payload exists anywhere), so the
        // forged annotation claims genesis depth; the receiving ephemeral
        // adopts it inside the state's trust envelope.
        head_generations: proto::GClock::from((1, head_event)),
    }
}

fn event_only_update(
    server: &proto::EntityId,
    client: &proto::EntityId,
    source_query: proto::QueryId,
    ev: proto::Event,
) -> proto::NodeUpdate {
    proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from: *server,
        to: *client,
        body: proto::NodeUpdateBody::SubscriptionUpdate {
            items: vec![proto::SubscriptionUpdateItem {
                entity_id: ev.entity_id,
                model: ev.model,
                content: proto::UpdateContent::EventOnly(vec![Attested::opt(ev, None).into()]),
                predicate_relevance: vec![],
                source_queries: vec![source_query],
            }],
        },
        schema: vec![],
    }
}

/// R4 visibility (D1 plan section 5): an entity fetched through the Get
/// response path must notify an established matching query.
///
/// The entity reaches the server's storage without ever passing through the
/// server's reactor (raw storage write), so no subscription push can mask
/// the lane under test: the ONLY way the client learns about the entity is
/// its own ctx.get, and the established query for exactly that title must
/// see an Add. Red today: the Get response path raw-writes storage next to
/// a live TODO, mediating no entity and notifying nothing, so the query
/// result set silently misses an entity the node now holds.
#[tokio::test]
async fn test_get_fetch_notifies_established_matching_query() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;

    // Establish the query FIRST: its initial set is empty because nothing
    // matches yet, and the watcher subscribes after initialization so no
    // Initial entry is expected.
    let query = ctx_c.query_wait::<RecordView>("title = 'fetched-title'").await?;
    assert_eq!(query.ids(), vec![]);
    let watcher = TestWatcher::changeset();
    let _handle = query.subscribe(&watcher);
    let record_model = server.catalog.model_id_for(Record::collection().as_str()).expect("Record registered by query");
    let title_property = server.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by query");
    let artist_property = server.catalog.resolve(Record::collection().as_str(), "artist").expect("Record.artist registered by query");

    // Forge the entity directly into SERVER storage. The server's reactor
    // never fires (raw storage write), so the client's remote subscription
    // cannot deliver it; only the Get lane can.
    let x_id = proto::EntityId::new();
    let state = forge_state(title_property, artist_property, "fetched-title", "ghost-artist", proto::EventId::from_bytes([9u8; 32]));
    let entity_state = proto::EntityState { entity_id: x_id, model: record_model, state };
    let server_collection = ctx_s.collection(&Record::collection()).await?;
    server_collection.set_state(Attested::opt(entity_state, None)).await?;

    // The fetch itself works today: the client materializes the entity.
    let view = ctx_c.get::<RecordView>(x_id).await?;
    assert_eq!(view.title().unwrap(), "fetched-title");

    // Settle, then assert visibility: the established matching query must
    // have learned about the entity from the fetch.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(query.ids(), vec![x_id], "fetched entity must join the established matching query's result set");
    assert_eq!(watcher.drain(), vec![vec![(x_id, ChangeKind::Add)]], "the fetch must notify the established query exactly once");

    Ok(())
}

/// R4 safety (D1 plan section 5): a stale fetch must not regress a newer
/// local state.
///
/// The client is deliberately AHEAD of the server (a forged EventOnly
/// descendant applied locally, buffer at parity per R9). A second ctx.get
/// then delivers the server's older state. Red today: the Get response path
/// raw-writes the fetched state over the newer persisted buffer, so the
/// buffer head regresses to the server's stale head while the resident
/// stays ahead; a later rehydration would serve the stale state.
#[tokio::test]
async fn test_stale_fetch_cannot_regress_newer_state() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;

    // Relay context for the EventOnly delivery below.
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

    // Advance the CLIENT past the server with a forged linear descendant.
    // The server never sees this event, so its Get responses are now stale
    // relative to the client.
    let title_property = server.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");
    let ev = forge_title_event(title_property, rec_id, &[&genesis], "t1-client-ahead");
    let forged_head = proto::Clock::from(vec![ev.id()]);
    client.handle_message(proto::NodeMessage::Update(event_only_update(&server.id, &client.id, _relay_context.query_id(), ev))).await?;
    assert_eq!(view.title().unwrap(), "t1-client-ahead");
    assert_eq!(view.entity().head().clone(), forged_head);

    // M3 parity: the buffer rests at the forged head before the fetch.
    let collection = ctx_c.collection(&Record::collection()).await?;
    assert_eq!(collection.get_state(rec_id).await?.payload.state.head, forged_head, "precondition: buffer at parity before the fetch");

    // A second get fetches the server's OLDER state.
    let refetched = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(refetched.title().unwrap(), "t1-client-ahead", "resident must stay ahead");

    // The stale fetch must not have regressed the persisted buffer.
    let buffered_head = collection.get_state(rec_id).await?.payload.state.head;
    assert_eq!(buffered_head, forged_head, "a stale fetch must not regress the persisted state buffer");

    Ok(())
}

/// R4 reentrancy exercise (D1 plan section 2.9): get_entity racing a notify
/// storm on the same entity must not deadlock.
///
/// The amended Get lane notifies AFTER apply returns, from the feeder, so
/// the reactor's non-reentrant evaluation lock is never taken from inside
/// evaluation. This is a liveness guard, asserted by wall-clock timeout: it
/// passes today (the cycle is not reachable) and must keep passing once the
/// Get lane notifies (M4).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_racing_notify_storm_does_not_deadlock() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;

    // A live matching query so every delivered event triggers reactor
    // evaluation (the notify storm has an audience).
    let _query = ctx_c.query_wait::<RecordView>("artist = 'a0'").await?;

    let (rec_id, genesis) = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };
    let view = ctx_c.get::<RecordView>(rec_id).await?;

    // Concurrent get storm on the same entity.
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let ctx = ctx_c.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..15 {
                let _ = ctx.get::<RecordView>(rec_id).await;
            }
        }));
    }

    // Notify storm: a forged linear chain delivered while the gets run.
    let title_property = server.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");
    let mut parent = genesis;
    for i in 0..20 {
        let ev = forge_title_event(title_property, rec_id, &[&parent], &format!("t{}", i + 1));
        client.handle_message(proto::NodeMessage::Update(event_only_update(&server.id, &client.id, _query.query_id(), ev.clone()))).await?;
        parent = ev;
    }

    tokio::time::timeout(Duration::from_secs(15), async {
        for task in tasks {
            task.await.expect("get task must not panic");
        }
    })
    .await
    .expect("get storm racing a notify storm must not deadlock");

    assert_eq!(view.title().unwrap(), "t20", "the full forged chain applied");

    Ok(())
}
