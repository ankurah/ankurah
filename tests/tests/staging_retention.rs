mod common;

use ankurah::core::node_applier::NodeApplier;
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

/// The LWW OperationSet for a title write.
fn title_ops(title_property: proto::EntityId, title: &str) -> proto::OperationSet {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(title_property), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)]))
}

/// Forge a Record LWW event setting `title`, parented on the given parent
/// EVENTS (generation stamped from their payloads; the registry ban).
fn forge_title_event(title_property: proto::EntityId, entity_id: proto::EntityId, parents: &[&proto::Event], title: &str) -> proto::Event {
    let model = parents.first().expect("non-genesis test events have a parent").model;
    ankurah_tests::forge::event_with_parents(entity_id, model, title_ops(title_property, title), parents)
}

/// Forge a Record LWW event with an EXPLICIT claimed generation, for parents
/// that are deliberately fabricated ids (no true generation exists).
fn forge_title_event_claiming(
    title_property: proto::EntityId,
    model: proto::EntityId,
    entity_id: proto::EntityId,
    parent: proto::Clock,
    title: &str,
    generation: u32,
) -> proto::Event {
    ankurah_tests::forge::event_claiming(entity_id, model, title_ops(title_property, title), parent, generation)
}

fn event_only_item(event: proto::Event) -> proto::SubscriptionUpdateItem {
    proto::SubscriptionUpdateItem {
        entity_id: event.entity_id,
        model: event.model,
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(event, None).into()]),
        predicate_relevance: vec![],
    }
}

type Setup = (
    Node<SledStorageEngine, PermissiveAgent>,
    Node<SledStorageEngine, PermissiveAgent>,
    proto::EntityId,
    proto::Event,
    proto::EntityId,
    RecordView,
    ankurah::core::livequery::LiveQuery<RecordView>,
    LocalProcessConnection<SledStorageEngine, PermissiveAgent, SledStorageEngine, PermissiveAgent>,
);

/// Server + connected ephemeral client with a relay context established, plus
/// one committed Record the client holds resident. The connection is returned
/// because dropping it deregisters the peers.
async fn setup() -> Result<Setup> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;
    // Relay context so apply_updates accepts updates from this peer; it is
    // returned because dropping the query unsubscribes it.
    let relay_context = ctx_c.query_wait::<RecordView>("title = 'no-such-title'").await?;

    let (rec_id, genesis) = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };
    let record_title = server.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");
    // Materialize the record on the client so it has real local state; the
    // view is returned because the entity set holds only weak references.
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");

    Ok((server, client, rec_id, genesis, record_title, view, relay_context, conn))
}

/// R5 (B3 residual): a child whose parent is missing returns a retryable
/// error, survives across deliveries in the node-held staging area, and
/// integrates via descendant re-drive when the parent arrives in a LATER
/// delivery. Red today: staging dies with each applier call, so the child is
/// dropped, the item errors, and the parent's later arrival converges to the
/// parent alone.
#[tokio::test]
async fn r5_buffered_child_integrates_when_parent_arrives_later() -> Result<()> {
    let (server, client, rec_id, genesis, record_title, _view, _relay, _conn) = setup().await?;
    let ctx_c = client.context(c)?;

    assert_eq!(
        client.get_resident_entity(rec_id).expect("client holds the record resident").head(),
        proto::Clock::from(vec![genesis.id()])
    );
    let parent = forge_title_event(record_title, rec_id, &[&genesis], "parent-title");
    let child = forge_title_event(record_title, rec_id, &[&parent], "child-title");
    let child_id = child.id();

    // Delivery 1: the child alone is retained but remains a retryable item
    // error until its dependency arrives. That retry lease cooperates with
    // atomic capacity backpressure.
    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(child)])
        .await
        .expect_err("a dangling child stays under sender retry while buffered");
    assert!(
        client.staging_contains_for_test(&Record::collection(), &child_id),
        "the dangling child must be retained in the node-held staging area"
    );

    // Delivery 2: the missing parent. Descendant re-drive must schedule the
    // buffered child behind it and converge the entity to the child's state.
    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(parent)]).await.expect("the parent applies cleanly");

    let resident = client.get_resident_entity(rec_id).expect("record still resident");
    assert_eq!(
        resident.head(),
        proto::Clock::from(vec![child_id.clone()]),
        "re-drive must integrate the buffered child on top of its parent"
    );
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "child-title");

    // Promotion removes the child from staging (retention is for waiting
    // events only), and the applied state is durable.
    assert!(!client.staging_contains_for_test(&Record::collection(), &child_id), "commit promotes the event out of staging");
    let collection = ctx_c.collection(&Record::collection()).await?;
    let stored = collection.get_state(rec_id).await?;
    assert_eq!(stored.payload.state.head, proto::Clock::from(vec![child_id]), "the persisted buffer sees the re-driven apply");

    Ok(())
}

/// R6 (268-B): a dangling-parent event on a node whose parent never arrives
/// is typed NeedsEvents, retained in the node-held staging area, observable,
/// and does not perturb the entity while its retry lease remains active.
#[tokio::test]
async fn r6_dangling_parent_event_is_buffered_and_observable() -> Result<()> {
    let (server, client, rec_id, genesis, record_title, _view, _relay, _conn) = setup().await?;
    let ctx_c = client.context(c)?;

    let unknown_parent = proto::EventId::from_bytes([9u8; 32]);
    // Fabricated parent: no true generation exists; 2 is a plausible claim.
    let orphan =
        forge_title_event_claiming(record_title, genesis.model, rec_id, proto::Clock::from(vec![unknown_parent]), "orphan-title", 2);
    let orphan_id = orphan.id();

    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(orphan)])
        .await
        .expect_err("a dangling-parent event is buffered under a retryable error");

    assert!(
        client.staging_contains_for_test(&Record::collection(), &orphan_id),
        "the orphan must be retained in the node-held staging area"
    );
    let (len, evictions, _cap) = client.staging_probe_for_test(&Record::collection());
    assert!(len >= 1, "retention is observable");
    assert_eq!(evictions, 0, "no cap pressure here");

    // The orphan waits; the entity is unperturbed.
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");

    Ok(())
}

/// Node-held staging belongs to the current system and must be discarded
/// under the same hard-reset fence as residents and durable collections.
#[tokio::test]
async fn hard_reset_discards_buffered_old_system_events() -> Result<()> {
    let (server, client, rec_id, genesis, record_title, _view, _relay, _conn) = setup().await?;
    let unknown_parent = proto::EventId::from_bytes([0xA5; 32]);
    let orphan =
        forge_title_event_claiming(record_title, genesis.model, rec_id, proto::Clock::from(vec![unknown_parent]), "old-system-orphan", 2);
    let orphan_id = orphan.id();

    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(orphan)])
        .await
        .expect_err("the orphan remains under retry before reset");
    assert!(client.staging_contains_for_test(&Record::collection(), &orphan_id));

    client.system.hard_reset().await?;

    let (len, evictions, _cap) = client.staging_probe_for_test(&Record::collection());
    assert_eq!(len, 0, "old-system bodies must not survive into a successor system");
    assert_eq!(evictions, 0, "reset clearing is not capacity eviction");
    assert!(!client.staging_contains_for_test(&Record::collection(), &orphan_id));

    Ok(())
}

/// R8 (bounds): flooding distinct orphaned events past the cap rejects new
/// admissions atomically, preserves retained bodies, and keeps reads live.
#[tokio::test]
async fn r8_orphan_flood_is_bounded_and_observable() -> Result<()> {
    let (server, client, rec_id, genesis, record_title, _view, _relay, _conn) = setup().await?;
    let ctx_c = client.context(c)?;

    let (_, _, cap) = client.staging_probe_for_test(&Record::collection());
    let flood = cap + 5;

    // Distinct unknown entities with unknown parents: each event types
    // NeedsState at plan time (no remote round-trip) and is retained. The
    // item errors stay on the M5-pinned typed surface; retention is
    // independent of the ack verdict.
    let items: Vec<proto::SubscriptionUpdateItem> = (0..flood)
        .map(|i| {
            let entity = proto::EntityId::new();
            let mut seed = [0u8; 32];
            seed[..8].copy_from_slice(&(i as u64).to_le_bytes());
            let parent = proto::EventId::from_bytes(seed);
            // Fabricated parents: explicit plausible claims (2), unverifiable by design.
            event_only_item(forge_title_event_claiming(record_title, genesis.model, entity, proto::Clock::from(vec![parent]), "flood", 2))
        })
        .collect();

    NodeApplier::apply_updates_for_test(&client, &server.id, items).await.expect_err("items beyond the cap receive retryable backpressure");

    let (len, evictions, cap_now) = client.staging_probe_for_test(&Record::collection());
    assert_eq!(cap_now, cap);
    assert_eq!(len, cap, "the staging area holds exactly the cap under flood");
    assert_eq!(evictions, 0, "correctness-bearing retained events are never evicted");

    // The node stays live: reads still answer and a legitimate update still
    // applies end to end.
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");

    assert_eq!(client.get_resident_entity(rec_id).expect("record still resident").head(), proto::Clock::from(vec![genesis.id()]));

    Ok(())
}

/// R1 (268-A): redelivering an already-committed batch is success for ack
/// purposes, applies nothing, and drifts no head. The planner's plan-time
/// head-containment skip plus apply_event's own idempotency make a lost-ack
/// sender retry harmless, and the retention sweep leaves nothing staged
/// behind the skip.
#[tokio::test]
async fn r1_redelivered_committed_batch_is_idempotent() -> Result<()> {
    let (server, client, rec_id, genesis, record_title, _view, _relay, _conn) = setup().await?;
    let ctx_c = client.context(c)?;

    assert_eq!(client.get_resident_entity(rec_id).expect("record resident").head(), proto::Clock::from(vec![genesis.id()]));
    let ev = forge_title_event(record_title, rec_id, &[&genesis], "once");
    let ev_id = ev.id();

    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(ev.clone())]).await.expect("first delivery applies");
    assert_eq!(client.get_resident_entity(rec_id).unwrap().head(), proto::Clock::from(vec![ev_id.clone()]));

    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(ev)])
        .await
        .expect("redelivery of a committed batch is success, not an error");

    assert_eq!(client.get_resident_entity(rec_id).unwrap().head(), proto::Clock::from(vec![ev_id.clone()]), "no head drift");
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "once", "single application");
    assert!(!client.staging_contains_for_test(&Record::collection(), &ev_id), "the skip leaves nothing staged");

    Ok(())
}
