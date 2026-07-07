mod common;

use ankurah::core::node_applier::NodeApplier;
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

type Setup = (
    Node<SledStorageEngine, PermissiveAgent>,
    Node<SledStorageEngine, PermissiveAgent>,
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

    let rec_id = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    // Materialize the record on the client so it has real local state; the
    // view is returned because the entity set holds only weak references.
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");

    Ok((server, client, rec_id, view, relay_context, conn))
}

/// R5 (B3 residual): a child whose parent is missing buffers as a non-error
/// outcome, survives across deliveries in the node-held staging area, and
/// integrates via descendant re-drive when the parent arrives in a LATER
/// delivery. Red today: staging dies with each applier call, so the child is
/// dropped, the item errors, and the parent's later arrival converges to the
/// parent alone.
#[tokio::test]
async fn r5_buffered_child_integrates_when_parent_arrives_later() -> Result<()> {
    let (server, client, rec_id, _view, _relay, _conn) = setup().await?;
    let ctx_c = client.context(c)?;

    let head = client.get_resident_entity(rec_id).expect("client holds the record resident").head();
    let parent = forge_title_event(rec_id, head, "parent-title");
    let parent_id = parent.id();
    let child = forge_title_event(rec_id, proto::Clock::from(vec![parent_id.clone()]), "child-title");
    let child_id = child.id();

    // Delivery 1: the child alone. Post-flip this is a buffered non-error
    // outcome (the M5 note: NeedsEvents becomes a non-error at the M8
    // retention flip); the event is retained, observable, not dropped.
    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(child)])
        .await
        .expect("a dangling child buffers as a non-error outcome");
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
/// and does not perturb the entity. Red today: the per-call area drops it and
/// the item surfaces as an error.
#[tokio::test]
async fn r6_dangling_parent_event_is_buffered_and_observable() -> Result<()> {
    let (server, client, rec_id, _view, _relay, _conn) = setup().await?;
    let ctx_c = client.context(c)?;

    let unknown_parent = proto::EventId::from_bytes([9u8; 32]);
    let orphan = forge_title_event(rec_id, proto::Clock::from(vec![unknown_parent]), "orphan-title");
    let orphan_id = orphan.id();

    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(orphan)])
        .await
        .expect("a dangling-parent event buffers as a non-error outcome");

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

/// R8 (bounds): flooding distinct orphaned events past the cap evicts
/// oldest-first, counted and observable, and the node stays live. Red today:
/// nothing accumulates node-side, so len and evictions both read zero.
#[tokio::test]
async fn r8_orphan_flood_is_bounded_and_observable() -> Result<()> {
    let (server, client, rec_id, _view, _relay, _conn) = setup().await?;
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
            event_only_item(forge_title_event(entity, proto::Clock::from(vec![parent]), "flood"))
        })
        .collect();

    let _ = NodeApplier::apply_updates_for_test(&client, &server.id, items).await;

    let (len, evictions, cap_now) = client.staging_probe_for_test(&Record::collection());
    assert_eq!(cap_now, cap);
    assert_eq!(len, cap, "the staging area holds exactly the cap under flood");
    assert_eq!(evictions as usize, flood - cap, "cap eviction is oldest-first and counted");

    // The node stays live: reads still answer and a legitimate update still
    // applies end to end.
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");

    let head = client.get_resident_entity(rec_id).expect("record still resident").head();
    let legit = forge_title_event(rec_id, head, "still-alive");
    let legit_id = legit.id();
    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(legit)]).await.expect("a clean update still applies");
    assert_eq!(client.get_resident_entity(rec_id).unwrap().head(), proto::Clock::from(vec![legit_id]));

    Ok(())
}

/// R1 (268-A): redelivering an already-committed batch is success for ack
/// purposes, applies nothing, and drifts no head. The planner's plan-time
/// head-containment skip plus apply_event's own idempotency make a lost-ack
/// sender retry harmless, and the retention sweep leaves nothing staged
/// behind the skip.
#[tokio::test]
async fn r1_redelivered_committed_batch_is_idempotent() -> Result<()> {
    let (server, client, rec_id, _view, _relay, _conn) = setup().await?;
    let ctx_c = client.context(c)?;

    let head = client.get_resident_entity(rec_id).expect("record resident").head();
    let ev = forge_title_event(rec_id, head, "once");
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
