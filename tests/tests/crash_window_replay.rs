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

/// Forge a Record LWW event setting one property, parented on the given
/// parent EVENTS (generation stamped from their payloads; the registry ban).
fn forge_lww_event(entity_id: proto::EntityId, parents: &[&proto::Event], property: proto::EntityId, value: &str) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(property), Some(Value::String(value.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    ankurah_tests::forge::event_with_parents(
        entity_id,
        parents.first().expect("the forged update has a creation parent").model,
        proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])),
        parents,
    )
}

fn event_only_item(event: proto::Event) -> proto::SubscriptionUpdateItem {
    proto::SubscriptionUpdateItem {
        entity_id: event.entity_id,
        model: event.model,
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(event, None).into()]),
        predicate_relevance: vec![],
    }
}

/// R10, commit-lane shape (REV 4, the third face of the gap-jump class): an
/// event can be durably committed while the entity never incorporated its
/// operations (the crash window between commit_event and save_state leaves
/// exactly this; here the log write stands in for the crash). A linear
/// descendant then compares StrictDescends against the stale head, and the
/// apply's head jump must NOT orphan the committed-but-unincorporated
/// event's operations: the head it installs transitively claims them, which
/// makes the loss undetectable afterward. Red today: the StrictDescends arm
/// applies only the incoming event's operations.
#[tokio::test]
async fn r10_commit_lane_replays_committed_but_unincorporated_gap() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let (rec_id, genesis) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        let mut events = trx.commit_and_return_events().await?;
        (id, events.remove(0))
    };
    // Hold a view so the resident stays alive (the entity set is weak).
    let view = ctx.get::<RecordView>(rec_id).await?;
    assert_eq!(node.get_resident_entity(rec_id).expect("record resident").head(), proto::Clock::from(vec![genesis.id()]));

    // The crash simulation: e1 lands in the durable log but the entity never
    // applies it (no state write followed the commit).
    let artist_property = node.catalog.resolve(Record::collection().as_str(), "artist").expect("Record.artist registered by create");
    let title_property = node.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");
    let e1 = forge_lww_event(rec_id, &[&genesis], artist_property, "a1");
    let collection = ctx.collection(&Record::collection()).await?;
    collection.add_event(&Attested::opt(e1.clone(), None)).await?;

    // A linear descendant-only transaction arrives through the commit lane.
    let e2 = forge_lww_event(rec_id, &[&e1], title_property, "t2");
    let e2_id = e2.id();
    node.commit_remote_transaction(&c, proto::TransactionId::new(), vec![Attested::opt(e2, None)])
        .await
        .expect("a linear descendant of committed history applies");

    assert_eq!(view.title().unwrap(), "t2");
    assert_eq!(view.artist().unwrap(), "a1", "the committed-but-unincorporated event's operations must be replayed, not orphaned");
    assert_eq!(
        node.get_resident_entity(rec_id).unwrap().head(),
        proto::Clock::from(vec![e2_id.clone()]),
        "the head is honest: it claims exactly the applied lineage"
    );
    let stored = collection.get_state(rec_id).await?;
    assert_eq!(stored.payload.state.head, proto::Clock::from(vec![e2_id]));

    Ok(())
}

/// R10, streaming shape: the same gap through the PerItem EventOnly arm. The
/// client's log holds e1 (committed) while its resident and state buffer sit
/// at genesis; an EventOnly delivery of e2 (child of e1) must incorporate
/// e1's operations on the way to e2's head. Red today: the jump orphans them
/// in the resident AND the uniformly-persisted state buffer, making the
/// corruption durable and invisible.
#[tokio::test]
async fn r10_streaming_lane_replays_committed_but_unincorporated_gap() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;
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
    assert_eq!(client.get_resident_entity(rec_id).expect("client resident").head(), proto::Clock::from(vec![genesis.id()]));

    // Crash simulation on the client: e1 committed to the local log, never
    // incorporated.
    let artist_property = server.catalog.resolve(Record::collection().as_str(), "artist").expect("Record.artist registered by create");
    let title_property = server.catalog.resolve(Record::collection().as_str(), "title").expect("Record.title registered by create");
    let e1 = forge_lww_event(rec_id, &[&genesis], artist_property, "a1");
    let collection = ctx_c.collection(&Record::collection()).await?;
    collection.add_event(&Attested::opt(e1.clone(), None)).await?;

    // EventOnly delivery of the linear descendant.
    let e2 = forge_lww_event(rec_id, &[&e1], title_property, "t2");
    let e2_id = e2.id();
    NodeApplier::apply_updates_for_test(&client, &server.id, vec![event_only_item(e2)])
        .await
        .expect("a linear descendant of committed history applies");

    assert_eq!(view.title().unwrap(), "t2");
    assert_eq!(view.artist().unwrap(), "a1", "the committed-but-unincorporated event's operations must be replayed, not orphaned");
    assert_eq!(client.get_resident_entity(rec_id).unwrap().head(), proto::Clock::from(vec![e2_id.clone()]));
    let stored = collection.get_state(rec_id).await?;
    assert_eq!(stored.payload.state.head, proto::Clock::from(vec![e2_id]), "the persisted buffer carries the repaired state");

    Ok(())
}
