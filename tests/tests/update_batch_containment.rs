mod common;

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::storage::StorageEngine;
use ankurah::core::value::Value;
use ankurah::proto::{self, Attested};
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent, View};
use ankurah_storage_sled::SledStorageEngine;
use ankurah_tests::state_stream_fixture::ScriptedConnection;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

use common::{Record, RecordView};

/// Forge a Record LWW event setting `title`, parented on the given clock.
fn forge_title_event(title_prop: proto::PropertyId, entity_id: proto::EntityId, parent: proto::Clock, title: &str) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set(title_prop, Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    proto::Event::update(
        entity_id,
        parent,
        proto::AuthorId::Unknown,
        proto::OperationSet::from_backends(BTreeMap::from([("lww".to_owned(), ops)])),
    )
}

fn event_delta(event: proto::Event) -> proto::EntityDelta {
    proto::EntityDelta {
        entity_id: event.entity_id,
        content: proto::DeltaContent::EventBridge { events: vec![Attested::opt(event, None).into()] },
    }
}

/// A multi-event bridge delivered child-first must apply parent-first:
/// wire order is untrusted for the whole entity delta. Without the sort, the
/// child gap-jumps the head (its parents resolve via staging) and the parent's operations are silently dropped as
/// already-history.
#[tokio::test]
async fn test_event_only_multi_event_wire_order_is_untrusted() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let conn = ScriptedConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await.unwrap();

    let ctx_s = server.context_async(c).await?;
    let ctx_c = client.context_async(c).await?;

    let rec_id = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "t0".to_owned(), artist: "a0".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let relay = ctx_c.query_wait::<RecordView>("true").await?;
    // Local initialization can precede the registered stream that this test scripts.
    relay.wait_durable_answered().await?;
    let view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(view.title().unwrap(), "t0");

    let title_prop = common::resolved_prop(&server, Record::descriptor(), "title");
    let artist_prop = common::resolved_prop(&server, Record::descriptor(), "artist");

    // Forge parent (writes artist) then child (writes title), and put the
    // CHILD first on the wire.
    let ev_parent = forge_title_event(title_prop, rec_id, view.entity().head().clone(), "ignored");
    // rebuild parent event to write artist instead of title
    let ev_parent = {
        use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
        use ankurah::core::value::Value;
        let backend = LWWBackend::new();
        backend.set(artist_prop, Some(Value::String("artist-p1".to_owned())));
        let ops = backend.to_operations().unwrap().expect("ops");
        proto::Event::update(
            rec_id,
            ev_parent.parent.clone(),
            proto::AuthorId::Unknown,
            proto::OperationSet::from_backends(std::collections::BTreeMap::from([("lww".to_owned(), ops)])),
        )
    };
    let ev_child = forge_title_event(title_prop, rec_id, proto::Clock::singleton(&ev_parent), "t-child");
    let (id_parent, id_child) = (ev_parent.id(), ev_child.id());

    let delta = proto::EntityDelta {
        entity_id: rec_id,
        content: proto::DeltaContent::EventBridge {
            events: vec![Attested::opt(ev_child, None).into(), Attested::opt(ev_parent, None).into()],
        },
    };
    assert!(!conn.deliver(&relay, vec![delta]).await?, "valid child-first bridge keeps its stream active");

    // Both events applied in causal order: the parent's write survives.
    assert_eq!(view.title().unwrap(), "t-child", "child's write applied");
    assert_eq!(view.artist().unwrap(), "artist-p1", "parent's write must not be dropped by wire order");

    let ids: std::collections::HashSet<_> = client.storage.dump_entity_events(rec_id).await?.iter().map(|e| e.payload.id()).collect();
    assert!(ids.contains(&id_parent) && ids.contains(&id_child), "both events durable on the client");

    Ok(())
}

/// A malformed publication fails its registered origin. An earlier complete
/// publication remains durable; a later valid frame on that failed origin
/// cannot apply. The unknown entity must leave neither state nor an event.
#[tokio::test]
async fn test_unknown_entity_fails_stream_without_corrupting_prior_publication() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let conn = ScriptedConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await.unwrap();

    let ctx_s = server.context_async(c).await?;
    let ctx_c = client.context_async(c).await?;

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
    let relay = ctx_c.query_wait::<RecordView>("true").await?;
    // The malicious publications must target an established remote origin.
    relay.wait_durable_answered().await?;
    let view_a = ctx_c.get::<RecordView>(a_id).await?;
    let view_b = ctx_c.get::<RecordView>(b_id).await?;
    assert_eq!(view_a.title().unwrap(), "a0");
    assert_eq!(view_b.title().unwrap(), "b0");

    let title_prop = common::resolved_prop(&server, Record::descriptor(), "title");

    // Forge successive publications: valid A, an unknown non-creation
    // entity, then valid B, all on the same registered stream origin.
    let ev_a = forge_title_event(title_prop, a_id, view_a.entity().head().clone(), "a1");
    let ev_b = forge_title_event(title_prop, b_id, view_b.entity().head().clone(), "b1");
    let unknown_id = proto::EntityId::random();
    let ev_unknown = forge_title_event(title_prop, unknown_id, proto::Clock::genesis(proto::EventId::from_bytes([7u8; 32])), "ghost");
    let (id_ev_a, id_ev_b) = (ev_a.id(), ev_b.id());

    assert!(
        conn.deliver(&relay, vec![event_delta(ev_a), event_delta(ev_unknown), event_delta(ev_b)]).await?,
        "the malformed publication must fail the registered stream"
    );

    assert_eq!(view_a.title().unwrap(), "a1", "earlier complete publication remains applied");
    assert_eq!(view_b.title().unwrap(), "b0", "later same-origin publication must not apply");

    // The events that advance canonical state are durable on the client. The
    // unknown entity's invalid event never enters the append-only store.
    let a_events: Vec<_> = client.storage.dump_entity_events(a_id).await?.iter().map(|e| e.payload.id()).collect();
    let b_events: Vec<_> = client.storage.dump_entity_events(b_id).await?.iter().map(|e| e.payload.id()).collect();
    let unknown_events: Vec<_> = client.storage.dump_entity_events(unknown_id).await?.iter().map(|e| e.payload.id()).collect();
    assert!(a_events.contains(&id_ev_a), "A's forged event must be committed on the client");
    assert!(!b_events.contains(&id_ev_b), "later same-origin event must not be committed");
    assert!(unknown_events.is_empty(), "the unknown entity's invalid event must not enter the append-only store");

    // The speculative empty-head resident for the unknown entity was evicted:
    // a phantom resident would satisfy get() with an empty-state view, while
    // eviction forces a retrieval attempt that fails (server never had it).
    let phantom = ctx_c.get::<RecordView>(unknown_id).await;
    assert!(phantom.is_err(), "phantom empty-head resident must be evicted, got {phantom:?}");

    Ok(())
}
