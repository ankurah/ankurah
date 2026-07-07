//! Phase A "epoch flip" integration tests (RFC 5.5): after the wire switch,
//! USER-collection entities emit the id-keyed (v2) LWW encoding -- state
//! buffer header 0xA2, diff version byte 2, resolved Identifier selections --
//! while the catalog and system collections stay name-keyed (0xA1/v1, the RFC
//! 4 bootstrap exemption). These tests pin the observable consequences of the
//! flip end to end: on-disk bytes, a two-node round trip, the legacy
//! read-fallback with lazy rewrite-on-save, sled materialization through the
//! hint projection, and fail-closed resolution.

mod common;

use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::value::Value;
use ankurah::proto;
use ankurah::{policy::DEFAULT_CONTEXT as c, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

use common::{Record, RecordView};

// -- helpers ----------------------------------------------------------------

/// The LWW state-buffer version header (first byte). Kept in sync with
/// `core/src/property/backend/lww.rs` (0xA1 = name-keyed v1, 0xA2 = id-keyed
/// v2). Hard-coded here because the constants are private to core.
const LWW_STATE_V1: u8 = 0xA1;
const LWW_STATE_V2: u8 = 0xA2;

/// Mirror of the private `LWWDiff` header so a test can read a commit event's
/// LWW operation version. Bincode serializes a struct as its fields in order,
/// so this decodes byte-for-byte the same as the real type.
#[derive(serde::Deserialize)]
struct LwwDiffHeader {
    version: u8,
    #[allow(dead_code)]
    data: Vec<u8>,
}

/// The first byte of the "lww" state buffer stored for `id` in `collection`.
async fn lww_state_version(node: &Node<SledStorageEngine, PermissiveAgent>, collection: &str, id: proto::EntityId) -> Result<u8> {
    let storage = node.collections.get(&collection.into()).await?;
    let state = storage.get_state(id).await?;
    let buffer = state.payload.state.state_buffers.0.get("lww").expect("entity has an lww backend").clone();
    Ok(buffer[0])
}

/// The LWW diff version byte carried by `event` (its "lww" operation).
fn lww_diff_version(event: &proto::Event) -> u8 {
    let ops = event.operations.0.get("lww").expect("commit event carries an lww operation");
    let header: LwwDiffHeader = bincode::deserialize(&ops[0].diff).expect("lww diff decodes");
    header.version
}

/// The allocated property id of a Record LWW field on a node (for locating its
/// catalog `_ankurah_property` entity), sourced from the catalog after the
/// record's create-time registration has landed.
fn record_property_id(node: &Node<SledStorageEngine, PermissiveAgent>, name: &str) -> proto::EntityId {
    node.catalog.resolve("record", name).expect("record property resolves in the catalog after registration")
}

// -- (a) create + commit emits 0xA2 state and a v2 diff; catalog/system stay v1

#[tokio::test]
async fn user_entity_state_is_0xa2_and_diff_is_v2_catalog_and_system_stay_v1() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create + commit a user entity; capture its commit events.
    let (rec_id, events) = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "alpha".to_owned(), artist: "bob".to_owned() }).await?;
        let id = rec.id();
        let events = trx.commit_and_return_events().await?;
        (id, events)
    };

    // The stored user-entity state buffer is 0xA2 (id-keyed v2).
    assert_eq!(lww_state_version(&node, "record", rec_id).await?, LWW_STATE_V2, "user entity state buffer must be 0xA2 after the flip");

    // Its commit event's LWW diff version byte is 2.
    let record_event = events.iter().find(|e| e.entity_id == rec_id).expect("a commit event for the record");
    assert_eq!(lww_diff_version(record_event), 2, "user entity commit diff must be LWW v2");

    // A catalog entity (the `title` property definition) stays 0xA1: the
    // registration triggered by create wrote it name-keyed (bootstrap exemption).
    let title_property_id = record_property_id(&node, "title");
    assert_eq!(
        lww_state_version(&node, "_ankurah_property", title_property_id).await?,
        LWW_STATE_V1,
        "catalog property entity must remain 0xA1 (name-keyed)"
    );

    // The system root stays 0xA1 as well.
    let root_id = node.system.root().unwrap().payload.entity_id;
    assert_eq!(lww_state_version(&node, "_ankurah_system", root_id).await?, LWW_STATE_V1, "system root must remain 0xA1 (name-keyed)");

    Ok(())
}

// -- (b) two-node create/edit/fetch round trip over v2 payloads --------------

#[tokio::test]
async fn two_node_v2_round_trip_over_local_process() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context_async(c).await;
    let ctx_c = client.context_async(c).await;

    // Create on the server.
    let rec_id = {
        let trx = ctx_s.begin();
        let rec = trx.create(&Record { title: "alpha".to_owned(), artist: "bob".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };

    // Readable on the client (v2 payload projected through the client's binding).
    let client_view = ctx_c.get::<RecordView>(rec_id).await?;
    assert_eq!(client_view.title().unwrap(), "alpha", "client reads the v2 title");
    assert_eq!(client_view.artist().unwrap(), "bob", "client reads the v2 artist");

    // Edit on the client; the resulting diff is v2 and converges on the server.
    {
        let trx = ctx_c.begin();
        let editable = client_view.edit(&trx)?;
        editable.title().set(&"beta".to_owned())?;
        trx.commit().await?;
    }

    // Server reflects the client's v2 edit.
    let server_view = ctx_s.get::<RecordView>(rec_id).await?;
    assert_eq!(server_view.title().unwrap(), "beta", "server converges on the client's v2 edit");
    assert_eq!(server_view.artist().unwrap(), "bob", "untouched field survives");

    // Both stored buffers are 0xA2.
    assert_eq!(lww_state_version(&server, "record", rec_id).await?, LWW_STATE_V2, "server stores 0xA2");
    assert_eq!(lww_state_version(&client, "record", rec_id).await?, LWW_STATE_V2, "client stores 0xA2");

    Ok(())
}

// -- (c) legacy 0xA1 state loads, reads, and lazily rewrites to 0xA2 on edit -

#[tokio::test]
async fn legacy_v1_state_reads_then_rewrites_to_0xa2_on_edit() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Register the record schema so the node's binding can resolve record
    // fields (create+commit any record; we only need the catalog warmed).
    {
        let trx = ctx.begin();
        trx.create(&Record { title: "seed".to_owned(), artist: "seed".to_owned() }).await?;
        trx.commit().await?;
    }

    // Build a LEGACY name-keyed (0xA1) state for a NEW record id, exactly as a
    // pre-flip node would have written it, and set it directly into storage.
    let legacy_id = proto::EntityId::new();
    let legacy_event_id = proto::EventId::from_bytes([5u8; 32]);
    let legacy_state = {
        let backend = LWWBackend::new(); // default NameKeyedV1 -> 0xA1
        backend.set("title".into(), Some(Value::String("legacy-title".to_owned())));
        backend.set("artist".into(), Some(Value::String("legacy-artist".to_owned())));
        let ops = backend.to_operations().unwrap().unwrap();
        backend.apply_operations_with_event(&ops, legacy_event_id.clone()).unwrap();
        let buffer = backend.to_state_buffer().unwrap();
        assert_eq!(buffer[0], LWW_STATE_V1, "seeded legacy buffer must be 0xA1");
        let mut state_buffers = BTreeMap::new();
        state_buffers.insert("lww".to_owned(), buffer);
        proto::State { state_buffers: proto::StateBuffers(state_buffers), head: proto::Clock::from(vec![legacy_event_id]) }
    };
    let storage = node.collections.get(&"record".into()).await?;
    let entity_state = proto::EntityState { entity_id: legacy_id, collection: "record".into(), state: legacy_state };
    storage.set_state(proto::Attested::opt(entity_state, None)).await?;

    // Read it back through the node: values readable despite the 0xA1 encoding.
    let view = ctx.get::<RecordView>(legacy_id).await?;
    assert_eq!(view.title().unwrap(), "legacy-title", "legacy 0xA1 title readable through the node");
    assert_eq!(view.artist().unwrap(), "legacy-artist", "legacy 0xA1 artist readable");

    // Edit + commit rewrites the stored buffer to 0xA2 (lazy rewrite-on-save).
    {
        let trx = ctx.begin();
        let editable = view.edit(&trx)?;
        editable.title().set(&"upgraded-title".to_owned())?;
        trx.commit().await?;
    }
    assert_eq!(lww_state_version(&node, "record", legacy_id).await?, LWW_STATE_V2, "edit rewrites the legacy buffer to 0xA2");

    // The rewritten entity still reads correctly (edited + untouched fields).
    let reread = ctx.get::<RecordView>(legacy_id).await?;
    assert_eq!(reread.title().unwrap(), "upgraded-title", "edited value survives the rewrite");
    assert_eq!(reread.artist().unwrap(), "legacy-artist", "untouched legacy field survives the rewrite");

    Ok(())
}

// -- (d) sled materialization of a v2 buffer via the hint projection ---------

#[tokio::test]
async fn sled_materialization_of_v2_entity_matches_field_predicate() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Create a user entity (stored as a 0xA2 buffer).
    let rec_id = {
        let trx = ctx.begin();
        let rec = trx.create(&Record { title: "findme".to_owned(), artist: "bob".to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    assert_eq!(lww_state_version(&node, "record", rec_id).await?, LWW_STATE_V2, "entity stored as 0xA2");

    // A predicate fetch on a field must return the entity: sled materializes
    // rows by parsing state buffers UNBOUND, so this proves the display-name
    // HINT in the 0xA2 buffer projects the id-keyed value for materialization.
    let results = ctx.fetch::<RecordView>("title = 'findme'").await?;
    let ids: Vec<_> = results.iter().map(|v| v.id()).collect();
    assert!(ids.contains(&rec_id), "predicate fetch on a v2-materialized field must return the entity");
    assert_eq!(results.len(), 1, "exactly the one matching entity");
    assert_eq!(results[0].artist().unwrap(), "bob", "materialized entity projects all fields");

    Ok(())
}

// -- (e) resolution wiring: fail closed on unknown; resolve untouched model --

#[tokio::test]
async fn resolution_fails_closed_on_unknown_property() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    // Register the record schema (create+commit one) so the model is known but
    // the queried property is not.
    {
        let trx = ctx.begin();
        trx.create(&Record { title: "alpha".to_owned(), artist: "bob".to_owned() }).await?;
        trx.commit().await?;
    }

    // A fetch referencing a property no record field defines fails closed with
    // UnknownProperty (RFC 5.3 AC5).
    let err = ctx.fetch::<RecordView>("nonexistent_field = 'x'").await.expect_err("unknown property must fail closed");
    let msg = format!("{err}");
    assert!(msg.contains("unknown property") && msg.contains("nonexistent_field"), "error must name the unknown property, got: {msg}");

    Ok(())
}

#[tokio::test]
async fn untouched_model_fetch_answers_empty() -> Result<()> {
    // A model that is READ before any write. Rev 4 (RFC 5.1) deleted the
    // cache_compiled id overlay: ids exist only in the catalog and its
    // registration responses. Under the REN 2 revision (plan decision 25b)
    // the read REGISTERS the compiled model at first use -- an idempotent
    // upsert -- and then answers EMPTY because the freshly registered
    // collection holds no entities.
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context_async(c).await;

    let results = ctx.fetch::<RecordView>("title = 'anything'").await?;
    assert!(results.is_empty(), "a just-registered collection holds no entities");
    assert!(node.catalog.resolve("record", "title").is_some(), "first-use registration fed the catalog");

    // (Typo'd references in a REGISTERED collection still fail closed, AC5;
    // pinned in registration_lifecycle.rs.)

    Ok(())
}
