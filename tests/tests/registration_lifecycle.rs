//! A11b: the client-side registration lifecycle
//! (specs/model-property-metadata/rfc.md section 5.2).
//!
//! These tests drive registration through the ORDINARY client surface --
//! `trx.create`, `ctx.register::<M>()`, and the read paths -- rather than
//! hand-built RegisterSchema requests (that lower layer is covered by
//! schema_registration.rs / catalog_map.rs). They assert the five lifecycle
//! behaviors the RFC pins:
//!
//!   a. auto-assert: `trx.create` on an ephemeral registers durably;
//!   b. offline queue: a create with no durable peer queues, and a later
//!      reconnect drains it to the durable;
//!   c. explicit `register::<M>()` on a durable: catalog entries appear
//!      locally, and a second call is a no-op (heads unchanged);
//!   d. read-path cache: `fetch` overlays the compiled schema locally
//!      WITHOUT durably registering (durable catalog stays empty);
//!   e. hard_reset flushes the ensured latch and the compiled overlay.

mod common;
use common::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

// Distinct models per behavior so the derived collections never collide.
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Widget {
    pub label: String,
    pub size: i32,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Gadget {
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Gizmo {
    pub title: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Doohickey {
    pub tag: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Contraption {
    pub state: String,
}

async fn connected_pair(
) -> anyhow::Result<(TestNode, TestNode, LocalProcessConnection<SledStorageEngine, PermissiveAgent, SledStorageEngine, PermissiveAgent>)> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    Ok((server, client, conn))
}

/// Catalog map updates arrive asynchronously on the durable side (reactor
/// notify after the registration commit); poll until `resolve` answers or
/// time out.
async fn wait_resolve(node: &TestNode, collection: &str, name: &str) -> Option<EntityId> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(id) = node.catalog.resolve(collection, name) {
            return Some(id);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The property id the durable node WOULD derive for (collection, anchor,
/// backend, value_type) under its own system root.
fn derived_property_id(server: &TestNode, collection: &str, anchor: &str, backend: &str, value_type: &str) -> EntityId {
    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = proto::schema_id::model_entity_id(&root, collection);
    proto::schema_id::property_entity_id(&root, &model_id, anchor, backend, value_type)
}

// (a) Auto-assert: create a derived model on the ephemeral; the durable's
// catalog resolves the derived property id. `create` awaits the RegisterSchema
// request internally, so the durable has executed registration by the time
// commit returns; the durable's map catches up via its reactor subscription
// (hence the poll).
#[tokio::test]
async fn auto_assert_create_registers_on_durable() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    let ctx = client.context_async(DEFAULT_CONTEXT).await;
    let trx = ctx.begin();
    trx.create(&Widget { label: "hello".into(), size: 42 }).await?;
    trx.commit().await?;

    // The durable resolves (collection, field) to the derived property id.
    let label_id = wait_resolve(&server, "widget", "label").await.expect("durable resolves widget.label after auto-assert");
    assert_eq!(label_id, derived_property_id(&server, "widget", "label", "yrs", "string"), "String field -> (yrs, string)");

    let size_id = wait_resolve(&server, "widget", "size").await.expect("durable resolves widget.size");
    assert_eq!(size_id, derived_property_id(&server, "widget", "size", "lww", "i32"), "i32 field -> (lww, i32)");

    // The model entity is indexed by its collection with the struct name.
    let model = server.catalog.model_by_collection("widget").expect("model present on durable");
    assert_eq!(model.name, "Widget");

    Ok(())
}

// (b) Offline queue: connect, go ready, DISCONNECT (drop the connection so
// the ephemeral has no durable peer), then `trx.create` -- which QUEUES the
// registration (no commit needed; create is enough to trigger ensure). Then
// reconnect with a NEW connection; `register_peer` drains the queue to the
// durable, whose catalog then gains the entries.
//
// Shaping note (reported): a commit is NOT attempted while disconnected. An
// ephemeral with no durable peer cannot relay a commit (relay_to_required_peers
// has no peer), so committing offline would be a separate concern; the RFC's
// registration trigger fires on `create` itself, so `create` (without commit)
// exercises the queue-and-drain path cleanly. The context was created BEFORE
// disconnecting (an ephemeral needs a peer to become system-ready).
#[tokio::test]
async fn offline_create_queues_then_drains_on_reconnect() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    server.catalog.wait_catalog_ready().await;

    // Build the context while connected (join_system needs a peer).
    let ctx = client.context_async(DEFAULT_CONTEXT).await;

    // DISCONNECT: dropping the connection deregisters the peer on both sides,
    // so the ephemeral now has no durable peer.
    drop(conn);
    // Let the deregistration propagate.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // create (no commit) -- triggers ensure_registered, which queues because
    // there is no durable peer. Best-effort, so this does not error.
    {
        let trx = ctx.begin();
        let _w = trx.create(&Gadget { name: "offline".into() }).await?;
        // Drop the trx without committing; the registration is already queued.
    }

    // The durable does NOT have the schema yet (it was never reachable).
    assert!(server.catalog.resolve("gadget", "name").is_none(), "durable has nothing before reconnect");

    // RECONNECT with a fresh connection; register_peer drains the queue.
    let _conn2 = LocalProcessConnection::new(&server, &client).await?;
    // (client re-joins the same system; wait for the peer to be back.)
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client did not reconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // After the drain, the durable's catalog gains the queued registration.
    let name_id = wait_resolve(&server, "gadget", "name").await.expect("durable gains gadget.name after drain");
    assert_eq!(name_id, derived_property_id(&server, "gadget", "name", "yrs", "string"));

    Ok(())
}

// (c) Explicit register::<M>() on a durable node's context: catalog entries
// exist locally afterwards, and a second call is a no-op (catalog heads
// unchanged, using the same head-comparison pattern as schema_registration.rs).
#[tokio::test]
async fn explicit_register_is_strict_and_idempotent() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // Strict register: propagates errors (here, succeeds).
    ctx.register::<Gizmo>().await?;

    // Catalog entries exist locally after the explicit register.
    let title_id = wait_resolve(&server, "gizmo", "title").await.expect("gizmo.title resolves after register");
    assert_eq!(title_id, derived_property_id(&server, "gizmo", "title", "yrs", "string"));

    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = proto::schema_id::model_entity_id(&root, "gizmo");
    let head_before = catalog_head(&server, "_ankurah_property", title_id).await?;
    let membership_id = proto::schema_id::membership_entity_id(&root, &model_id, &title_id);
    let ms_head_before = catalog_head(&server, "_ankurah_model_property", membership_id).await?;

    // Second call: the collection is latched as ensured, so it is a pure
    // no-op -- no new events, catalog heads unchanged.
    ctx.register::<Gizmo>().await?;

    let head_after = catalog_head(&server, "_ankurah_property", title_id).await?;
    let ms_head_after = catalog_head(&server, "_ankurah_model_property", membership_id).await?;
    assert_eq!(head_before, head_after, "second register must not mint new property events");
    assert_eq!(ms_head_before, ms_head_after, "second register must not mint new membership events");

    Ok(())
}

// (d) Read-path cache: a fresh durable node with NO registration; a fetch
// overlays the compiled schema so `resolve` answers, while the durable
// CATALOG COLLECTIONS remain empty (storage get_state for the derived property
// id errors NotFound). Read paths must not durably register.
#[tokio::test]
async fn read_path_caches_without_registering() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // A fetch (with a predicate referencing a field) overlays the compiled
    // schema. No entities exist, so the result is empty; the point is the
    // side-effect on the catalog overlay.
    let results = ctx.fetch::<DoohickeyView>("tag = 'x'").await?;
    assert!(results.is_empty(), "no entities were created");

    // The compiled overlay makes resolve() answer for the derived property...
    let tag_id = server.catalog.resolve("doohickey", "tag").expect("compiled overlay resolves doohickey.tag");
    assert_eq!(tag_id, derived_property_id(&server, "doohickey", "tag", "yrs", "string"));

    // ...but NOTHING was durably registered: the property entity is absent
    // from storage (get_state errors NotFound), proving the read path did not
    // write the catalog.
    let storage = server.collections.get(&"_ankurah_property".into()).await?;
    match storage.get_state(tag_id).await {
        Err(ankurah::error::RetrievalError::EntityNotFound(_)) => {}
        Ok(_) => panic!("read path must NOT durably register the property entity"),
        Err(e) => panic!("expected EntityNotFound, got {e:?}"),
    }

    Ok(())
}

// (e) hard_reset clears the ensured latch and the compiled overlay: after
// reset, resolve is None again for the overlay entries.
#[tokio::test]
async fn hard_reset_clears_ensured_and_overlay() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // Populate the overlay via a read path, and the durable catalog + latch
    // via an explicit register.
    let _ = ctx.fetch::<WidgetView>("label = 'x'").await?;
    ctx.register::<Gizmo>().await?;
    wait_resolve(&server, "gizmo", "title").await.expect("gizmo resolves before reset");
    assert!(server.catalog.resolve("widget", "label").is_some(), "overlay populated before reset");

    // hard_reset flushes the map, the ensured latch, and the pending queue
    // (RFC 5.2: root-scoped derivations must not survive).
    server.system.hard_reset().await?;

    assert!(server.catalog.resolve("gizmo", "title").is_none(), "durable overlay gone after reset");
    assert!(server.catalog.resolve("widget", "label").is_none(), "compiled overlay gone after reset");
    assert_eq!(server.catalog.counts(), (0, 0, 0), "catalog map empty after reset");

    Ok(())
}

/// The stored catalog head for an entity (head-comparison helper; mirrors
/// schema_registration.rs).
async fn catalog_head(node: &TestNode, collection: &str, id: EntityId) -> anyhow::Result<proto::Clock> {
    let storage = node.collections.get(&collection.into()).await?;
    Ok(storage.get_state(id).await?.payload.state.head)
}

// (f) The edit-only gap: `Transaction::edit` is sync (cache-only), so the
// COMMIT closes the gap by ensure-registering any touched collection whose
// compiled schema is known but not ensured (RFC 5.2 "durable write on first
// mutating use"). A node that only ever fetch-edits a model still latches
// its registration at its first commit.
#[tokio::test]
async fn edit_only_commit_registers() -> anyhow::Result<()> {
    let (server, client_a, _conn_a) = connected_pair().await?;

    // Client A creates the entity (and auto-registers the model durably).
    let ctx_a = client_a.context_async(DEFAULT_CONTEXT).await;
    let id = {
        let trx = ctx_a.begin();
        let c = trx.create(&Contraption { state: "raw".into() }).await?;
        let id = c.id();
        trx.commit().await?;
        id
    };

    // Client B never creates: it fetch-edits only.
    let client_b = ephemeral_sled_setup().await?;
    let _conn_b = LocalProcessConnection::new(&server, &client_b).await?;
    client_b.system.wait_system_ready().await;
    let ctx_b = client_b.context_async(DEFAULT_CONTEXT).await;

    let view = ctx_b.get::<ContraptionView>(id).await?;
    assert!(!client_b.catalog.is_ensured("contraption"), "read paths must not latch registration");

    let trx = ctx_b.begin();
    view.edit(&trx)?.state().replace("polished")?;
    trx.commit().await?;

    assert!(client_b.catalog.is_ensured("contraption"), "the edit-only commit must ensure registration");
    Ok(())
}

// RFC 4 erratum 2 resolution: a custom Property type DECLARES its own
// normative value_type through the trait's associated const, and the derive
// carries it into the compiled schema, the registration request, the
// catalog, and the property-id derivation. `Stars` is a HAND-WRITTEN impl
// producing `Value::I64`, so it declares "i64" (the derive(Property) macro
// pins "string" for its JSON-string serialization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stars(i64);

impl ankurah::Property for Stars {
    const VALUE_TYPE: &'static str = "i64";
    fn into_value(&self) -> Result<Option<ankurah::value::Value>, ankurah::property::PropertyError> {
        Ok(Some(ankurah::value::Value::I64(self.0)))
    }
    fn from_value(value: Option<ankurah::value::Value>) -> Result<Self, ankurah::property::PropertyError> {
        match value {
            Some(ankurah::value::Value::I64(v)) => Ok(Stars(v)),
            Some(other) => Err(ankurah::property::PropertyError::InvalidVariant { given: other, ty: "Stars".to_owned() }),
            None => Err(ankurah::property::PropertyError::Missing),
        }
    }
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Review {
    pub rating: Stars,
}

#[tokio::test]
async fn custom_property_type_declares_its_value_type() -> anyhow::Result<()> {
    // Compile-time: the schema static carries the trait-declared value_type.
    let schema = Review::schema();
    let field = schema.field_by_name("rating").expect("rating field in schema");
    assert_eq!(field.value_type, "i64", "hand impl declares its real wire type");
    assert_eq!(field.backend, "lww");

    // And it flows through registration: the catalog records "i64" and the
    // property id derives from it.
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    ctx.register::<Review>().await?;

    let root = node.system.root().unwrap().payload.entity_id;
    let model_id = proto::schema_id::model_entity_id(&root, "review");
    let expected = proto::schema_id::property_entity_id(&root, &model_id, "rating", "lww", "i64");
    assert_eq!(wait_resolve(&node, "review", "rating").await, Some(expected), "derivation must use the declared value_type");
    let def = node.catalog.property_by_id(&expected).expect("catalog property def");
    assert_eq!(def.value_type, "i64");
    Ok(())
}
