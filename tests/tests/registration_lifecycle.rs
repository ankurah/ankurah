//! A11b: the client-side registration lifecycle
//! (specs/model-property-metadata/rfc.md section 5.2, rev 4), as scoped for
//! the write-only catalog phase.
//!
//! These tests drive registration through the ORDINARY client surface --
//! `ctx.register::<M>()` -- rather than hand-built RegisterSchema requests
//! (that lower layer is covered by schema_registration.rs / catalog_map.rs).
//! They pin the lifecycle behaviors wired in this phase:
//!
//!   a. explicit `register::<M>()` on a durable: catalog entries appear
//!      locally, and a second call is a no-op (heads unchanged);
//!   b. strict offline: an explicit register with no durable peer fails
//!      without latching (only the durable allocator may mint ids);
//!   c. hard_reset flushes the map and the ensured latch (allocations
//!      belong to one system);
//!   d. a custom Property type's declared VALUE_TYPE flows through the
//!      compiled schema into the allocated catalog definition.
//!
//! Excised with the read flip (write-only phase): first-use registration
//! triggers (`trx.create`, predicate fetch, typed direct get) and the
//! no-peer fallback that proceeds from a fully bound compatible schema --
//! their entry point (`ensure_schema_for_use`) rides uncalled -- plus the
//! relay-warmed offline scenarios and the fail-loud unregistered read.
//! Their tests return with that wiring (successor notes in
//! core/src/schema/catalog.rs).

mod common;
use ankurah::proto::PropertyId;
use common::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

// Distinct models per behavior so the collections never collide.
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Gadget {
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Gizmo {
    pub title: String,
}

async fn connected_pair(
) -> anyhow::Result<(TestNode, TestNode, LocalProcessConnection<SledStorageEngine, PermissiveAgent, SledStorageEngine, PermissiveAgent>)> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    Ok((server, client, conn))
}

/// The durable fold runs synchronously under the allocator mutex, but the
/// forwarded-response fold lands in a separate task; poll until `resolve`
/// answers or time out.
async fn wait_resolve(node: &TestNode, collection: &str, name: &str) -> Option<EntityId> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(PropertyId::EntityId(id)) = resolve_by_collection(node, collection, name) {
            return Some(id);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn resolve_by_collection(node: &TestNode, collection: &str, name: &str) -> Option<PropertyId> {
    let model = node.catalog.model_id_for(collection)?;
    node.catalog.resolve(&model, name)
}

/// The stored catalog head for an entity (head-comparison helper; mirrors
/// schema_registration.rs).
async fn catalog_head(node: &TestNode, collection: &str, id: EntityId) -> anyhow::Result<proto::Clock> {
    Ok(node.collections.get(&proto::CollectionId::fixed_name(collection)).await?.get_state(id).await?.payload.state.head)
}

// (a) Explicit register::<M>() on a durable node's context: catalog entries
// exist locally afterwards, and a second call is a no-op (catalog heads
// unchanged, using the same head-comparison pattern as
// schema_registration.rs).
#[tokio::test]
async fn explicit_register_is_strict_and_idempotent() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // Strict register: propagates errors (here, succeeds).
    ctx.register::<Gizmo>().await?;

    // Catalog entries exist locally after the explicit register; the ids
    // are this durable's allocations.
    let title_id = wait_resolve(&server, "gizmo", "title").await.expect("gizmo.title resolves after register");
    let model_id = server.catalog.model_by_label("gizmo").expect("gizmo model").id;
    let membership = server.catalog.membership(&model_id, &title_id).expect("gizmo.title membership");

    let head_before = catalog_head(&server, "_ankurah_property", title_id).await?;
    let ms_head_before = catalog_head(&server, "_ankurah_model_property", membership.id).await?;

    // Second call: the collection is latched as ensured, so it is a pure
    // no-op -- no new events, catalog heads unchanged.
    ctx.register::<Gizmo>().await?;

    let head_after = catalog_head(&server, "_ankurah_property", title_id).await?;
    let ms_head_after = catalog_head(&server, "_ankurah_model_property", membership.id).await?;
    assert_eq!(head_before, head_after, "second register must not mint new property events");
    assert_eq!(ms_head_before, ms_head_after, "second register must not mint new membership events");

    Ok(())
}

// (b) Strict offline: only the durable allocator may mint ids, so an explicit
// register with no durable peer fails with an actionable error and must not
// latch. Reconnecting makes the same register succeed.
#[tokio::test]
async fn offline_register_is_strict_reconnect_proceeds() -> anyhow::Result<()> {
    let (server, client, conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    // Build the context while connected (join_system needs a peer).
    let ctx = client.context_async(DEFAULT_CONTEXT).await;

    // DISCONNECT: dropping the connection deregisters the peer on both
    // sides, so the ephemeral now has no durable peer.
    drop(conn);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let err = ctx.register::<Gadget>().await.expect_err("offline register of an unregistered collection must fail");
    assert!(err.to_string().contains("gadget"), "actionable strict error naming the collection, got: {err}");
    assert!(resolve_by_collection(&server, "gadget", "name").is_none(), "nothing reached the durable");
    assert!(!client.catalog.is_ensured("gadget"), "a strict failure must not latch");

    // RECONNECT: the same register now forwards, allocates, and latches.
    let _conn2 = LocalProcessConnection::new(&server, &client).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client did not reconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    ctx.register::<Gadget>().await?;
    assert!(client.catalog.is_ensured("gadget"), "the forwarded registration latches on ack");
    let name_id = wait_resolve(&server, "gadget", "name").await.expect("durable allocates gadget.name after reconnect");
    assert_eq!(
        resolve_by_collection(&client, "gadget", "name"),
        Some(PropertyId::EntityId(name_id)),
        "client map seeded from the response"
    );

    Ok(())
}

// (c) hard_reset clears the map and the ensured latch: allocations belong
// to one system and must not survive into another (RFC 5.2).
#[tokio::test]
async fn hard_reset_clears_ensured_and_map() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    ctx.register::<Gizmo>().await?;
    wait_resolve(&server, "gizmo", "title").await.expect("gizmo resolves before reset");
    assert!(server.catalog.is_ensured("gizmo"));

    server.system.hard_reset().await?;

    assert!(resolve_by_collection(&server, "gizmo", "title").is_none(), "map flushed after reset");
    assert!(!server.catalog.is_ensured("gizmo"), "ensured latch flushed after reset");
    assert_eq!(server.catalog.counts(), (0, 0, 0), "catalog map empty after reset");

    Ok(())
}

// RFC 4 erratum 2 resolution: a custom Property type DECLARES its own
// normative value_type through the trait's associated const, and the derive
// carries it into the compiled schema, the registration request, the
// catalog, and the canonical compatibility check on a lookup hit. `Stars` is
// a HAND-WRITTEN impl producing
// `Value::I64`, so it declares "i64" (the derive(Property) macro pins
// "string" for its JSON-string serialization).
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

    // And it flows through registration: the catalog records "i64" as part
    // of the allocated definition.
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    ctx.register::<Review>().await?;

    let rating_id = wait_resolve(&node, "review", "rating").await.expect("review.rating resolves after register");
    let def = node.catalog.property_by_id(&rating_id).expect("catalog property def");
    assert_eq!(def.value_type, "i64", "the catalog stores the declared value_type as the canonical type");
    assert_eq!(def.backend, "lww");
    Ok(())
}
