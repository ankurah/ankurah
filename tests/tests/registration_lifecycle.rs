//! A11b: the client-side registration lifecycle
//! (specs/model-property-metadata/rfc.md section 5.2, rev 4).
//!
//! These tests drive registration through the ORDINARY client surface --
//! `trx.create`, `ctx.register::<M>()`, and predicate-query paths -- rather than
//! hand-built RegisterSchema requests (that lower layer is covered by
//! schema_registration.rs / catalog_map.rs). They assert the lifecycle
//! behaviors the RFC pins:
//!
//!   a. auto-assert: `trx.create` on an ephemeral registers durably, and
//!      the ids everywhere are the durable's allocations;
//!   b. strict offline: creating into a NEVER-registered collection with
//!      no durable peer fails at create ("connect once first"), while an
//!      exact schema whose every field is already bound compatibly keeps
//!      writing offline;
//!   c. explicit `register::<M>()` on a durable: catalog entries appear
//!      locally, and a second call is a no-op (heads unchanged);
//!   d. predicate queries register at first use (REN 2 revised, plan decision
//!      25b), while direct entity-id gets only cache and load;
//!   e. hard_reset flushes the map and the ensured latch (allocations
//!      belong to one system);
//!   f. fail-loud offline read (RFC 5.3 addendum): with no durable peer, a
//!      fetch over a never-registered collection surfaces the loud
//!      UnregisteredCollection error instead of answering empty.

mod common;
use ankurah::proto::PropertyId;
use common::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

// Distinct models per behavior so the collections never collide.
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

mod offline_v1 {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Evolving {
        pub label: String,
    }
}

mod offline_v2 {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Evolving {
        pub label: String,
        pub added: i64,
    }
}

mod wrong_explicit_widget {
    use ankurah::Model;
    use serde::{Deserialize, Serialize};

    #[derive(Model, Debug, Serialize, Deserialize)]
    #[model(id = "AAAAAAAAAAAAAAAAAAAAAA")]
    pub struct Widget {
        pub label: String,
        pub size: i32,
    }
}

mod warm_optional_v1 {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct OfflineWarm {
        pub value: Option<i64>,
    }
}

mod warm_optional_v2 {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct OfflineWarm {
        pub value: i64,
    }
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

// (a) Auto-assert: create on the ephemeral; the durable executes the
// registration (allocating the ids) and both sides converge on the same
// allocations. `create` awaits the RegisterSchema response internally, so
// the client map is seeded on ack; the durable's own map is updated
// synchronously by the executor.
#[tokio::test]
async fn auto_assert_create_registers_on_durable() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    let ctx = client.context_async(DEFAULT_CONTEXT).await;
    let trx = ctx.begin();
    trx.create(&Widget { label: "hello".into(), size: 42 }).await?;
    trx.commit().await?;

    // The durable resolves (collection, field) to its own allocations, with
    // the normative (backend, value_type) pairs recorded in the catalog.
    let label_id = wait_resolve(&server, "widget", "label").await.expect("durable resolves widget.label after auto-assert");
    let label = server.catalog.property_by_id(&label_id).expect("label def");
    assert_eq!((label.backend.as_str(), label.value_type.as_str()), ("yrs", "string"), "String field -> (yrs, string)");

    let size_id = wait_resolve(&server, "widget", "size").await.expect("durable resolves widget.size");
    let size = server.catalog.property_by_id(&size_id).expect("size def");
    assert_eq!((size.backend.as_str(), size.value_type.as_str()), ("lww", "i32"), "i32 field -> (lww, i32)");

    // The client's map was seeded from the SchemaRegistered response: the
    // SAME ids, no waiting on the catalog subscription.
    assert_eq!(
        resolve_by_collection(&client, "widget", "label"),
        Some(PropertyId::EntityId(label_id)),
        "response-fed client map agrees with the allocator"
    );
    assert_eq!(resolve_by_collection(&client, "widget", "size"), Some(PropertyId::EntityId(size_id)));

    // The model entity is indexed by its collection with the struct name.
    let model = server.catalog.model_by_label("widget").expect("model present on durable");
    assert_eq!(model.name, "Widget");

    Ok(())
}

// (b) Strict offline (rev 4, plan decisions 16/22): a create into a
// NEVER-registered collection with no durable peer fails at create with an
// actionable error; after reconnecting, the same create succeeds. A
// fully and compatibly bound schema keeps working offline: the reassertion is
// deferrable and only warns.
#[tokio::test]
async fn offline_create_unregistered_is_strict_registered_proceeds() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    server.catalog.wait_catalog_ready().await;

    // Build the context while connected (join_system needs a peer), and
    // register Widget while connected so it is a KNOWN collection later.
    let ctx = client.context_async(DEFAULT_CONTEXT).await;
    ctx.register::<Widget>().await?;
    assert!(client.catalog.model_by_label("widget").is_some(), "widget bound while connected");

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

    // A NEVER-registered collection cannot mint identity offline: strict
    // error at create ("connect once first").
    {
        let trx = ctx.begin();
        let err =
            trx.create(&Gadget { name: "offline".into() }).await.expect_err("offline create into an unregistered collection must fail");
        let msg = err.to_string();
        assert!(msg.contains("unregistered collection 'gadget'"), "actionable strict error, got: {msg}");
    }
    assert!(resolve_by_collection(&server, "gadget", "name").is_none(), "nothing reached the durable");
    assert!(!client.catalog.is_ensured("gadget"), "a strict failure must not latch");

    // An explicit model id is part of the exact binding. The ordinary Widget
    // model and its compatible fields must not satisfy a declaration bound to
    // a different, nonexistent model id.
    {
        let trx = ctx.begin();
        let err = trx
            .create(&wrong_explicit_widget::Widget { label: "wrong-model".into(), size: 2 })
            .await
            .expect_err("offline fallback must validate the compiled explicit model id");
        assert!(err.to_string().contains("unconfirmed schema"), "expected an exact-binding failure, got: {err}");
    }

    // The fully and compatibly bound Widget shape keeps writing offline (no
    // commit attempted: an ephemeral cannot relay a commit without a peer;
    // create alone exercises the registration trigger).
    {
        let trx = ctx.begin();
        let _w = trx.create(&Widget { label: "offline-ok".into(), size: 1 }).await?;
    }

    // RECONNECT: the same Gadget create now registers and succeeds.
    let _conn2 = LocalProcessConnection::new(&server, &client).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client did not reconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    {
        let trx = ctx.begin();
        let _g = trx.create(&Gadget { name: "online".into() }).await?;
    }
    let name_id = wait_resolve(&server, "gadget", "name").await.expect("durable allocates gadget.name after reconnect");
    assert_eq!(
        resolve_by_collection(&client, "gadget", "name"),
        Some(PropertyId::EntityId(name_id)),
        "client map seeded from the response"
    );

    Ok(())
}

#[tokio::test]
async fn offline_reassert_requires_every_compiled_field_to_be_bound() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    let ctx = client.context_async(DEFAULT_CONTEXT).await;
    ctx.register::<offline_v1::Evolving>().await?;

    drop(conn);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let trx = ctx.begin();
    let error = trx
        .create(&offline_v2::Evolving { label: "known".into(), added: 1 })
        .await
        .expect_err("an unavailable reassertion must not emit an unregistered field as Name residue");
    assert!(error.to_string().contains("unconfirmed schema"), "expected a schema confirmation failure, got: {error}");

    Ok(())
}

/// A catalog-proven no-peer binding is sufficient to address an offline
/// write, but it is not an allocator confirmation. Reconnecting must still
/// reassert the exact schema so mutable metadata such as membership
/// optionality converges.
#[tokio::test]
async fn offline_binding_reasserts_mutable_metadata_after_reconnect() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let server_ctx = server.context_async(DEFAULT_CONTEXT).await;
    server_ctx.register::<warm_optional_v1::OfflineWarm>().await?;

    let client = ephemeral_sled_setup().await?;
    let connection = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    let ctx = client.context_async(DEFAULT_CONTEXT).await;
    wait_resolve(&client, "offlinewarm", "value").await.expect("catalog warm copied the existing definition");
    assert!(!client.catalog.is_ensured("offlinewarm"), "catalog knowledge alone is not local registration confirmation");

    drop(connection);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let trx = ctx.begin();
    trx.create(&warm_optional_v2::OfflineWarm { value: 7 }).await?;
    assert!(!client.catalog.is_ensured("offlinewarm"), "a local no-peer proof must not latch allocator confirmation");

    let _reconnected = LocalProcessConnection::new(&server, &client).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client did not reconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    ctx.register::<warm_optional_v2::OfflineWarm>().await?;
    assert!(client.catalog.is_ensured("offlinewarm"), "successful reassertion confirms the exact declaration");
    let model = server.catalog.model_by_label("offlinewarm").unwrap().id;
    let property = match resolve_by_collection(&server, "offlinewarm", "value").unwrap() {
        PropertyId::EntityId(id) => id,
        other => panic!("registered property resolved to {other:?}"),
    };
    assert_eq!(server.catalog.membership(&model, &property).unwrap().optional, Some(false));

    Ok(())
}

// (c) Explicit register::<M>() on a durable node's context: catalog entries
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

// (d) Predicate-query paths register at first use (REN 2 revised, plan
// decision 25b): a compiled model's first query triggers the idempotent registration
// upsert, so resolution runs against authoritative rows instead of
// failing loud as unregistered. The fetch still answers EMPTY -- the
// freshly registered collection holds no entities -- and a second,
// explicit register is a no-op against the same rows.
#[tokio::test]
async fn predicate_read_path_registers_at_first_use() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // The compiled schema anticipates `doohickey`; the catalog does not
    // know it yet. The fetch registers it at first use and answers empty.
    let results = ctx.fetch::<DoohickeyView>("tag = 'x'").await?;
    assert!(results.is_empty(), "a just-registered collection holds no entities");

    // The predicate read durably registered and latched the collection.
    let tag_id = resolve_by_collection(&server, "doohickey", "tag");
    assert!(tag_id.is_some(), "first-use registration fed the catalog");
    assert!(server.catalog.is_ensured("doohickey"), "first-use registration latches");

    // The explicit register is an idempotent no-op against the same rows.
    ctx.register::<Doohickey>().await?;
    assert_eq!(resolve_by_collection(&server, "doohickey", "tag"), tag_id, "re-register must not re-mint");
    let results = ctx.fetch::<DoohickeyView>("tag = 'x'").await?;
    assert!(results.is_empty(), "no entities were created");

    // A typo'd property in a REGISTERED collection still fails closed (AC5).
    let err = ctx.fetch::<DoohickeyView>("tyop = 'x'").await.expect_err("unknown property in a registered collection fails closed");
    assert!(format!("{err:?}").to_lowercase().contains("unknown"), "fail-closed unknown-property error");

    Ok(())
}

// (e) hard_reset clears the map and the ensured latch: allocations belong
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

// (f) Fail-loud offline read (RFC 5.3 addendum, plan decision 25b second
// ruling): with no durable peer, a fetch over a NEVER-registered compiled
// collection cannot run first-use registration, and the reference surfaces
// the loud UnregisteredCollection error naming the collection -- never an
// empty resultset, and never the weaker unknown-property shape.
#[tokio::test]
async fn offline_read_unregistered_fails_loud() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    server.catalog.wait_catalog_ready().await;

    // Context built while connected (join needs a peer); the catalog warms
    // (empty) via the context kick. Contraption is never registered.
    let ctx = client.context_async(DEFAULT_CONTEXT).await;

    drop(conn);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let err =
        ctx.fetch::<ContraptionView>("state = 'x'").await.expect_err("offline fetch over a never-registered collection must fail loud");
    let msg = err.to_string();
    assert!(msg.contains("not registered") && msg.contains("contraption"), "loud error naming the collection, got: {msg}");
    assert!(!client.catalog.is_ensured("contraption"), "a failed first-use registration must not latch");

    Ok(())
}

/// The stored catalog head for an entity (head-comparison helper; mirrors
/// schema_registration.rs).
async fn catalog_head(node: &TestNode, collection: &str, id: EntityId) -> anyhow::Result<proto::Clock> {
    let _model =
        ankurah::core::schema::system_model_id(collection).ok_or_else(|| anyhow::anyhow!("{collection} is not a system catalog model"))?;
    Ok(node.storage.get_state(id).await?.payload.state.head)
}

// (f) A typed direct get is itself a schema-dependent use: it admits the exact
// schema before decoding the entity's identity-keyed fields. The resulting
// view can then be edited without a second registration round trip.
#[tokio::test]
async fn direct_get_registers_before_edit() -> anyhow::Result<()> {
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

    // Client B never creates: it gets and edits only.
    let client_b = ephemeral_sled_setup().await?;
    let _conn_b = LocalProcessConnection::new(&server, &client_b).await?;
    client_b.system.wait_system_ready().await;
    let ctx_b = client_b.context_async(DEFAULT_CONTEXT).await;

    let view = ctx_b.get::<ContraptionView>(id).await?;
    assert!(client_b.catalog.is_ensured("contraption"), "a typed direct id get must admit its exact schema before decoding");

    let trx = ctx_b.begin();
    view.edit(&trx)?.state().replace("polished")?;
    trx.commit().await?;

    assert!(client_b.catalog.is_ensured("contraption"), "the admitted binding remains available through the edit-only commit");
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
