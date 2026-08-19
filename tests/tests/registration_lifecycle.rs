//! A11b: the client-side registration lifecycle
//!, as scoped for
//! the write-only catalog phase.
//!
//! These tests drive registration through the ORDINARY client surface --
//! `ctx.register_model::<M>()` -- rather than hand-built RegisterSchema requests
//! (that lower layer is covered by schema_registration.rs / catalog_map.rs).
//! They pin the lifecycle behaviors:
//!
//!   a. auto-assert: `trx.create` on an ephemeral registers durably, and
//!      the ids everywhere are the durable's allocations;
//!   b. strict offline: creating into a NEVER-registered collection with
//!      no durable peer fails at create, while an exact schema whose every
//!      field is already bound compatibly keeps writing offline;
//!   c. explicit `register_model::<M>()` propagates every failure and is idempotent (heads
//!      unchanged on repeat), and fails offline leaving the descriptor
//!      unresolved;
//!   d. predicate queries and typed direct gets register at first use;
//!   e. hard_reset flushes the map and steps the epoch past every
//!      resolved cell (allocations belong to one system);
//!   f. fail-loud offline read: with no durable peer, a fetch over a
//!      never-registered collection errs loudly instead of answering empty;
//!   g. a custom Property type's declared VALUE_TYPE flows through the
//!      compiled schema into the allocated catalog definition.
//!
//! Excised with the read flip: the relay-warmed offline reassertion
//! scenario (a client's map learning another node's registrations needs the
//! catalog subscriptions) and predicate-resolution assertions (unknown
//! properties failing closed). Their tests return with that machinery.

mod common;
use ankurah::core::session::SessionSet;
use ankurah::proto::PropertyId;
use common::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

/// Whether this compiled shape's identity cells are resolved for the node's
/// current schema epoch -- the registered-right-now probe, read from the
/// descriptor itself.
fn schema_registered(node: &TestNode, schema: &'static ankurah::core::schema::ModelStructDescriptor) -> bool {
    node.system.schema_epoch().is_some_and(|epoch| schema.resolved.get(epoch).is_some())
}

// Distinct models per behavior so the collections never collide.
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Gadget {
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Gizmo {
    pub title: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Widget {
    pub label: String,
    pub size: i32,
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
    #[model(id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    pub struct Widget {
        pub label: String,
        pub size: i32,
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
    // A test names a catalog collection the way a person does, by its
    // reserved label; `system_model_id` is the door from that label to the
    // identity everything past it addresses.
    let model = ankurah::core::schema::system_model_id(collection).expect("a built-in collection label");
    Ok(node.collections.get(&model).await?.get_state(id).await?.payload.state.head)
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
    let (_, model) = server.catalog.model_by_label("widget").expect("model present on durable");
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
    ctx.register_model::<Widget>().await?;
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
        assert!(msg.contains("never been registered") && msg.contains("'gadget'"), "actionable strict error, got: {msg}");
    }
    assert!(resolve_by_collection(&server, "gadget", "name").is_none(), "nothing reached the durable");
    assert!(!schema_registered(&client, Gadget::descriptor()), "a strict failure must leave the descriptor unresolved");

    // An explicit model id is part of the exact binding. The ordinary Widget
    // model and its compatible fields must not satisfy a declaration bound to
    // a different, nonexistent model id.
    {
        let trx = ctx.begin();
        let err = trx
            .create(&wrong_explicit_widget::Widget { label: "wrong-model".into(), size: 2 })
            .await
            .expect_err("offline fallback must validate the compiled explicit model id");
        assert!(err.to_string().contains("unconfirmed"), "expected an exact-binding failure, got: {err}");
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
    ctx.register_model::<offline_v1::Evolving>().await?;

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
    assert!(error.to_string().contains("unconfirmed"), "expected a schema confirmation failure, got: {error}");

    Ok(())
}

// (d) Predicate-query paths HEAL. A compiled declaration the catalog cannot
// prove is a system that has not been told about this shape yet, not a
// mistake in the query, so the read registers the declaration and then
// answers -- here, EMPTY, because the collection it just defined holds no
// entities. A second, explicit register is a no-op against the same rows.
#[tokio::test]
async fn predicate_read_path_heals_and_defines() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // The compiled schema anticipates `doohickey`; the catalog does not know
    // it. The read defines it and answers.
    let results = ctx.fetch::<DoohickeyView>("tag = 'x'").await?;
    assert!(results.is_empty(), "a just-registered collection holds no entities");

    let tag_id = resolve_by_collection(&server, "doohickey", "tag");
    assert!(tag_id.is_some(), "the healing read fed the catalog");
    assert!(schema_registered(&server, Doohickey::descriptor()), "the healing read resolves the descriptor");

    // A second register is idempotent against the same rows.
    ctx.register_model::<Doohickey>().await?;
    assert_eq!(resolve_by_collection(&server, "doohickey", "tag"), tag_id, "re-register must not re-mint");

    // The fail-closed rejection of a typo'd property in a registered
    // collection is predicate-resolution behavior; its assertion returns
    // with the propertyid-resolution PR.

    Ok(())
}

// (d) The delta a healing read registers is INCREMENTAL. A binary compiled
// against a model that has since grown a field registers THAT FIELD, against
// the model already there: nothing is re-minted and no catalog row already
// written is rewritten. That is what makes healing something every read can
// afford to do, rather than something to be avoided.
#[tokio::test]
async fn healing_registers_only_the_added_field() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // The system knows the one-field shape.
    ctx.register_model::<offline_v1::Evolving>().await?;
    let (model, _) = server.catalog.model_by_label("evolving").expect("evolving model");
    let Some(PropertyId::EntityId(label)) = resolve_by_collection(&server, "evolving", "label") else {
        anyhow::bail!("evolving.label resolves after the first registration");
    };
    let (membership, _) = server.catalog.membership(&model, &label).expect("evolving.label membership");
    let heads_before = (
        catalog_head(&server, "_ankurah_model", model).await?,
        catalog_head(&server, "_ankurah_property", label).await?,
        catalog_head(&server, "_ankurah_model_property", membership).await?,
    );

    // A binary compiled against the two-field shape reads; healing registers
    // the difference and the read answers.
    let results = ctx.fetch::<offline_v2::EvolvingView>("added = 1").await?;
    assert!(results.is_empty(), "nothing was ever created in this collection");

    assert_eq!(server.catalog.model_by_label("evolving").expect("evolving model").0, model, "the model must not be re-minted");
    assert_eq!(
        resolve_by_collection(&server, "evolving", "label"),
        Some(PropertyId::EntityId(label)),
        "the known field keeps its identity"
    );
    let heads_after = (
        catalog_head(&server, "_ankurah_model", model).await?,
        catalog_head(&server, "_ankurah_property", label).await?,
        catalog_head(&server, "_ankurah_model_property", membership).await?,
    );
    assert_eq!(heads_before, heads_after, "registering the delta must not rewrite what was already registered");

    let Some(PropertyId::EntityId(added)) = resolve_by_collection(&server, "evolving", "added") else {
        anyhow::bail!("the healing read must register the field this binary added");
    };
    assert!(server.catalog.membership(&model, &added).is_some(), "the added field joins the model already there");
    assert!(schema_registered(&server, offline_v2::Evolving::descriptor()), "the two-field declaration binds after healing");

    Ok(())
}

// (d) Where healing stops: registering is a write, so a credential that
// cannot write cannot heal. The read then says BOTH things -- the model is
// not registered, and this credential may not register it -- rather than
// reporting a declared model as unknown or defining schema on behalf of a
// principal that may not define any.
#[tokio::test]
async fn read_only_credential_cannot_heal() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    // A session-less source reads, but can never name the single principal a
    // registration acts as.
    let ctx = server.context(SessionSet::new())?;

    let error = ctx.fetch::<GadgetView>("name = 'x'").await.expect_err("a read that cannot heal must not answer");
    let msg = error.to_string();
    assert!(msg.contains("gadget") && msg.contains("may not register"), "the error must name the model and the refusal, got: {msg}");
    assert!(server.catalog.model_by_label("gadget").is_none(), "a refused read must define nothing");
    assert!(!schema_registered(&server, Gadget::descriptor()), "a refused read must resolve nothing");

    Ok(())
}

// (f) Fail-loud read over a never-registered collection with nowhere to heal
// from: the reference surfaces a loud error naming the collection -- never an
// empty resultset, and never the weaker unknown-property shape. Only the
// durable allocator mints identity, so with no peer connected the read can
// neither prove the model nor register it.
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
    assert!(msg.contains("never been registered") && msg.contains("contraption"), "loud error naming the collection, got: {msg}");
    assert!(!schema_registered(&client, Contraption::descriptor()), "a failed first-use registration must leave the descriptor unresolved");

    Ok(())
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
    assert!(schema_registered(&client_b, Contraption::descriptor()), "a typed direct id get must resolve its exact schema before decoding");

    let trx = ctx_b.begin();
    view.edit(&trx)?.state()?.replace("polished")?;
    trx.commit().await?;

    assert!(schema_registered(&client_b, Contraption::descriptor()), "the resolved binding remains available through the edit-only commit");
    Ok(())
}

// (a) Explicit register_model::<M>() on a durable node's context: catalog entries
// exist locally afterwards, and a second call is a no-op (catalog heads
// unchanged, using the same head-comparison pattern as
// schema_registration.rs).
#[tokio::test]
async fn explicit_register_is_strict_and_idempotent() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // Strict register: propagates errors (here, succeeds).
    ctx.register_model::<Gizmo>().await?;

    // Catalog entries exist locally after the explicit register; the ids
    // are this durable's allocations.
    let title_id = wait_resolve(&server, "gizmo", "title").await.expect("gizmo.title resolves after register");
    let (model_id, _) = server.catalog.model_by_label("gizmo").expect("gizmo model");
    let (membership, _) = server.catalog.membership(&model_id, &title_id).expect("gizmo.title membership");

    let head_before = catalog_head(&server, "_ankurah_property", title_id).await?;
    let ms_head_before = catalog_head(&server, "_ankurah_model_property", membership).await?;

    // Second call: the collection is latched as ensured, so it is a pure
    // no-op -- no new events, catalog heads unchanged.
    ctx.register_model::<Gizmo>().await?;

    let head_after = catalog_head(&server, "_ankurah_property", title_id).await?;
    let ms_head_after = catalog_head(&server, "_ankurah_model_property", membership).await?;
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

    let err = ctx.register_model::<Gadget>().await.expect_err("offline register of an unregistered collection must fail");
    assert!(err.to_string().contains("gadget"), "actionable strict error naming the collection, got: {err}");
    assert!(resolve_by_collection(&server, "gadget", "name").is_none(), "nothing reached the durable");
    assert!(!schema_registered(&client, Gadget::descriptor()), "a strict failure must leave the descriptor unresolved");

    // RECONNECT: the same register now forwards, allocates, and latches.
    let _conn2 = LocalProcessConnection::new(&server, &client).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client did not reconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    ctx.register_model::<Gadget>().await?;
    assert!(schema_registered(&client, Gadget::descriptor()), "the forwarded registration resolves on ack");
    let name_id = wait_resolve(&server, "gadget", "name").await.expect("durable allocates gadget.name after reconnect");
    assert_eq!(
        resolve_by_collection(&client, "gadget", "name"),
        Some(PropertyId::EntityId(name_id)),
        "client map seeded from the response"
    );

    Ok(())
}

// (c) hard_reset clears the map and the ensured latch: allocations belong
// to one system and must not survive into another.
#[tokio::test]
async fn hard_reset_clears_ensured_and_map() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    ctx.register_model::<Gizmo>().await?;
    wait_resolve(&server, "gizmo", "title").await.expect("gizmo resolves before reset");
    assert!(schema_registered(&server, Gizmo::descriptor()));

    server.system.hard_reset().await?;

    assert!(resolve_by_collection(&server, "gizmo", "title").is_none(), "map flushed after reset");
    assert!(!schema_registered(&server, Gizmo::descriptor()), "descriptor unreadable after reset (the epoch moved on)");
    assert_eq!(server.catalog.counts(), (0, 0, 0), "catalog map empty after reset");

    Ok(())
}

// A custom Property type DECLARES its own
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
    let schema = Review::descriptor();
    let field = schema.field_by_name("rating").expect("rating field in schema");
    assert_eq!(field.value_type, "i64", "hand impl declares its real wire type");
    assert_eq!(field.backend, "lww");

    // And it flows through registration: the catalog records "i64" as part
    // of the allocated definition.
    let node = durable_sled_setup().await?;
    let ctx = node.context_async(DEFAULT_CONTEXT).await;
    ctx.register_model::<Review>().await?;

    let rating_id = wait_resolve(&node, "review", "rating").await.expect("review.rating resolves after register");
    let def = node.catalog.property_by_id(&rating_id).expect("catalog property def");
    assert_eq!(def.value_type, "i64", "the catalog stores the declared value_type as the canonical type");
    assert_eq!(def.backend, "lww");
    Ok(())
}
