//! Registration through the typed client API.

mod common;
use ankurah::core::{
    connector::PeerConnectionError,
    error::{NodeHaltReason, RetrievalError},
    schema::registration::RegistrationError,
    session::{Session, SessionSet},
};
use ankurah::proto::PropertyId;
use common::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

/// Whether this compiled shape's identity cells are resolved for the node's
/// current system epoch -- the registered-right-now probe, read from the
/// descriptor itself.
fn schema_registered(node: &TestNode, schema: &'static ankurah::core::schema::ModelStructDescriptor) -> bool {
    node.system.system_epoch().is_some_and(|epoch| schema.resolved.get(epoch).is_some())
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
    client.system.wait_system_ready().await?;
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
    let model = node.catalog.model_id_for(collection).unwrap()?;
    node.catalog.property_id(&model, name).unwrap()
}

/// The stored catalog head for an entity (head-comparison helper; mirrors
/// schema_registration.rs).
async fn catalog_head(node: &TestNode, collection: &str, id: EntityId) -> anyhow::Result<proto::Clock> {
    Ok(node.collections.get(&proto::CollectionId::fixed_name(collection)).await?.get_state(id).await?.payload.state.head)
}

// (a) Auto-assert: create on the ephemeral; the durable executes the
// registration (allocating the ids) and both sides converge on the same
// allocations. `create` awaits the RegisterSchema response internally --
// the ack binds the client's compiled cells -- and each side's raw catalog
// resolution follows its own projection to the same ids.
#[tokio::test]
async fn auto_assert_create_registers_on_durable() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.wait_ready().await?;

    let ctx = client.context_async(DEFAULT_CONTEXT).await.unwrap();
    let trx = ctx.begin();
    trx.create(&Widget { label: "hello".into(), size: 42 }).await?;
    trx.commit().await?;

    // The durable resolves (collection, field) to its own allocations, with
    // the normative (backend, value_type) pairs recorded in the catalog.
    let label_id = wait_resolve(&server, "widget", "label").await.expect("durable resolves widget.label after auto-assert");
    let label = server.catalog.property_by_id(&label_id).unwrap().expect("label def");
    assert_eq!((label.backend.as_str(), label.value_type.as_str()), ("yrs", "string"), "String field -> (yrs, string)");

    let size_id = wait_resolve(&server, "widget", "size").await.expect("durable resolves widget.size");
    let size = server.catalog.property_by_id(&size_id).unwrap().expect("size def");
    assert_eq!((size.backend.as_str(), size.value_type.as_str()), ("lww", "i32"), "i32 field -> (lww, i32)");

    // The client catalog converges separately from descriptor binding on ack.
    assert_eq!(wait_resolve(&client, "widget", "label").await, Some(label_id), "the client's projection converges on the allocator's ids");
    assert_eq!(wait_resolve(&client, "widget", "size").await, Some(size_id));

    // The model entity is indexed by its collection with the struct name.
    let (_, model) = server.catalog.model_by_label("widget").unwrap().expect("model present on durable");
    assert_eq!(model.name, "Widget");

    Ok(())
}

#[tokio::test]
async fn query_rejects_unknown_names_before_registration() -> anyhow::Result<()> {
    use ankql::ast::{ComparisonOperator, Expr, InfixOperator, Parsed, PathExpr, Predicate, Selection};

    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let ctx = Context::new(client.clone(), DEFAULT_CONTEXT);
    assert!(client.system.system_epoch().is_none());

    for text in [
        "bogus = 'x'",
        "label = 'x' AND bogus = 1",
        "NOT (size = 1 OR bogus = 2)",
        "bogus IS NULL",
        "size IN (1, bogus)",
        "true ORDER BY bogus",
        "widget.bogus = 'x'",
    ] {
        let selection = ankql::parser::parse_selection(text)?;
        let error = ctx.query::<WidgetView>(selection).err().expect("unknown fields must fail synchronously");
        assert!(error.to_string().contains("unknown property 'bogus'"), "{text}: {error}");
    }

    let unknown = Expr::<Parsed>::Path(PathExpr::simple("bogus"));
    for expr in [
        Expr::InfixExpr {
            left: Box::new(Expr::Path(PathExpr::simple("size"))),
            operator: InfixOperator::Add,
            right: Box::new(unknown.clone()),
        },
        Expr::Predicate(Predicate::IsNull(Box::new(unknown))),
    ] {
        let selection = Selection::from(Predicate::Comparison {
            left: Box::new(expr),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(1.into())),
        });
        let error = ctx.query::<WidgetView>(selection).err().expect("nested expressions must validate names");
        assert!(error.to_string().contains("unknown property 'bogus'"), "{error}");
    }

    let query = ctx.query::<WidgetView>("widget.label = 'x' ORDER BY id")?;
    assert!(query.selection().peek().is_none());
    let error = query.update_selection("label = 'x' AND bogus = 1").expect_err("updates must also reject unknown fields");
    assert!(error.to_string().contains("unknown property 'bogus'"), "{error}");
    assert!(query.error().peek().is_none(), "a rejected update must leave the current query intact");
    assert!(server.catalog.model_by_label("widget").unwrap().is_none());

    let _conn = LocalProcessConnection::new(&server, &client).await?;
    tokio::time::timeout(Duration::from_secs(5), query.wait_initialized()).await??;
    assert!(server.catalog.model_by_label("widget").unwrap().is_some());
    assert!(schema_registered(&client, Widget::descriptor()));
    assert!(query.error().peek().is_none());
    assert!(query.peek().is_empty());
    Ok(())
}

#[tokio::test]
async fn query_update_before_initial_resolution_uses_latest_selection() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let ctx = server.context(DEFAULT_CONTEXT)?;
    let trx = ctx.begin();
    trx.create(&Widget { label: "old".into(), size: 1 }).await?;
    let latest = trx.create(&Widget { label: "latest".into(), size: 2 }).await?.id();
    trx.commit().await?;

    for cached in [false, true] {
        let client = ephemeral_sled_setup().await?;
        let ctx = Context::new(client.clone(), DEFAULT_CONTEXT);
        let mut args = nocache("label = 'old'")?;
        args.cached = cached;
        let query = ctx.query::<WidgetView>(args)?;
        query.update_selection("label = 'intermediate'")?;
        query.update_selection("label = 'latest'")?;
        assert!(query.selection().peek().is_none());

        let _conn = LocalProcessConnection::new(&server, &client).await?;
        tokio::time::timeout(Duration::from_secs(5), query.wait_durable_answered()).await??;
        assert_eq!(query.ids(), vec![latest]);
        assert_eq!(query.selection().peek().expect("resolved selection").1, 3);
        assert!(query.error().peek().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn local_registration_resolution_does_not_require_a_write_credential() -> anyhow::Result<()> {
    use ankurah::core::schema::registration::RegistrationError;

    let node = durable_sled_setup().await?;
    let writer = node.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let id = trx.create(&Widget { label: "present".into(), size: 1 }).await?.id();
    trx.commit().await?;
    let model = writer.register_model::<Widget>().await?;
    let before = node.catalog.counts();

    for count in [0, 2] {
        let sessions = SessionSet::new();
        for _ in 0..count {
            sessions.own(&Session::new(DEFAULT_CONTEXT));
        }
        assert!(sessions.write_credential().is_err());
        let context = node.context(sessions)?;
        assert_eq!(context.register_model::<Widget>().await?, model);
        assert_eq!(context.get::<WidgetView>(id).await?.label()?, "present");
        assert_eq!(context.fetch::<WidgetView>("label = 'present'").await?.len(), 1);

        let error = context.register_model::<Gadget>().await.expect_err("unavailable credentials must not grant privilege");
        assert!(matches!(error, RegistrationError::PolicyDenied { .. }));
        assert_eq!(node.catalog.counts(), before);
        assert!(!schema_registered(&node, Gadget::descriptor()));
    }
    Ok(())
}

#[tokio::test]
async fn query_update_can_retry_failed_initial_registration() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    let sessions = SessionSet::new();
    let ctx = client.context(sessions.clone())?;
    let query = ctx.query::<GadgetView>("name = 'old'")?;
    let error = tokio::time::timeout(Duration::from_secs(5), query.wait_initialized()).await?.expect_err("no write credential");
    assert!(error.to_string().contains("refused by policy"), "{error}");
    assert!(query.selection().peek().is_none());

    let credentialed = SessionSet::from(DEFAULT_CONTEXT);
    sessions.attach(&credentialed);
    query.update_selection("name = 'new'")?;
    tokio::time::timeout(Duration::from_secs(5), query.wait_durable_answered()).await??;
    assert!(server.catalog.model_by_label("gadget").unwrap().is_some());
    assert!(query.error().peek().is_none());
    assert_eq!(query.selection().peek().expect("resolved selection").1, 2);
    Ok(())
}

#[tokio::test]
async fn query_wait_defers_until_system_ready() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let ctx = Context::new(client.clone(), DEFAULT_CONTEXT);
    let query = ctx.query_wait::<GadgetView>("name = 'x'");
    tokio::pin!(query);
    tokio::select! {
        biased;
        _ = &mut query => panic!("query_wait must wait for the first system connection"),
        _ = tokio::task::yield_now() => {}
    }
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    let query = tokio::time::timeout(Duration::from_secs(5), query).await??;
    assert!(server.catalog.model_by_label("gadget").unwrap().is_some());
    assert!(query.error().peek().is_none());
    Ok(())
}

#[tokio::test]
async fn query_wait_rejects_unknown_names_without_registering() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let ctx = server.context(DEFAULT_CONTEXT)?;
    let error = ctx.query_wait::<GadgetView>("bogus = 'x'").await.err().expect("unknown field");
    assert!(error.to_string().contains("unknown property 'bogus'"), "{error}");
    assert!(server.catalog.model_by_label("gadget").unwrap().is_none());
    Ok(())
}

#[tokio::test]
async fn query_registers_missing_fields_of_an_existing_model() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.context(DEFAULT_CONTEXT)?.register_model::<offline_v1::Evolving>().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await?;
    client.wait_ready().await?;
    let before = resolve_by_collection(&client, "evolving", "label").expect("existing field");
    assert!(resolve_by_collection(&client, "evolving", "added").is_none());
    assert!(!schema_registered(&client, offline_v2::Evolving::descriptor()));

    let ctx = client.context(DEFAULT_CONTEXT)?;
    let error =
        ctx.query::<offline_v2::EvolvingView>("added = 1 AND bogus = 2").err().expect("an unregistered field must not hide a later typo");
    assert!(error.to_string().contains("unknown property 'bogus'"), "{error}");

    let query = ctx.query::<offline_v2::EvolvingView>("label = 'x'")?;
    tokio::time::timeout(Duration::from_secs(5), query.wait_initialized()).await??;
    assert!(schema_registered(&client, offline_v2::Evolving::descriptor()));
    assert_eq!(resolve_by_collection(&server, "evolving", "label"), Some(before));
    assert!(resolve_by_collection(&server, "evolving", "added").is_some(), "even unqueried fields must be registered");
    assert!(query.error().peek().is_none());
    Ok(())
}

#[tokio::test]
async fn query_registration_failure_reaches_signal_and_waiters() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    let ctx = client.context(SessionSet::new())?;
    let query = ctx.query::<GadgetView>("name = 'x'")?;
    let error =
        tokio::time::timeout(Duration::from_secs(5), query.wait_initialized()).await?.expect_err("registration needs a write credential");
    assert!(error.to_string().contains("refused by policy"), "{error}");
    assert_eq!(query.error().peek().expect("error signal").to_string(), error.to_string());

    let error = tokio::time::timeout(Duration::from_secs(5), ctx.query_wait::<GadgetView>("name = 'x'"))
        .await?
        .err()
        .expect("query_wait must return the registration failure");
    assert!(error.to_string().contains("refused by policy"), "{error}");
    assert!(server.catalog.model_by_label("gadget").unwrap().is_none());
    assert!(!schema_registered(&client, Gadget::descriptor()));
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
    client.system.wait_system_ready().await?;
    server.wait_ready().await?;

    // Wait until Widget is known to the client's catalog before disconnecting.
    let ctx = client.context_async(DEFAULT_CONTEXT).await.unwrap();
    ctx.register_model::<Widget>().await?;
    wait_resolve(&client, "widget", "label").await.expect("the projection delivers widget's rows while connected");
    assert!(client.catalog.model_by_label("widget").unwrap().is_some(), "widget known to the client's catalog while connected");

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
    assert_eq!(wait_resolve(&client, "gadget", "name").await, Some(name_id), "the client's projection converges on the allocator's ids");

    Ok(())
}

#[tokio::test]
async fn offline_reassert_requires_every_compiled_field_to_be_bound() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await?;
    let ctx = client.context_async(DEFAULT_CONTEXT).await.unwrap();
    ctx.register_model::<offline_v1::Evolving>().await?;
    // Wait until Evolving is known locally before testing offline reassertion.
    wait_resolve(&client, "evolving", "label").await.expect("the projection delivers evolving's rows while connected");

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

#[tokio::test]
async fn descriptor_reasserts_mutable_catalog_metadata() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    let mut declaration = proto::RegisterModel::from(Widget::descriptor());
    declaration.name = "Temporary name".into();
    declaration.properties[0].optional = true;
    client.request(server.id, &DEFAULT_CONTEXT, proto::NodeRequestBody::RegisterSchema { model: declaration }).await?;

    server.context(DEFAULT_CONTEXT)?.register_model::<Widget>().await?;

    let (model_id, model) = server.catalog.model_by_label("widget").unwrap().expect("widget model");
    assert_eq!(model.name, "Widget");
    let property = server.catalog.property_by_name(&model_id, "label").unwrap().expect("widget.label").0;
    let membership = server.catalog.membership(&model_id, &property).unwrap().expect("widget.label membership").1;
    assert!(!membership.optional);
    Ok(())
}

// A predicate read registers an unknown compiled schema before querying it.
#[tokio::test]
async fn predicate_read_path_heals_and_defines() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.wait_ready().await?;
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

    Ok(())
}

// Healing an evolved descriptor registers only its new field.
#[tokio::test]
async fn healing_registers_only_the_added_field() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.wait_ready().await?;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // The system knows the one-field shape.
    ctx.register_model::<offline_v1::Evolving>().await?;
    let (model, _) = server.catalog.model_by_label("evolving").unwrap().expect("evolving model");
    let Some(PropertyId::EntityId(label)) = resolve_by_collection(&server, "evolving", "label") else {
        anyhow::bail!("evolving.label resolves after the first registration");
    };
    let (membership, _) = server.catalog.membership(&model, &label).unwrap().expect("evolving.label membership");
    let heads_before = (
        catalog_head(&server, "_ankurah_model", model).await?,
        catalog_head(&server, "_ankurah_property", label).await?,
        catalog_head(&server, "_ankurah_model_property", membership).await?,
    );

    // A binary compiled against the two-field shape reads; healing registers
    // the difference and the read answers.
    let results = ctx.fetch::<offline_v2::EvolvingView>("added = 1").await?;
    assert!(results.is_empty(), "nothing was ever created in this collection");

    assert_eq!(server.catalog.model_by_label("evolving").unwrap().expect("evolving model").0, model, "the model must not be re-minted");
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
    assert!(server.catalog.membership(&model, &added).unwrap().is_some(), "the added field joins the model already there");
    assert!(schema_registered(&server, offline_v2::Evolving::descriptor()), "the two-field declaration binds after healing");

    Ok(())
}

// A credential that cannot register schema cannot heal a read.
#[tokio::test]
async fn read_only_credential_cannot_heal() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.wait_ready().await?;
    // A session-less source reads, but can never name the single principal a
    // registration acts as.
    let ctx = server.context(SessionSet::new())?;

    let error = ctx.fetch::<GadgetView>("name = 'x'").await.expect_err("a read that cannot heal must not answer");
    let msg = error.to_string();
    assert!(msg.contains("gadget") && msg.contains("refused by policy"), "the error must name the model and the refusal, got: {msg}");
    assert!(server.catalog.model_by_label("gadget").unwrap().is_none(), "a refused read must define nothing");
    assert!(!schema_registered(&server, Gadget::descriptor()), "a refused read must resolve nothing");

    Ok(())
}

// An offline read cannot heal a never-registered collection.
#[tokio::test]
async fn offline_read_unregistered_fails_loud() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await?;
    client.wait_ready().await?;

    // Complete the initial catalog answers before going offline. Contraption is never registered.
    let ctx = client.context_async(DEFAULT_CONTEXT).await.unwrap();

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
    let ctx_a = client_a.context_async(DEFAULT_CONTEXT).await.unwrap();
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
    client_b.system.wait_system_ready().await?;
    let ctx_b = client_b.context_async(DEFAULT_CONTEXT).await.unwrap();

    let view = ctx_b.get::<ContraptionView>(id).await?;
    assert!(schema_registered(&client_b, Contraption::descriptor()), "a typed direct id get must resolve its exact schema before decoding");

    let trx = ctx_b.begin();
    view.edit(&trx)?.state()?.replace("polished")?;
    trx.commit().await?;

    assert!(schema_registered(&client_b, Contraption::descriptor()), "the resolved binding remains available through the edit-only commit");
    Ok(())
}

#[tokio::test]
async fn transaction_get_binds_the_model_before_field_access() -> anyhow::Result<()> {
    let (server, client_a, _conn_a) = connected_pair().await?;
    let ctx_a = client_a.context_async(DEFAULT_CONTEXT).await.unwrap();
    let id = {
        let trx = ctx_a.begin();
        let entity = trx.create(&Contraption { state: "ready".into() }).await?;
        let id = entity.id();
        trx.commit().await?;
        id
    };

    let client_b = ephemeral_sled_setup().await?;
    let _conn_b = LocalProcessConnection::new(&server, &client_b).await?;
    client_b.system.wait_system_ready().await?;
    let ctx_b = client_b.context_async(DEFAULT_CONTEXT).await.unwrap();
    let trx = ctx_b.begin();
    let entity = trx.get::<Contraption>(&id).await?;

    assert_eq!(entity.state()?.value().as_deref(), Some("ready"));
    Ok(())
}

// (a) Explicit register_model::<M>() on a durable node's context: catalog entries
// exist locally afterwards, and a second call is a no-op (catalog heads
// unchanged, using the same head-comparison pattern as
// schema_registration.rs).
#[tokio::test]
async fn explicit_register_is_strict_and_idempotent() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.wait_ready().await?;
    let ctx = server.context(DEFAULT_CONTEXT)?;

    // Strict register: propagates errors (here, succeeds).
    ctx.register_model::<Gizmo>().await?;

    // Catalog entries exist locally after the explicit register; the ids
    // are this durable's allocations.
    let title_id = wait_resolve(&server, "gizmo", "title").await.expect("gizmo.title resolves after register");
    let (model_id, _) = server.catalog.model_by_label("gizmo").unwrap().expect("gizmo model");
    let (membership, _) = server.catalog.membership(&model_id, &title_id).unwrap().expect("gizmo.title membership");

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
    server.wait_ready().await?;
    client.wait_ready().await?;

    // Build the context after the initial system root has been adopted.
    let ctx = client.context_async(DEFAULT_CONTEXT).await.unwrap();

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
    assert_eq!(wait_resolve(&client, "gadget", "name").await, Some(name_id), "the client's projection converges on the allocator's ids");

    Ok(())
}

#[tokio::test]
async fn halted_node_cannot_reregister_even_an_already_bound_descriptor() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (server, client, _conn) = connected_pair().await?;
        let server_ctx = server.context(DEFAULT_CONTEXT)?;
        let trx = server_ctx.begin();
        let gizmo = trx.create(&Gizmo { title: "retained binding".into() }).await?;
        let gizmo_id = gizmo.id();
        drop(gizmo);
        trx.commit().await?;

        let ctx = client.context_async(DEFAULT_CONTEXT).await?;
        let retained_view = ctx.get::<GizmoView>(gizmo_id).await?;
        let epoch = client.system.system_epoch().expect("initialized epoch");
        let binding = Gizmo::descriptor().resolved.get(epoch).expect("Gizmo is registered before halting");
        assert!(schema_registered(&client, Gizmo::descriptor()));
        assert!(!schema_registered(&client, Gadget::descriptor()));
        assert!(server.catalog.model_by_label("gadget").unwrap().is_none());

        let other = durable_sled_setup().await?;
        let root = client.system.root().expect("current root");
        let proposed = other.system.root().expect("other root");
        let halt_reason = NodeHaltReason::SystemReplacement { current: root.payload.entity_id, proposed: proposed.payload.entity_id };
        client.set_allow_system_replacement(true);
        assert_eq!(client.system.adopt_system(proposed).await, Err(PeerConnectionError::NodeHalted(halt_reason.clone())));

        let bound_error = ctx.register_model::<Gizmo>().await.expect_err("halting must take priority over the already-bound fast path");
        assert!(matches!(bound_error, RegistrationError::Retrieval(RetrievalError::NodeHalted(error)) if error == halt_reason));
        let unbound_error = ctx.register_model::<Gadget>().await.expect_err("halting must prevent new registration");
        assert!(matches!(unbound_error, RegistrationError::Retrieval(RetrievalError::NodeHalted(error)) if error == halt_reason));

        assert_eq!(client.state().peek(), NodeState::Halted(halt_reason));
        assert_eq!(client.system.system_epoch(), None);
        assert_eq!(Gizmo::descriptor().resolved.get(epoch), Some(binding), "existing views keep their original bindings");
        assert_eq!(retained_view.title()?, "retained binding");
        assert!(Gadget::descriptor().resolved.get(epoch).is_none());
        assert!(server.catalog.model_by_label("gadget").unwrap().is_none(), "failed registration must not allocate on the durable");
        Ok(())
    })
    .await?
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
    let ctx = node.context_async(DEFAULT_CONTEXT).await.unwrap();
    ctx.register_model::<Review>().await?;

    let rating_id = wait_resolve(&node, "review", "rating").await.expect("review.rating resolves after register");
    let def = node.catalog.property_by_id(&rating_id).unwrap().expect("catalog property def");
    assert_eq!(def.value_type, "i64", "the catalog stores the declared value_type as the canonical type");
    assert_eq!(def.backend, "lww");
    Ok(())
}
