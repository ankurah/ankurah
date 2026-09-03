//! Catalog projection and resolution behavior.

mod common;
use ankurah::core::schema::registration::RegistrationError;
use ankurah::core::storage::StorageEngine;
use ankurah::PropertyId;
use common::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

fn schema_registered(node: &TestNode, schema: &'static ankurah::core::schema::ModelStructDescriptor) -> bool {
    node.system.schema_epoch().is_some_and(|epoch| schema.resolved.get(epoch).is_some())
}

fn album_entry(name: &str, backend: &str, value_type: &str, optional: bool) -> proto::RegisterModel {
    proto::RegisterModel {
        label: "album".into(),
        name: "Album".into(),
        explicit_id: None,
        build_id: [0u8; 16],
        properties: vec![proto::RegisterProperty {
            name: name.into(),
            renamed_from: None,
            backend: backend.into(),
            value_type: value_type.into(),
            target_label: None,
            explicit_id: None,
            build_id: [0u8; 16],
            optional,
        }],
    }
}

fn album_request() -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema { model: album_entry("name", "yrs", "string", false) }
}

fn album_year_request() -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema { model: album_entry("year", "lww", "i64", true) }
}

async fn connected_pair(
) -> anyhow::Result<(TestNode, TestNode, LocalProcessConnection<SledStorageEngine, PermissiveAgent, SledStorageEngine, PermissiveAgent>)> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    Ok((server, client, conn))
}

fn expect_registered(resp: proto::NodeResponseBody) -> proto::RegisteredModel {
    match resp {
        proto::NodeResponseBody::SchemaRegistered { model } => model,
        other => panic!("expected SchemaRegistered, got {other}"),
    }
}

async fn wait_resolve<SE>(node: &Node<SE, PermissiveAgent>, collection: &str, name: &str) -> Option<EntityId>
where SE: StorageEngine + Send + Sync + 'static {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(model) = node.catalog.model_id_for(collection) {
            if let Some(PropertyId::EntityId(id)) = node.catalog.resolve(&model, name) {
                return Some(id);
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize, label: &str) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(2), async {
        while counter.load(Ordering::Acquire) < expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for {expected} {label}; observed {}", counter.load(Ordering::Acquire)))
}

#[tokio::test]
async fn durable_map_resolves_after_registration() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    server.catalog.wait_ready().await?;

    let models = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let (model_id, property_id, membership_id) = (models.id, models.properties[0].id, models.properties[0].membership_id);

    let resolved = wait_resolve(&server, "album", "name").await.expect("catalog should resolve album.name on the durable node");
    assert_eq!(resolved, property_id, "resolution lands on the allocated property id");

    let prop = server.catalog.property_by_id(&property_id).expect("property def present");
    assert_eq!(prop.name, "name");
    assert_eq!(prop.backend, "yrs");
    assert_eq!(prop.value_type, "string");
    assert_eq!(prop.minted_for, Some(model_id));

    let (found_model_id, model) = server.catalog.model_by_label("album").expect("model def present");
    assert_eq!(found_model_id, model_id);
    assert_eq!(model.name, "Album");

    let (found_membership_id, membership) = server.catalog.membership(&model_id, &property_id).expect("membership present");
    assert_eq!(found_membership_id, membership_id);
    assert!(!membership.optional, "a required field's membership is not optional");

    Ok(())
}

#[tokio::test]
async fn durable_map_resolves_after_reconstruction() -> anyhow::Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let model_id = {
        let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
        node.system.create().await?;
        node.catalog.wait_ready().await?;
        let model_id = node.context_async(DEFAULT_CONTEXT).await.register_model::<Album>().await?;
        wait_resolve(&node, "album", "name").await.expect("catalog should resolve the registered model");
        model_id
    };

    let node = Node::new_durable(engine, PermissiveAgent::new());
    node.catalog.wait_ready().await?;

    assert_eq!(node.catalog.model_id_for("album"), Some(model_id));
    assert!(node.catalog.resolve(&model_id, "name").is_some());
    Ok(())
}

#[tokio::test]
async fn durable_map_updates_incrementally() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_ready().await?;

    let models = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let model_id = models.id;
    let name_property_id = models.properties[0].id;
    wait_resolve(&server, "album", "name").await.expect("name resolves");

    let year = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_year_request()).await?);
    let year_property_id = year.properties[0].id;
    let year_id = wait_resolve(&server, "album", "year").await.expect("year resolves incrementally");
    assert_eq!(year_id, year_property_id, "resolution answers year with the allocated id");

    assert!(server.catalog.membership(&model_id, &name_property_id).is_some(), "name remains a membership of album");
    assert!(server.catalog.membership(&model_id, &year_property_id).is_some(), "year joined album's membership set");

    Ok(())
}

#[tokio::test]
async fn an_answered_ephemeral_carries_the_catalog_it_never_registered() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    server.catalog.wait_ready().await?;
    server.context_async(DEFAULT_CONTEXT).await.register_model::<Album>().await?;
    let model = server.catalog.model_id_for("album").expect("the durable registered album");

    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    client.catalog.wait_synced().await?;

    assert_eq!(client.catalog.model_id_for("album"), Some(model), "an answered catalog carries what the durable holds");
    assert!(client.catalog.resolve(&model, "name").is_some(), "and resolves its properties without registering them");

    Ok(())
}

#[tokio::test]
async fn hard_reset_rejects_stale_schema_registration_response() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let held_responses = Arc::new(AtomicUsize::new(0));
    let (_connection, gate) = {
        let held_responses = held_responses.clone();
        GatedConnection::new(&server, &client, move |message| {
            if matches!(
                message,
                proto::NodeMessage::Response(proto::NodeResponse { body: proto::NodeResponseBody::SchemaRegistered { .. }, .. })
            ) {
                held_responses.fetch_add(1, Ordering::AcqRel);
                true
            } else {
                false
            }
        })
    };
    client.system.wait_system_ready().await;

    let old_client = client.clone();
    let old_registration = tokio::spawn(async move {
        let descriptor = Album::descriptor();
        old_client.catalog.ensure_schema_for_use(&old_client, &DEFAULT_CONTEXT, descriptor).await
    });
    wait_for_count(&held_responses, 1, "old-epoch SchemaRegistered response").await?;

    client.system.hard_reset().await?;
    gate.release_first(&client, 1).await;
    let old_result = tokio::time::timeout(Duration::from_secs(1), old_registration)
        .await
        .expect("the released old response must release its request future")
        .expect("old registration task must not panic");
    assert!(matches!(old_result, Err(RegistrationError::SystemChanged)));
    assert_eq!(client.catalog.counts(), (0, 0, 0), "no catalog row survives the reset");
    assert!(!schema_registered(&client, Album::descriptor()));

    client.system.join_system(server.system.root().expect("server root")).await?;
    client.catalog.wait_synced().await?;
    assert!(wait_resolve(&client, "album", "name").await.is_some(), "the re-subscribed projection refills from the durable");
    assert!(!schema_registered(&client, Album::descriptor()), "the old epoch's response must not satisfy the replacement epoch");
    client.catalog.ensure_schema_for_use(&client, &DEFAULT_CONTEXT, Album::descriptor()).await?;
    assert!(schema_registered(&client, Album::descriptor()), "the replacement epoch binds from the refilled catalog's proof");

    Ok(())
}

#[tokio::test]
async fn standing_query_identity_and_version_do_not_regress() -> anyhow::Result<()> {
    let (server, client, _connection) = connected_pair().await?;
    let query_id = proto::QueryId::new();
    let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    let subscribe = |collection: &str, version| proto::NodeRequestBody::SubscribeQuery {
        query_id,
        collection: proto::CollectionId::fixed_name(collection),
        selection: selection.clone(),
        version,
        known_matches: Vec::new(),
    };

    assert!(matches!(
        client.request(server.id, &DEFAULT_CONTEXT, subscribe(ankurah::core::schema::MODEL_COLLECTION_ID, 2)).await?,
        proto::NodeResponseBody::QuerySubscribed { .. }
    ));
    let stale = client.request(server.id, &DEFAULT_CONTEXT, subscribe(ankurah::core::schema::MODEL_COLLECTION_ID, 1)).await?;
    assert!(matches!(stale, proto::NodeResponseBody::Error(message) if message.contains("stale subscription version")));
    let rebound = client.request(server.id, &DEFAULT_CONTEXT, subscribe(ankurah::core::schema::PROPERTY_COLLECTION_ID, 3)).await?;
    assert!(matches!(rebound, proto::NodeResponseBody::Error(message) if message.contains("already bound")));
    Ok(())
}

#[tokio::test]
async fn catalog_projection_does_not_retain_node() -> anyhow::Result<()> {
    async fn assert_releases(weak: ankurah::core::node::WeakNode<SledStorageEngine, PermissiveAgent>, kind: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("dropping the last {kind} handle must free the node"));
    }

    let durable = durable_sled_setup().await?;
    durable.catalog.wait_ready().await?;
    let durable_weak = durable.weak();
    drop(durable);
    assert_releases(durable_weak, "durable").await;

    let ephemeral = ephemeral_sled_setup().await?;
    let ephemeral_weak = ephemeral.weak();
    drop(ephemeral);
    assert_releases(ephemeral_weak, "ephemeral").await;

    Ok(())
}

#[tokio::test]
async fn uninitialized_durable_does_not_retain_catalog_manager() -> anyhow::Result<()> {
    let engine = Arc::new(SledStorageEngine::new_test()?);
    let node = Node::new_durable(engine.clone(), PermissiveAgent::new());
    tokio::time::timeout(Duration::from_secs(1), node.system.wait_loaded()).await.expect("empty durable storage must finish loading");
    assert!(!node.system.is_system_ready());
    assert!(node.system.root().is_none());

    let weak = node.weak();
    drop(node);
    tokio::time::timeout(Duration::from_secs(1), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an uninitialized durable node must not be retained by catalog startup");
    tokio::time::timeout(Duration::from_secs(1), async {
        while Arc::strong_count(&engine) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pending catalog queries must release storage after the node drops");

    Ok(())
}

#[tokio::test]
async fn hard_reset_flushes_catalog() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    server.catalog.wait_ready().await?;

    expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    wait_resolve(&server, "album", "name").await.expect("server resolves before reset");
    let (models, properties, memberships) = server.catalog.counts();
    assert!(models > 0 && properties > 0 && memberships > 0, "catalog is populated before reset");

    server.system.hard_reset().await?;

    assert_eq!(server.catalog.counts(), (0, 0, 0), "catalog cleared after hard_reset");
    assert!(server.catalog.model_id_for("album").is_none(), "resolve returns nothing after hard_reset");

    Ok(())
}

#[tokio::test]
async fn rename_updates_resolution() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_ready().await?;

    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let property_id = first.properties[0].id;
    wait_resolve(&server, "album", "name").await.expect("resolves under original name");

    let mut rename_entry = album_entry("title", "yrs", "string", false);
    rename_entry.properties[0].renamed_from = Some("name".into());
    let rename = proto::NodeRequestBody::RegisterSchema { model: rename_entry };
    let renamed = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename).await?);
    assert_eq!(renamed.properties[0].id, property_id, "the hint preserves the lineage id");

    let renamed_id = wait_resolve(&server, "album", "title").await.expect("resolves under new name after rename");
    assert_eq!(renamed_id, property_id, "rename keeps the allocated id (hint-moved lineage)");
    let model_id = server.catalog.model_id_for("album").expect("album remains registered");
    assert!(server.catalog.resolve(&model_id, "name").is_none(), "old display name no longer resolves");

    let ankurah::ModelId::EntityId(model_eid) = model_id else { panic!("album is catalog-backed") };
    assert_eq!(server.catalog.property_by_name(&model_eid, "title").map(|(id, _)| id), Some(property_id));
    assert!(server.catalog.property_by_name(&model_eid, "name").is_none(), "old name removed from the name lookup");

    Ok(())
}
