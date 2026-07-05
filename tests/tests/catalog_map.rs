//! A7: the catalog map (specs/model-property-metadata/rfc.md section 5.2).
//!
//! Each node maintains an in-memory catalog map warmed by subscribing to
//! the three catalog collections. Durable nodes warm from local storage and
//! keep fresh via a policy-free reactor subscription; ephemeral nodes get
//! initial state plus updates through the ordinary subscription relay once a
//! context triggers `ensure_subscribed`. These tests drive RegisterSchema
//! over the wire (the same schema-less-durable harness as
//! schema_registration.rs) and assert the map resolves.

mod common;
use common::*;
use std::time::Duration;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

fn album_request() -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into() }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "name".into(),
            name: "name".into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![proto::MembershipDescriptor {
            collection: "album".into(),
            property: proto::PropertyRef::Anchor("name".into()),
            optional: false,
        }],
    }
}

/// Register a second property `year` on the album model (incremental).
fn album_year_request() -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "year".into(),
            name: "year".into(),
            backend: "lww".into(),
            value_type: "i64".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![proto::MembershipDescriptor {
            collection: "album".into(),
            property: proto::PropertyRef::Anchor("year".into()),
            optional: true,
        }],
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

fn expect_success(resp: proto::NodeResponseBody) {
    match resp {
        proto::NodeResponseBody::Success => {}
        other => panic!("expected Success, got {other}"),
    }
}

/// Catalog updates arrive asynchronously (reactor notify on the durable
/// side, subscription relay on the ephemeral side), so poll until the map
/// resolves the given (collection, name) or time out.
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

// Test 1: durable node -- register schema, map resolves (collection,name) ->
// property id; membership optional flag visible; wait_catalog_ready resolves.
#[tokio::test]
async fn durable_map_resolves_after_registration() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    // wait_catalog_ready must resolve (durable warm marks ready at startup).
    server.catalog.wait_catalog_ready().await;
    assert!(server.catalog.is_catalog_ready());

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    let property_id = wait_resolve(&server, "album", "name").await.expect("catalog should resolve album.name on the durable node");

    let root = server.system.root().unwrap().payload.entity_id;
    let expected_model = proto::schema_id::model_entity_id(&root, "album");
    let expected_property = proto::schema_id::property_entity_id(&root, &expected_model, "name", "yrs", "string");
    assert_eq!(property_id, expected_property);

    // Property definition is parsed.
    let prop = server.catalog.property_by_id(&property_id).expect("property def present");
    assert_eq!(prop.name, "name");
    assert_eq!(prop.backend, "yrs");
    assert_eq!(prop.value_type, "string");
    assert_eq!(prop.minted_for, Some(expected_model));

    // Model is indexed by collection.
    let model = server.catalog.model_by_collection("album").expect("model def present");
    assert_eq!(model.id, expected_model);
    assert_eq!(model.name, "Album");

    // Membership carries the (required) optional flag.
    let membership = server.catalog.membership(&expected_model, &property_id).expect("membership present");
    assert_eq!(membership.optional, Some(false), "required membership => optional=Some(false)");

    Ok(())
}

// Test 2: incremental -- a second registration (new property) updates the map
// without restart.
#[tokio::test]
async fn durable_map_updates_incrementally() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    wait_resolve(&server, "album", "name").await.expect("name resolves");

    // Second registration adds `year` -- no restart.
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_year_request()).await?);
    let year_id = wait_resolve(&server, "album", "year").await.expect("year resolves incrementally");

    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = proto::schema_id::model_entity_id(&root, "album");
    let expected_year = proto::schema_id::property_entity_id(&root, &model_id, "year", "lww", "i64");
    assert_eq!(year_id, expected_year);

    // Both properties are now memberships of the album model.
    let memberships = server.catalog.memberships_of(&model_id);
    assert_eq!(memberships.len(), 2, "album now has two memberships");

    Ok(())
}

// Test 3: ephemeral -- connect to a durable that already has catalog; create a
// context (triggers ensure_subscribed); wait_catalog_ready; map is warm.
#[tokio::test]
async fn ephemeral_map_warms_from_relay() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    // The durable side already has the catalog before the ephemeral subscribes.
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    wait_resolve(&server, "album", "name").await.expect("server resolves first");

    // Creating a context triggers ensure_subscribed on the ephemeral node.
    let _ctx = client.context_async(DEFAULT_CONTEXT).await;
    client.catalog.wait_catalog_ready().await;
    assert!(client.catalog.is_catalog_ready());

    // The ephemeral map is warm: resolve works over the relay.
    let property_id = wait_resolve(&client, "album", "name").await.expect("ephemeral resolves album.name via relay");
    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = proto::schema_id::model_entity_id(&root, "album");
    let expected = proto::schema_id::property_entity_id(&root, &model_id, "name", "yrs", "string");
    assert_eq!(property_id, expected);

    Ok(())
}

// Test 4: ephemeral live update -- register more schema via the durable while
// the ephemeral is connected; the ephemeral map picks it up (relay path).
#[tokio::test]
async fn ephemeral_map_live_update() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    // Subscribe the ephemeral node before the second registration.
    let _ctx = client.context_async(DEFAULT_CONTEXT).await;
    client.catalog.wait_catalog_ready().await;
    wait_resolve(&client, "album", "name").await.expect("ephemeral has name");

    // Register `year` on the durable while the ephemeral is subscribed.
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_year_request()).await?);

    // The ephemeral map picks it up live.
    let year_id = wait_resolve(&client, "album", "year").await.expect("ephemeral picks up year live");
    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = proto::schema_id::model_entity_id(&root, "album");
    let expected_year = proto::schema_id::property_entity_id(&root, &model_id, "year", "lww", "i64");
    assert_eq!(year_id, expected_year);

    Ok(())
}

// Test 5: hard_reset flushes -- after reset, map empty and not ready.
#[tokio::test]
async fn hard_reset_flushes_catalog() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    server.catalog.wait_catalog_ready().await;

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    wait_resolve(&server, "album", "name").await.expect("server resolves before reset");
    let (models, properties, memberships) = server.catalog.counts();
    assert!(models > 0 && properties > 0 && memberships > 0, "map is populated before reset");

    // hard_reset must flush the catalog map and clear readiness (RFC 5.2).
    server.system.hard_reset().await?;

    assert!(!server.catalog.is_catalog_ready(), "catalog not ready after hard_reset");
    assert_eq!(server.catalog.counts(), (0, 0, 0), "catalog map cleared after hard_reset");
    assert!(server.catalog.resolve("album", "name").is_none(), "resolve returns nothing after hard_reset");

    Ok(())
}

// Test 6: rename follow-up -- the display-name index updates (old name gone,
// new present) while the derived property id is unchanged.
#[tokio::test]
async fn rename_updates_display_names() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let property_id = wait_resolve(&server, "album", "name").await.expect("resolves under original name");

    // Rename: the anchor pins the lineage, the display name moves to "title".
    let rename = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "name".into(),
            name: "title".into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, rename).await?);

    // New display name resolves to the SAME property id; old name is gone.
    let renamed_id = wait_resolve(&server, "album", "title").await.expect("resolves under new name after rename");
    assert_eq!(renamed_id, property_id, "rename keeps the derived id (anchor-pinned)");
    assert!(server.catalog.resolve("album", "name").is_none(), "old display name no longer resolves");

    // The global sibling index reflects the current display name.
    assert_eq!(server.catalog.siblings_by_name("title"), vec![property_id]);
    assert!(server.catalog.siblings_by_name("name").is_empty(), "old name removed from the sibling index");

    Ok(())
}
