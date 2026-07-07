//! A7: the catalog map (specs/model-property-metadata/rfc.md section 5.2).
//!
//! Each node maintains an in-memory catalog map warmed by subscribing to
//! the three catalog collections. Durable nodes warm from local storage and
//! keep fresh via a policy-free reactor subscription; ephemeral nodes get
//! initial state plus updates through the ordinary subscription relay once a
//! context triggers `ensure_subscribed`. These tests drive RegisterSchema
//! over the wire (the same schema-less-durable harness as
//! schema_registration.rs) and assert the map resolves to the ids the
//! allocator handed back in the SchemaRegistered response.

mod common;
use common::*;
use std::time::Duration;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

fn album_request() -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            name: "name".into(),
            renamed_from: None,
            backend: "yrs".into(),
            value_type: "string".into(),
            target_collection: None,
            explicit_id: None,
        }],
        memberships: vec![proto::MembershipDescriptor {
            collection: "album".into(),
            property: proto::PropertyRef::Name("name".into()),
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
            name: "year".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "i64".into(),
            target_collection: None,
            explicit_id: None,
        }],
        memberships: vec![proto::MembershipDescriptor {
            collection: "album".into(),
            property: proto::PropertyRef::Name("year".into()),
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

/// Unpack a SchemaRegistered response (the resolved definitions, ids included).
fn expect_registered(
    resp: proto::NodeResponseBody,
) -> (Vec<proto::RegisteredModel>, Vec<proto::RegisteredProperty>, Vec<proto::RegisteredMembership>) {
    match resp {
        proto::NodeResponseBody::SchemaRegistered { models, properties, memberships } => (models, properties, memberships),
        other => panic!("expected SchemaRegistered, got {other}"),
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
// the allocated property id; membership optional flag visible;
// wait_catalog_ready resolves.
#[tokio::test]
async fn durable_map_resolves_after_registration() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    // wait_catalog_ready must resolve (durable warm marks ready at startup).
    server.catalog.wait_catalog_ready().await;
    assert!(server.catalog.is_catalog_ready());

    // The allocator hands back the resolved ids in the response.
    let (models, properties, memberships) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let (model_id, property_id, membership_id) = (models[0].id, properties[0].id, memberships[0].id);

    // The reactor-fed map resolves the display name to the allocated id.
    let resolved = wait_resolve(&server, "album", "name").await.expect("catalog should resolve album.name on the durable node");
    assert_eq!(resolved, property_id, "the map resolves to the allocated property id");

    // Property definition is parsed.
    let prop = server.catalog.property_by_id(&property_id).expect("property def present");
    assert_eq!(prop.name, "name");
    assert_eq!(prop.backend, "yrs");
    assert_eq!(prop.value_type, "string");
    assert_eq!(prop.minted_for, Some(model_id));

    // Model is indexed by collection at the allocated id.
    let model = server.catalog.model_by_collection("album").expect("model def present");
    assert_eq!(model.id, model_id);
    assert_eq!(model.name, "Album");

    // Membership carries the (required) optional flag, at the allocated id.
    let membership = server.catalog.membership(&model_id, &property_id).expect("membership present");
    assert_eq!(membership.id, membership_id);
    assert_eq!(membership.optional, Some(false), "required membership => optional=Some(false)");

    Ok(())
}

// Test 2: incremental -- a second registration (new property) updates the map
// without restart.
#[tokio::test]
async fn durable_map_updates_incrementally() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    let (models, _, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let model_id = models[0].id;
    wait_resolve(&server, "album", "name").await.expect("name resolves");

    // Second registration adds `year` -- no restart.
    let (_, properties, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_year_request()).await?);
    let year_property_id = properties[0].id;
    let year_id = wait_resolve(&server, "album", "year").await.expect("year resolves incrementally");
    assert_eq!(year_id, year_property_id, "the map resolves year to the allocated id");

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
    let (_, properties, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let property_id = properties[0].id;
    wait_resolve(&server, "album", "name").await.expect("server resolves first");

    // Creating a context triggers ensure_subscribed on the ephemeral node.
    let _ctx = client.context_async(DEFAULT_CONTEXT).await;
    client.catalog.wait_catalog_ready().await;
    assert!(client.catalog.is_catalog_ready());

    // The ephemeral map is warm: resolve works over the relay, to the SAME
    // allocated id the durable executor returned.
    let resolved = wait_resolve(&client, "album", "name").await.expect("ephemeral resolves album.name via relay");
    assert_eq!(resolved, property_id, "the ephemeral map resolves to the allocated id");

    Ok(())
}

// Test 4: ephemeral live update -- register more schema via the durable while
// the ephemeral is connected; the ephemeral map picks it up (relay path).
#[tokio::test]
async fn ephemeral_map_live_update() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    // Subscribe the ephemeral node before the second registration.
    let _ctx = client.context_async(DEFAULT_CONTEXT).await;
    client.catalog.wait_catalog_ready().await;
    wait_resolve(&client, "album", "name").await.expect("ephemeral has name");

    // Register `year` on the durable while the ephemeral is subscribed.
    let (_, properties, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_year_request()).await?);
    let year_property_id = properties[0].id;

    // The ephemeral map picks it up live, at the allocated id.
    let year_id = wait_resolve(&client, "album", "year").await.expect("ephemeral picks up year live");
    assert_eq!(year_id, year_property_id, "the ephemeral map resolves year to the allocated id");

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

    expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
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
// new present) while the allocated property id is unchanged.
#[tokio::test]
async fn rename_updates_resolution_and_sibling_index() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    let (_, properties, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let property_id = properties[0].id;
    wait_resolve(&server, "album", "name").await.expect("resolves under original name");

    // Rename: the renamed_from hint moves the display name to "title" WITHOUT
    // re-keying (same allocated property id).
    let rename = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            name: "title".into(),
            renamed_from: Some("name".into()),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_collection: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };
    let (_, renamed, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename).await?);
    assert_eq!(renamed[0].id, property_id, "the hint preserves the lineage id");

    // New display name resolves to the SAME property id; old name is gone.
    let renamed_id = wait_resolve(&server, "album", "title").await.expect("resolves under new name after rename");
    assert_eq!(renamed_id, property_id, "rename keeps the allocated id (hint-moved lineage)");
    assert!(server.catalog.resolve("album", "name").is_none(), "old display name no longer resolves");

    // The global sibling index reflects the current display name.
    assert_eq!(server.catalog.siblings_by_name("title"), vec![property_id]);
    assert!(server.catalog.siblings_by_name("name").is_empty(), "old name removed from the sibling index");

    Ok(())
}

// The catalog subscription is a CACHE -- an accelerator and offline
// enabler (maintainer ruling 2026-07-06; the three catalog queries run
// cached: true). After syncing while connected, a disconnected ephemeral
// still resolves the collection from its map, still builds a schema
// binding, and a cached query over the known collection initializes
// OFFLINE from local storage instead of erroring or hanging. Uses
// common's Album model: the wire registration above defines `album.name`,
// which is the only reference the predicate resolves.
#[tokio::test]
async fn ephemeral_resolves_known_collection_offline_from_cache() -> anyhow::Result<()> {
    use ankurah::signals::With;

    let (server, client, conn) = connected_pair().await?;
    let ctx = client.context_async(DEFAULT_CONTEXT).await;

    expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let name_id = wait_resolve(&client, "album", "name").await.expect("client map warms while connected");

    drop(conn);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Resolution and binding still answer from the cached map.
    assert_eq!(client.catalog.resolve("album", "name"), Some(name_id), "cache survives disconnect");
    assert!(client.catalog.binding_for(&"album".into()).is_some(), "binding built from the cache offline");

    // A cached live query initializes offline: resolution runs against the
    // cached catalog, activation reads local storage (empty), and the relay
    // registration waits quietly for a reconnect.
    let lq = ctx.query::<AlbumView>("name = 'x'")?;
    lq.wait_initialized().await;
    lq.error().with(|e| assert!(e.is_none(), "offline cached query must not error: {e:?}"));
    assert!(lq.ids().is_empty(), "no local entities for a fresh cache");

    Ok(())
}
