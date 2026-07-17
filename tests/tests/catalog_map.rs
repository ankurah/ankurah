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
use ankql::ast::PropertyId;
use ankurah::core::{
    error::{MutationError, RetrievalError},
    livequery::EntityLiveQuery,
    node::MatchArgs,
    property::{backend::lww::LWWBackend, backend::PropertyBackend},
    storage::{StorageCollection, StorageEngine},
    value::Value,
};
use async_trait::async_trait;
use common::*;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;

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
async fn wait_resolve<SE>(node: &Node<SE, PermissiveAgent>, collection: &str, name: &str) -> Option<EntityId>
where SE: StorageEngine + Send + Sync + 'static {
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

async fn wait_for_count(counter: &AtomicUsize, expected: usize, label: &str) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(2), async {
        while counter.load(Ordering::Acquire) < expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for {expected} {label}; observed {}", counter.load(Ordering::Acquire)))
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

// A connected peer may accept the three catalog subscriptions but never
// answer them. Catalog readiness is an optimization boundary, not an
// authority boundary: one shared grace period must release context creation
// to use the local cache, while the relay subscriptions remain alive and can
// populate that cache if their responses arrive later.
#[tokio::test]
async fn ephemeral_catalog_warm_has_one_bounded_remote_deadline() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let server_ctx = server.context_async(DEFAULT_CONTEXT).await;
    server_ctx.register::<Album>().await?;

    let client = ephemeral_sled_setup().await?;
    let held_responses = Arc::new(AtomicUsize::new(0));
    let (_connection, gate) = {
        let held_responses = held_responses.clone();
        GatedConnection::new(&server, &client, move |message| match message {
            proto::NodeMessage::Response(response) if matches!(&response.body, proto::NodeResponseBody::QuerySubscribed { .. }) => {
                held_responses.fetch_add(1, Ordering::AcqRel);
                true
            }
            _ => false,
        })
    };
    client.system.wait_system_ready().await;

    tokio::time::timeout(Duration::from_secs(4), client.context_async(DEFAULT_CONTEXT))
        .await
        .expect("one shared catalog grace period must bound a silent connected peer");
    assert!(client.catalog.is_catalog_ready(), "deadline fallback must make the local cache usable");
    assert_eq!(held_responses.load(Ordering::Acquire), 3, "all three catalog subscriptions should share the same deadline");

    gate.release_held(&client).await;
    wait_resolve(&client, "album", "name").await.expect("late relay responses must still populate the retained catalog subscriptions");

    Ok(())
}

// The first context caller owns only its wait, not the shared catalog setup.
// Cancelling it after the remote requests are in flight must leave the
// node-owned warm running so a later context can join it and complete.
#[tokio::test]
async fn cancelling_first_context_does_not_cancel_catalog_warm() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let server_ctx = server.context_async(DEFAULT_CONTEXT).await;
    server_ctx.register::<Album>().await?;

    let client = ephemeral_sled_setup().await?;
    let held_responses = Arc::new(AtomicUsize::new(0));
    let (_connection, gate) = {
        let held_responses = held_responses.clone();
        GatedConnection::new(&server, &client, move |message| match message {
            proto::NodeMessage::Response(response) if matches!(&response.body, proto::NodeResponseBody::QuerySubscribed { .. }) => {
                held_responses.fetch_add(1, Ordering::AcqRel);
                true
            }
            _ => false,
        })
    };
    client.system.wait_system_ready().await;

    let first_context = {
        let client = client.clone();
        tokio::spawn(async move { client.context_async(DEFAULT_CONTEXT).await })
    };
    wait_for_count(&held_responses, 3, "held catalog responses").await?;
    first_context.abort();
    let _ = first_context.await;

    let second_context = {
        let client = client.clone();
        tokio::spawn(async move { client.context_async(DEFAULT_CONTEXT).await })
    };
    tokio::task::yield_now().await;
    assert!(!second_context.is_finished(), "the later context should join the still-running shared warm");
    assert_eq!(held_responses.load(Ordering::Acquire), 3, "cancellation must not launch a duplicate catalog warm");

    gate.release_held(&client).await;
    tokio::time::timeout(Duration::from_secs(2), second_context)
        .await
        .expect("the later context must finish when the original warm completes")
        .expect("the later context task must not panic");
    assert!(client.catalog.is_catalog_ready());
    wait_resolve(&client, "album", "name").await.expect("the completed shared warm must populate the catalog");

    Ok(())
}

// The manager owns its subscription guards, so their callbacks must retain it
// weakly. Exercise both durable and ephemeral warm paths and prove the catalog
// allocation disappears after the owning node/connection are dropped.
#[tokio::test]
async fn catalog_subscription_callbacks_do_not_retain_manager() -> anyhow::Result<()> {
    let durable = durable_sled_setup().await?;
    durable.catalog.wait_catalog_ready().await;
    let durable_probe = durable.catalog.liveness_probe();
    let durable_weak = durable.weak();
    drop(durable);
    tokio::time::timeout(Duration::from_secs(1), async {
        while durable_probe() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("durable catalog subscription callback must not form a retain cycle");
    assert!(durable_weak.upgrade().is_none());

    let (server, client, connection) = connected_pair().await?;
    client.context_async(DEFAULT_CONTEXT).await;
    let ephemeral_probe = client.catalog.liveness_probe();
    let ephemeral_weak = client.weak();
    drop(connection);
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), async {
        while ephemeral_probe() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ephemeral catalog subscription callbacks must not form a retain cycle");
    assert!(ephemeral_weak.upgrade().is_none());
    drop(server);

    Ok(())
}

// Constructing a durable node without creating or loading a system is a valid
// idle state. Catalog startup must not retain the managers forever while
// waiting for readiness that may never arrive.
#[tokio::test]
async fn uninitialized_durable_does_not_retain_catalog_manager() -> anyhow::Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    tokio::time::timeout(Duration::from_secs(1), node.system.wait_loaded()).await.expect("empty durable storage must finish loading");
    assert!(!node.system.is_system_ready());
    assert!(node.system.root().is_none());

    let probe = node.catalog.liveness_probe();
    let weak = node.weak();
    drop(node);
    tokio::time::timeout(Duration::from_secs(1), async {
        while probe() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an uninitialized durable node must not be retained by a catalog readiness task");
    assert!(weak.upgrade().is_none());

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
// common's Album model. The raw wire registration deliberately bypasses the
// client's registration latch while still defining the model's complete
// compiled schema, so the later offline query has every field binding it
// needs but no allocator confirmation to reuse.
#[tokio::test]
async fn ephemeral_resolves_known_collection_offline_from_cache() -> anyhow::Result<()> {
    use ankurah::signals::With;

    let (server, client, conn) = connected_pair().await?;
    let ctx = client.context_async(DEFAULT_CONTEXT).await;

    let (models, properties, memberships) = ankurah::core::schema::registration_request(Album::schema());
    expect_registered(
        client.request(server.id, &DEFAULT_CONTEXT, proto::NodeRequestBody::RegisterSchema { models, properties, memberships }).await?,
    );
    let name_id = wait_resolve(&client, "album", "name").await.expect("client map warms while connected");
    wait_resolve(&client, "album", "year").await.expect("client map warms the complete compiled schema while connected");

    drop(conn);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Resolution still answers from the cached map (the binding surface was
    // replaced by name->id resolution in the PropertyKey refactor).
    assert_eq!(client.catalog.resolve("album", "name"), Some(name_id), "cache survives disconnect");
    assert!(client.catalog.resolve("album", "name").is_some(), "resolution answers from the cache offline");

    // A cached live query initializes offline: resolution runs against the
    // cached catalog, activation reads local storage (empty), and the relay
    // registration waits quietly for a reconnect.
    let lq = ctx.query::<AlbumView>("name = 'x'")?;
    lq.wait_initialized().await;
    lq.error().with(|e| assert!(e.is_none(), "offline cached query must not error: {e:?}"));
    assert!(lq.ids().is_empty(), "no local entities for a fresh cache");
    assert!(!client.catalog.is_ensured("album"), "offline compatibility proof must not masquerade as allocator confirmation");

    Ok(())
}

// A synchronous context does not start the ephemeral catalog subscription.
// First-use registration can still initialize a typed query because its
// response feeds the exact binding directly into the map. A later selection
// update has no context data with which to start a policy-aware catalog warm,
// so an unknown compiled field must fail promptly rather than waiting forever
// on readiness that no task will establish.
#[tokio::test]
async fn typed_selection_update_fails_promptly_with_cold_catalog() -> anyhow::Result<()> {
    use ankurah::signals::With;

    let (_server, client, _conn) = connected_pair().await?;
    assert!(!client.catalog.is_catalog_ready(), "the synchronous context scenario requires a cold catalog");
    let ctx = client.context(DEFAULT_CONTEXT)?;

    let lq = ctx.query_wait::<AlbumView>("name = 'x'").await?;
    assert!(client.catalog.is_ensured("album"), "first-use registration confirms the exact compiled schema");
    assert!(!client.catalog.is_catalog_ready(), "the registration response must not masquerade as a completed catalog warm");

    let error = tokio::time::timeout(Duration::from_secs(2), lq.update_selection_wait("not_a_field = 'x'"))
        .await
        .expect("an unknown typed selection update must not wait for cold-catalog readiness")
        .expect_err("the unknown property must surface from the waiter");
    assert!(error.to_string().contains("unknown property 'not_a_field'"), "unexpected update error: {error}");
    lq.error().with(|error| {
        let error = error.as_ref().expect("the unknown property must remain observable on the live query");
        assert!(error.to_string().contains("unknown property 'not_a_field'"), "unexpected update error: {error}");
    });

    Ok(())
}

// A schema-less query has no compiled binding from which a cold miss could be
// classified. Its update path must reuse the original subscription context to
// warm the catalog before resolving a newly introduced property reference.
#[tokio::test]
async fn schema_less_selection_update_warms_cold_catalog() -> anyhow::Result<()> {
    use ankurah::signals::With;

    let (server, client, _conn) = connected_pair().await?;
    let server_ctx = server.context_async(DEFAULT_CONTEXT).await;
    server_ctx.register::<Album>().await?;

    assert!(!client.catalog.is_catalog_ready(), "the synchronous raw-query scenario requires a cold catalog");
    let args: MatchArgs = "true".try_into()?;
    let lq = EntityLiveQuery::new(&client, Album::collection(), args, DEFAULT_CONTEXT)?;
    lq.wait_initialized().await;
    assert!(!client.catalog.is_catalog_ready(), "a property-free raw selection must not masquerade as a catalog warm");

    tokio::time::timeout(Duration::from_secs(5), lq.update_selection_wait("name = 'x'"))
        .await
        .expect("a valid schema-less update must not wait forever on a cold catalog")?;
    assert!(client.catalog.is_catalog_ready(), "the update must warm the catalog before resolving the property");
    assert!(client.catalog.resolve("album", "name").is_some(), "the warm must make the registered property resolvable");
    lq.error().with(|error| assert!(error.is_none(), "the valid raw update must not error: {error:?}"));

    let error = tokio::time::timeout(Duration::from_secs(2), lq.update_selection_wait("not_a_field = 'x'"))
        .await
        .expect("an unknown schema-less update must fail promptly after the catalog is warm")
        .expect_err("the unknown raw property must surface from the waiter");
    assert!(error.to_string().contains("unknown property 'not_a_field'"), "unexpected raw update error: {error}");
    lq.error().with(|error| {
        let error = error.as_ref().expect("the unknown raw property must remain observable on the live query");
        assert!(error.to_string().contains("unknown property 'not_a_field'"), "unexpected raw update error: {error}");
    });

    Ok(())
}

// With no durable peer, the same catalog setup must become ready from the
// local cache instead of waiting for a remote establishment that cannot
// arrive. This preserves the catalog subscription's offline-enabler role.
#[tokio::test]
async fn ephemeral_catalog_warm_remains_offline_capable() -> anyhow::Result<()> {
    let (_server, client, conn) = connected_pair().await?;
    drop(conn);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(!client.catalog.is_catalog_ready(), "no context has started the catalog warm yet");
    tokio::time::timeout(Duration::from_secs(2), client.context_async(DEFAULT_CONTEXT))
        .await
        .expect("offline catalog setup must complete from the local cache");
    assert!(client.catalog.is_catalog_ready(), "offline local-cache activation must mark the catalog ready");

    Ok(())
}

// Resolution is not the only initialization failure point. A local storage
// fetch can fail before the reactor's pre-notify hook runs; that terminal
// error must still release waiters instead of leaving the LiveQuery pending.
#[tokio::test]
async fn durable_activation_error_releases_initialization_waiters() -> anyhow::Result<()> {
    use ankurah::signals::With;

    let node = durable_sled_setup().await?;
    let collection = proto::CollectionId::from("unregistered_rows");
    let storage = node.collections.get(&collection).await?;
    storage
        .set_state(proto::Attested::opt(
            proto::EntityState { entity_id: proto::EntityId::new(), model: proto::EntityId::new(), state: proto::State::default() },
            None,
        ))
        .await?;

    // The raw, property-free selection resolves, then Sled's fetch fails while
    // reconstructing the stored row because this collection has no catalog
    // model id. That reaches the post-resolution activation-error branch.
    let args: MatchArgs = "true".try_into()?;
    let query = EntityLiveQuery::new(&node, collection, args, DEFAULT_CONTEXT)?;
    tokio::time::timeout(Duration::from_secs(2), query.wait_initialized())
        .await
        .expect("terminal activation error must release initialization waiters");
    query.error().with(|error| {
        let error = error.as_ref().expect("activation error must remain observable");
        assert!(error.to_string().contains("no model id known for collection 'unregistered_rows'"), "unexpected error: {error}");
    });

    Ok(())
}
