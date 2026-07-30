//! Integration tests for the per-node schema-catalog service.
//!
//! Here, the **catalog map** is the node-local materialized projection owned
//! by `CatalogManager`, and the **catalog lifecycle** is its warm, feed,
//! registration fold, and reset epoch. These tests drive those boundaries
//! through real durable and ephemeral nodes: they verify allocated ids enter
//! the projection, reactor updates maintain it, and hard reset either rejects
//! or drains every old-epoch effect before clearing storage and bindings.
//! Pure parsing and secondary-index invariants remain unit tests beside the
//! projection in `core/src/schema/catalog/map.rs`.

mod common;
use ankurah::core::error::{MutationError, RetrievalError};
use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
use ankurah::core::schema::{MODEL_COLLECTION_ID, MODEL_PROPERTY_COLLECTION_ID, PROPERTY_COLLECTION_ID};
use ankurah::core::storage::{StorageCollection, StorageEngine};
use ankurah::core::value::Value;
use ankurah::PropertyId;
use async_trait::async_trait;
use common::*;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Barrier,
    },
    time::Duration,
};
use tokio::sync::Notify;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

fn album_entry(name: &str, backend: &str, value_type: &str, optional: bool) -> proto::RegisterModel {
    proto::RegisterModel {
        label: "album".into(),
        name: "Album".into(),
        explicit_id: None,
        unique_id: None,
        properties: vec![proto::RegisterProperty {
            name: name.into(),
            renamed_from: None,
            backend: backend.into(),
            value_type: value_type.into(),
            target_label: None,
            explicit_id: None,
            unique_id: None,
            optional,
        }],
    }
}

fn album_request() -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema { models: vec![album_entry("name", "yrs", "string", false)] }
}

/// Register a second property `year` on the album model (incremental).
fn album_year_request() -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema { models: vec![album_entry("year", "lww", "i64", true)] }
}

fn model_row_creation(label: &str, name: &str) -> anyhow::Result<proto::Event> {
    let backend = LWWBackend::new();
    backend.set("label".into(), Some(Value::String(label.into())));
    backend.set("name".into(), Some(Value::String(name.into())));
    let backend_operations = backend.to_operations()?.expect("the row has fields");
    let model = proto::ModelId::System(proto::SystemModel::Model);
    let mut operations = proto::OperationSet::from_backends(BTreeMap::from([("lww".into(), backend_operations)]));
    operations.push(proto::Operation::Membership(proto::Membership::Add(model)));
    Ok(proto::Event {
        collection: proto::CollectionId::fixed_name(MODEL_COLLECTION_ID),
        entity_id: EntityId::new(),
        operations,
        parent: proto::Clock::default(),
    })
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
fn expect_registered(resp: proto::NodeResponseBody) -> Vec<proto::RegisteredModel> {
    match resp {
        proto::NodeResponseBody::SchemaRegistered { models } => models,
        other => panic!("expected SchemaRegistered, got {other}"),
    }
}

/// The durable fold runs synchronously under the allocator mutex, but the
/// forwarded-response fold lands in a separate task, so poll until the map
/// resolves the given (collection, name) or time out.
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

/// Park the durable warm's catalog scans and count storage deletions, at
/// main's StorageEngine seam (`collection()` + `delete_all_collections`).
#[derive(Default)]
struct DurableWarmGate {
    warm_scans: AtomicUsize,
    scan_entered: Notify,
    released: AtomicBool,
    release: Notify,
    delete_calls: AtomicUsize,
}

impl DurableWarmGate {
    async fn wait_for_first_scan(&self) {
        loop {
            let notified = self.scan_entered.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.warm_scans.load(Ordering::Acquire) > 0 {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_release(&self) {
        loop {
            let notified = self.release.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release.notify_waiters();
    }
}

struct GatedWarmEngine {
    inner: Arc<SledStorageEngine>,
    gate: Arc<DurableWarmGate>,
}

#[async_trait]
impl StorageEngine for GatedWarmEngine {
    type Value = Vec<u8>;

    async fn collection(&self, id: &proto::CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        let inner = self.inner.collection(id).await?;
        // Gate only the three catalog collections; the system collection must
        // load freely or node construction itself would park.
        if [MODEL_COLLECTION_ID, PROPERTY_COLLECTION_ID, MODEL_PROPERTY_COLLECTION_ID].contains(&id.as_ref()) {
            Ok(Arc::new(GatedCatalogCollection { collection: id.clone(), inner, gate: self.gate.clone() }))
        } else {
            Ok(inner)
        }
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let result = self.inner.delete_all_collections().await;
        self.gate.delete_calls.fetch_add(1, Ordering::AcqRel);
        result
    }
}

struct GatedCatalogCollection {
    collection: proto::CollectionId,
    inner: Arc<dyn StorageCollection>,
    gate: Arc<DurableWarmGate>,
}

#[async_trait]
impl StorageCollection for GatedCatalogCollection {
    async fn set_state(&self, state: proto::Attested<proto::EntityState>) -> Result<bool, MutationError> {
        self.inner.set_state(state).await
    }

    async fn get_state(&self, id: proto::EntityId) -> Result<proto::Attested<proto::EntityState>, RetrievalError> {
        self.inner.get_state(id).await
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<proto::Attested<proto::EntityState>>, RetrievalError> {
        // The warm is recognizable by its full-scan selection; registration's
        // duplicate-check lookups are keyed and pass through. One warm entry
        // per generation is observable on the models catalog, the first
        // collection each warm scans.
        if matches!(selection.predicate, ankql::ast::Predicate::True) {
            if self.collection.as_ref() == MODEL_COLLECTION_ID {
                self.gate.warm_scans.fetch_add(1, Ordering::AcqRel);
                self.gate.scan_entered.notify_waiters();
            }
            if !self.gate.released.load(Ordering::Acquire) {
                self.gate.wait_for_release().await;
            }
        }
        self.inner.fetch_states(selection).await
    }

    async fn add_event(&self, entity_event: &proto::Attested<proto::Event>) -> Result<bool, MutationError> {
        self.inner.add_event(entity_event).await
    }

    async fn get_events(&self, event_ids: Vec<proto::EventId>) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.get_events(event_ids).await
    }

    async fn dump_entity_events(&self, id: proto::EntityId) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.dump_entity_events(id).await
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
    let models = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let (model_id, property_id, membership_id) = (models[0].id, models[0].properties[0].id, models[0].properties[0].membership_id);

    // The executor's synchronous fold resolves the display name to the
    // allocated id.
    let resolved = wait_resolve(&server, "album", "name").await.expect("catalog should resolve album.name on the durable node");
    assert_eq!(resolved, property_id, "the map resolves to the allocated property id");

    // Property definition is parsed.
    let prop = server.catalog.property_by_id(&property_id).expect("property def present");
    assert_eq!(prop.name, "name");
    assert_eq!(prop.backend, "yrs");
    assert_eq!(prop.value_type, "string");
    assert_eq!(prop.minted_for, Some(model_id));

    // Model is indexed by collection at the allocated id.
    let model = server.catalog.model_by_label("album").expect("model def present");
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

    let models = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let model_id = models[0].id;
    wait_resolve(&server, "album", "name").await.expect("name resolves");

    // Second registration adds `year` -- no restart.
    let year = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_year_request()).await?);
    let year_property_id = year[0].properties[0].id;
    let year_id = wait_resolve(&server, "album", "year").await.expect("year resolves incrementally");
    assert_eq!(year_id, year_property_id, "the map resolves year to the allocated id");

    // Both properties are now memberships of the album model.
    let memberships = server.catalog.memberships_of(&model_id);
    assert_eq!(memberships.len(), 2, "album now has two memberships");

    Ok(())
}

// A durable restart can still be scanning catalog storage when a destructive
// reset begins. The old generation holds a warm lease for the complete scan,
// so storage deletion must wait for that drain; otherwise Sled could
// drop/recreate its trees while the scan is using stale collection handles.
// A canceled reset retains its drain for the next attempt, and a concurrent
// reset waits behind the same fence.
#[tokio::test]
async fn hard_reset_drains_in_flight_durable_catalog_warm() -> anyhow::Result<()> {
    let inner = Arc::new(SledStorageEngine::new_test()?);

    // Seed one complete durable epoch, then drop its owners so the gated node
    // below reconstructs from real persisted catalog collections.
    let seed = Node::new_durable(inner.clone(), PermissiveAgent::new());
    seed.system.create().await?;
    let seed_context = seed.context_async(DEFAULT_CONTEXT).await;
    seed_context.register_model::<Album>().await?;
    let seed_probe = seed.catalog.liveness_probe();
    drop(seed_context);
    drop(seed);
    tokio::time::timeout(Duration::from_secs(1), async {
        while seed_probe() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the seed node must release the shared engine before reconstruction");

    let gate = Arc::new(DurableWarmGate::default());
    let node = Node::new_durable(Arc::new(GatedWarmEngine { inner: inner.clone(), gate: gate.clone() }), PermissiveAgent::new());
    tokio::time::timeout(Duration::from_secs(2), gate.wait_for_first_scan())
        .await
        .expect("reconstructed durable warm must reach the gated catalog scan");
    assert!(node.system.is_system_ready(), "the persisted root must be ready before the durable warm scans storage");

    // Reset A parks at the warm's drain: begin_reset invalidates the warm's
    // fence and waits out its lease, so deletion cannot start while the scan
    // is parked in storage.
    let reset_node = node.clone();
    let reset = tokio::spawn(async move { reset_node.system.hard_reset().await });
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert_eq!(gate.delete_calls.load(Ordering::Acquire), 0, "storage deletion must wait for the durable warm lease");
    assert!(!reset.is_finished(), "hard_reset must remain parked behind the durable warm");

    // Cancel reset A at its await point. The catalog must retain the
    // invalidated fence so retry B resumes the same drain instead of seeing
    // `resetting = true`, taking no owner, and deleting immediately.
    reset.abort();
    assert!(reset.await.expect_err("reset A must be canceled").is_cancelled());
    let retry_reset_node = node.clone();
    let retry_reset = tokio::spawn(async move { retry_reset_node.system.hard_reset().await });
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(gate.delete_calls.load(Ordering::Acquire), 0, "a canceled reset's retry must resume its retained drain");
    assert!(!retry_reset.is_finished(), "reset B must remain parked behind reset A's durable fence");

    // Reset C must drain the same retained fence before deleting.
    let second_reset_node = node.clone();
    let second_reset = tokio::spawn(async move { second_reset_node.system.hard_reset().await });
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(gate.delete_calls.load(Ordering::Acquire), 0, "a concurrent reset must not bypass the retained drain");
    assert!(!second_reset.is_finished(), "reset C must wait behind the same drain");

    gate.release();
    tokio::time::timeout(Duration::from_secs(2), retry_reset)
        .await
        .expect("retried hard_reset must complete after the durable warm is released")
        .expect("retried hard_reset task must not panic")?;
    tokio::time::timeout(Duration::from_secs(2), second_reset)
        .await
        .expect("reset C must complete after retry B")
        .expect("reset C task must not panic")?;
    assert_eq!(gate.delete_calls.load(Ordering::Acquire), 2);

    // Recreate the root in place. The ready transition must launch exactly
    // one new-generation warm, after which registration repopulates the
    // freshly-cleared catalog normally.
    node.system.create().await?;
    tokio::time::timeout(Duration::from_secs(2), node.catalog.wait_catalog_ready())
        .await
        .expect("the replacement durable warm must become ready");
    let context = node.context_async(DEFAULT_CONTEXT).await;
    context.register_model::<Album>().await?;
    wait_resolve(&node, "album", "name").await.expect("schema registration must repopulate the replacement catalog");
    assert_eq!(gate.warm_scans.load(Ordering::Acquire), 2, "startup and replacement generations must each warm exactly once");

    Ok(())
}

// An admitted feed callback is an old-epoch effect even after it has sampled
// the current generation. Reset must drain the callback before clearing the
// map so it cannot resume afterward and resurrect the delivered row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_reset_drains_admitted_catalog_feed_update() -> anyhow::Result<()> {
    let node = durable_sled_setup().await?;
    node.catalog.wait_catalog_ready().await;

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    node.catalog.hook_next_feed_apply({
        let entered = entered.clone();
        let release = release.clone();
        move || {
            entered.wait();
            release.wait();
        }
    });

    let event = model_row_creation("old_feed", "OldFeed")?;
    let commit_node = node.clone();
    let commit = tokio::spawn(async move {
        commit_node.commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![proto::Attested::opt(event, None)]).await
    });
    let entered_wait = entered.clone();
    tokio::time::timeout(Duration::from_secs(1), tokio::task::spawn_blocking(move || entered_wait.wait()))
        .await
        .expect("feed callback must reach the post-admission pause")
        .expect("barrier waiter must not panic");

    let reset_node = node.clone();
    let reset = tokio::spawn(async move { reset_node.system.hard_reset().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while node.catalog.registration_epoch_is_current() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reset must invalidate the epoch before waiting for the feed callback");
    assert!(!reset.is_finished(), "reset must drain the admitted feed callback before clearing");

    let release_wait = release.clone();
    tokio::task::spawn_blocking(move || release_wait.wait()).await.expect("release barrier must not panic");
    tokio::time::timeout(Duration::from_secs(1), commit)
        .await
        .expect("feed-producing commit must finish after release")
        .expect("commit task must not panic")?;
    tokio::time::timeout(Duration::from_secs(1), reset)
        .await
        .expect("reset must finish after the callback drains")
        .expect("reset task must not panic")?;

    assert_eq!(node.catalog.counts(), (0, 0, 0), "old feed delivery must not survive reset");
    assert!(node.catalog.model_by_label("old_feed").is_none());
    Ok(())
}

// Forwarded registration may remain pending while reset clears the system.
// Its old response must be rejected at schema ingress, never folded or
// latched into either the cleared epoch or a replacement epoch.
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
    let old_registration = tokio::spawn(async move { old_client.catalog.ensure_registered(&DEFAULT_CONTEXT, Album::descriptor()).await });
    wait_for_count(&held_responses, 1, "old-epoch SchemaRegistered response").await?;

    // Do not cancel the request future: pending-request cleanup is response
    // driven. Reset invalidates its owner, then the held response exercises
    // the real admission rejection and releases the waiter.
    client.system.hard_reset().await?;
    gate.release_first(&client, 1).await;
    let old_result = tokio::time::timeout(Duration::from_secs(1), old_registration)
        .await
        .expect("the rejected old response must release its request future")
        .expect("old registration task must not panic");
    assert!(old_result.is_err(), "old-epoch registration must not succeed after reset");
    assert_eq!(client.catalog.counts(), (0, 0, 0));
    assert!(!client.catalog.is_ensured("album"));

    // Rejoining rearms a fresh registration owner. A second request must go
    // over the wire (not hit a stale ensured latch) and its current response
    // may populate the replacement epoch normally.
    client.system.join_system(server.system.root().expect("server root")).await?;
    let new_client = client.clone();
    let new_registration = tokio::spawn(async move { new_client.catalog.ensure_registered(&DEFAULT_CONTEXT, Album::descriptor()).await });
    wait_for_count(&held_responses, 2, "old and replacement SchemaRegistered responses").await?;
    gate.release_last(&client, 1).await;
    tokio::time::timeout(Duration::from_secs(1), new_registration)
        .await
        .expect("the replacement registration must complete")
        .expect("replacement registration task must not panic")?;
    assert!(client.catalog.is_ensured("album"));
    assert!(wait_resolve(&client, "album", "name").await.is_some());

    Ok(())
}

// A response admitted before reset may fold, but reset must wait until both
// the map update and ensured latch are complete and then clear them together.
// This pins the former check-then-fold TOCTOU directly.
#[tokio::test]
async fn hard_reset_drains_admitted_schema_registration_fold() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    client.catalog.pause_next_registration_fold(entered.clone(), release.clone());

    let registration_client = client.clone();
    let registration =
        tokio::spawn(async move { registration_client.catalog.ensure_registered(&DEFAULT_CONTEXT, Album::descriptor()).await });
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .expect("registration response must pause after acquiring its fold lease");

    let reset_client = client.clone();
    let reset = tokio::spawn(async move { reset_client.system.hard_reset().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while client.catalog.registration_epoch_is_current() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reset must invalidate the epoch before waiting for the fold");
    assert!(!reset.is_finished(), "reset must drain the admitted response fold before clearing");

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), registration)
        .await
        .expect("registration must finish after the fold is released")
        .expect("registration task must not panic")?;
    tokio::time::timeout(Duration::from_secs(1), reset)
        .await
        .expect("reset must finish after the fold lease drains")
        .expect("reset task must not panic")?;

    assert_eq!(client.catalog.counts(), (0, 0, 0), "admitted old-epoch fold must be cleared by reset");
    assert!(!client.catalog.is_ensured("album"), "old-epoch ensured latch must be cleared by reset");
    Ok(())
}

// The reset/readiness hooks installed on SystemManager hold catalog-manager
// clones, and the durable warm runs as a detached task. Neither may retain
// the node: dropping it must release the catalog inner and the node alike,
// on both node kinds.
#[tokio::test]
async fn catalog_hooks_do_not_retain_manager() -> anyhow::Result<()> {
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
    .expect("durable catalog hooks must not form a retain cycle");
    assert!(durable_weak.upgrade().is_none());

    let ephemeral = ephemeral_sled_setup().await?;
    let ephemeral_probe = ephemeral.catalog.liveness_probe();
    let ephemeral_weak = ephemeral.weak();
    drop(ephemeral);
    tokio::time::timeout(Duration::from_secs(1), async {
        while ephemeral_probe() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ephemeral catalog hooks must not form a retain cycle");
    assert!(ephemeral_weak.upgrade().is_none());

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

    // hard_reset must flush the catalog map and clear readiness.
    server.system.hard_reset().await?;

    assert!(!server.catalog.is_catalog_ready(), "catalog not ready after hard_reset");
    assert_eq!(server.catalog.counts(), (0, 0, 0), "catalog map cleared after hard_reset");
    assert!(server.catalog.model_id_for("album").is_none(), "resolve returns nothing after hard_reset");

    Ok(())
}

// Test 6: rename follow-up -- the display-name index updates (old name gone,
// new present) while the allocated property id is unchanged.
#[tokio::test]
async fn rename_updates_resolution_and_sibling_index() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    server.catalog.wait_catalog_ready().await;

    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let property_id = first[0].properties[0].id;
    wait_resolve(&server, "album", "name").await.expect("resolves under original name");

    // Rename: the renamed_from hint moves the display name to "title" WITHOUT
    // re-keying (same allocated property id).
    let mut rename_entry = album_entry("title", "yrs", "string", false);
    rename_entry.properties[0].renamed_from = Some("name".into());
    let rename = proto::NodeRequestBody::RegisterSchema { models: vec![rename_entry] };
    let renamed = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename).await?);
    assert_eq!(renamed[0].properties[0].id, property_id, "the hint preserves the lineage id");

    // New display name resolves to the SAME property id; old name is gone.
    let renamed_id = wait_resolve(&server, "album", "title").await.expect("resolves under new name after rename");
    assert_eq!(renamed_id, property_id, "rename keeps the allocated id (hint-moved lineage)");
    assert!(
        server.catalog.resolve(&server.catalog.model_id_for("album").expect("album remains registered"), "name").is_none(),
        "old display name no longer resolves"
    );

    // The global sibling index reflects the current display name.
    assert_eq!(server.catalog.siblings_by_name("title"), vec![property_id]);
    assert!(server.catalog.siblings_by_name("name").is_empty(), "old name removed from the sibling index");

    Ok(())
}
