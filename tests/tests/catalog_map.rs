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
use ankurah::core::{
    error::{MutationError, RetrievalError},
    livequery::EntityLiveQuery,
    node::MatchArgs,
    property::{backend::lww::LWWBackend, backend::PropertyBackend, PropertyKey},
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

#[derive(Default)]
struct DurableWarmGate {
    list_calls: AtomicUsize,
    list_entered: Notify,
    released: AtomicBool,
    release: Notify,
    delete_calls: AtomicUsize,
    hold_delete: AtomicBool,
    delete_entered: Notify,
    delete_released: AtomicBool,
    delete_release: Notify,
}

impl DurableWarmGate {
    async fn wait_for_first_list(&self) {
        loop {
            let notified = self.list_entered.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.list_calls.load(Ordering::Acquire) > 0 {
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

    async fn wait_for_delete(&self) {
        loop {
            let notified = self.delete_entered.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.delete_calls.load(Ordering::Acquire) > 0 {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_delete_release(&self) {
        loop {
            let notified = self.delete_release.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.delete_released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn release_delete(&self) {
        self.delete_released.store(true, Ordering::Release);
        self.delete_release.notify_waiters();
    }
}

struct GatedListEngine {
    inner: Arc<SledStorageEngine>,
    gate: Arc<DurableWarmGate>,
}

#[derive(Default)]
struct CatalogFetchGate {
    fetch_calls: AtomicUsize,
    fetch_entered: Notify,
    released: AtomicBool,
    release: Notify,
    delete_calls: AtomicUsize,
}

impl CatalogFetchGate {
    async fn wait_for_first_fetch(&self) {
        loop {
            let notified = self.fetch_entered.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.fetch_calls.load(Ordering::Acquire) > 0 {
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

struct GatedCatalogFetchEngine {
    inner: Arc<SledStorageEngine>,
    gate: Arc<CatalogFetchGate>,
}

struct GatedCatalogFetchCollection {
    inner: Arc<dyn StorageCollection>,
    catalog: bool,
    gate: Arc<CatalogFetchGate>,
}

#[async_trait]
impl StorageCollection for GatedCatalogFetchCollection {
    async fn set_state(&self, state: proto::Attested<proto::EntityState>) -> Result<bool, MutationError> {
        self.inner.set_state(state).await
    }

    async fn get_state(&self, id: proto::EntityId) -> Result<proto::Attested<proto::EntityState>, RetrievalError> {
        self.inner.get_state(id).await
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<proto::Attested<proto::EntityState>>, RetrievalError> {
        if self.catalog && self.gate.fetch_calls.fetch_add(1, Ordering::AcqRel) == 0 {
            self.gate.fetch_entered.notify_waiters();
            self.gate.wait_for_release().await;
        }
        self.inner.fetch_states(selection).await
    }

    async fn add_event(&self, event: &proto::Attested<proto::Event>) -> Result<bool, MutationError> { self.inner.add_event(event).await }

    async fn get_events(&self, event_ids: Vec<proto::EventId>) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.get_events(event_ids).await
    }

    async fn dump_entity_events(&self, id: proto::EntityId) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        self.inner.dump_entity_events(id).await
    }
}

#[async_trait]
impl StorageEngine for GatedCatalogFetchEngine {
    type Value = Vec<u8>;

    async fn collection(&self, id: &proto::CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        let inner = <SledStorageEngine as StorageEngine>::collection(self.inner.as_ref(), id).await?;
        Ok(Arc::new(GatedCatalogFetchCollection {
            inner,
            catalog: ankurah::core::schema::is_catalog_collection(id),
            gate: self.gate.clone(),
        }))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        self.gate.delete_calls.fetch_add(1, Ordering::AcqRel);
        <SledStorageEngine as StorageEngine>::delete_all_collections(self.inner.as_ref()).await
    }

    async fn list_collections(&self) -> Result<Vec<proto::CollectionId>, RetrievalError> {
        <SledStorageEngine as StorageEngine>::list_collections(self.inner.as_ref()).await
    }

    fn set_property_resolver(&self, resolver: std::sync::Weak<dyn ankurah::core::property::PropertyResolver>) {
        <SledStorageEngine as StorageEngine>::set_property_resolver(self.inner.as_ref(), resolver);
    }
}

#[async_trait]
impl StorageEngine for GatedListEngine {
    type Value = Vec<u8>;

    async fn collection(&self, id: &proto::CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        <SledStorageEngine as StorageEngine>::collection(self.inner.as_ref(), id).await
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let result = <SledStorageEngine as StorageEngine>::delete_all_collections(self.inner.as_ref()).await;
        self.gate.delete_calls.fetch_add(1, Ordering::AcqRel);
        self.gate.delete_entered.notify_waiters();
        if self.gate.hold_delete.load(Ordering::Acquire) {
            self.gate.wait_for_delete_release().await;
        }
        result
    }

    async fn list_collections(&self) -> Result<Vec<proto::CollectionId>, RetrievalError> {
        // Capture the old engine's names first. The reset barrier must keep
        // those names from being returned after deletion; otherwise the warm
        // would use its stale snapshot to re-materialize dropped trees.
        let collections = <SledStorageEngine as StorageEngine>::list_collections(self.inner.as_ref()).await?;
        if self.gate.list_calls.fetch_add(1, Ordering::AcqRel) == 0 {
            self.gate.list_entered.notify_waiters();
            self.gate.wait_for_release().await;
        }
        Ok(collections)
    }

    fn set_property_resolver(&self, resolver: std::sync::Weak<dyn ankurah::core::property::PropertyResolver>) {
        <SledStorageEngine as StorageEngine>::set_property_resolver(self.inner.as_ref(), resolver);
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

/// Build a model-catalog snapshot that never existed on the serving peer.
/// The stale-generation test injects it into an otherwise genuine held
/// QuerySubscribed response so old and replacement snapshots are observably
/// distinct in both the catalog map and durable cache.
fn forged_model_delta(collection: &str) -> (EntityId, proto::Attested<proto::EntityState>, proto::EntityDelta) {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Name("collection".to_owned()), Some(Value::String(collection.to_owned())));
    backend.set(PropertyKey::Name("name".to_owned()), Some(Value::String("Stale generation".to_owned())));
    let operations = backend.to_operations().unwrap().expect("forged model has fields");
    let event_id = proto::EventId::from_bytes([0x6B; 32]);
    backend.apply_operations_with_event(&operations, event_id.clone()).unwrap();
    let entity_id = EntityId::new();
    let model = ankurah::core::schema::well_known_model_id(ankurah::core::schema::MODEL_COLLECTION_ID)
        .expect("model catalog has a well-known model id");
    let state = proto::Attested::opt(
        proto::EntityState {
            entity_id,
            model,
            state: proto::State {
                state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_owned(), backend.to_state_buffer().unwrap())])),
                head: proto::Clock::from(vec![event_id]),
            },
        },
        None,
    );
    let delta = proto::EntityDelta { entity_id, model, content: proto::DeltaContent::StateSnapshot { state: state.clone().into() } };
    (entity_id, state, delta)
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

// A hard-reset hook may invalidate a detached catalog warm while its remote
// snapshots are still in flight. Releasing that old generation must neither
// repopulate/ready the cleared catalog nor roll back the newer claimant's
// latch; only the new generation's responses may publish.
#[tokio::test]
async fn reset_invalidates_held_catalog_warm_generation() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let server_ctx = server.context_async(DEFAULT_CONTEXT).await;
    server_ctx.register::<Album>().await?;

    let client = ephemeral_sled_setup().await?;
    let held_responses = Arc::new(AtomicUsize::new(0));
    let held_response_keys = Arc::new(Mutex::new(Vec::new()));
    let (_connection, gate) = {
        let held_responses = held_responses.clone();
        let held_response_keys = held_response_keys.clone();
        GatedConnection::new(&server, &client, move |message| match message {
            proto::NodeMessage::Response(response) if matches!(&response.body, proto::NodeResponseBody::QuerySubscribed { .. }) => {
                if let proto::NodeResponseBody::QuerySubscribed { query_id, .. } = &response.body {
                    held_response_keys.lock().unwrap().push((
                        response.request_id.clone(),
                        response.from.clone(),
                        response.to.clone(),
                        query_id.clone(),
                    ));
                }
                held_responses.fetch_add(1, Ordering::AcqRel);
                true
            }
            _ => false,
        })
    };
    client.system.wait_system_ready().await;

    let old_context = {
        let client = client.clone();
        tokio::spawn(async move { client.context_async(DEFAULT_CONTEXT).await })
    };
    wait_for_count(&held_responses, 3, "old-generation catalog responses").await?;

    // Exercise the real destructive ordering: invalidate + drain before
    // storage deletion, then clear system/reactor state and finish the catalog
    // reset. A waiter started in the rootless reset->join gap must remain
    // parked instead of claiming a generation against the old peer.
    client.system.hard_reset().await?;
    assert!(!client.catalog.is_catalog_ready());
    assert_eq!(client.catalog.counts(), (0, 0, 0));
    let replacement_setup = {
        let client = client.clone();
        tokio::spawn(async move { client.catalog.ensure_subscribed(DEFAULT_CONTEXT, &client).await })
    };
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert!(!replacement_setup.is_finished(), "catalog setup must wait for a replacement system root");
    assert_eq!(held_responses.load(Ordering::Acquire), 3, "the rootless gap must not launch a new catalog warm");

    // Rejoin the same authority only so this test can launch a visible
    // replacement generation on the existing gated connection.
    client.system.join_system(server.system.root().expect("server system root")).await?;
    wait_for_count(&held_responses, 6, "old and new catalog responses").await?;

    // Deliver only the replacement generation (the last three responses),
    // leaving the obsolete responses held until after the new listeners and
    // readiness are fully published.
    gate.release_last(&client, 3).await;
    tokio::time::timeout(Duration::from_secs(2), replacement_setup)
        .await
        .expect("the replacement warm must complete from its own responses")
        .expect("the replacement setup task must not panic");
    assert!(client.catalog.is_catalog_ready());
    let replacement_name = wait_resolve(&client, "album", "name").await.expect("the replacement generation must populate the catalog");
    let replacement_counts = client.catalog.counts();

    // Reset cancellation releases the old caller and removes its relay state
    // even though the response is still in the connection gate.
    tokio::time::timeout(Duration::from_secs(1), old_context)
        .await
        .expect("reset must release the obsolete caller")
        .expect("the obsolete context task must not panic");

    // Replace one genuine old response with a forged model snapshot that is
    // absent from the authority and replacement generation. It retains the
    // real old request/query ids, so the only reason it cannot mutate map and
    // storage is old-generation invalidation at the response admission fence.
    let (request_id, from, to, query_id) = held_response_keys.lock().unwrap()[0].clone();
    let (stale_id, stale_state, stale_delta) = forged_model_delta("stale_before_reset");
    client
        .handle_message(proto::NodeMessage::Response(proto::NodeResponse {
            request_id,
            from,
            to,
            body: proto::NodeResponseBody::QuerySubscribed { query_id, deltas: vec![stale_delta] },
            schema: vec![stale_state],
        }))
        .await?;

    // Deliver the remaining genuine stale responses after replacement
    // publication. None may disturb readiness or contaminate the new cache.
    gate.release_held(&client).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(client.catalog.is_catalog_ready(), "stale responses must not disturb replacement readiness");
    assert_eq!(client.catalog.counts(), replacement_counts, "stale responses must not contaminate the replacement catalog");
    assert_eq!(client.catalog.resolve("album", "name"), Some(replacement_name));
    assert!(client.catalog.model_by_collection("stale_before_reset").is_none(), "forged old model must not enter the replacement map");
    let model_storage = client.collections.get(&proto::CollectionId::fixed_name(ankurah::core::schema::MODEL_COLLECTION_ID)).await?;
    assert!(model_storage.get_state(stale_id).await.is_err(), "forged old model must not be persisted after the reset clear");

    Ok(())
}

// A durable restart can still be scanning catalog storage when a destructive
// reset begins. The old generation must retain a reset lease for the complete
// scan, otherwise Sled can drop/recreate its trees while that scan is using
// stale collection handles.
#[tokio::test]
async fn hard_reset_drains_in_flight_durable_catalog_warm() -> anyhow::Result<()> {
    let inner = Arc::new(SledStorageEngine::new_test()?);

    // Seed one complete durable epoch, then drop its owners so the gated node
    // below reconstructs from real persisted catalog collections.
    let seed = Node::new_durable(inner.clone(), PermissiveAgent::new());
    seed.system.create().await?;
    let seed_context = seed.context_async(DEFAULT_CONTEXT).await;
    seed_context.register::<Album>().await?;
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
    let node = Node::new_durable(Arc::new(GatedListEngine { inner: inner.clone(), gate: gate.clone() }), PermissiveAgent::new());
    tokio::time::timeout(Duration::from_secs(2), gate.wait_for_first_list())
        .await
        .expect("reconstructed durable warm must reach the gated collection listing");
    assert!(node.system.is_system_ready(), "the persisted root must be ready before the durable warm scans storage");

    let reset_node = node.clone();
    let reset = tokio::spawn(async move { reset_node.system.hard_reset().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while node.system.is_system_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hard_reset must enter its catalog barrier");

    // Once readiness closes, the reset task runs until it reaches the warm's
    // fence. Deletion is therefore impossible while list_collections remains
    // parked; without the lease it is observed here immediately.
    tokio::task::yield_now().await;
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

    // Reset C must additionally serialize behind retry B's lifecycle guard.
    let second_reset_node = node.clone();
    let second_reset = tokio::spawn(async move { second_reset_node.system.hard_reset().await });
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(gate.delete_calls.load(Ordering::Acquire), 0, "a concurrent reset must not bypass the retained drain");
    assert!(!second_reset.is_finished(), "reset C must wait for reset B's lifecycle guard");

    gate.release();
    tokio::time::timeout(Duration::from_secs(2), retry_reset)
        .await
        .expect("retried hard_reset must complete after the durable warm is released")
        .expect("retried hard_reset task must not panic")?;
    tokio::time::timeout(Duration::from_secs(2), second_reset)
        .await
        .expect("the serialized reset C must complete after retry B")
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
    context.register::<Album>().await?;
    wait_resolve(&node, "album", "name").await.expect("schema registration must repopulate the replacement catalog");
    assert_eq!(gate.list_calls.load(Ordering::Acquire), 2, "startup and replacement generations must each warm exactly once");

    Ok(())
}

// A rootless node has no old root to force a later create through the
// mismatch path. If reset is canceled after engine deletion but before the
// async call returns, create must observe SystemManager's persistent
// incomplete-reset flag, finish the reset, and only then publish a root.
#[tokio::test]
async fn rootless_cancelled_reset_is_resumed_before_create() -> anyhow::Result<()> {
    let gate = Arc::new(DurableWarmGate::default());
    gate.hold_delete.store(true, Ordering::Release);
    let node = Node::new_durable(
        Arc::new(GatedListEngine { inner: Arc::new(SledStorageEngine::new_test()?), gate: gate.clone() }),
        PermissiveAgent::new(),
    );
    tokio::time::timeout(Duration::from_secs(1), node.system.wait_loaded()).await.expect("rootless durable storage must finish loading");
    assert!(node.system.root().is_none());

    let reset_node = node.clone();
    let reset = tokio::spawn(async move { reset_node.system.hard_reset().await });
    tokio::time::timeout(Duration::from_secs(1), gate.wait_for_delete()).await.expect("rootless reset must reach the post-deletion gate");
    assert_eq!(gate.delete_calls.load(Ordering::Acquire), 1);
    reset.abort();
    assert!(reset.await.expect_err("rootless reset must be canceled").is_cancelled());
    assert!(!node.system.is_system_ready());

    // Recovery performs a second deletion/finish before create writes the
    // root. Release both the delete return and the later durable warm.
    gate.release_delete();
    gate.release();
    node.system.create().await?;
    assert_eq!(gate.delete_calls.load(Ordering::Acquire), 2, "create must resume the incomplete reset before publishing its root");
    assert!(node.system.is_system_ready());
    assert!(node.system.root().is_some());
    tokio::time::timeout(Duration::from_secs(1), node.catalog.wait_catalog_ready())
        .await
        .expect("catalog must rearm after recovered reset and root creation");

    Ok(())
}

// A cached ephemeral catalog activation reads local storage before its relay
// snapshot arrives. It belongs to the same catalog generation and must drain
// before hard_reset deletes those local trees.
#[tokio::test]
async fn hard_reset_drains_in_flight_ephemeral_catalog_cache_fetch() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let server_context = server.context_async(DEFAULT_CONTEXT).await;
    server_context.register::<Album>().await?;

    let inner = Arc::new(SledStorageEngine::new_test()?);
    let cached = Node::new(inner.clone(), PermissiveAgent::new());
    let cached_connection = LocalProcessConnection::new(&server, &cached).await?;
    cached.system.wait_system_ready().await;
    cached.context_async(DEFAULT_CONTEXT).await;
    assert!(cached.catalog.is_catalog_ready());
    let cached_probe = cached.catalog.liveness_probe();
    drop(cached_connection);
    drop(cached);
    tokio::time::timeout(Duration::from_secs(1), async {
        while cached_probe() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the cache-seeding client must release the shared engine");

    let gate = Arc::new(CatalogFetchGate::default());
    let client = Node::new(Arc::new(GatedCatalogFetchEngine { inner: inner.clone(), gate: gate.clone() }), PermissiveAgent::new());
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let context_client = client.clone();
    let context = tokio::spawn(async move { context_client.context_async(DEFAULT_CONTEXT).await });
    tokio::time::timeout(Duration::from_secs(2), gate.wait_for_first_fetch())
        .await
        .expect("cached catalog activation must reach the gated local fetch");

    let reset_client = client.clone();
    let reset = tokio::spawn(async move { reset_client.system.hard_reset().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while client.system.is_system_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("hard_reset must enter its catalog barrier");
    tokio::task::yield_now().await;
    assert_eq!(gate.delete_calls.load(Ordering::Acquire), 0, "storage deletion must wait for cached activation's lease");
    assert!(!reset.is_finished(), "hard_reset must remain parked behind the cached catalog fetch");

    gate.release();
    tokio::time::timeout(Duration::from_secs(2), reset)
        .await
        .expect("hard_reset must complete after the cached fetch is released")
        .expect("hard_reset task must not panic")?;
    assert_eq!(gate.delete_calls.load(Ordering::Acquire), 1);
    assert!(!client.catalog.is_catalog_ready());
    assert_eq!(client.catalog.counts(), (0, 0, 0));
    client.system.join_system(server.system.root().expect("server root")).await?;
    tokio::time::timeout(Duration::from_secs(1), context)
        .await
        .expect("reset must release the invalidated context setup")
        .expect("context task must not panic");

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
    let old_registration =
        tokio::spawn(async move { old_client.catalog.ensure_registered(&old_client, &DEFAULT_CONTEXT, Album::schema()).await });
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
    let new_registration =
        tokio::spawn(async move { new_client.catalog.ensure_registered(&new_client, &DEFAULT_CONTEXT, Album::schema()).await });
    wait_for_count(&held_responses, 2, "old and replacement SchemaRegistered responses").await?;
    gate.release_last(&client, 1).await;
    tokio::time::timeout(Duration::from_secs(1), new_registration)
        .await
        .expect("the replacement registration must complete")
        .expect("replacement registration task must not panic")?;
    assert!(client.catalog.is_ensured("album"));
    assert!(client.catalog.resolve("album", "name").is_some());

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

    tokio::time::timeout(Duration::from_secs(2), lq.update_selection_wait("not_a_field = 'x'"))
        .await
        .expect("an unknown typed selection update must not wait for cold-catalog readiness")?;
    lq.error().with(|error| {
        let error = error.as_ref().expect("the unknown property must surface on the live query");
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

    tokio::time::timeout(Duration::from_secs(2), lq.update_selection_wait("not_a_field = 'x'"))
        .await
        .expect("an unknown schema-less update must fail promptly after the catalog is warm")?;
    lq.error().with(|error| {
        let error = error.as_ref().expect("the unknown raw property must surface on the live query");
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
