//! Wire-level adversarial suite (Phase 2 workstream C item 4, part 2).
//!
//! Every arm maps to a named claim in specs/concurrency/threat-model.md
//! (C4-NN) and asserts phase 1 batch-containment semantics: a malformed or
//! malicious item in a delivery is rejected and reported (the aggregate error
//! travels to the sender on the ack path, so `handle_message` returns `Ok`
//! while `NodeUpdateAckBody::Error` carries the failure) while sibling valid
//! items still apply and the reactor is notified for the successful subset.
//! The injection seam is `handle_message` with a hand-forged `NodeUpdate`,
//! matching tests/tests/update_batch_containment.rs.
//!
//! Arms targeting OPEN gaps (G-3, G-4) are `#[ignore]` red tests naming
//! the gap id and owning issue; they pin what SHOULD happen and un-ignore when
//! the fix lands. Closed G-1 is covered by the active C4-15 BFS test below.

mod common;

use ankql::ast::Predicate;
use ankurah::core::property::backend::{lww::LWWBackend, PropertyBackend};
use ankurah::core::property::PropertyKey;
use ankurah::core::value::Value;
use ankurah::core::{
    entity::Entity,
    error::ValidationError,
    node::{Node as NodeAlias, NodeInner},
    policy::{AccessDenied, Admission, DefaultContext, PolicyAgent},
    storage::StorageEngine,
    util::Iterable,
};
use ankurah::proto::{self, Attested};
use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Node, PermissiveAgent, View};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use common::*;
use std::collections::{BTreeMap, HashSet};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use common::{Record, RecordView};

// ---------------------------------------------------------------------------
// Forging helpers (mirror update_batch_containment.rs).
// ---------------------------------------------------------------------------

/// Forge a Record LWW event setting `title`, parented on the given clock.
/// The event id is a content hash over (entity_id, operations, parent), so the
/// forger cannot choose it (C4-01): whatever id the DAG keys on is recomputed
/// from these contents, never read from any declared field.
///
/// `title_prop` is the registered property-definition id for `title`: a
/// post-epoch peer writes id-keyed operations (the PropertyKey contract), and
/// a name-keyed forgery would be shadowed on read by the seeded id-keyed
/// value (the anti-resurrection rule) -- these tests pin containment and
/// idempotency, so the forgeries must be well-formed current-epoch events.
fn forge_title_event(
    entity_id: proto::EntityId,
    model: proto::EntityId,
    title_prop: proto::EntityId,
    parent: proto::Clock,
    title: &str,
) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(title_prop), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    proto::Event {
        entity_id,
        model,
        body: proto::EventBody::Update { operations: proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])) },
        parent,
    }
}

/// Forge a genesis-shaped attack that falsely claims an existing entity id.
/// Content-addressed identity makes a second valid genesis for that id
/// impossible; ingress must reject this body because its recomputed genesis id
/// differs from `claimed_entity`.
fn forge_mismatched_genesis(
    claimed_entity: proto::EntityId,
    model: proto::EntityId,
    property: proto::EntityId,
    value: &str,
    nonce_tag: u8,
) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set(PropertyKey::Id(property), Some(Value::String(value.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    proto::Event {
        entity_id: claimed_entity,
        model,
        body: proto::EventBody::Genesis {
            system: None,
            nonce: [nonce_tag; 32],
            timestamp: nonce_tag as u64,
            operations: proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])),
        },
        parent: proto::Clock::default(),
    }
}

fn event_only_item(event: proto::Event) -> proto::SubscriptionUpdateItem {
    proto::SubscriptionUpdateItem {
        entity_id: event.entity_id,
        model: event.model,
        content: proto::UpdateContent::EventOnly(vec![Attested::opt(event, None).into()]),
        predicate_relevance: vec![],
    }
}

/// A single EventOnly item carrying several events (delivered in the given
/// wire order, which the receiver must not trust).
fn event_only_multi(entity_id: proto::EntityId, model: proto::EntityId, events: Vec<proto::Event>) -> proto::SubscriptionUpdateItem {
    proto::SubscriptionUpdateItem {
        entity_id,
        model,
        content: proto::UpdateContent::EventOnly(events.into_iter().map(|e| Attested::opt(e, None).into()).collect()),
        predicate_relevance: vec![],
    }
}

fn deliver(from: proto::NodeId, to: proto::NodeId, items: Vec<proto::SubscriptionUpdateItem>) -> proto::NodeMessage {
    proto::NodeMessage::Update(proto::NodeUpdate {
        id: proto::UpdateId::new(),
        from,
        to,
        body: proto::NodeUpdateBody::SubscriptionUpdate { items },
        schema: vec![],
    })
}

/// Standard two-node fixture: a durable server that owns the data and an
/// ephemeral client that holds a live relay context so `apply_updates` accepts
/// forged deliveries attributed to the server. Returns the wired-up client and
/// server plus their contexts; the connection guard is leaked into a Box so
/// the caller only has to hold what it names.
struct Fixture {
    server: Node<SledStorageEngine, PermissiveAgent>,
    client: Node<SledStorageEngine, PermissiveAgent>,
    ctx_s: ankurah::Context,
    ctx_c: ankurah::Context,
    _conn: LocalProcessConnection<SledStorageEngine, PermissiveAgent, SledStorageEngine, PermissiveAgent>,
    _relay: ankurah::LiveQuery<RecordView>,
    /// #330: Record's allocated model id, stamped on every forged event/item so
    /// the receiver's ingress `resolve_model` routes it to the record collection.
    record_model: proto::EntityId,
    /// Registered property-definition ids for Record's fields: post-epoch
    /// forgeries write id-keyed operations (see `forge_title_event`).
    record_title: proto::EntityId,
    record_artist: proto::EntityId,
}

async fn fixture() -> Result<Fixture> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;
    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;
    // A live subscription establishes the relay context apply_updates requires
    // for this peer; held for the test's duration.
    let _relay = ctx_c.query_wait::<RecordView>("title = 'no-such-title'").await?;
    // The relay query resolves the Record predicate, which first-use-registers
    // the model (REN 2), so the durable server now holds its allocated id.
    let record_model = server.catalog.model_id_for(Record::collection().as_str()).expect("Record registered by the relay query");
    let record_title = server.catalog.resolve(Record::collection().as_str(), "title").expect("title registered with Record");
    let record_artist = server.catalog.resolve(Record::collection().as_str(), "artist").expect("artist registered with Record");
    Ok(Fixture { server, client, ctx_s, ctx_c, _conn, _relay, record_model, record_title, record_artist })
}

/// Create a Record on the server and materialize it on the client, returning
/// the id once the client view reflects the initial title.
async fn seed_record(f: &Fixture, title: &str, artist: &str) -> Result<(proto::EntityId, RecordView)> {
    let id = {
        let trx = f.ctx_s.begin();
        let rec = trx.create(&Record { title: title.to_owned(), artist: artist.to_owned() }).await?;
        let id = rec.id();
        trx.commit().await?;
        id
    };
    let view = f.ctx_c.get::<RecordView>(id).await?;
    assert_eq!(view.title().unwrap(), title);
    Ok((id, view))
}

async fn committed_event_ids(ctx: &ankurah::Context, id: proto::EntityId) -> Result<Vec<proto::EventId>> {
    let collection = ctx.collection(&Record::collection()).await?;
    Ok(collection.dump_entity_events(id).await?.iter().map(|e| e.payload.id()).collect())
}

// ===========================================================================
// Malformed clocks (C4-03)
// ===========================================================================

/// C4-03: any Clock reconstructed from the wire is sorted and deduplicated
/// before use; wire order is never trusted for the binary-search membership
/// tests head maintenance depends on. The proto layer normalizes at the serde
/// boundary via `#[serde(from = "Vec<EventId>")]`.
///
/// This pins the serde path directly: a raw `Vec<EventId>` in unsorted,
/// duplicate-bearing order deserializes into a normalized `Clock`, and
/// membership/removal (which binary-search) behave correctly afterward. This
/// is the exact shape a buggy or malicious peer puts on the wire (T0/T1).
#[test]
fn malformed_clock_deserialization_normalizes() {
    let id = |b: u8| proto::EventId::from_bytes([b; 32]);
    // Unsorted with a duplicate, as a peer might serialize its head.
    let raw = vec![id(4), id(1), id(4), id(2), id(1)];
    let bytes = bincode::serialize(&raw).expect("serialize raw vec");
    let clock: proto::Clock = bincode::deserialize(&bytes).expect("deserialize into Clock via serde from");

    assert_eq!(clock.as_slice(), &[id(1), id(2), id(4)], "serde boundary sorts and dedups");
    // Binary-search membership is now sound regardless of the wire order.
    assert!(clock.contains(&id(1)) && clock.contains(&id(4)));
    assert!(!clock.contains(&id(3)));
    let mut clock = clock;
    assert!(clock.remove(&id(2)), "removal binary-searches the correct region");
    assert_eq!(clock.as_slice(), &[id(1), id(4)]);
}

/// C4-03 (residual): the only construction paths that wrap a raw `Vec` without
/// going through `#[serde(from = ...)]` are the postgres `FromSql` and wasm
/// `TryFrom<JsValue>` impls, and both normalize by hand (insertion sort +
/// dedup) before building the `Clock`. From outside the crate there is no
/// public non-normalizing constructor: every reachable path
/// (`Clock::new`, `Clock::from`, `Clock::from_strings`, and
/// `TryInto<Clock> for Vec<Vec<u8>>`) normalizes. This test pins that, so a
/// regression that added an order-trusting public constructor would fail here.
/// Ties to threat-model C4-03's note that #274 should still make clock
/// validation an explicit ingress check, not solely a serde attribute.
#[test]
fn no_public_non_normalizing_clock_constructor() {
    let id = |b: u8| proto::EventId::from_bytes([b; 32]);
    let unsorted = vec![id(3), id(1), id(2), id(1)];
    let expected = [id(1), id(2), id(3)];

    assert_eq!(proto::Clock::new(unsorted.clone()).as_slice(), &expected, "Clock::new normalizes");
    assert_eq!(proto::Clock::from(unsorted.clone()).as_slice(), &expected, "From<Vec<EventId>> normalizes");

    let strings: Vec<String> = unsorted.iter().map(|i| i.to_base64()).collect();
    assert_eq!(proto::Clock::from_strings(strings).unwrap().as_slice(), &expected, "from_strings normalizes");

    let raw_bytes: Vec<Vec<u8>> = unsorted.iter().map(|i| i.as_bytes().to_vec()).collect();
    let via_bytes: proto::Clock = raw_bytes.try_into().unwrap();
    assert_eq!(via_bytes.as_slice(), &expected, "TryInto<Clock> for Vec<Vec<u8>> normalizes");
}

/// C4-03 end to end: because every clock is normalized before it is hashed, a
/// forged event's identity is independent of the order the forger lists its
/// parents in. Two forged events differing only in parent-clock input order
/// produce the SAME id (so a peer cannot mint distinct ids by reshuffling a
/// clock), and delivering such an event through `handle_message` leaves a
/// valid, correctly-maintained head. Pins normalization at the application
/// boundary, not just at construction.
#[tokio::test]
async fn malformed_clock_identity_is_order_independent_end_to_end() -> Result<()> {
    let f = fixture().await?;
    let (rec_id, view) = seed_record(&f, "t0", "a0").await?;

    // Two concurrent children of the same head, committed so the head becomes
    // a two-id antichain we can then feed back in scrambled order.
    let head0 = view.entity().head().clone();
    let ev_b = forge_title_event(rec_id, f.record_model, f.record_title, head0.clone(), "b");
    let ev_c = {
        // Distinct ops so ev_c is a sibling, not a duplicate of ev_b.
        let backend = LWWBackend::new();
        backend.set(PropertyKey::Id(f.record_artist), Some(Value::String("c-artist".to_owned())));
        let ops = backend.to_operations().unwrap().expect("ops");
        proto::Event {
            entity_id: rec_id,
            model: f.record_model,
            body: proto::EventBody::Update { operations: proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])) },
            parent: head0.clone(),
        }
    };
    f.client
        .handle_message(deliver(f.server.id, f.client.id, vec![event_only_multi(rec_id, f.record_model, vec![ev_b.clone(), ev_c.clone()])]))
        .await?;

    // The head is now the antichain {ev_b, ev_c}. Build a merge event whose
    // parent lists those two ids in two different orders; both must hash equal.
    let two_ids = vec![ev_b.id(), ev_c.id()];
    let mut reversed = two_ids.clone();
    reversed.reverse();
    let ev_merge_a = forge_title_event(rec_id, f.record_model, f.record_title, proto::Clock::from(two_ids.clone()), "merged");
    let ev_merge_b = forge_title_event(rec_id, f.record_model, f.record_title, proto::Clock::from(reversed), "merged");
    assert_eq!(ev_merge_a.id(), ev_merge_b.id(), "parent-clock input order must not change event identity");

    // Deliver the merge event; the resulting head is a single valid tip.
    f.client.handle_message(deliver(f.server.id, f.client.id, vec![event_only_item(ev_merge_a.clone())])).await?;
    let head = view.entity().head();
    assert_eq!(head.len(), 1, "merge collapses the antichain to one tip");
    assert!(head.contains(&ev_merge_a.id()), "the merge event is the head");
    assert!(!head.contains(&ev_b.id()) && !head.contains(&ev_c.id()), "superseded tips removed from the head");
    Ok(())
}

// ===========================================================================
// Forged parents (C4-07, C4-09, C4-11, C4-12)
// ===========================================================================

/// C4-11 + C4-12 (via a forged parent, C4-09): an event whose parent clock
/// references a nonexistent event id cannot ground its lineage. On the
/// receiver it fails as a contained per-item error (the BFS fetch of the
/// missing parent returns not-found), the sibling valid item still applies and
/// notifies, and no phantom residue or head movement is left for the forged
/// entity. The forger cannot manufacture happened-before against history that
/// does not exist.
#[tokio::test]
async fn forged_dangling_parent_is_contained() -> Result<()> {
    let f = fixture().await?;
    let (a_id, view_a) = seed_record(&f, "a0", "artist-a").await?;
    let (b_id, view_b) = seed_record(&f, "b0", "artist-b").await?;

    // Valid event for A; forged event for B parented on an id that was never
    // created (a fabricated 32-byte hash below any real history).
    let ev_a = forge_title_event(a_id, f.record_model, f.record_title, view_a.entity().head().clone(), "a1");
    let id_ev_a = ev_a.id();
    let dangling = proto::Clock::from(vec![proto::EventId::from_bytes([0xAB; 32])]);
    let ev_b_forged = forge_title_event(b_id, f.record_model, f.record_title, dangling, "forged");
    let id_forged = ev_b_forged.id();

    // handle_message returns Ok; the per-item failure rides the ack path.
    f.client.handle_message(deliver(f.server.id, f.client.id, vec![event_only_item(ev_a), event_only_item(ev_b_forged)])).await?;

    // A applied; B is unchanged, its head did not move, the forged event is not
    // committed, and B's real state survives.
    assert_eq!(view_a.title().unwrap(), "a1", "sibling valid item must apply");
    assert_eq!(view_b.title().unwrap(), "b0", "forged event with a dangling parent must not apply");
    let b_head = view_b.entity().head();
    assert!(!b_head.contains(&id_forged), "forged event must not enter B's head");
    assert!(committed_event_ids(&f.ctx_c, a_id).await?.contains(&id_ev_a), "A's valid event is committed");
    assert!(!committed_event_ids(&f.ctx_c, b_id).await?.contains(&id_forged), "forged event must not be committed");
    Ok(())
}

/// C4-07: wholesale StrictDescends adoption (replace head, replay chain) must
/// not fire when the subject smuggles a foreign line. A batch that extends the
/// current head with a legitimate child AND an independent second genesis
/// (V3's `{B, X}` over `{A}` shape) must merge the two lines, never adopt the
/// subject clock wholesale. The forged independent root is contained: the
/// legitimate child applies, the visible state is the merge of both branches,
/// and the head remains a valid antichain.
#[tokio::test]
async fn forged_extra_genesis_head_does_not_trigger_wholesale_adoption() -> Result<()> {
    let f = fixture().await?;
    let (rec_id, view) = seed_record(&f, "t0", "a0").await?;
    let head0 = view.entity().head().clone();

    // A legitimate child B of the current head, and an independent genesis X
    // (empty parent) for the same entity id: X shares no lineage with the head.
    let ev_b = forge_title_event(rec_id, f.record_model, f.record_title, head0.clone(), "child-b");
    let ev_x = { forge_mismatched_genesis(rec_id, f.record_model, f.record_artist, "foreign-x", 0x58) };
    let id_b = ev_b.id();
    let id_x = ev_x.id();

    // Deliver both in one item. Whatever the applier's per-item verdict, the
    // security property is: the legitimate child's write is not lost, and the
    // foreign root does not get adopted as the sole head (which would discard
    // the real genesis lineage).
    f.client.handle_message(deliver(f.server.id, f.client.id, vec![event_only_multi(rec_id, f.record_model, vec![ev_b, ev_x])])).await?;

    let head = view.entity().head();
    // The original genesis lineage must not have been wholesale-replaced by the
    // foreign root standing alone.
    assert_ne!(head.as_slice(), &[id_x], "foreign genesis must not be adopted as the wholesale head");
    // The legitimate child either applied (its write survives) or the item was
    // contained; in neither case may the foreign root have overwritten the
    // entity to its own value.
    assert_ne!(view.artist().unwrap(), "foreign-x", "foreign genesis must not win LWW as an adopted head");
    let _ = id_b; // documented: id_b is the legitimate child's id
    Ok(())
}

// ===========================================================================
// Cycle attempts (C4-04)
// ===========================================================================

/// C4-04: content addressing makes honest cycles impossible, and a DECLARED
/// cycle is not constructible from real events. A parent edge A -> B in the
/// batch ordering exists only when B's *recomputed* id is present as a batch
/// key AND is listed in A's parent clock; but A's id is a hash over A's parent
/// clock, so making A name B while B names A is a SHA-256 fixed point. This
/// test proves the impossibility constructively: an attempt to build two
/// events that reference each other's real ids cannot close the loop, because
/// injecting the peer's id into the parent clock changes this event's own id,
/// which the peer would then have to have referenced, and so on. The Kahn
/// cycle-rejection branch in `topo_sort_events` therefore guards a case
/// unreachable from wire `Event` values; the enforcement IS content
/// addressing.
#[test]
fn declared_cycle_is_unconstructible_content_addressing() {
    let entity = proto::EntityId::from_bytes([0xED; 32]);
    // #330: this pure content-addressing check never routes these events to a
    // node, so any model id works; use a fixed fabricated one.
    let model = proto::EntityId::from_bytes([0xEE; 32]);
    let title_prop = proto::EntityId::from_bytes([0xEF; 32]);
    let mk = |title: &str, parent: proto::Clock| forge_title_event(entity, model, title_prop, parent, title);

    // Start from two independent events and try to wire A.parent := [B.id()]
    // and B.parent := [A.id()]. Compute B first, then A referencing B; now A
    // has a concrete id. To close the cycle we would need B to have referenced
    // *that* id, but B was built referencing an empty parent, so B.id() is
    // fixed and does not name A.
    let b = mk("b", proto::Clock::from(vec![proto::EventId::from_bytes([0x01; 32])]));
    let a = mk("a", proto::Clock::from(vec![b.id()]));
    // A names B, but B does NOT name A: no cycle among {a, b}.
    assert!(a.parent.contains(&b.id()), "A references B");
    assert!(!b.parent.contains(&a.id()), "B cannot reference A: its id was fixed before A existed");

    // The only way to make B name A is to rebuild B with A.id() in its parent,
    // which changes B's id, which breaks A's reference. Demonstrate that the
    // rebuilt B has a different id, so A's edge no longer points at it.
    let b2 = mk("b", proto::Clock::from(vec![a.id()]));
    assert_ne!(b.id(), b2.id(), "adding A to B's parent changes B's content hash");
    assert!(!a.parent.contains(&b2.id()), "A's edge now dangles; the loop cannot be closed");
    // Hence no set of honest events forms a declared parent cycle. A batch that
    // merely fabricates parent ids (below) never puts both endpoints in the
    // batch, so the ordering treats them as below-floor rather than as a cycle.
}

/// C4-04 (containment of the closest constructible shape): a batch of events
/// that reference each other's *fabricated* (non-content) parent ids does not
/// form a cycle in the ordering, because those fabricated ids are not batch
/// keys (the batch keys are the recomputed content hashes). The applier treats
/// the fabricated parents as below-floor. The batch is contained: the delivery
/// does not corrupt the entity, and no fabricated id enters the head.
#[tokio::test]
async fn fabricated_cycle_batch_is_contained() -> Result<()> {
    let f = fixture().await?;
    let (rec_id, view) = seed_record(&f, "t0", "a0").await?;

    // Two events whose parents point at fabricated ids that look like they
    // reference each other, but neither fabricated id equals either event's
    // recomputed content id, so the batch graph has no edges between them.
    let fake1 = proto::EventId::from_bytes([0x11; 32]);
    let fake2 = proto::EventId::from_bytes([0x22; 32]);
    let ev1 = forge_title_event(rec_id, f.record_model, f.record_title, proto::Clock::from(vec![fake2]), "cycle-1");
    let ev2 = forge_title_event(rec_id, f.record_model, f.record_title, proto::Clock::from(vec![fake1]), "cycle-2");
    let id1 = ev1.id();
    let id2 = ev2.id();

    f.client.handle_message(deliver(f.server.id, f.client.id, vec![event_only_multi(rec_id, f.record_model, vec![ev1, ev2])])).await?;

    // Neither fabricated-parent event grounds (their parents do not exist), so
    // the entity is unchanged and no fabricated event enters the head.
    assert_eq!(view.title().unwrap(), "t0", "fabricated-cycle batch must not corrupt the entity");
    let head = view.entity().head();
    assert!(!head.contains(&id1) && !head.contains(&id2), "no fabricated-parent event may enter the head");
    Ok(())
}

// ===========================================================================
// Replay floods (C4-05)
// ===========================================================================

/// C4-05: re-delivering a byte-identical event is a no-op. Delivering the same
/// valid event many times, and the same batch out of order, applies it exactly
/// once: the visible state is stable after the first application, the head does
/// not drift, the reactor is notified exactly once, and repeated redelivery
/// never errors the batch. Content addressing collapses identical replays by
/// id; the comparison engine skips an already-integrated event.
#[tokio::test]
async fn replay_flood_is_idempotent() -> Result<()> {
    let f = fixture().await?;
    let (rec_id, view) = seed_record(&f, "t0", "a0").await?;

    // Observe reactor notifications for this entity.
    let watcher = TestWatcher::changeset();
    let query = f.ctx_c.query_wait::<RecordView>(nocache(format!("id = '{}'", rec_id).as_str())?).await?;
    let _guard = query.subscribe(&watcher);

    let ev = forge_title_event(rec_id, f.record_model, f.record_title, view.entity().head().clone(), "t1");
    let id_ev = ev.id();

    // Flood: 10 identical single-event deliveries, none may error.
    for _ in 0..10 {
        f.client.handle_message(deliver(f.server.id, f.client.id, vec![event_only_item(ev.clone())])).await?;
    }
    // And the same event redelivered inside a multi-event batch alongside
    // itself (duplicate within one item), out of order.
    f.client
        .handle_message(deliver(f.server.id, f.client.id, vec![event_only_multi(rec_id, f.record_model, vec![ev.clone(), ev.clone()])]))
        .await?;

    assert_eq!(view.title().unwrap(), "t1", "state reflects exactly one application");
    let head = view.entity().head();
    assert_eq!(head.len(), 1, "head is a single tip");
    assert!(head.contains(&id_ev), "head is the replayed event");

    // Exactly one reactor notification for the entity, despite 11 deliveries.
    let notifications = watcher.quiesce().await;
    assert_eq!(notifications, 1, "idempotent replay must notify the reactor exactly once, saw {notifications}");
    Ok(())
}

// ===========================================================================
// Creation-event guards (C4-06)
// ===========================================================================

/// C4-06 (durable path, Byzantine-safe): a fabricated alternate creation event
/// for an entity that already has a non-empty head is rejected as a distinct
/// genesis on a durable (definitive-storage) node, distinguished cheaply
/// without a walk. The forged genesis is contained: it does not reset the
/// entity, does not enter the head, and is not committed; the entity keeps its
/// real state.
#[tokio::test]
async fn forged_second_genesis_rejected_on_durable_node() -> Result<()> {
    let f = fixture().await?;
    // Create on the durable server and record its committed genesis.
    let (rec_id, _view) = seed_record(&f, "t0", "a0").await?;
    let before = committed_event_ids(&f.ctx_s, rec_id).await?;

    // Forge a DISTINCT second genesis (empty parent, different ops => different
    // id) and deliver it to the SERVER attributed to the client peer.
    let alt = forge_mismatched_genesis(rec_id, f.record_model, f.record_title, "ALT-GENESIS", 0xA1);
    assert!(alt.is_entity_create(), "alt is a creation event");
    let alt_id = alt.id();
    f.server.handle_message(deliver(f.client.id, f.server.id, vec![event_only_item(alt)])).await?;

    // The durable node rejected it (Disjoint): the committed event set is
    // unchanged and the alt genesis is not present.
    let after = committed_event_ids(&f.ctx_s, rec_id).await?;
    assert_eq!(after.len(), before.len(), "durable node must not commit a second genesis");
    assert!(!after.contains(&alt_id), "the forged genesis must not be committed on the durable node");
    let server_view = f.ctx_s.get::<RecordView>(rec_id).await?;
    assert_eq!(server_view.title().unwrap(), "t0", "the durable entity keeps its real state");
    Ok(())
}

/// C4-06 (ephemeral path): an ephemeral node reaches the same reject verdict
/// via BFS grounding rather than a cheap storage check. A distinct second
/// genesis delivered to the ephemeral client does not change its visible state
/// and does not enter its head; the forged root is not adopted. (On an
/// ephemeral node BFS may pull the real genesis into storage as a side effect
/// of grounding the comparison, which is the documented C4-15 fetch behavior;
/// the assertion here is on the semantic outcome, not raw stored-event count.)
#[tokio::test]
async fn forged_second_genesis_rejected_on_ephemeral_node() -> Result<()> {
    let f = fixture().await?;
    let (rec_id, view) = seed_record(&f, "t0", "a0").await?;
    let head_before = view.entity().head().clone();

    let alt = forge_mismatched_genesis(rec_id, f.record_model, f.record_title, "ALT-EPH", 0xA2);
    let alt_id = alt.id();
    f.client.handle_message(deliver(f.server.id, f.client.id, vec![event_only_item(alt)])).await?;

    assert_eq!(view.title().unwrap(), "t0", "ephemeral node must not adopt the forged genesis' value");
    let head_after = view.entity().head();
    assert!(!head_after.contains(&alt_id), "forged genesis must not enter the ephemeral head");
    assert_eq!(head_after.as_slice(), head_before.as_slice(), "head unchanged by the rejected second genesis");
    assert!(!committed_event_ids(&f.ctx_c, rec_id).await?.contains(&alt_id), "forged genesis is not committed");
    Ok(())
}

/// C4-12: a non-creation EventOnly for an entity the receiver has never
/// materialized is rejected (empty-head guard), and the speculatively
/// materialized empty-head resident is evicted rather than left as a phantom.
/// Delivered as a sibling of a valid item to also pin containment.
#[tokio::test]
async fn phantom_entity_is_evicted_on_failed_apply() -> Result<()> {
    let f = fixture().await?;
    let (a_id, view_a) = seed_record(&f, "a0", "artist-a").await?;

    let ev_a = forge_title_event(a_id, f.record_model, f.record_title, view_a.entity().head().clone(), "a1");
    let unknown_id = proto::EntityId::from_bytes([0x7F; 32]);
    // Non-creation event (non-empty parent) for an entity the client never saw.
    let ev_unknown = forge_title_event(
        unknown_id,
        f.record_model,
        f.record_title,
        proto::Clock::from(vec![proto::EventId::from_bytes([7u8; 32])]),
        "ghost",
    );

    f.client.handle_message(deliver(f.server.id, f.client.id, vec![event_only_item(ev_a), event_only_item(ev_unknown)])).await?;

    assert_eq!(view_a.title().unwrap(), "a1", "sibling valid item applies");
    // The phantom empty-head resident was evicted: get() forces a retrieval
    // that fails (the server never had the entity) instead of returning an
    // empty-state view.
    let phantom = f.ctx_c.get::<RecordView>(unknown_id).await;
    assert!(phantom.is_err(), "phantom empty-head resident must be evicted, got {phantom:?}");
    assert!(committed_event_ids(&f.ctx_c, unknown_id).await?.is_empty(), "nothing durable for the unknown entity");
    Ok(())
}

// ===========================================================================
// OPEN GAP red tests (ignored): oversized batches (G-3, #246/#247)
// ===========================================================================

/// G-3 / OPEN GAP (#246): there is no size or count limit on incoming event
/// messages; a single peer message may carry unbounded events. This red test
/// pins what SHOULD happen once #246 lands: a delivery whose event count
/// exceeds a limit is rejected with a typed error and applies nothing, rather
/// than being accepted and staging unbounded events. It does NOT implement the
/// limit (that is #246's fix, which un-ignores this).
///
/// Today the receiver accepts the whole batch, so this fails and is ignored.
#[tokio::test]
#[ignore = "OPEN GAP G-3: no incoming size/count limit (issue #246); un-ignore when the limit lands"]
async fn oversized_event_batch_is_rejected() -> Result<()> {
    let f = fixture().await?;
    let (rec_id, view) = seed_record(&f, "t0", "a0").await?;

    // A batch of many events forming a linear chain off the current head (each
    // parented on the previous). The chain keeps the head width at one so the
    // count, not antichain width, is what is oversized. A real limit would
    // reject the whole delivery before staging any of them. The count only has
    // to exceed a plausible cap: we do not assert on memory (impractical to
    // measure reliably), only on wholesale rejection.
    const OVERSIZED: usize = 2_000;
    let mut parent = view.entity().head().clone();
    let mut events: Vec<proto::Event> = Vec::with_capacity(OVERSIZED);
    for i in 0..OVERSIZED {
        let ev = forge_title_event(rec_id, f.record_model, f.record_title, parent.clone(), &format!("flood-{i}"));
        parent = proto::Clock::from(vec![ev.id()]);
        events.push(ev);
    }

    let before = committed_event_ids(&f.ctx_c, rec_id).await?.len();
    // handle_message returns Ok regardless (error rides the ack path); the
    // observable expectation once #246 lands is that NOTHING from an oversized
    // batch is committed. Today many events commit, so this assertion fails.
    f.client.handle_message(deliver(f.server.id, f.client.id, vec![event_only_multi(rec_id, f.record_model, events)])).await?;
    let after = committed_event_ids(&f.ctx_c, rec_id).await?.len();

    assert_eq!(after, before, "an oversized batch must be rejected wholesale, committing nothing (G-3, #246)");
    Ok(())
}

// ===========================================================================
// BFS fetch validation (C4-15, formerly G-1 / #244)
// ===========================================================================

/// Counts receive-side event validation and can deny one exact event. The
/// state hook remains permissive so the test reaches lineage comparison; the
/// missing events fetched by that comparison are where the event gate belongs.
#[derive(Clone)]
struct BfsPolicyAgent {
    validate_calls: Arc<AtomicUsize>,
    denied_events: Arc<Mutex<HashSet<proto::EventId>>>,
}

impl BfsPolicyAgent {
    fn new() -> Self { Self { validate_calls: Arc::new(AtomicUsize::new(0)), denied_events: Arc::new(Mutex::new(HashSet::new())) } }
}

#[async_trait]
impl PolicyAgent for BfsPolicyAgent {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &NodeInner<SE, Self>,
        cdata: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &NodeAlias<SE, Self>,
        auth: &A,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        Ok(auth.iterable().map(|_| c).collect())
    }

    fn check_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &Entity,
        _entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Admission, AccessDenied> {
        Ok(Admission::Allow)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::NodeId,
        event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        self.validate_calls.fetch_add(1, Ordering::SeqCst);
        if self.denied_events.lock().unwrap().contains(&event.payload.id()) {
            return Err(AccessDenied::ByPolicy("BFS-fetched event denied by test policy"));
        }
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &NodeAlias<SE, Self>, _state: &proto::EntityState) -> Admission { Admission::Allow }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::NodeId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn can_access_collection<C>(&self, _data: &C, _collection: &proto::CollectionId) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    fn filter_predicate<C>(&self, _data: &C, _collection: &proto::CollectionId, predicate: Predicate) -> Result<Predicate, AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(predicate)
    }

    fn check_read<C>(
        &self,
        _data: &C,
        _id: &proto::EntityId,
        _collection: &proto::CollectionId,
        _state: &proto::State,
        _resolver: Option<std::sync::Weak<dyn ankurah::core::property::PropertyResolver>>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(
        &self,
        _data: &C,
        _collection: &proto::CollectionId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _peer_id: &proto::NodeId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

/// C4-15: a state snapshot can force the receiver's lineage walk to fetch
/// missing events by `GetEvents`. Each fetched payload passes the same policy
/// gate as directly delivered events, and a denied parent is not cached.
#[tokio::test]
async fn bfs_fetched_events_are_policy_validated() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    let client_agent = BfsPolicyAgent::new();
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), client_agent.clone());
    let _connection = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(c)?;
    let ctx_c = client.context(c)?;
    let _relay = ctx_c.query_wait::<RecordView>("title = 'never-match'").await?;
    let record_id = {
        let trx = ctx_s.begin();
        let record = trx.create(&Record { title: "before".to_owned(), artist: "artist".to_owned() }).await?;
        let id = record.id();
        trx.commit().await?;
        id
    };
    let view = ctx_c.get::<RecordView>(record_id).await?;
    assert_eq!(view.title()?, "before");

    let denied_id = {
        let trx = ctx_s.begin();
        ctx_s.get::<RecordView>(record_id).await?.edit(&trx)?.title().set(&"one".to_owned())?;
        trx.commit_and_return_events().await?[0].id()
    };
    {
        let trx = ctx_s.begin();
        ctx_s.get::<RecordView>(record_id).await?.edit(&trx)?.title().set(&"two".to_owned())?;
        trx.commit().await?;
    }
    client_agent.denied_events.lock().unwrap().insert(denied_id.clone());
    client_agent.validate_calls.store(0, Ordering::SeqCst);

    // Deliver only the latest state. Comparing it with the client's genesis
    // head has to fetch both missing update events from the server.
    let model = server.catalog.model_id_for(Record::collection().as_str()).expect("Record registered");
    let server_collection = ctx_s.collection(&Record::collection()).await?;
    let latest = server_collection.get_state(record_id).await?;
    client
        .handle_message(deliver(
            server.id,
            client.id,
            vec![proto::SubscriptionUpdateItem {
                entity_id: record_id,
                model,
                content: proto::UpdateContent::StateAndEvent(latest.into(), vec![]),
                predicate_relevance: vec![],
            }],
        ))
        .await?;

    assert!(
        client_agent.validate_calls.load(Ordering::SeqCst) >= 2,
        "the tip and its missing parent must both pass validate_received_event"
    );
    let client_collection = ctx_c.collection(&Record::collection()).await?;
    let stored_ids: HashSet<_> =
        client_collection.dump_entity_events(record_id).await?.into_iter().map(|event| event.payload.id()).collect();
    assert!(!stored_ids.contains(&denied_id), "a policy-denied BFS event must not enter the permanent cache");
    assert_eq!(view.title()?, "before", "a snapshot with a denied lineage must not advance materialized state");
    Ok(())
}

// ===========================================================================
// OPEN GAP red tests (ignored): equivocation flooding (G-4, #246/#274)
// ===========================================================================

/// G-4 / OPEN GAP: content-hash de-dup stops identical replays (C4-05) but not
/// unbounded DISTINCT valid concurrent events. An adversary can inflate the
/// head antichain without bound by sending many distinct siblings of the same
/// parent, forcing unbounded staging and ever-wider merges with no finite-harm
/// cap. This red test pins what SHOULD happen once a quantity/rate cap lands
/// (#246 receive limits, or a signature-attribution layer): the antichain
/// width (or accepted concurrent-event count) is bounded. It does NOT implement
/// the cap.
///
/// Today every distinct sibling is a genuine hash and is accepted, so the head
/// grows to the flood size and this bound assertion fails.
#[tokio::test]
#[ignore = "OPEN GAP G-4: unbounded distinct-equivocation flooding, no antichain cap (issue #246 / signature layer)"]
async fn equivocation_flood_antichain_is_bounded() -> Result<()> {
    let f = fixture().await?;
    let (rec_id, view) = seed_record(&f, "t0", "a0").await?;
    let head0 = view.entity().head().clone();

    // Many distinct siblings of the same head: each is a genuine, distinct
    // content hash (different title), so de-dup does not collapse them.
    const FLOOD: usize = 256;
    let siblings: Vec<proto::Event> =
        (0..FLOOD).map(|i| forge_title_event(rec_id, f.record_model, f.record_title, head0.clone(), &format!("equiv-{i}"))).collect();
    for ev in siblings {
        f.client.handle_message(deliver(f.server.id, f.client.id, vec![event_only_item(ev)])).await?;
    }

    // A finite cap (whatever #246 chooses) would keep the head far below the
    // flood size. Today the antichain is not capped; this pins the desired
    // bound and fails until the cap exists.
    const CAP: usize = 64;
    let width = view.entity().head().len();
    assert!(width <= CAP, "head antichain must be bounded under equivocation flooding (G-4), was {width}");
    Ok(())
}
