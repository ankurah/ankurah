#![cfg(test)]

use super::*;
use crate::{
    error::MutationError,
    peer_subscription::RemoteQuerySubscriber,
    policy::{PermissiveAgent, DEFAULT_CONTEXT},
    storage::StorageCollection,
};
use ankurah_signals::With;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

/// Test gate shared between a [`GatedEngine`] and the test body. Fetches against the
/// gated collection park until the test opens the gate, so an activation can be held
/// open at its storage fetch while the test drives a competing activation.
struct Gate {
    open_tx: tokio::sync::watch::Sender<bool>,
    open_rx: tokio::sync::watch::Receiver<bool>,
    fetches_started: AtomicUsize,
    states: Mutex<Vec<proto::Attested<proto::EntityState>>>,
}

impl Gate {
    fn new() -> Arc<Self> {
        let (open_tx, open_rx) = tokio::sync::watch::channel(false);
        Arc::new(Self { open_tx, open_rx, fetches_started: AtomicUsize::new(0), states: Mutex::new(Vec::new()) })
    }

    fn open(&self) { self.open_tx.send(true).expect("gate receiver dropped"); }

    fn fetches_started(&self) -> usize { self.fetches_started.load(Ordering::SeqCst) }

    /// Stand-in for remote deltas having been applied to local storage
    fn add_state(&self, state: proto::Attested<proto::EntityState>) { self.states.lock().unwrap().push(state); }

    async fn wait_open(&self) {
        let mut rx = self.open_rx.clone();
        while !*rx.borrow_and_update() {
            rx.changed().await.expect("gate sender dropped");
        }
    }
}

/// Storage stub whose fetch path parks on the gate for one target collection.
/// Other collections (notably the system catalog, fetched at node construction)
/// pass through so node setup is not entangled with the gate.
struct GatedEngine {
    gated_collection: CollectionId,
    gate: Arc<Gate>,
}

impl GatedEngine {
    fn new(gated_collection: CollectionId) -> (Self, Arc<Gate>) {
        let gate = Gate::new();
        (Self { gated_collection, gate: gate.clone() }, gate)
    }
}

#[async_trait::async_trait]
impl StorageEngine for GatedEngine {
    type Value = ();

    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        Ok(Arc::new(GatedCollection { gated: *id == self.gated_collection, gate: self.gate.clone() }))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> { Ok(false) }
}

struct GatedCollection {
    gated: bool,
    gate: Arc<Gate>,
}

#[async_trait::async_trait]
impl StorageCollection for GatedCollection {
    async fn set_state(&self, _state: proto::Attested<proto::EntityState>) -> Result<bool, MutationError> { Ok(true) }

    async fn get_state(&self, id: proto::EntityId) -> Result<proto::Attested<proto::EntityState>, RetrievalError> {
        Err(RetrievalError::EntityNotFound(id))
    }

    async fn fetch_states(&self, _selection: &ankql::ast::Selection) -> Result<Vec<proto::Attested<proto::EntityState>>, RetrievalError> {
        if self.gated {
            self.gate.fetches_started.fetch_add(1, Ordering::SeqCst);
            self.gate.wait_open().await;
            Ok(self.gate.states.lock().unwrap().clone())
        } else {
            Ok(Vec::new())
        }
    }

    async fn add_event(&self, _entity_event: &proto::Attested<proto::Event>) -> Result<bool, MutationError> { Ok(true) }

    async fn get_events(&self, _event_ids: Vec<proto::EventId>) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        Ok(Vec::new())
    }

    async fn dump_entity_events(&self, _id: proto::EntityId) -> Result<Vec<proto::Attested<proto::Event>>, RetrievalError> {
        Ok(Vec::new())
    }
}

/// Tracing subscriber that records event messages so a test can assert on them.
/// Thread-local via set_default; sufficient here because the current-thread test
/// runtime polls every spawned task on this thread.
#[derive(Clone)]
struct RecordingSubscriber(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for RecordingSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool { true }
    fn new_span(&self, _attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id { tracing::span::Id::from_u64(1) }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct MessageVisitor<'a>(&'a mut String);
        impl tracing::field::Visit for MessageVisitor<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    use std::fmt::Write;
                    let _ = write!(self.0, "{:?}", value);
                }
            }
        }
        let mut message = String::new();
        event.record(&mut MessageVisitor(&mut message));
        self.0.lock().unwrap().push(message);
    }
}

/// Drive the current-thread runtime until the condition holds. Cooperative scheduling
/// makes this deterministic: each yield lets every ready task run to its next await point.
async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("wait_until timed out: {what}");
}

fn latched_error(query: &EntityLiveQuery) -> Option<String> { query.error().with(|e| e.as_ref().map(|e| e.to_string())) }

/// On relay-bearing nodes two same-version activations fire by design: the local
/// initialization task (serve-from-cache) and the relay's subscription_established
/// (after initial deltas apply). Nothing but timing ordered them before the activation
/// lock existed: both could read initialized_version == 0, both took the reactor's add
/// path, and the reactor refused the duplicate with "Query ... already exists".
/// This pins the required behavior, not just the absence of the error: the second
/// activation must wait on the activation lock (no second add fetch while the first
/// holds the gate), then run as an update after the winner's add.
#[tokio::test]
async fn racing_same_version_activations_coalesce_into_add_then_update() {
    let trace_messages = Arc::new(Mutex::new(Vec::new()));
    let _trace_guard = tracing::subscriber::set_default(RecordingSubscriber(trace_messages.clone()));

    let collection_id = CollectionId::fixed_name("album");
    let (engine, gate) = GatedEngine::new(collection_id.clone());
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());

    // The constructor spawns the local initialization task, which runs activate(1) and
    // parks inside the gated storage fetch while holding the activation lock
    let args: MatchArgs = "name = 'x'".try_into().unwrap();
    let query = EntityLiveQuery::new(&node, collection_id, args, DEFAULT_CONTEXT).unwrap();
    wait_until("local activation reaches the storage fetch", || gate.fetches_started() == 1).await;

    // Drive the remote half exactly as the relay does after initial deltas apply
    let remote = {
        let weak = query.weak();
        tokio::spawn(async move { weak.subscription_established(1).await })
    };

    // Let the remote activation run as far as it can get. Serialized, it parks on the
    // activation lock before any storage access; unserialized, it would reach the add
    // path's storage fetch and bump the counter to two while the gate is still held
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    let fetches_while_gated = gate.fetches_started();

    gate.open();
    remote.await.expect("remote activation task panicked");

    // A double add latches "Query ... already exists"; an update before the add would
    // latch "Query not found for update". A correctly coalesced run latches nothing.
    assert_eq!(latched_error(&query), None, "no activation error may latch");

    // The coalesced activation waited instead of starting a second add fetch
    assert_eq!(fetches_while_gated, 1, "second activation must wait on the activation lock, not start its own add fetch");
    // ... and still ran afterwards, as the update path's own fetch
    assert_eq!(gate.fetches_started(), 2, "coalesced activation must run as an update after the winner's add");

    // Coalescing must not be silent: the waiter announced itself in traces
    let query_id = query.query_id().to_string();
    let saw_contention =
        trace_messages.lock().unwrap().iter().any(|m| m.contains("waiting for in-flight activation") && m.contains(&query_id));
    assert!(saw_contention, "contended activation must emit the coalescing debug line");

    assert_eq!(query.0.initialized_version.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert!(query.resultset().is_loaded());
    query.wait_initialized().await;

    // The query must remain fully operational after the race: a selection update takes
    // the update path against the state the winning add registered
    tokio::time::timeout(std::time::Duration::from_secs(30), query.update_selection_wait("name = 'y'"))
        .await
        .expect("selection update timed out")
        .expect("selection update failed");
    assert_eq!(latched_error(&query), None, "selection update after the race may not latch an error");
    assert_eq!(query.0.initialized_version.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert_eq!(gate.fetches_started(), 3);
}

/// Probe: documents current behavior on relay-bearing nodes, not an endorsement of it.
/// The local serve-from-cache activation alone releases wait_initialized(), so a reader
/// gating on it (Context::query_wait does exactly this) can observe a loaded but EMPTY
/// resultset on a fresh query before the authoritative remote refresh arrives. The remote
/// refresh later rewrites the resultset via the update path. If the maintainer decides
/// fresh relay-backed queries should hold readers until the remote answer, this test is
/// the fixture to flip.
#[tokio::test]
async fn relay_node_wait_initialized_releases_local_results_before_remote_refresh() {
    let collection_id = CollectionId::fixed_name("album");
    let (engine, gate) = GatedEngine::new(collection_id.clone());
    // Normal timing: the local fetch is unimpeded
    gate.open();

    // Ephemeral node: has a subscription relay. No durable peer connects, so the remote
    // half stays pending, exactly like the window before a connection is established
    let node = Node::new(Arc::new(engine), PermissiveAgent::new());

    // cached args, so the local serve-from-cache task runs alongside the pending remote
    let args = MatchArgs::from(ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None });
    let query = EntityLiveQuery::new(&node, collection_id.clone(), args, DEFAULT_CONTEXT).unwrap();

    // Released by the LOCAL activation alone: loaded, initialized, and empty
    tokio::time::timeout(std::time::Duration::from_secs(30), query.wait_initialized()).await.expect("wait_initialized timed out");
    assert!(query.resultset().is_loaded());
    assert_eq!(query.resultset().len(), 0, "reader observes the empty local answer as initialized");
    assert_eq!(query.0.initialized_version.load(std::sync::atomic::Ordering::Relaxed), 1);

    // The authoritative remote answer arrives afterwards: deltas land in storage, then
    // the relay activates. Only now does the resultset reflect the remote entity
    let entity_state = proto::EntityState { entity_id: proto::EntityId::new(), collection: collection_id, state: proto::State::default() };
    gate.add_state(entity_state.into());
    query.weak().subscription_established(1).await;

    assert_eq!(latched_error(&query), None);
    assert_eq!(query.resultset().len(), 1, "remote refresh rewrites the resultset the reader already saw as empty");
}
