#[allow(unused)]
pub use ankurah::signals::{Peek, Subscribe};
#[allow(unused)]
pub use ankurah_connector_local_process::LocalProcessConnection;
pub use ankurah_storage_sled::SledStorageEngine;
use tracing::Level;

#[allow(unused)]
pub use ankurah::{
    changes::{ChangeKind, ChangeSet},
    core::node::nocache,
    error::MutationError,
    model::View,
    policy::DEFAULT_CONTEXT,
    proto,
    signals::{
        broadcast::{BroadcastListener, IntoBroadcastListener},
        subscribe::IntoSubscribeListener,
    },
    Context, EntityId, LiveQuery, Model, Node, PermissiveAgent,
};
use serde::{Deserialize, Serialize};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Debug, Clone, Model, Serialize, Deserialize)]
pub struct Pet {
    pub name: String,
    pub age: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    pub name: String,
    pub year: String,
}

/// LWW-backed model for testing Last-Write-Wins conflict resolution
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Record {
    #[active_type(LWW)]
    pub title: String,
    #[active_type(LWW)]
    pub artist: String,
}

// ---------------------------------------------------------------------------
// GatedConnection: a LocalProcessConnection variant that lets a test hold
// selected inbound messages to one node and release them on demand. Used to
// make subscription-ordering races deterministic (see
// inter_node::subscription_empty_events_from_noop_delta). Lives in the test
// crate so the shipped connector keeps its minimal API.
// ---------------------------------------------------------------------------

use ankurah::core::connector::{PeerSender, SendError};
use tokio::sync::mpsc;

#[derive(Clone)]
struct GatedSender {
    sender: mpsc::Sender<proto::NodeMessage>,
    node_id: proto::EntityId,
}

#[async_trait::async_trait]
impl PeerSender for GatedSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.sender.try_send(message).map_err(|_| SendError::ConnectionClosed)?;
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.node_id }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// Handle for holding/releasing messages the gated side would otherwise deliver.
#[derive(Clone)]
pub struct MessageGate {
    filter: Arc<dyn Fn(&proto::NodeMessage) -> bool + Send + Sync>,
    held: Arc<Mutex<Vec<proto::NodeMessage>>>,
}

impl MessageGate {
    /// Release all currently-held messages back into the gated node, in arrival order.
    pub async fn release_held<SE, PA>(&self, node: &Node<SE, PA>)
    where
        SE: ankurah::core::storage::StorageEngine + Send + Sync + 'static,
        PA: ankurah::policy::PolicyAgent + Send + Sync + 'static,
    {
        self.release_first(node, usize::MAX).await;
    }

    /// Release at most the first `count` held messages, preserving arrival
    /// order and leaving later generations gated.
    pub async fn release_first<SE, PA>(&self, node: &Node<SE, PA>, count: usize)
    where
        SE: ankurah::core::storage::StorageEngine + Send + Sync + 'static,
        PA: ankurah::policy::PolicyAgent + Send + Sync + 'static,
    {
        let drained: Vec<_> = {
            let mut held = self.held.lock().unwrap();
            let count = count.min(held.len());
            held.drain(..count).collect()
        };
        for message in drained {
            let _ = node.handle_message(message).await;
        }
    }

    /// Release at most the last `count` held messages, preserving their
    /// arrival order and leaving earlier generations gated.
    pub async fn release_last<SE, PA>(&self, node: &Node<SE, PA>, count: usize)
    where
        SE: ankurah::core::storage::StorageEngine + Send + Sync + 'static,
        PA: ankurah::policy::PolicyAgent + Send + Sync + 'static,
    {
        let drained: Vec<_> = {
            let mut held = self.held.lock().unwrap();
            let start = held.len().saturating_sub(count);
            held.drain(start..).collect()
        };
        for message in drained {
            let _ = node.handle_message(message).await;
        }
    }
}

/// One-directional gated connection: messages destined for `gated` that match
/// the filter are parked in the gate instead of being handled, until released.
/// All other messages (both directions) are handled immediately.
pub struct GatedConnection {
    task_a: tokio::task::JoinHandle<()>,
    task_b: tokio::task::JoinHandle<()>,
}

impl GatedConnection {
    /// `other` <-> `gated`. Only messages arriving at `gated` are subject to the gate.
    pub fn new<SE1, PA1, SE2, PA2>(
        other: &Node<SE1, PA1>,
        gated: &Node<SE2, PA2>,
        filter: impl Fn(&proto::NodeMessage) -> bool + Send + Sync + 'static,
    ) -> (Self, MessageGate)
    where
        SE1: ankurah::core::storage::StorageEngine + Send + Sync + 'static,
        PA1: ankurah::policy::PolicyAgent + Send + Sync + 'static,
        SE2: ankurah::core::storage::StorageEngine + Send + Sync + 'static,
        PA2: ankurah::policy::PolicyAgent + Send + Sync + 'static,
    {
        let (other_tx, mut other_rx) = mpsc::channel(1024);
        let (gated_tx, mut gated_rx) = mpsc::channel(1024);

        other
            .register_peer(
                proto::Presence {
                    node_id: gated.id,
                    durable: gated.durable,
                    system_root: gated.system.root(),
                    protocol_version: proto::PROTOCOL_VERSION,
                },
                Box::new(GatedSender { sender: gated_tx, node_id: gated.id }),
            )
            .expect("gated peers use the current protocol version");
        gated
            .register_peer(
                proto::Presence {
                    node_id: other.id,
                    durable: other.durable,
                    system_root: other.system.root(),
                    protocol_version: proto::PROTOCOL_VERSION,
                },
                Box::new(GatedSender { sender: other_tx, node_id: other.id }),
            )
            .expect("gated peers use the current protocol version");

        let gate = MessageGate { filter: Arc::new(filter), held: Arc::new(Mutex::new(Vec::new())) };

        // Messages flowing INTO `other` are always handled immediately.
        let task_a = {
            let node = other.clone();
            tokio::spawn(async move {
                while let Some(message) = other_rx.recv().await {
                    let node = node.clone();
                    tokio::spawn(async move {
                        let _ = node.handle_message(message).await;
                    });
                }
            })
        };

        // Messages flowing INTO `gated` are parked if they match the filter.
        let task_b = {
            let node = gated.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                while let Some(message) = gated_rx.recv().await {
                    if (gate.filter)(&message) {
                        gate.held.lock().unwrap().push(message);
                        continue;
                    }
                    let node = node.clone();
                    tokio::spawn(async move {
                        let _ = node.handle_message(message).await;
                    });
                }
            })
        };

        (Self { task_a, task_b }, gate)
    }
}

impl Drop for GatedConnection {
    fn drop(&mut self) {
        self.task_a.abort();
        self.task_b.abort();
    }
}

// Initialize tracing for tests
#[ctor::ctor]
fn init_tracing() {
    // if LOG_LEVEL env var is set, use it
    if let Ok(level) = std::env::var("LOG_LEVEL") {
        tracing_subscriber::fmt().with_max_level(Level::from_str(&level).unwrap()).with_test_writer().init();
    } else {
        tracing_subscriber::fmt().with_max_level(Level::INFO).with_test_writer().init();
    }
}

/// Generic watcher that accumulates notifications and provides async waiting methods
#[derive(Clone)]
pub struct TestWatcher<T, U> {
    changes: Arc<Mutex<Vec<T>>>,
    notify: Arc<Notify>,
    transform: Arc<dyn Fn(T) -> U + Send + Sync>,
}

impl<T> TestWatcher<T, T> {
    pub fn new() -> Self { Self { changes: Arc::new(Mutex::new(Vec::new())), notify: Arc::new(Notify::new()), transform: Arc::new(|x| x) } }
}

impl<T, U> TestWatcher<T, U> {
    /// Creates a new generic watcher with identity transform (T -> T)
    pub fn transform(transform: impl Fn(T) -> U + Send + Sync + 'static) -> TestWatcher<T, U> {
        Self { changes: Arc::new(Mutex::new(Vec::new())), notify: Arc::new(Notify::new()), transform: Arc::new(transform) }
    }
}

impl<R: View + Send + Sync + 'static> TestWatcher<ChangeSet<R>, Vec<(proto::EntityId, ChangeKind)>> {
    /// Creates a new changeset watcher that transforms ChangeSet<R> to Vec<(EntityId, ChangeKind)>
    pub fn changeset() -> TestWatcher<ChangeSet<R>, Vec<(proto::EntityId, ChangeKind)>> {
        TestWatcher {
            changes: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            transform: Arc::new(|changeset: ChangeSet<R>| changeset.changes.iter().map(|c| (c.entity().id(), c.into())).collect()),
        }
    }
    pub fn changeset_with_event_ids() -> TestWatcher<ChangeSet<R>, Vec<(proto::EntityId, ChangeKind, Vec<proto::EventId>)>> {
        TestWatcher {
            changes: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            transform: Arc::new(|changeset: ChangeSet<R>| {
                changeset.changes.iter().map(|c| (c.entity().id(), c.into(), c.events().iter().map(|e| e.payload.id()).collect())).collect()
            }),
        }
    }

    /// Drains and returns all accumulated changesets, sorting each changeset by the first element of the tuple
    /// Importantly, this does not sort the list of changesets, only the items within each changeset
    pub fn drain_sorted(&self) -> Vec<Vec<(proto::EntityId, ChangeKind)>> {
        let mut changes = self.drain();
        for change in changes.iter_mut() {
            change.sort_by_key(|c| c.0);
        }
        changes
    }
}
impl<T, U> TestWatcher<T, U> {
    pub fn notify(&self, item: T) {
        self.changes.lock().unwrap().push(item);
        self.notify.notify_waiters();
    }

    /// Takes (empties and returns) all accumulated items, applying the transform
    pub fn drain(&self) -> Vec<U> { self.changes.lock().unwrap().drain(..).map(|item| (self.transform)(item)).collect() }

    /// Waits for exactly `count` items to accumulate, then drains and returns them
    pub async fn take(&self, count: usize) -> Result<Vec<U>, anyhow::Error> {
        if !self.wait_for_count(count, Some(Duration::from_secs(10))).await {
            return Err(anyhow::anyhow!("take({}) timed out waiting for items (waited 10 seconds, got {} items)", count, self.count()));
        }
        let mut changes = self.changes.lock().unwrap();
        Ok(changes.drain(0..count).map(|item| (self.transform)(item)).collect())
    }

    /// Returns the current number of accumulated changesets without draining them
    pub fn count(&self) -> usize { self.changes.lock().unwrap().len() }

    /// Waits for at least one changeset to be accumulated (with 10 second timeout)
    pub async fn wait(&self) -> bool { self.wait_for_count(1, Some(Duration::from_secs(10))).await }

    /// Waits for at least 1 item, then takes and returns exactly the first one
    #[track_caller]
    pub fn take_one(&self) -> impl std::future::Future<Output = U> + '_ {
        let caller = std::panic::Location::caller();
        async move {
            let _success = self.wait_for_count(1, Some(Duration::from_secs(10))).await;
            let mut changes = self.changes.lock().unwrap();
            if changes.is_empty() {
                panic!(
                    "take_one() timed out waiting for items (waited 10 seconds, got {} items) at {}:{}:{}",
                    changes.len(),
                    caller.file(),
                    caller.line(),
                    caller.column()
                );
            }
            let item = changes.remove(0);
            (self.transform)(item)
        }
    }

    #[track_caller]
    pub async fn take_one_with_timeout(&self, timeout: Duration) -> U {
        let caller = std::panic::Location::caller();
        let _success = self.wait_for_count(1, Some(timeout)).await;
        let mut changes = self.changes.lock().unwrap();
        if changes.is_empty() {
            panic!(
                "take_one_with_timeout() timed out waiting for items (waited {:?}, got {} items) at {}:{}:{}",
                timeout,
                changes.len(),
                caller.file(),
                caller.line(),
                caller.column()
            );
        }
        let item = changes.remove(0);
        (self.transform)(item)
    }

    /// Waits 100ms for any additional items, then returns the count (useful for asserting quiescence)
    pub async fn quiesce(&self) -> usize {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.count()
    }
    pub async fn quiesce_drain(&self) -> Vec<U> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.drain()
    }

    /// Wait for at least `count` items to accumulate, then drain and return all items
    pub async fn take_when(&self, count: usize) -> Vec<U> {
        self.wait_for_count(count, Some(Duration::from_secs(10))).await;
        self.drain()
    }

    /// Waits for at least `count` changesets to be accumulated, with optional timeout
    pub async fn wait_for_count(&self, count: usize, timeout: Option<Duration>) -> bool {
        // Check if we already have enough changes
        {
            let changes = self.changes.lock().unwrap();
            if changes.len() >= count {
                return true;
            }
        }

        match timeout {
            Some(duration) => tokio::time::timeout(duration, async {
                loop {
                    self.notify.notified().await;
                    let changes = self.changes.lock().unwrap();
                    if changes.len() >= count {
                        return true;
                    }
                }
            })
            .await
            .unwrap_or(false),
            None => loop {
                self.notify.notified().await;
                let changes = self.changes.lock().unwrap();
                if changes.len() >= count {
                    return true;
                }
            },
        }
    }
}

impl<T, U> TestWatcher<T, U>
where T: Clone
{
    pub fn peek(&self) -> Vec<U> { self.changes.lock().unwrap().iter().map(|item| (self.transform)(item.clone())).collect() }
}

impl<T: Send + 'static, U> IntoBroadcastListener<T> for &TestWatcher<T, U> {
    fn into_broadcast_listener(self) -> BroadcastListener<T> {
        let changes = self.changes.clone();
        let notify = self.notify.clone();
        BroadcastListener::Payload(Arc::new(move |item: T| {
            changes.lock().unwrap().push(item);
            notify.notify_waiters();
        }))
    }
}

impl<T: Send + 'static, U> IntoSubscribeListener<T> for &TestWatcher<T, U> {
    fn into_subscribe_listener(self) -> Box<dyn Fn(T) + Send + Sync> {
        let changes = self.changes.clone();
        let notify = self.notify.clone();
        Box::new(move |item: T| {
            changes.lock().unwrap().push(item);
            notify.notify_waiters();
        })
    }
}

/// Macro to create a sorted vector from a list of items
/// Usage: sorted![1, 3, 2] -> vec![1, 2, 3]
#[macro_export]
macro_rules! sorted {
    ($($item:expr),* $(,)?) => {{
        let mut vec = vec![$($item),*];
        vec.sort();
        vec
    }};
}

// Macro to create a sorted vector from a list of tuples - sorted by the first element of the tuple
#[macro_export]
macro_rules! sortby_t0 {
    ($($item:expr),* $(,)?) => {{
        let mut vec = vec![$($item),*];
        vec.sort_by_key(|i| i.0);
        vec
    }};
}

#[allow(unused)]
pub async fn create_albums(ctx: &Context, years: impl IntoIterator<Item = u32>) -> Result<Vec<EntityId>, MutationError> {
    let trx = ctx.begin();
    let mut ids = Vec::new();
    for year in years {
        let album = trx.create(&Album { name: format!("Album {}", year), year: year.to_string() }).await?;
        ids.push(album.id());
    }
    trx.commit().await?;
    Ok(ids)
}
#[allow(unused)]
pub fn years(query: &LiveQuery<AlbumView>) -> Vec<String> { query.peek().iter().map(|a| a.year().unwrap_or_default()).collect() }

#[allow(unused)]
pub async fn durable_sled_setup() -> Result<Node<SledStorageEngine, PermissiveAgent>, anyhow::Error> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node)
}

#[allow(unused)]
pub async fn ephemeral_sled_setup() -> Result<Node<SledStorageEngine, PermissiveAgent>, anyhow::Error> {
    let node = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    Ok(node)
}

// ============================================================================
// DAG STRUCTURE VERIFICATION (TestDag and assert_dag! macro)
// ============================================================================

use std::collections::HashMap;

/// Test helper for tracking event labels based on creation order.
///
/// Labels are assigned in the order events are enumerated (A, B, C...).
/// This provides stable, predictable labels for DAG verification.
#[derive(Debug, Default)]
pub struct TestDag {
    next_label: char,
    /// Map from EventId to label
    pub id_to_label: HashMap<proto::EventId, char>,
    /// Map from label to EventId
    pub label_to_id: HashMap<char, proto::EventId>,
}

impl TestDag {
    pub fn new() -> Self { Self { next_label: 'A', id_to_label: HashMap::new(), label_to_id: HashMap::new() } }

    /// Enumerate events from a commit, assigning labels in order.
    pub fn enumerate(&mut self, events: Vec<proto::Event>) {
        for event in events {
            let id = event.id();
            if self.id_to_label.contains_key(&id) {
                continue; // Already registered
            }
            let label = self.next_label;
            self.next_label = (label as u8 + 1) as char;
            self.id_to_label.insert(id.clone(), label);
            self.label_to_id.insert(label, id);
        }
    }

    /// Get the label for an EventId, if registered.
    pub fn label(&self, id: &proto::EventId) -> Option<char> { self.id_to_label.get(id).copied() }

    /// Get the EventId for a label, if registered.
    pub fn id(&self, label: char) -> Option<&proto::EventId> { self.label_to_id.get(&label) }

    /// Convert a Clock to sorted labels for assertion.
    pub fn clock_labels(&self, clock: &proto::Clock) -> Vec<char> {
        let mut labels: Vec<char> = clock.as_slice().iter().filter_map(|id| self.id_to_label.get(id).copied()).collect();
        labels.sort();
        labels
    }

    /// Verify DAG structure against expected parent relationships.
    pub fn verify(&self, events: &[proto::Attested<proto::Event>], expected: &HashMap<char, Vec<char>>) -> Result<(), String> {
        let mut errors = Vec::new();

        // Build actual parent map from events
        let mut actual_parents: HashMap<char, Vec<char>> = HashMap::new();
        for event in events {
            let id = event.payload.id();
            if let Some(&label) = self.id_to_label.get(&id) {
                let parents: Vec<char> =
                    event.payload.parent.as_slice().iter().filter_map(|pid| self.id_to_label.get(pid).copied()).collect();
                actual_parents.insert(label, parents);
            }
        }

        // Verify each expected relationship
        for (&label, expected_parents) in expected {
            let actual = actual_parents.get(&label);
            match actual {
                None => {
                    errors.push(format!("Label {} not found in events", label));
                }
                Some(actual) => {
                    let mut expected_sorted = expected_parents.clone();
                    expected_sorted.sort();
                    let mut actual_sorted = actual.clone();
                    actual_sorted.sort();

                    if expected_sorted != actual_sorted {
                        let expected_str: String = expected_sorted.iter().collect();
                        let actual_str: String = actual_sorted.iter().collect();
                        if expected_parents.is_empty() {
                            errors.push(format!("{} => [] (expected) vs {} => [{}] (actual)", label, label, actual_str));
                        } else {
                            errors.push(format!("{} => [{}] (expected) vs {} => [{}] (actual)", label, expected_str, label, actual_str));
                        }
                    }
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "DAG structure mismatch:\n\n{}\n\nRegistered labels: {:?}",
                errors.join("\n"),
                self.label_to_id.keys().collect::<Vec<_>>()
            ))
        }
    }
}

/// Macro for declarative DAG structure verification.
///
/// Verifies the parent relationships in a DAG of events using a TestDag
/// that was populated via `dag.enumerate()` during commits.
///
/// Usage:
/// ```ignore
/// let mut dag = TestDag::new();
/// dag.enumerate(trx1.commit().await?);
/// dag.enumerate(trx2.commit().await?);
///
/// let events = collection.dump_entity_events(id).await?;
/// assert_dag!(dag, events, {
///     A => [],         // genesis event (no parents)
///     B => [A],
///     C => [A],
/// });
/// ```
#[macro_export]
macro_rules! assert_dag {
    ($dag:expr, $events:expr, { $($label:ident => [$($parent:ident),* $(,)?]),* $(,)? }) => {{
        use std::collections::HashMap;

        let mut expected_parents: HashMap<char, Vec<char>> = HashMap::new();

        $(
            let label_char = stringify!($label).chars().next().unwrap();
            expected_parents.insert(label_char, vec![$(stringify!($parent).chars().next().unwrap()),*]);
        )*

        if let Err(msg) = $dag.verify(&$events, &expected_parents) {
            panic!("{}", msg);
        }
    }};
}

/// Macro for asserting that a Clock contains specific labeled events.
///
/// Usage:
/// ```ignore
/// clock_eq!(dag, head, [B, C]);  // head contains events B and C
/// ```
#[macro_export]
macro_rules! clock_eq {
    ($dag:expr, $clock:expr, [$($label:ident),* $(,)?]) => {{
        let expected: Vec<char> = vec![$(stringify!($label).chars().next().unwrap()),*];
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        let actual = $dag.clock_labels(&$clock);
        assert_eq!(actual, expected_sorted,
            "Clock mismatch: expected {:?}, got {:?}", expected_sorted, actual);
    }};
}

/// Start a test websocket server and return the server node, URL, and task handle
#[allow(unused)]
pub async fn start_test_server() -> anyhow::Result<(Node<SledStorageEngine, PermissiveAgent>, String, tokio::task::JoinHandle<()>)> {
    use ankurah_websocket_server::WebsocketServer;
    use rand::Rng;
    use tracing::info;

    // Create and initialize server node
    let server_storage = Arc::new(SledStorageEngine::new_test()?);
    let server_node = Node::new_durable(server_storage, PermissiveAgent::new());
    server_node.system.create().await?;

    let mut rng = rand::thread_rng();

    // Retry logic for port conflicts
    const MAX_PORT_RETRIES: usize = 10;
    let mut last_error = None;

    for attempt in 0..MAX_PORT_RETRIES {
        let port: u16 = rng.gen_range(20000..=65000);
        let bind_addr = format!("127.0.0.1:{}", port);
        let server_url = format!("ws://127.0.0.1:{}", port);

        info!("Attempt {} - Starting websocket server on {}", attempt + 1, server_url);

        // Start server in background task
        let server_node_clone = server_node.clone();
        let bind_addr_clone = bind_addr.clone();

        let server_task = tokio::spawn(async move {
            let mut server = WebsocketServer::new(server_node_clone);
            if let Err(e) = server.run(&bind_addr_clone).await {
                tracing::warn!("Test server error on {}: {}", bind_addr_clone, e);
            }
        });

        // Wait briefly for the TcpListener::bind() call to complete
        // If port is in use, the server task will complete immediately with an error
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Check if the server task is still running (successful bind) or completed (failed bind)
        if server_task.is_finished() {
            // Server task completed immediately, which means binding failed
            tracing::warn!("Failed to bind server on port {} (attempt {})", port, attempt + 1);
            last_error = Some(anyhow::anyhow!("Port {} binding failed", port));

            if attempt < MAX_PORT_RETRIES - 1 {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            continue;
        }

        // Server task is still running, which means TcpListener::bind() succeeded
        // The server is now listening and ready for connections
        info!("Successfully started websocket server on {} (attempt {})", server_url, attempt + 1);
        return Ok((server_node, server_url, server_task));
    }

    Err(anyhow::anyhow!("Failed to start test server after {} attempts. Last error: {:?}", MAX_PORT_RETRIES, last_error))
}
