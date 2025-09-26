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
    signals::{broadcast::IntoListener, subscribe::IntoSubscribeListener},
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
    pub async fn take(&self, count: usize) -> Vec<U> {
        self.wait_for_count(count, Some(Duration::from_secs(10))).await;
        let mut changes = self.changes.lock().unwrap();
        changes.drain(0..count).map(|item| (self.transform)(item)).collect()
    }

    /// Returns the current number of accumulated changesets without draining them
    pub fn count(&self) -> usize { self.changes.lock().unwrap().len() }

    /// Waits for at least one changeset to be accumulated (with 10 second timeout)
    pub async fn wait(&self) -> bool { self.wait_for_count(1, Some(Duration::from_secs(10))).await }

    /// Waits for at least 1 item, then takes and returns exactly the first one
    #[track_caller]
    pub fn take_one(&self) -> impl std::future::Future<Output = U> + '_ {
        async move {
            let _success = self.wait_for_count(1, Some(Duration::from_secs(10))).await;
            let mut changes = self.changes.lock().unwrap();
            if changes.is_empty() {
                panic!("take_one() timed out waiting for items (waited 10 seconds, got {} items)", changes.len());
            }
            let item = changes.remove(0);
            (self.transform)(item)
        }
    }

    /// Waits 100ms for any additional items, then returns the count (useful for asserting quiescence)
    pub async fn quiesce(&self) -> usize {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.count()
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

impl<T: Send + 'static, U> IntoListener<T> for &TestWatcher<T, U> {
    fn into_listener(self) -> Arc<dyn Fn(T) + Send + Sync> {
        let changes = self.changes.clone();
        let notify = self.notify.clone();
        Arc::new(move |item: T| {
            changes.lock().unwrap().push(item);
            notify.notify_waiters();
        })
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
