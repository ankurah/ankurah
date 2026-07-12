use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, Mutex, OnceLock, Weak,
    },
};

/// A sibling Node's synchronous teardown callback (see
/// [`CollectionSet::set_epoch_participant`](crate::collectionset::CollectionSet::set_epoch_participant)).
pub(crate) type EpochParticipant = dyn Fn() + Send + Sync;

/// Process-wide lease shared by every Node constructed from the exact same
/// `Arc<StorageEngine>`. A SystemManager-local lock cannot protect an engine
/// shared by two independently-built Nodes: one node's paused load could
/// otherwise resume after another node reset the engine and recreate old-root
/// rows. The weak registry preserves backend generality and cannot alias a
/// reused allocation address while the prior engine/fence is still alive.
pub(crate) struct StorageFence {
    pub(crate) gate: Arc<tokio::sync::RwLock<()>>,
    pub(crate) epoch: AtomicU64,
    pub(crate) generation: std::sync::RwLock<Arc<AtomicBool>>,
    pub(crate) epoch_changed: Arc<tokio::sync::Notify>,
    /// Per-Node teardown callbacks, keyed by the registering CollectionSet's
    /// identity so an advancing manager can skip its own entry (its own
    /// teardown runs inside its invalidation, with the reservation
    /// exemption). Entries are weak: a dropped Node just disappears.
    pub(crate) participants: Mutex<Vec<(usize, Weak<EpochParticipant>)>>,
}

pub(crate) fn shared_storage_fence<SE>(storage_engine: &Arc<SE>) -> Arc<StorageFence> {
    static REGISTRY: OnceLock<Mutex<HashMap<usize, Weak<StorageFence>>>> = OnceLock::new();
    let key = Arc::as_ptr(storage_engine).cast::<()>() as usize;
    let mut registry = REGISTRY.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap();
    if let Some(fence) = registry.get(&key).and_then(Weak::upgrade) {
        return fence;
    }
    let fence = Arc::new(StorageFence {
        gate: Arc::new(tokio::sync::RwLock::new(())),
        epoch: AtomicU64::new(0),
        generation: std::sync::RwLock::new(Arc::new(AtomicBool::new(true))),
        epoch_changed: Arc::new(tokio::sync::Notify::new()),
        participants: Mutex::new(Vec::new()),
    });
    registry.insert(key, Arc::downgrade(&fence));
    fence
}
