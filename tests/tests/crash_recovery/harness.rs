//! Crash and recovery fault injection harness (workstream C6).
//!
//! Deterministic crash points beat random kills. This module provides a
//! storage-engine wrapper that aborts the process (`std::process::abort`) the
//! instant a configured storage operation is about to run, plus the
//! child-process plumbing that drives real OS-level kill and reopen cycles over
//! a real on-disk sled directory.
//!
//! Architecture (same philosophy as the selective-deny PolicyAgent in
//! `commit_atomicity.rs`, but at the durability boundary):
//!
//! - The parent test spawns the SAME test binary as a child via
//!   `std::process::Command`, passing the crash scenario and a scratch sled
//!   directory through environment variables.
//! - The child builds a node backed by `CrashStorageEngine`, performs the
//!   workload, and aborts mid-write when the configured operation counter hits.
//!   Because the abort is a real process death, sled's on-disk state is exactly
//!   what survived to that point.
//! - The parent waits for the child to die by signal, then REOPENS the sled
//!   directory in-process and asserts the recovery invariants.
//!
//! The crash point is a "before" hook on the targeted operation: the wrapper
//! flushes sled (making every completed prior write durable) and then aborts
//! WITHOUT running the targeted operation. This models a crash at that exact
//! instant: everything before is on disk, this operation and everything after
//! never happened. That is precisely the adversarial window each scenario
//! needs (e.g. "event committed, state write never started").

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use ankurah::core::error::{MutationError, RetrievalError};
use ankurah::core::storage::{StorageCollection, StorageEngine, SystemRootClaim};
use ankurah::proto::{self, Attested, CollectionId, EntityId, EntityState, Event, EventId, SystemRootProof};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;

// ============================================================================
// CRASH CONFIGURATION
// ============================================================================

/// Which storage operation the crash hook targets. The `nth` field selects the
/// zero-based occurrence of that operation type, counted across ALL collections
/// on the engine (the counters live on the engine, not the collection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    /// Abort just before the nth `add_event` (the storage call inside
    /// `commit_event`) runs.
    BeforeAddEvent(usize),
    /// Abort just before the nth `set_state` runs.
    BeforeSetState(usize),
}

impl CrashPoint {
    /// Serialize to the wire form carried in an environment variable.
    pub fn to_env(&self) -> String {
        match self {
            CrashPoint::BeforeAddEvent(n) => format!("add_event:{n}"),
            CrashPoint::BeforeSetState(n) => format!("set_state:{n}"),
        }
    }

    /// Parse the environment form. Returns None for the empty/absent value.
    pub fn from_env(s: &str) -> Option<Self> {
        let (kind, n) = s.split_once(':')?;
        let n: usize = n.parse().ok()?;
        match kind {
            "add_event" => Some(CrashPoint::BeforeAddEvent(n)),
            "set_state" => Some(CrashPoint::BeforeSetState(n)),
            _ => None,
        }
    }
}

/// Environment variable naming the crash point for the child process. Presence
/// of this variable is what tells a test body it is running as the crash child.
pub const ENV_CRASH_POINT: &str = "ANKURAH_C6_CRASH_POINT";
/// Environment variable naming the on-disk sled directory shared parent/child.
pub const ENV_SLED_DIR: &str = "ANKURAH_C6_SLED_DIR";
/// Environment variable naming a sidecar file the child uses to hand identifiers
/// (entity/event ids) back to the parent. stdout cannot be relied upon across an
/// abort (libtest block-buffers a piped stdout, so the buffer is lost when the
/// process dies), so the child writes here and fsyncs before the risky
/// operation.
pub const ENV_HANDOFF_FILE: &str = "ANKURAH_C6_HANDOFF";

/// Stable durable identity shared by the crash child and its reopened parent.
/// A real deployment persists this seed; a fresh key on reopen would correctly
/// fail founder recognition against the stored system root.
pub fn crash_signing_key() -> ed25519_dalek::SigningKey { ed25519_dalek::SigningKey::from_bytes(&[0xC6; 32]) }

// ============================================================================
// FLUSH HOOK
// ============================================================================

/// Storage engines the crash wrapper can force to durability before aborting.
///
/// The wrapper must guarantee that writes completed before the crash point are
/// actually on disk, otherwise the "crash after commit_event" scenario would be
/// indistinguishable from "commit_event never reached disk" and the invariant
/// would be untestable. For sled this is an explicit flush; a server-backed
/// engine like postgres is durable on statement completion, so its hook is a
/// no-op.
pub trait CrashFlushable {
    fn crash_flush(&self);
}

impl CrashFlushable for SledStorageEngine {
    fn crash_flush(&self) {
        // Best effort: we are about to abort regardless, so a flush error must
        // not mask the crash. sled's `flush` fsyncs the log, making every prior
        // `insert` durable.
        if let Ok(db) = self.database.lock() {
            let _ = db.db.flush();
        }
    }
}

// Postgres autocommits each statement to a durable server that survives the
// node crash, so there is nothing to flush from the dying node's side.
#[cfg(feature = "postgres-crash")]
impl CrashFlushable for ankurah_storage_postgres::Postgres {
    fn crash_flush(&self) {}
}

// ============================================================================
// CRASH-INJECTING STORAGE WRAPPER
// ============================================================================

/// Shared crash state: the target point, the per-op counters, and an `armed`
/// gate. Operations are only counted and crashes only fire once armed, so the
/// `nth` occurrence in a [`CrashPoint`] is relative to the test workload rather
/// than to any bootstrap writes (e.g. `system.create()`) that happen first.
struct CrashState {
    crash: Option<CrashPoint>,
    armed: AtomicBool,
    add_event_counter: AtomicUsize,
    set_state_counter: AtomicUsize,
}

/// Wraps a storage engine, counting `add_event`/`set_state` operations and
/// aborting the process the instant the configured crash point is reached.
pub struct CrashStorageEngine<E: StorageEngine + CrashFlushable> {
    inner: Arc<E>,
    state: Arc<CrashState>,
}

impl<E: StorageEngine + CrashFlushable + 'static> CrashStorageEngine<E> {
    pub fn new(inner: Arc<E>, crash: Option<CrashPoint>) -> Self {
        Self {
            inner,
            state: Arc::new(CrashState {
                crash,
                armed: AtomicBool::new(false),
                add_event_counter: AtomicUsize::new(0),
                set_state_counter: AtomicUsize::new(0),
            }),
        }
    }

    /// Begin counting operations and enable the crash hook. Call this after any
    /// setup writes (system creation, seeding) so the crash point counts only
    /// the workload under test.
    pub fn arm(&self) { self.state.armed.store(true, Ordering::SeqCst); }
}

impl CrashState {
    /// Flush the inner engine and abort the process. Never returns.
    fn crash_now(&self, flush: &dyn CrashFlushable, label: &str) -> ! {
        eprintln!("C6 crash injection: aborting before {label}");
        flush.crash_flush();
        // A true OS-level kill of the node process. sled's on-disk state is now
        // whatever survived up to this point.
        std::process::abort();
    }

    /// Called before an `add_event`. Aborts if this occurrence is the target.
    fn check_add_event(&self, flush: &dyn CrashFlushable) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        let n = self.add_event_counter.fetch_add(1, Ordering::SeqCst);
        if let Some(CrashPoint::BeforeAddEvent(target)) = self.crash {
            if n == target {
                self.crash_now(flush, &format!("add_event #{n}"));
            }
        }
    }

    /// Called before a `set_state`. Aborts if this occurrence is the target.
    fn check_set_state(&self, flush: &dyn CrashFlushable) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        let n = self.set_state_counter.fetch_add(1, Ordering::SeqCst);
        if let Some(CrashPoint::BeforeSetState(target)) = self.crash {
            if n == target {
                self.crash_now(flush, &format!("set_state #{n}"));
            }
        }
    }
}

#[async_trait]
impl<E: StorageEngine + CrashFlushable + 'static> StorageEngine for CrashStorageEngine<E> {
    type Value = ();

    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        let inner = self.inner.collection(id).await?;
        Ok(Arc::new(CrashStorageCollection { inner, state: self.state.clone(), flush: self.inner.clone() }))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> { self.inner.delete_all_collections().await }

    async fn claim_system_root(&self, candidate: &SystemRootProof) -> Result<SystemRootClaim, MutationError> {
        self.inner.claim_system_root(candidate).await
    }

    async fn system_root_claim(&self) -> Result<Option<SystemRootProof>, RetrievalError> { self.inner.system_root_claim().await }

    async fn release_system_root_claim(&self, expected: &SystemRootProof) -> Result<bool, MutationError> {
        self.inner.release_system_root_claim(expected).await
    }

    // A wrapper must FORWARD the resolver injection: the trait default is a
    // no-op, and swallowing it leaves the inner engine unable to stamp model
    // ids on reconstructed envelopes (#330) or name columns from the catalog.
    fn set_property_resolver(&self, resolver: std::sync::Weak<dyn ankurah::core::property::PropertyResolver>) {
        self.inner.set_property_resolver(resolver);
    }
}

/// Wraps a storage collection, deferring to the shared engine-level counters so
/// that crash points count operations globally (a batch spanning collections
/// crashes at the right absolute operation).
struct CrashStorageCollection<E: CrashFlushable> {
    inner: Arc<dyn StorageCollection>,
    state: Arc<CrashState>,
    flush: Arc<E>,
}

#[async_trait]
impl<E: CrashFlushable + Send + Sync + 'static> StorageCollection for CrashStorageCollection<E> {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        self.state.check_set_state(self.flush.as_ref());
        self.inner.set_state(state).await
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> { self.inner.get_state(id).await }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        self.inner.fetch_states(selection).await
    }

    async fn add_event(&self, event: &Attested<Event>) -> Result<bool, MutationError> {
        self.state.check_add_event(self.flush.as_ref());
        self.inner.add_event(event).await
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        self.inner.get_events(event_ids).await
    }

    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        self.inner.dump_entity_events(id).await
    }
}

// ============================================================================
// CHILD / PARENT RE-EXEC PLUMBING
// ============================================================================

/// Read the crash point configured for this process, if any. When this returns
/// `Some`, the process is the crash child and should run the child workload.
pub fn child_crash_point() -> Option<CrashPoint> { std::env::var(ENV_CRASH_POINT).ok().and_then(|s| CrashPoint::from_env(&s)) }

/// Read the sled directory configured for this process.
pub fn child_sled_dir() -> Option<PathBuf> { std::env::var(ENV_SLED_DIR).ok().map(PathBuf::from) }

/// Durably write a `key=value` line to the handoff file, fsyncing so the value
/// survives the imminent crash. Call this in the child BEFORE the operation that
/// aborts. Appends, so multiple values (e.g. several event ids) can be recorded.
pub fn handoff_write(key: &str, value: &str) -> Result<()> {
    use std::io::Write;
    let path = std::env::var(ENV_HANDOFF_FILE).map_err(|_| anyhow::anyhow!("no handoff file configured"))?;
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(f, "{key}={value}")?;
    f.flush()?;
    f.sync_all()?; // durability: the crash must not lose this line
    Ok(())
}

/// Durably record a full attested event under `key` so the parent can
/// re-deliver the identical event after reopen (modelling "the peer that sent
/// the batch re-sends it"). Events are content-addressed, so a re-delivered
/// event keeps its original id. Encoded as base64(bincode) on one line.
pub fn handoff_write_event(key: &str, event: &Attested<Event>) -> Result<()> {
    use base64::Engine as _;
    let bytes = bincode::serialize(event)?;
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    handoff_write(key, &b64)
}

/// Parse the handoff file the child produced into key -> values (a key may
/// repeat). Missing file yields an empty map.
pub fn handoff_read(path: &Path) -> std::collections::HashMap<String, Vec<String>> {
    let mut map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    let Ok(contents) = std::fs::read_to_string(path) else {
        return map;
    };
    for line in contents.lines() {
        if let Some((k, v)) = line.split_once('=') {
            map.entry(k.to_string()).or_default().push(v.to_string());
        }
    }
    map
}

/// Outcome of running a crash child to completion.
pub struct ChildOutcome {
    pub status: std::process::ExitStatus,
    pub stdout: String,
    pub stderr: String,
    /// Parsed handoff file: identifiers the child recorded before crashing.
    pub handoff: std::collections::HashMap<String, Vec<String>>,
}

impl ChildOutcome {
    /// First value the child recorded for `key`, parsed as an [`EntityId`].
    pub fn entity_id(&self, key: &str) -> Option<EntityId> {
        self.handoff.get(key).and_then(|v| v.first()).and_then(|s| EntityId::from_base64(s.trim()).ok())
    }

    /// All values the child recorded for `key`, parsed as [`EventId`]s.
    pub fn event_ids(&self, key: &str) -> Vec<EventId> {
        self.handoff.get(key).map(|vs| vs.iter().filter_map(|s| EventId::from_base64(s.trim()).ok()).collect()).unwrap_or_default()
    }

    /// All full attested events the child recorded under `key`, in write order.
    pub fn events(&self, key: &str) -> Vec<Attested<Event>> {
        use base64::Engine as _;
        self.handoff
            .get(key)
            .map(|vs| {
                vs.iter()
                    .filter_map(|s| base64::engine::general_purpose::STANDARD.decode(s.trim()).ok())
                    .filter_map(|bytes| bincode::deserialize::<Attested<Event>>(&bytes).ok())
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl ChildOutcome {
    /// True when the child died from a signal (abort raises SIGABRT), which is
    /// how a `std::process::abort` crash presents to the parent on unix.
    pub fn crashed(&self) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            self.status.signal().is_some()
        }
        #[cfg(not(unix))]
        {
            // On non-unix, abort surfaces as a non-zero, non-clean exit.
            !self.status.success()
        }
    }
}

/// Spawn this same test binary as a child, running exactly the named test with
/// the crash point and sled directory injected. Waits for the child to die and
/// returns its outcome. The child is expected to abort; the caller asserts on
/// [`ChildOutcome::crashed`].
///
/// `test_name` must be the fully qualified test path as `cargo test` / libtest
/// filters it, e.g. `scenarios::child_commit_event_before_set_state`.
pub fn spawn_crash_child(test_name: &str, sled_dir: &Path, crash: CrashPoint) -> Result<ChildOutcome> {
    let sled_dir_str = sled_dir.to_string_lossy().into_owned();
    spawn_crash_child_with(test_name, crash, &[(ENV_SLED_DIR, &sled_dir_str)])
}

/// General child spawn: run exactly `test_name` with the crash point, a private
/// handoff file, and any extra environment variables (e.g. a sled directory or a
/// postgres URI). Waits for the child to die and returns its outcome.
pub fn spawn_crash_child_with(test_name: &str, crash: CrashPoint, extra_env: &[(&str, &str)]) -> Result<ChildOutcome> {
    let exe = std::env::current_exe()?;
    // A unique per-spawn handoff file in the temp dir.
    static NONCE: AtomicUsize = AtomicUsize::new(0);
    let n = NONCE.fetch_add(1, Ordering::SeqCst);
    let handoff_file = std::env::temp_dir().join(format!("ankurah-c6-handoff-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_file(&handoff_file);

    let mut cmd = std::process::Command::new(exe);
    cmd.arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        // libtest runs one thread so counters stay deterministic; also ensures
        // the abort is not racing sibling tests in the child binary.
        .arg("--test-threads=1")
        .env(ENV_CRASH_POINT, crash.to_env())
        .env(ENV_HANDOFF_FILE, &handoff_file);
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    let output = cmd.output()?;

    let handoff = handoff_read(&handoff_file);
    let _ = std::fs::remove_file(&handoff_file);

    Ok(ChildOutcome {
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        handoff,
    })
}

/// Allocate a fresh, unique scratch sled directory for a crash scenario. Placed
/// under the OS temp dir with a pid+nonce suffix so parallel runs never collide.
pub fn fresh_sled_dir(label: &str) -> PathBuf {
    static NONCE: AtomicUsize = AtomicUsize::new(0);
    let n = NONCE.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("ankurah-c6-{label}-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

// ============================================================================
// REOPEN INVARIANT ASSERTIONS (reusable)
// ============================================================================

/// Open a persistent sled engine at `dir` (the same directory a crashed child
/// wrote to). Reuses the production `with_path` opener so recovery goes through
/// the real reopen path, not a test shortcut.
pub fn reopen_sled(dir: &Path) -> Result<SledStorageEngine> { SledStorageEngine::with_path(dir.to_path_buf()) }

/// Minimal [`PropertyResolver`] for bare-engine (node-less) recovery probes:
/// the parent reads persisted states straight off the reopened engine, and the
/// bucket needs a model id to stamp reconstructed envelopes (#330). Maps
/// exactly the one collection under test to the model id the child allocated
/// (recorded in the handoff); property naming is irrelevant to these probes.
struct HandoffModelResolver {
    collection: String,
    model: EntityId,
}

impl ankurah::core::property::PropertyResolver for HandoffModelResolver {
    fn resolve(&self, _collection: &str, _name: &str) -> Option<EntityId> { None }
    fn name_for(&self, _id: &EntityId) -> Option<String> { None }
    fn model_id_for(&self, collection: &str) -> Option<EntityId> { (collection == self.collection).then_some(self.model) }
}

/// Wire a [`HandoffModelResolver`] into a reopened bare engine. The caller
/// must hold the returned `Arc` for as long as it reads through the engine
/// (the engine keeps only a `Weak`).
pub fn handoff_model_resolver(
    engine: &SledStorageEngine,
    collection: &str,
    model: EntityId,
) -> Arc<dyn ankurah::core::property::PropertyResolver> {
    let resolver: Arc<dyn ankurah::core::property::PropertyResolver> =
        Arc::new(HandoffModelResolver { collection: collection.to_string(), model });
    engine.set_property_resolver(Arc::downgrade(&resolver));
    resolver
}

/// Core recovery invariant: no persisted state may reference an event that is
/// missing from storage.
///
/// For every entity that has a persisted state in `collection`, each event id
/// in that state's head Clock must be fetchable from the same collection. A
/// violation means the node persisted a state whose causal frontier points at
/// events it does not have, which would make the entity unresolvable and break
/// convergence. Orphaned events (present but unreferenced) are explicitly
/// harmless and are NOT a violation.
pub async fn assert_state_heads_resolvable(collection: &Arc<dyn StorageCollection>, entity_ids: &[EntityId]) -> Result<()> {
    for id in entity_ids {
        let state = match collection.get_state(*id).await {
            Ok(state) => state,
            Err(RetrievalError::EntityNotFound(_)) => continue, // no persisted state: nothing to check
            Err(e) => return Err(e.into()),
        };
        let head = state.payload.state.head.clone();
        let head_ids: Vec<EventId> = head.as_slice().to_vec();
        if head_ids.is_empty() {
            continue;
        }
        let found = collection.get_events(head_ids.clone()).await?;
        let found_set: std::collections::HashSet<EventId> = found.iter().map(|e| e.payload.id()).collect();
        for hid in &head_ids {
            assert!(
                found_set.contains(hid),
                "recovery invariant violated: entity {} persisted head references event {} which is MISSING from storage",
                id.to_base64_short(),
                hid.to_base64_short(),
            );
        }
    }
    Ok(())
}

/// Whether a given entity has any persisted state after reopen.
pub async fn has_persisted_state(collection: &Arc<dyn StorageCollection>, id: EntityId) -> Result<bool> {
    match collection.get_state(id).await {
        Ok(_) => Ok(true),
        Err(RetrievalError::EntityNotFound(_)) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Whether a given event id is present in storage after reopen.
pub async fn event_present(collection: &Arc<dyn StorageCollection>, id: EventId) -> Result<bool> {
    let found = collection.get_events(vec![id.clone()]).await?;
    Ok(found.into_iter().any(|e| e.payload.id() == id))
}

/// Fetch a persisted state's head clock, or None if no state is persisted.
pub async fn persisted_head(collection: &Arc<dyn StorageCollection>, id: EntityId) -> Result<Option<proto::Clock>> {
    match collection.get_state(id).await {
        Ok(state) => Ok(Some(state.payload.state.head)),
        Err(RetrievalError::EntityNotFound(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}
