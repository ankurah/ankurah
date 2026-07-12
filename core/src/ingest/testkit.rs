//! Shared test doubles for the ingest pipeline's unit tests (executor,
//! state_apply). Test-only; compiled under cfg(test) from mod.rs.

use std::collections::BTreeMap;

use ankurah_proto::{Attested, EntityId, Event, EventId, OperationSet};
use async_trait::async_trait;

use super::staging::StagingArea;
use crate::entity::Entity;
use crate::error::{MutationError, RetrievalError};
use crate::ingest::PersistState;
use crate::retrieval::{GetEvents, GetState, SuspenseEvents};

/// Forge an honestly stamped event over in-scope parent events (registry
/// ban: the generation is read from the parent payloads in hand).
pub(crate) fn event(entity_id: EntityId, parents: &[&Attested<Event>]) -> Attested<Event> {
    Attested::opt(
        Event {
            entity_id,
            model: EntityId::from_bytes([0xEE; 16]),
            operations: OperationSet(BTreeMap::new()),
            parent: ankurah_proto::Clock::from(parents.iter().map(|p| p.payload.id()).collect::<Vec<_>>()),
            generation: Event::generation_from_parents(parents.iter().map(|p| p.payload.generation)),
        },
        None,
    )
}

/// Forge an honestly stamped event carrying a real LWW write. Ids are
/// content-addressed, so shapes that need DISTINCT SIBLINGS over one parent
/// (concurrent-edit shapes) must differ in operations; two empty-ops
/// children of the same parent are one event.
pub(crate) fn event_with_title(entity_id: EntityId, title: &str, parents: &[&Attested<Event>]) -> Attested<Event> {
    use crate::property::backend::{lww::LWWBackend, PropertyBackend};
    use crate::value::Value;
    let backend = LWWBackend::new();
    backend.set("title".into(), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().expect("LWW serializes").expect("a write produces operations");
    Attested::opt(
        Event {
            entity_id,
            model: EntityId::from_bytes([0xEE; 16]),
            operations: OperationSet(BTreeMap::from([("lww".to_owned(), ops)])),
            parent: ankurah_proto::Clock::from(parents.iter().map(|p| p.payload.id()).collect::<Vec<_>>()),
            generation: Event::generation_from_parents(parents.iter().map(|p| p.payload.generation)),
        },
        None,
    )
}

/// Getter whose `commit_event` fails for one designated event id (the
/// deterministic stand-in for a transient storage fault) and retains
/// successful commits (bodies included) so `event_stored` and
/// `get_local_event` answer like real storage.
///
/// Counts `get_event` calls (walk fetches: the honest zero-fetch instrument
/// for the O(1)-skip pins R-D2-3a/3c) and `get_local_event` calls (the
/// admission verifier's parent payload reads: the zero-read instrument for
/// the M4 covered-parent verification pin). `event_stored` is a cheap point
/// read and deliberately NOT counted (the storedness conjunct of obligation
/// (d) legitimately remains on a served fast path).
pub(crate) struct FailingCommitStore {
    pub(crate) staging: std::sync::Arc<StagingArea>,
    committed: std::sync::RwLock<BTreeMap<EventId, Attested<Event>>>,
    fail_commit_of: EventId,
    definitive: bool,
    get_event_calls: std::sync::atomic::AtomicUsize,
    get_local_event_calls: std::sync::atomic::AtomicUsize,
}

impl FailingCommitStore {
    pub(crate) fn new(staging: std::sync::Arc<StagingArea>, fail_commit_of: EventId) -> Self {
        Self {
            staging,
            committed: std::sync::RwLock::new(BTreeMap::new()),
            fail_commit_of,
            definitive: true,
            get_event_calls: std::sync::atomic::AtomicUsize::new(0),
            get_local_event_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Non-definitive (ephemeral) flavor: absence of an event is not
    /// proof of nonexistence, so the planner schedules speculatively.
    /// The adopted-history shapes live on ephemeral nodes.
    pub(crate) fn ephemeral(staging: std::sync::Arc<StagingArea>, fail_commit_of: EventId) -> Self {
        Self {
            staging,
            committed: std::sync::RwLock::new(BTreeMap::new()),
            fail_commit_of,
            definitive: false,
            get_event_calls: std::sync::atomic::AtomicUsize::new(0),
            get_local_event_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Number of `get_event` calls (walk fetches) since construction.
    pub(crate) fn get_event_calls(&self) -> usize { self.get_event_calls.load(std::sync::atomic::Ordering::Relaxed) }

    /// Number of `get_local_event` calls (admission verification payload
    /// reads) since construction.
    pub(crate) fn get_local_event_calls(&self) -> usize { self.get_local_event_calls.load(std::sync::atomic::Ordering::Relaxed) }
}

#[async_trait]
impl GetEvents for FailingCommitStore {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        self.get_event_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(event) = self.staging.get(event_id) {
            return Ok(event);
        }
        self.committed
            .read()
            .unwrap()
            .get(event_id)
            .map(|e| e.payload.clone())
            .ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }
    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        Ok(self.committed.read().unwrap().contains_key(event_id))
    }
    fn storage_is_definitive(&self) -> bool { self.definitive }
}

#[async_trait]
impl SuspenseEvents for FailingCommitStore {
    fn stage_event(&self, event: Event) { self.staging.stage(Attested::opt(event, None)); }
    async fn commit_event(&self, attested: &Attested<Event>) -> Result<(), MutationError> {
        if attested.payload.id() == self.fail_commit_of {
            return Err(MutationError::General("injected transient commit failure".into()));
        }
        self.committed.write().unwrap().insert(attested.payload.id(), attested.clone());
        self.staging.remove(&attested.payload.id());
        Ok(())
    }
    async fn get_local_event(&self, event_id: &EventId) -> Result<Option<Event>, RetrievalError> {
        self.get_local_event_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(event) = self.staging.get(event_id) {
            return Ok(Some(event));
        }
        Ok(self.committed.read().unwrap().get(event_id).map(|e| e.payload.clone()))
    }
}

/// GetState stub for adopted-state shapes: no stored state anywhere.
pub(crate) struct NoState;
#[async_trait]
impl GetState for NoState {
    async fn get_state(&self, _entity_id: EntityId) -> Result<Option<Attested<ankurah_proto::EntityState>>, RetrievalError> { Ok(None) }
}

pub(crate) struct NoopPersist;
#[async_trait]
impl PersistState for NoopPersist {
    async fn persist(&self, _entity: &Entity) -> Result<(), MutationError> { Ok(()) }
}

pub(crate) struct RecordingPersist(std::sync::atomic::AtomicBool);
impl RecordingPersist {
    pub(crate) fn new() -> Self { Self(std::sync::atomic::AtomicBool::new(false)) }
    pub(crate) fn called(&self) -> bool { self.0.load(std::sync::atomic::Ordering::SeqCst) }
}
#[async_trait]
impl PersistState for RecordingPersist {
    async fn persist(&self, _entity: &Entity) -> Result<(), MutationError> {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
