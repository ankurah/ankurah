//! The staging area: where events wait between arrival and commitment.
//!
//! Staging exists so an incoming event is BFS-discoverable (via `get_event`)
//! before it is committed to durable storage, and so events that cannot yet
//! apply (missing parents, missing entity state) survive until the thing they
//! are waiting for arrives. The former was always true per-call; the latter
//! requires staging to outlive a single applier call, which is what this type
//! provides when held at node scope. Getters consult it through the same
//! staging-then-storage discipline as before: `get_event` sees staging plus
//! storage, `event_stored` sees storage only.
//!
//! Entries leave by promotion (`remove` on commit) or by rejection (`remove`
//! on a policy/validation/lineage refusal: rejection is not buffering). The
//! cap is enforced at atomic batch admission. A batch that does not fit is
//! rejected whole; existing correctness-bearing entries are never evicted.
//! The receiver reports capacity pressure as a retryable acknowledgement, so
//! the sender retains the new batch until staged dependencies make room.
//!
//! All iteration surfaces are BTree-ordered: anything that walks staging must
//! be deterministic under the simulation harness.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};

use ankurah_proto::{Attested, Event, EventId};

/// Default bound on staged events per staging area. Generous for legitimate
/// in-flight windows (a delivery batch plus orphans awaiting parents), small
/// enough that an orphan flood cannot exhaust memory.
pub(crate) const DEFAULT_STAGING_CAP: usize = 10_000;

struct StagingInner {
    /// Staged events by id. `Attested` rather than bare `Event` because a
    /// buffered event must eventually be committed, and `commit_event`
    /// requires the attestations that arrived with it.
    events: BTreeMap<EventId, Attested<Event>>,
    /// Reverse index: parent id to the staged events that name it in their
    /// parent clock. This is the planner's descendant re-drive lookup: when a
    /// parent finally arrives, its buffered children are schedulable without
    /// scanning the whole area.
    children_by_parent: BTreeMap<EventId, BTreeSet<EventId>>,
}

/// Atomic staging admission failed because the batch's new unique ids would
/// exceed the area's configured capacity. Existing ids and duplicate ids
/// within the batch do not count toward `requested`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StagingCapacityError {
    pub(crate) capacity: usize,
    pub(crate) current: usize,
    pub(crate) requested: usize,
}

impl std::fmt::Display for StagingCapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "staging capacity {} cannot admit {} new events while holding {}", self.capacity, self.requested, self.current)
    }
}

impl std::error::Error for StagingCapacityError {}

impl From<StagingCapacityError> for crate::error::MutationError {
    fn from(value: StagingCapacityError) -> Self {
        crate::error::IngestError::StagingCapacity { capacity: value.capacity, current: value.current, requested: value.requested }.into()
    }
}

pub(crate) struct StagingArea {
    inner: RwLock<StagingInner>,
    cap: usize,
    rejected_batches: AtomicU64,
}

impl Default for StagingArea {
    /// The node-held per-collection areas are created lazily through
    /// `SafeMap::get_or_default`; default means the default cap.
    fn default() -> Self { Self::with_default_cap() }
}

impl StagingArea {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: RwLock::new(StagingInner { events: BTreeMap::new(), children_by_parent: BTreeMap::new() }),
            cap,
            rejected_batches: AtomicU64::new(0),
        }
    }

    pub fn with_default_cap() -> Self { Self::new(DEFAULT_STAGING_CAP) }

    /// The configured cap. Observability surface alongside `len` and
    /// `rejected_batches`, so bounds tests need not hardcode the default.
    #[allow(dead_code)]
    pub fn cap(&self) -> usize { self.cap }

    /// Atomically admit a whole batch. A batch is first deduplicated by event
    /// id, with the first envelope winning. Ids already present are no-ops.
    /// Capacity is checked and all insertions happen under the same write
    /// lock, so rejection cannot leave a partial batch or reverse-index
    /// residue behind.
    pub fn try_stage_batch<I>(&self, events: I) -> Result<(), StagingCapacityError>
    where I: IntoIterator<Item = Attested<Event>> {
        let mut incoming = BTreeMap::<EventId, Attested<Event>>::new();
        for attested in events {
            incoming.entry(attested.payload.id()).or_insert(attested);
        }

        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        incoming.retain(|id, _| !inner.events.contains_key(id));
        let requested = incoming.len();
        if inner.events.len() + requested > self.cap {
            self.rejected_batches.fetch_add(1, Ordering::Relaxed);
            return Err(StagingCapacityError { capacity: self.cap, current: inner.events.len(), requested });
        }

        for (id, attested) in incoming {
            Self::insert_admitted_locked(&mut inner, id, attested);
        }
        Ok(())
    }

    /// Admit one event using the same capacity discipline as a batch.
    pub fn try_stage(&self, attested: Attested<Event>) -> Result<(), StagingCapacityError> {
        self.try_stage_batch(std::iter::once(attested))
    }

    /// Test-fixture shorthand retained for the ingest module's existing unit
    /// tests. Production callers must choose and handle a fallible admission
    /// API explicitly.
    #[cfg(test)]
    pub fn stage(&self, attested: Attested<Event>) {
        self.try_stage(attested).expect("test staging fixture exceeds its configured capacity");
    }

    /// Insert after the caller has reserved capacity under the same write
    /// lock. Keeping this private prevents discovery or intake callers from
    /// bypassing the bound.
    fn insert_admitted_locked(inner: &mut StagingInner, id: EventId, attested: Attested<Event>) {
        for parent in attested.payload.parent.as_slice() {
            inner.children_by_parent.entry(parent.clone()).or_default().insert(id.clone());
        }
        inner.events.insert(id, attested);
    }

    /// Replace an already-staged entry's attested envelope. Event ids are
    /// content hashes, so the payload is identical by construction; what
    /// changes is the attestation set (the commit lanes attach check_event
    /// attestations this way). The reverse index is untouched. An absent id
    /// is admitted as a new single-item batch, without a two-lock race.
    pub fn restage(&self, attested: Attested<Event>) -> Result<(), StagingCapacityError> {
        let id = attested.payload.id();
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = inner.events.get_mut(&id) {
            *entry = attested;
            return Ok(());
        }
        if inner.events.len() >= self.cap {
            self.rejected_batches.fetch_add(1, Ordering::Relaxed);
            return Err(StagingCapacityError { capacity: self.cap, current: inner.events.len(), requested: 1 });
        }
        Self::insert_admitted_locked(&mut inner, id, attested);
        Ok(())
    }

    /// The staged event payload, if present. Mirrors what `get_event` needs.
    pub fn get(&self, id: &EventId) -> Option<Event> {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        inner.events.get(id).map(|a| a.payload.clone())
    }

    /// The staged event with its attestations, for commit or re-application.
    pub fn get_attested(&self, id: &EventId) -> Option<Attested<Event>> {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        inner.events.get(id).cloned()
    }

    pub fn contains(&self, id: &EventId) -> bool {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        inner.events.contains_key(id)
    }

    /// Remove an event (promotion on commit, or rejection). Cleans the
    /// reverse index synchronously, leaving no auxiliary bookkeeping behind.
    pub fn remove(&self, id: &EventId) -> Option<Attested<Event>> {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        Self::remove_locked(&mut inner, id)
    }

    fn remove_locked(inner: &mut StagingInner, id: &EventId) -> Option<Attested<Event>> {
        let attested = inner.events.remove(id)?;
        for parent in attested.payload.parent.as_slice() {
            if let Some(children) = inner.children_by_parent.get_mut(parent) {
                children.remove(id);
                if children.is_empty() {
                    inner.children_by_parent.remove(parent);
                }
            }
        }
        Some(attested)
    }

    /// Staged events whose parent clock names `parent`, in id order. The
    /// planner schedules these when `parent` becomes applied (descendant
    /// re-drive); id order keeps the schedule deterministic before the
    /// topological sort imposes causal order.
    pub fn staged_children_of(&self, parent: &EventId) -> Vec<EventId> {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        inner.children_by_parent.get(parent).map(|s| s.iter().cloned().collect()).unwrap_or_default()
    }

    // Observability surface: consumed by tests today, by the R8 bounds test
    // and D7 counters as they land.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        inner.events.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Total atomic batch admission rejections since construction.
    #[allow(dead_code)]
    pub fn rejected_batches(&self) -> u64 { self.rejected_batches.load(Ordering::Relaxed) }

    /// Compatibility counter for the public test probe. Atomic admission no
    /// longer evicts retained entries, so this is always zero.
    #[allow(dead_code)]
    pub fn evictions(&self) -> u64 { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::{Attestation, EntityId, OperationSet};
    use std::collections::BTreeMap;

    fn event(entity_id: EntityId, parents: &[&Attested<Event>]) -> Attested<Event> {
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

    #[test]
    fn stage_then_get_roundtrips() {
        let area = StagingArea::with_default_cap();
        let e = event(EntityId::new(), &[]);
        let id = e.payload.id();

        assert!(!area.contains(&id));
        area.try_stage(e.clone()).expect("single event fits");
        assert!(area.contains(&id));
        assert_eq!(area.get(&id).unwrap().id(), id);
        assert_eq!(area.get_attested(&id).unwrap().payload.id(), id);
        assert_eq!(area.len(), 1);
    }

    #[test]
    fn duplicate_ids_count_once_and_preserve_the_first_envelope() {
        let area = StagingArea::new(2);
        let first = event(EntityId::new(), &[]);
        let mut duplicate = first.clone();
        duplicate.attestations.push(Attestation(vec![0xDD]));
        let other = event(EntityId::new(), &[]);

        area.try_stage_batch([first.clone(), duplicate, other]).expect("one duplicate means only two slots are needed");

        assert_eq!(area.len(), 2);
        let staged = area.get_attested(&first.payload.id()).expect("first event is staged");
        assert!(staged.attestations.is_empty(), "the first envelope wins within a batch");
        assert_eq!(area.rejected_batches(), 0);
    }

    #[test]
    fn reverse_index_tracks_staged_children_per_parent() {
        let area = StagingArea::with_default_cap();
        let entity = EntityId::new();
        let genesis = event(entity, &[]);
        let genesis_id = genesis.payload.id();
        let child_a = event(entity, &[&genesis]);
        let child_b = event(EntityId::new(), &[&genesis]);

        area.try_stage_batch([child_a.clone(), child_b.clone()]).expect("children fit");

        let mut expected = vec![child_a.payload.id(), child_b.payload.id()];
        expected.sort();
        assert_eq!(area.staged_children_of(&genesis_id), expected);
    }

    #[test]
    fn remove_cleans_the_reverse_index() {
        let area = StagingArea::with_default_cap();
        let entity = EntityId::new();
        let genesis = event(entity, &[]);
        let genesis_id = genesis.payload.id();
        let child = event(entity, &[&genesis]);
        let child_id = child.payload.id();

        area.try_stage(child).expect("child fits");
        assert_eq!(area.staged_children_of(&genesis_id), vec![child_id.clone()]);

        let removed = area.remove(&child_id).expect("child was staged");
        assert_eq!(removed.payload.id(), child_id);
        assert!(area.staged_children_of(&genesis_id).is_empty());
        assert!(area.is_empty());
    }

    #[test]
    fn over_capacity_batch_is_rejected_atomically() {
        let area = StagingArea::new(2);
        let first = event(EntityId::new(), &[]);
        let second = event(EntityId::new(), &[]);
        let third = event(EntityId::new(), &[]);
        let ids = [first.payload.id(), second.payload.id(), third.payload.id()];

        let error = area.try_stage_batch([first, second, third]).expect_err("three new ids do not fit in two slots");

        assert_eq!(error, StagingCapacityError { capacity: 2, current: 0, requested: 3 });
        assert_eq!(area.len(), 0, "rejection inserts no prefix");
        assert!(ids.iter().all(|id| !area.contains(id)));
        assert_eq!(area.rejected_batches(), 1);
        assert_eq!(area.evictions(), 0, "atomic admission never evicts a retained event");
    }

    #[test]
    fn rejected_batch_leaves_existing_events_and_indexes_unchanged() {
        let area = StagingArea::new(2);
        let existing = event(EntityId::new(), &[]);
        let parent = event(EntityId::new(), &[]);
        let child_a = event(EntityId::new(), &[&parent]);
        let child_b = event(EntityId::new(), &[&parent]);
        let existing_id = existing.payload.id();
        let parent_id = parent.payload.id();

        area.try_stage(existing.clone()).expect("existing event fits");
        let error = area
            .try_stage_batch([existing, child_a.clone(), child_b.clone()])
            .expect_err("a batch that does not fit cannot evict retained entries");

        assert_eq!(error, StagingCapacityError { capacity: 2, current: 1, requested: 2 });
        assert_eq!(area.len(), 1);
        assert!(area.contains(&existing_id), "atomic rejection preserves retained entries");
        assert!(!area.contains(&child_a.payload.id()));
        assert!(!area.contains(&child_b.payload.id()));
        assert!(area.staged_children_of(&parent_id).is_empty());
        assert_eq!(area.evictions(), 0);
    }

    #[test]
    fn restage_replaces_in_place_but_cannot_bypass_capacity() {
        let area = StagingArea::new(1);
        let original = event(EntityId::new(), &[]);
        let mut updated = original.clone();
        updated.attestations.push(Attestation(vec![0xAA]));
        let other = event(EntityId::new(), &[]);

        area.try_stage(original).expect("first event fits");
        area.restage(updated.clone()).expect("replacement consumes no capacity");
        let error = area.restage(other.clone()).expect_err("an absent id still needs a free slot");

        assert_eq!(area.get_attested(&updated.payload.id()).expect("replacement is staged").attestations, updated.attestations);
        assert!(!area.contains(&other.payload.id()));
        assert_eq!(error, StagingCapacityError { capacity: 1, current: 1, requested: 1 });
    }

    #[test]
    fn stage_remove_churn_leaves_bounded_bookkeeping() {
        let area = StagingArea::new(1);
        let parent = event(EntityId::new(), &[]);

        for _ in 0..10_000 {
            let child = event(EntityId::new(), &[&parent]);
            let child_id = child.payload.id();
            area.try_stage(child).expect("removal frees the sole slot on every iteration");
            area.remove(&child_id).expect("just-staged child is present");
        }

        let inner = area.inner.read().unwrap_or_else(|e| e.into_inner());
        assert!(inner.events.is_empty());
        assert!(inner.children_by_parent.is_empty(), "removal synchronously clears all auxiliary bookkeeping");
        assert_eq!(area.rejected_batches(), 0);
    }
}
