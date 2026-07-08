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
//! Entries leave by promotion (`remove` on commit), by rejection (`remove` on
//! a policy/validation/lineage refusal: rejection is not buffering), or by
//! cap eviction. Eviction is a liveness valve against orphan flooding, not a
//! cache policy: it is counted, logged, and safe because a legitimately
//! evicted event is re-delivered by its sender's retry and re-application is
//! idempotent. Per-source rate limiting (#274) is the proper bound on forged
//! breadth; the cap holds the line until then.
//!
//! All iteration surfaces are BTree-ordered: anything that walks staging must
//! be deterministic under the simulation harness.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
    /// Arrival order for cap eviction, oldest first. May contain ids that
    /// have since been removed; eviction skips them lazily.
    arrival: VecDeque<EventId>,
}

pub(crate) struct StagingArea {
    inner: RwLock<StagingInner>,
    cap: usize,
    evictions: AtomicU64,
}

impl Default for StagingArea {
    /// The node-held per-collection areas are created lazily through
    /// `SafeMap::get_or_default`; default means the default cap.
    fn default() -> Self { Self::with_default_cap() }
}

impl StagingArea {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: RwLock::new(StagingInner { events: BTreeMap::new(), children_by_parent: BTreeMap::new(), arrival: VecDeque::new() }),
            cap,
            evictions: AtomicU64::new(0),
        }
    }

    pub fn with_default_cap() -> Self { Self::new(DEFAULT_STAGING_CAP) }

    /// The configured cap. Observability surface alongside `len` and
    /// `evictions`, so bounds tests need not hardcode the default.
    #[allow(dead_code)]
    pub fn cap(&self) -> usize { self.cap }

    /// Stage an event. A no-op if the id is already staged: event ids are
    /// content hashes, so an identical id carries identical content and
    /// re-staging (sender retry, duplicate delivery) must not reset arrival
    /// order or duplicate index entries.
    pub fn stage(&self, attested: Attested<Event>) {
        let id = attested.payload.id();
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if inner.events.contains_key(&id) {
            return;
        }
        for parent in attested.payload.parent.as_slice() {
            inner.children_by_parent.entry(parent.clone()).or_default().insert(id.clone());
        }
        inner.events.insert(id.clone(), attested);
        inner.arrival.push_back(id);

        while inner.events.len() > self.cap {
            // Arrival entries for already-removed events are skipped; only a
            // live entry counts as an eviction.
            let Some(oldest) = inner.arrival.pop_front() else { break };
            if let Some(evicted) = Self::remove_locked(&mut inner, &oldest) {
                self.evictions.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    event_id = %oldest,
                    entity_id = %evicted.payload.entity_id,
                    cap = self.cap,
                    "staging area over cap; evicting oldest staged event (sender retry re-delivers)"
                );
            }
        }
    }

    /// Replace an already-staged entry's attested envelope. Event ids are
    /// content hashes, so the payload is identical by construction; what
    /// changes is the attestation set (the commit lanes attach check_event
    /// attestations this way). Arrival order and the reverse index are
    /// untouched. Falls back to a plain stage when the id is absent (cap
    /// eviction raced the caller), so the envelope is never lost.
    pub fn restage(&self, attested: Attested<Event>) {
        let id = attested.payload.id();
        {
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = inner.events.get_mut(&id) {
                *entry = attested;
                return;
            }
        }
        self.stage(attested);
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
    /// reverse index; the arrival queue is cleaned lazily by eviction.
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

    /// Total cap evictions since construction. Observability: a nonzero value
    /// under normal operation means either an orphan flood or a cap set too
    /// low for the workload.
    #[allow(dead_code)]
    pub fn evictions(&self) -> u64 { self.evictions.load(Ordering::Relaxed) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::{EntityId, OperationSet};
    use std::collections::BTreeMap;

    fn event(entity_id: EntityId, parent_ids: &[EventId]) -> Attested<Event> {
        Attested::opt(crate::test_gen::stamped(entity_id, "test", OperationSet(BTreeMap::new()), parent_ids), None)
    }

    #[test]
    fn stage_then_get_roundtrips() {
        let area = StagingArea::with_default_cap();
        let e = event(EntityId::new(), &[]);
        let id = e.payload.id();

        assert!(!area.contains(&id));
        area.stage(e.clone());
        assert!(area.contains(&id));
        assert_eq!(area.get(&id).unwrap().id(), id);
        assert_eq!(area.get_attested(&id).unwrap().payload.id(), id);
        assert_eq!(area.len(), 1);
    }

    #[test]
    fn restaging_same_id_is_a_noop() {
        let area = StagingArea::with_default_cap();
        let e = event(EntityId::new(), &[]);
        area.stage(e.clone());
        area.stage(e);
        assert_eq!(area.len(), 1);
    }

    #[test]
    fn reverse_index_tracks_staged_children_per_parent() {
        let area = StagingArea::with_default_cap();
        let entity = EntityId::new();
        let genesis = event(entity, &[]);
        let genesis_id = genesis.payload.id();
        let child_a = event(entity, &[genesis_id.clone()]);
        let child_b = event(EntityId::new(), &[genesis_id.clone()]);

        area.stage(child_a.clone());
        area.stage(child_b.clone());

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
        let child = event(entity, &[genesis_id.clone()]);
        let child_id = child.payload.id();

        area.stage(child);
        assert_eq!(area.staged_children_of(&genesis_id), vec![child_id.clone()]);

        let removed = area.remove(&child_id).expect("child was staged");
        assert_eq!(removed.payload.id(), child_id);
        assert!(area.staged_children_of(&genesis_id).is_empty());
        assert!(area.is_empty());
    }

    #[test]
    fn cap_eviction_is_oldest_first_and_counted() {
        let area = StagingArea::new(2);
        let first = event(EntityId::new(), &[]);
        let second = event(EntityId::new(), &[]);
        let third = event(EntityId::new(), &[]);
        let first_id = first.payload.id();

        area.stage(first);
        area.stage(second);
        assert_eq!(area.evictions(), 0);

        area.stage(third);
        assert_eq!(area.len(), 2);
        assert_eq!(area.evictions(), 1);
        assert!(!area.contains(&first_id), "oldest staged event is the one evicted");
    }

    #[test]
    fn eviction_skips_arrival_entries_already_removed() {
        let area = StagingArea::new(2);
        let first = event(EntityId::new(), &[]);
        let second = event(EntityId::new(), &[]);
        let third = event(EntityId::new(), &[]);
        let fourth = event(EntityId::new(), &[]);
        let first_id = first.payload.id();
        let second_id = second.payload.id();

        area.stage(first);
        area.stage(second);
        area.remove(&first_id);
        area.stage(third);
        assert_eq!(area.evictions(), 0, "removal freed capacity; no eviction");

        area.stage(fourth);
        assert_eq!(area.evictions(), 1);
        assert!(!area.contains(&second_id), "first's stale arrival entry is skipped; second is the oldest live entry");
    }

    #[test]
    fn eviction_cleans_the_reverse_index() {
        let area = StagingArea::new(1);
        let entity = EntityId::new();
        let genesis = event(entity, &[]);
        let genesis_id = genesis.payload.id();
        let child = event(entity, &[genesis_id.clone()]);
        let other = event(EntityId::new(), &[]);

        area.stage(child);
        assert_eq!(area.staged_children_of(&genesis_id).len(), 1);

        area.stage(other);
        assert!(area.staged_children_of(&genesis_id).is_empty(), "evicted child must leave no index residue");
    }
}
