//! The bounded in-memory UNVERIFIED set (plan REV 4, D2-3/D2-4).
//!
//! Records the ids of events ADMITTED WITHOUT the generation equation check:
//! the adopted-history lanes (an integrated-but-unstored backfill below the
//! adopted horizon, state-adoption cargo) and any admission whose parents
//! were not locally resolvable at the boundary. Membership makes an event's
//! generation INELIGIBLE for the M5 accelerations (prechecks consume only
//! verified, non-saturated values; derivations 5b-ii); the set stores ids
//! only, never generation values (the registry ban: generations live only
//! on event payloads).
//!
//! Loss is safe by design: restart, eviction, or overflow degrades to
//! default-eligible, which the suppress-only usage discipline caps at a
//! wasted or missed shortcut and walk-time edge checks later demote (plan
//! section 4). That is why this is memory-only and FIFO-bounded, never
//! persisted.
//!
//! This struct is the node's ACCELERATION-ELIGIBILITY CONTEXT: the one
//! node-level object every comparison site already receives (apply_event,
//! apply_state, and the bridge walk all take it), which is why it also
//! carries the M6 KILL-SWITCH (plan M6): a runtime flag that makes every
//! generation CONSUMER in the comparison dormant (P1/P2 prechecks,
//! generation-first level scheduling, walk-time edge checks) while stamping
//! and admission verification stay on. Same reachability argument that
//! homed this set on WeakEntitySet (REV 5 section G): the consumers are the
//! apply-path comparisons, and this is the object that reaches them.

use ankurah_proto::EventId;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// Default bound: sized like the applied-set default (plan D2-5), generously
/// above the largest expected bridge batch (the lane benches exercise
/// 5000-event bridges). The container holds each id twice (the FIFO order
/// plus the membership set), so a full set is roughly 4 MiB of ids per node
/// at 32 bytes an id, plus set overhead.
pub(crate) const DEFAULT_UNVERIFIED_CAP: usize = 65536;

/// Bounded FIFO set of event ids admitted without generation verification,
/// plus the node-wide generation-acceleration kill-switch (see the module
/// doc for why the switch lives here).
#[derive(Debug)]
pub struct UnverifiedEvents {
    inner: Mutex<Inner>,
    cap: usize,
    /// The M6 kill-switch (plan M6): when set, every generation consumer in
    /// the comparison is bypassed (prechecks, schedule keying, edge checks);
    /// stamping and admission verification are expressly NOT gated on this.
    accel_disabled: AtomicBool,
}

#[derive(Debug, Default)]
struct Inner {
    order: VecDeque<EventId>,
    members: HashSet<EventId>,
}

impl Default for UnverifiedEvents {
    fn default() -> Self { Self::with_cap(DEFAULT_UNVERIFIED_CAP) }
}

impl UnverifiedEvents {
    pub fn with_cap(cap: usize) -> Self {
        Self { inner: Mutex::new(Inner::default()), cap: cap.max(1), accel_disabled: AtomicBool::new(false) }
    }

    /// Throw (or clear) the generation-acceleration kill-switch (plan M6).
    /// With the switch thrown, comparisons run exactly as they would with no
    /// generation machinery: no precheck consults an operand, the level
    /// drain schedules in plain id order, and no walk-time edge check is
    /// evaluated. Verdicts are identical either way (the suppress-only
    /// discipline; the kill-switch equivalence pins assert it), so throwing
    /// the switch can only cost the accelerations. Stamping and admission
    /// verification are NOT affected: events keep their hashed generations
    /// and forged stamps keep rejecting typed.
    pub fn set_accelerations_disabled(&self, disabled: bool) { self.accel_disabled.store(disabled, Ordering::Release); }

    /// Whether the kill-switch is thrown (consumed by the comparison at its
    /// entry point, once per comparison).
    pub fn accelerations_disabled(&self) -> bool { self.accel_disabled.load(Ordering::Acquire) }

    /// Record an admitted-unverified event id. Oldest-first eviction at the
    /// cap: an evicted id becomes default-eligible again, the documented safe
    /// degradation.
    pub fn insert(&self, id: EventId) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.members.insert(id.clone()) {
            return; // already tracked; keep its original eviction position
        }
        inner.order.push_back(id);
        while inner.order.len() > self.cap {
            if let Some(evicted) = inner.order.pop_front() {
                inner.members.remove(&evicted);
            }
        }
    }

    /// Whether an event was admitted unverified (and is therefore ineligible
    /// for generation-consuming accelerations, D2-4).
    pub fn contains(&self, id: &EventId) -> bool { self.inner.lock().unwrap().members.contains(id) }

    pub fn len(&self) -> usize { self.inner.lock().unwrap().order.len() }

    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(b: u8) -> EventId { EventId::from_bytes([b; 32]) }

    #[test]
    fn insert_contains_and_dedup() {
        let set = UnverifiedEvents::with_cap(8);
        assert!(set.is_empty());
        set.insert(id(1));
        set.insert(id(1));
        set.insert(id(2));
        assert!(set.contains(&id(1)) && set.contains(&id(2)) && !set.contains(&id(3)));
        assert_eq!(set.len(), 2, "re-inserting an id must not duplicate it");
    }

    #[test]
    fn cap_evicts_oldest_first() {
        let set = UnverifiedEvents::with_cap(3);
        for b in 1..=4u8 {
            set.insert(id(b));
        }
        assert_eq!(set.len(), 3);
        assert!(!set.contains(&id(1)), "oldest id evicted at the cap (safe: it becomes default-eligible)");
        assert!(set.contains(&id(2)) && set.contains(&id(3)) && set.contains(&id(4)));
    }
}
