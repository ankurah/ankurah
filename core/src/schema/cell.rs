use append_only_vec::AppendOnlyVec;
use std::sync::atomic::{AtomicU32, Ordering};

/// Identifies a node's system-scoped entities and descriptor bindings. Each node gets a fresh epoch;
/// it never changes systems in place. Connections and disconnections retain the same epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SystemEpoch(u32);

static NEXT_SYSTEM_EPOCH: AtomicU32 = AtomicU32::new(1);

impl SystemEpoch {
    /// Reserved for temporary entities and pinned schema lookups before system readiness.
    pub const BOOTSTRAP: SystemEpoch = SystemEpoch(0);

    pub(crate) fn allocate() -> Self {
        let raw = NEXT_SYSTEM_EPOCH.fetch_add(1, Ordering::Relaxed);
        assert!(raw != 0, "system epoch allocator exhausted (u32 wrapped)");
        Self(raw)
    }
}

/// A cell that is either pinned to a static value, or writable once per system epoch.
#[derive(Debug)]
pub enum PerSystemOnceCell<T: Copy> {
    Pinned(T),
    PerEpoch(AppendOnlyVec<(u32, T)>),
}

impl<T: Copy> PerSystemOnceCell<T> {
    pub const fn per_epoch() -> Self { Self::PerEpoch(AppendOnlyVec::new()) }

    /// The identity for `epoch`; pinned identities ignore the epoch.
    pub fn get(&self, epoch: SystemEpoch) -> Option<T> {
        match self {
            Self::Pinned(value) => Some(*value),
            Self::PerEpoch(entries) => entries.iter().find(|(e, _)| *e == epoch.0).map(|(_, value)| *value),
        }
    }

    /// Record a resolution without replacing an epoch's first value.
    pub fn set(&self, epoch: SystemEpoch, value: T) {
        match self {
            Self::Pinned(_) => {}
            Self::PerEpoch(entries) => {
                entries.push((epoch.0, value));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(n: u32) -> SystemEpoch { SystemEpoch(n) }

    #[test]
    fn first_entry_for_an_epoch_wins_for_its_lifetime() {
        let cell: PerSystemOnceCell<u8> = PerSystemOnceCell::per_epoch();
        cell.set(epoch(1), 10);
        cell.set(epoch(1), 20); // a later differing append cannot retype the epoch
        assert_eq!(cell.get(epoch(1)), Some(10));
    }

    #[test]
    fn epochs_are_isolated() {
        let cell: PerSystemOnceCell<u8> = PerSystemOnceCell::per_epoch();
        cell.set(epoch(1), 10);
        cell.set(epoch(2), 20);
        assert_eq!(cell.get(epoch(1)), Some(10));
        assert_eq!(cell.get(epoch(2)), Some(20));
        assert_eq!(cell.get(epoch(3)), None, "an unentered epoch misses; it never borrows another epoch's identity");
    }

    #[test]
    fn pinned_is_valid_at_every_epoch_and_ignores_writes() {
        let cell = PerSystemOnceCell::Pinned(7u8);
        assert_eq!(cell.get(epoch(0)), Some(7));
        assert_eq!(cell.get(epoch(99)), Some(7));
        cell.set(epoch(0), 8);
        assert_eq!(cell.get(epoch(0)), Some(7));
    }

    #[test]
    fn allocator_issues_distinct_values() {
        let a = SystemEpoch::allocate();
        let b = SystemEpoch::allocate();
        assert_ne!(a, b);
    }
}
