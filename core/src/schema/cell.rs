//! Descriptor-resident identities keyed by a node's schema generation.

use append_only_vec::AppendOnlyVec;
use std::sync::atomic::{AtomicU32, Ordering};

/// A node's schema generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaEpoch(u32);

static NEXT_SCHEMA_EPOCH: AtomicU32 = AtomicU32::new(1);

impl SchemaEpoch {
    /// Reserved for bootstrap entities, whose identities are all pinned.
    pub const BOOTSTRAP: SchemaEpoch = SchemaEpoch(0);

    pub(crate) fn allocate() -> Self {
        let raw = NEXT_SCHEMA_EPOCH.fetch_add(1, Ordering::Relaxed);
        assert!(raw != 0, "schema epoch allocator exhausted (u32 wrapped)");
        Self(raw)
    }
}

/// A universal identity or an append-only identity resolved per epoch.
#[derive(Debug)]
pub enum SchemaOnceCell<T: Copy> {
    Pinned(T),
    PerEpoch(AppendOnlyVec<(u32, T)>),
}

impl<T: Copy> SchemaOnceCell<T> {
    pub const fn per_epoch() -> Self { Self::PerEpoch(AppendOnlyVec::new()) }

    /// The identity for `epoch`; pinned identities ignore the epoch.
    pub fn get(&self, epoch: SchemaEpoch) -> Option<T> {
        match self {
            Self::Pinned(value) => Some(*value),
            Self::PerEpoch(entries) => entries.iter().find(|(e, _)| *e == epoch.0).map(|(_, value)| *value),
        }
    }

    /// Record a resolution without replacing an epoch's first value.
    pub fn set(&self, epoch: SchemaEpoch, value: T) {
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

    fn epoch(n: u32) -> SchemaEpoch { SchemaEpoch(n) }

    #[test]
    fn first_entry_for_an_epoch_wins_for_its_lifetime() {
        let cell: SchemaOnceCell<u8> = SchemaOnceCell::per_epoch();
        cell.set(epoch(1), 10);
        cell.set(epoch(1), 20); // a later differing append cannot retype the epoch
        assert_eq!(cell.get(epoch(1)), Some(10));
    }

    #[test]
    fn epochs_are_isolated() {
        let cell: SchemaOnceCell<u8> = SchemaOnceCell::per_epoch();
        cell.set(epoch(1), 10);
        cell.set(epoch(2), 20);
        assert_eq!(cell.get(epoch(1)), Some(10));
        assert_eq!(cell.get(epoch(2)), Some(20));
        assert_eq!(cell.get(epoch(3)), None, "an unentered epoch misses; it never borrows another epoch's identity");
    }

    #[test]
    fn pinned_is_valid_at_every_epoch_and_ignores_writes() {
        let cell = SchemaOnceCell::Pinned(7u8);
        assert_eq!(cell.get(epoch(0)), Some(7));
        assert_eq!(cell.get(epoch(99)), Some(7));
        cell.set(epoch(0), 8);
        assert_eq!(cell.get(epoch(0)), Some(7));
    }

    #[test]
    fn benign_duplicate_appends_read_identically() {
        let cell: SchemaOnceCell<u8> = SchemaOnceCell::per_epoch();
        cell.set(epoch(5), 42);
        cell.set(epoch(5), 42);
        cell.set(epoch(5), 42);
        assert_eq!(cell.get(epoch(5)), Some(42));
    }

    #[test]
    fn allocator_issues_distinct_values() {
        let a = SchemaEpoch::allocate();
        let b = SchemaEpoch::allocate();
        assert_ne!(a, b);
    }
}
