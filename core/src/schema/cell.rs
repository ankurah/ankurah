//! Descriptor-resident resolution cells and the schema epoch that keys them.
//!
//! A compiled model's durable identities (its `ModelId`, each field's
//! `PropertyId`) are resolved against the catalog exactly once per schema
//! epoch per process, then live ON the `&'static` descriptor: any code past
//! the registration gate reads identity off the type instead of carrying it
//! through containers.

use append_only_vec::AppendOnlyVec;
use std::sync::atomic::{AtomicU32, Ordering};

/// One system's resolution generation. A node holds the epoch it was
/// assigned at its not-ready-to-ready transition; every cell read passes
/// that held value explicitly, because a process can host several resident
/// nodes with different current epochs, so there is no meaningful process
/// "current" to consult.
///
/// The global counter below is an allocator of distinct values only, never
/// an ambient authority: it guarantees two nodes in one process get
/// different epochs and a reset-and-rejoin never reuses one, so no cell
/// entry from a previous system can collide with the next. u32 rather than
/// a narrower tag: a wrapped tag could validate a stale identity, which is
/// a wrong answer where a miss is a mechanical error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaEpoch(u32);

static NEXT_SCHEMA_EPOCH: AtomicU32 = AtomicU32::new(1);

impl SchemaEpoch {
    /// The reserved epoch of everything that predates a ready system: the
    /// bootstrap system rows a node materializes before create/load/join
    /// completes. The allocator never issues 0, and registration cannot run
    /// before a system is ready, so no per-epoch identity ever resolves
    /// under it -- only `Pinned` identities, which are valid at every epoch,
    /// exactly what a pre-system entity can honestly resolve.
    pub const BOOTSTRAP: SchemaEpoch = SchemaEpoch(0);

    /// Allocate the next distinct epoch. Called only at a node's
    /// not-ready-to-ready transition (create, durable load, join); a system
    /// reset therefore gets a fresh epoch on rejoin, same-root rejoin
    /// included, and a redundant ready-marking on an already-ready node
    /// allocates nothing.
    pub(crate) fn allocate() -> Self { Self(NEXT_SCHEMA_EPOCH.fetch_add(1, Ordering::Relaxed)) }
}

impl std::fmt::Display for SchemaEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

/// A descriptor's resolved durable identity, per schema epoch.
///
/// `Pinned` is a compile-time-constant identity (a system model or system
/// property): valid in every epoch, never registered. `PerEpoch` accumulates
/// `(epoch, identity)` entries as registration resolves the descriptor under
/// each epoch. The first entry for an epoch is FINAL for that epoch's
/// lifetime: entries are never replaced or pruned, because resolution per
/// node and epoch is deterministic, an epoch's identities must not shift
/// under running code, and pruning by number could starve a live low-epoch
/// node in a multi-node process. In practice the list holds one entry per
/// resident node, plus one after a rare dev reset.
#[derive(Debug)]
pub enum SchemaOnceCell<T: Copy> {
    Pinned(T),
    PerEpoch(AppendOnlyVec<(u32, T)>),
}

impl<T: Copy> SchemaOnceCell<T> {
    /// An empty per-epoch cell, const so the derive can build it inside a
    /// `static` descriptor initializer.
    pub const fn per_epoch() -> Self { Self::PerEpoch(AppendOnlyVec::new()) }

    /// The identity resolved under `epoch`, if any. `Pinned` ignores the
    /// argument: a pinned identity is valid at every epoch. A miss means
    /// this descriptor has not passed the registration gate under `epoch`
    /// (or the caller's epoch is stale after a reset) and surfaces as a
    /// resolution error at the caller, never as a wrong identity.
    pub fn get(&self, epoch: SchemaEpoch) -> Option<T> {
        match self {
            Self::Pinned(value) => Some(*value),
            Self::PerEpoch(entries) => entries.iter().find(|(e, _)| *e == epoch.0).map(|(_, value)| *value),
        }
    }

    /// Record `value` as `epoch`'s resolution. Concurrent gates may append
    /// the same mapping twice; the scan in [`Self::get`] takes the first
    /// entry, and resolution per node and epoch is deterministic, so
    /// duplicates are benign -- that is why there is no lock here, and why a
    /// later differing append (first-wins) cannot change what the epoch
    /// already reads. `Pinned` ignores writes: a pinned identity is already
    /// resolved at every epoch.
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
