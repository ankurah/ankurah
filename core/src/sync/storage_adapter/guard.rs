//! A storage transaction lent to one change through the storage adapter: an apply, a
//! creation or a drop.
//!
//! Such a change takes locks, reads and writes through several map calls,
//! some of them made by the crate. A change that fails part way, or whose
//! caller drops it at any await, must leave the transaction unable to commit,
//! so that no half-made change lands.

use std::ops::{Deref, DerefMut};

use crate::storage::StorageTransaction;

/// A storage transaction lent to one change through the storage adapter, from the
/// change's start until it completes: dropped before then, it poisons the
/// transaction.
pub(super) struct PoisonGuard<'t> {
    trx: &'t mut (dyn StorageTransaction + 't),

    /// Set as the change completes, after which dropping leaves the
    /// transaction to its owner.
    completed: bool,
}

impl<'t> PoisonGuard<'t> {
    /// Guard `trx` from the start of a change.
    pub(super) fn new(trx: &'t mut (dyn StorageTransaction + 't)) -> Self { Self { trx, completed: false } }

    /// The change completed: the transaction is its owner's to commit.
    pub(super) fn complete<T>(mut self, outcome: T) -> T {
        self.completed = true;
        outcome
    }
}

impl<'t> Deref for PoisonGuard<'t> {
    type Target = dyn StorageTransaction + 't;

    fn deref(&self) -> &Self::Target { &*self.trx }
}

impl DerefMut for PoisonGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut *self.trx }
}

impl Drop for PoisonGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.trx.poison();
        }
    }
}
