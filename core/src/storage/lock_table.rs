//! Lock names kept in one process's memory: for an engine whose storage
//! transactions wait, within the process, for the names another holds.

use std::collections::BTreeSet;
use std::sync::Mutex;

use tokio::sync::Notify;

use super::LockName;

/// The names one engine's open storage transactions hold, which a
/// transaction asking for one waits on.
#[derive(Default)]
pub struct LockTable {
    held: Mutex<BTreeSet<LockName>>,
    released: Notify,
}

/// The names one storage transaction holds in a [`LockTable`], released when
/// it ends.
pub struct HeldLocks<'t> {
    table: &'t LockTable,
    names: Vec<LockName>,
}

impl<'t> HeldLocks<'t> {
    /// None yet, of `table`.
    pub fn of(table: &'t LockTable) -> Self { Self { table, names: Vec::new() } }

    /// Wait until no other transaction holds `name`, then hold it until this
    /// one ends.
    pub async fn take(&mut self, name: &LockName) {
        loop {
            // Waiting begins before the check, so a release between the two
            // still wakes it.
            let released = self.table.released.notified();
            if self.table.held.lock().unwrap().insert(name.clone()) {
                self.names.push(name.clone());
                return;
            }

            released.await;
        }
    }
}

impl Drop for HeldLocks<'_> {
    fn drop(&mut self) {
        if self.names.is_empty() {
            return;
        }

        let mut held = self.table.held.lock().unwrap();
        for name in &self.names {
            held.remove(name);
        }

        drop(held);
        self.table.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use super::*;
    use crate::test_helpers::map_conformance::fixtures::poll_once;

    #[tokio::test]
    async fn a_name_another_transaction_holds_is_taken_once_that_one_ends() {
        let table = LockTable::default();
        let mut first = HeldLocks::of(&table);
        first.take(&LockName::new("l")).await;

        let (mut second, name) = (HeldLocks::of(&table), LockName::new("l"));
        let mut taking = pin!(second.take(&name));
        assert!(poll_once(taking.as_mut()).is_pending(), "the second waits while the first holds the name");

        drop(first);
        assert!(poll_once(taking.as_mut()).is_ready(), "the second takes the name once the first ends");
    }

    #[tokio::test]
    async fn a_name_held_keeps_no_other_name_from_being_taken() {
        let table = LockTable::default();
        let mut first = HeldLocks::of(&table);
        first.take(&LockName::new("l")).await;

        let mut second = HeldLocks::of(&table);
        assert!(poll_once(pin!(second.take(&LockName::new("k")))).is_ready());
    }

    #[tokio::test]
    async fn a_take_dropped_while_it_waits_leaves_the_name_free_once_its_holder_ends() {
        let table = LockTable::default();
        let mut first = HeldLocks::of(&table);
        first.take(&LockName::new("l")).await;

        let mut abandoned = HeldLocks::of(&table);
        assert!(poll_once(pin!(abandoned.take(&LockName::new("l")))).is_pending());
        drop(abandoned);
        drop(first);

        let mut next = HeldLocks::of(&table);
        assert!(poll_once(pin!(next.take(&LockName::new("l")))).is_ready());
    }
}
