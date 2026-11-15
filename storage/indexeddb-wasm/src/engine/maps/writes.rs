//! What lands of the maps in a readwrite transaction: a storage
//! transaction's map writes and raised lock versions at its commit, and a
//! reset's emptied maps, cleared versions and new epoch.

use ankurah_core::error::MutationError;
use ankurah_core::storage::pending_writes::PendingWrites;
use ankurah_core::storage::HeldLocks;
use wasm_bindgen::JsValue;
use web_sys::IdbTransaction;

use super::layout::{self, LOCK_VERSIONS, MAPS};
use super::requests;
use super::versions::{Noted, VersionRequests};
use super::MapError;

/// What a storage transaction that made map calls lands at its commit: its
/// map writes, under the versions and epoch it noted. It holds the
/// transaction's names in the engine for as long as the commit holds it:
/// until the commit's native transaction has completed, or the commit itself
/// is dropped.
pub(in crate::engine) struct MapLanding<'e> {
    pub(super) noted: Noted,
    pub(super) pending: PendingWrites,
    pub(super) _held: HeldLocks<'e>,
}

impl MapLanding<'_> {
    /// Read the epoch and every noted version again in `transaction`, a
    /// readwrite transaction over [`STORES`](layout::STORES), before it
    /// writes anything: a conflict if any moved since the storage transaction
    /// noted it.
    pub(in crate::engine) async fn compare(&self, transaction: &IdbTransaction) -> Result<(), MutationError> {
        let versions = requests::store(transaction, LOCK_VERSIONS)?;
        let again = VersionRequests::request(&versions, self.noted.versions.keys())?;

        match again.answer().await? == self.noted {
            true => Ok(()),
            false => Err(MutationError::WriteConflict),
        }
    }

    /// Raise every noted version, and make the pending map writes, in
    /// `transaction`, once [`compare`](Self::compare) found nothing moved:
    /// the requests are made, not awaited, since the transaction's commit
    /// carries any failure. A version at the last a lock keeps fails the
    /// write before the commit, and the transaction then lands nothing.
    pub(in crate::engine) fn write(&mut self, transaction: &IdbTransaction) -> Result<(), MapError> {
        let versions = requests::store(transaction, LOCK_VERSIONS)?;
        for (name, version) in &self.noted.versions {
            let next = layout::next_version_value(*version).ok_or_else(|| MapError::LastVersion { name: name.clone() })?;
            versions.put_with_key(&next, &layout::version_key(name)).map_err(MapError::thrown)?;
        }

        let maps = requests::store(transaction, MAPS)?;
        for (map, changes) in self.pending.take() {
            if changes.cleared {
                let whole_map: JsValue = layout::whole_map(&map)?.into();
                maps.delete(&whole_map).map_err(MapError::thrown)?;
            }

            for (key, value) in changes.keys {
                let key = layout::entry_key(&map, &key);
                match value {
                    Some(value) => maps.put_with_key(&layout::bytes(&value), &key).map_err(MapError::thrown)?,

                    // A cleared map holds nothing for a delete to remove.
                    None if changes.cleared => continue,
                    None => maps.delete(&key).map_err(MapError::thrown)?,
                };
            }
        }

        Ok(())
    }
}

/// Empty every map and clear every lock's version, then put a new reset
/// epoch, in `transaction`, a reset's readwrite transaction over
/// [`STORES`](layout::STORES): every map transaction and snapshot that noted
/// the epoch before then fails as a conflict. The requests are made, not
/// awaited: the transaction's commit carries any failure.
pub(in crate::engine) fn reset(transaction: &IdbTransaction) -> Result<(), MapError> {
    requests::store(transaction, MAPS)?.clear().map_err(MapError::thrown)?;

    let versions = requests::store(transaction, LOCK_VERSIONS)?;
    versions.clear().map_err(MapError::thrown)?;
    versions.put_with_key(&layout::new_epoch(), &layout::epoch_key()).map_err(MapError::thrown)?;

    Ok(())
}
