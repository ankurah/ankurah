//! The readonly transactions a storage transaction's, or a snapshot's, map
//! reads are made in, and what its first map call noted of the database's
//! state.

mod asked;

use std::collections::BTreeMap;

use ankurah_core::error::MutationError;
use ankurah_core::storage::{LockName, MapRead, MapRows};
use send_wrapper::SendWrapper;
use web_sys::{IdbObjectStore, IdbTransactionMode};

use self::asked::Asked;
use super::layout::{LOCK_VERSIONS, MAPS, STORES};
use super::requests::{self, Request};
use super::versions::{version_request, versions_read, Noted, VersionRequests};
use super::MapError;
use crate::database::Database;

/// One storage transaction's, or one snapshot's, readonly transactions, none
/// begun until its first call.
#[derive(Default)]
pub(super) struct Reading {
    /// The object stores of the last readonly transaction a call found
    /// nothing noted had moved in, once one has: the one later calls make
    /// their requests in while it is active.
    stores: Option<SendWrapper<ReadStores>>,

    /// What the first call noted, once one has answered.
    noted: Option<Noted>,
}

/// The object stores of one readonly transaction over the maps.
struct ReadStores {
    maps: IdbObjectStore,
    versions: IdbObjectStore,
}

impl Reading {
    /// Whether a call has noted the database's state.
    pub(super) fn has_noted(&self) -> bool { self.noted.is_some() }

    /// What the calls noted, for a commit to compare.
    pub(super) fn into_noted(self) -> Option<Noted> { self.noted }

    /// Note the versions of `locks`, and answer `reads`, from what the
    /// database holds, in one readonly transaction. Every request is made
    /// before any is awaited, but for each reverse range's cursor, which
    /// moves on to its next entry only as the last is read: the call must be
    /// polled as each of the cursor's steps succeeds, as an await on it is,
    /// or the transaction ends under it. The transaction is the last in which
    /// a call found nothing noted had moved, while it is still active;
    /// otherwise a new one, which also reads again what the calls before
    /// noted, and fails the call with a write conflict if any of it moved.
    pub(super) async fn read(&mut self, database: &Database, locks: &[LockName], reads: &[MapRead]) -> Result<Vec<MapRows>, MutationError> {
        // Its requests and their answers are the browser's, which never leave
        // its one thread.
        SendWrapper::new(self.read_in_this_thread(database, locks, reads)).await
    }

    async fn read_in_this_thread(
        &mut self,
        database: &Database,
        locks: &[LockName],
        reads: &[MapRead],
    ) -> Result<Vec<MapRows>, MutationError> {
        if let Some(stores) = &self.stores {
            match Requests::make(stores, None, locks, reads) {
                Ok(requests) => return self.answer(requests).await,
                Err(refused) if refused.is_inactive_transaction() => {}
                Err(refused) => return Err(refused.into()),
            }
        }

        let stores = ReadStores::begin(database).await?;
        let again = VersionRequests::request(&stores.versions, self.noted.iter().flat_map(|noted| noted.versions.keys()))?;
        let requests = Requests::make(&stores, Some(again), locks, reads)?;
        let rows = self.answer(requests).await?;

        // Only now that it has found nothing moved may a later call make its
        // requests in it unchecked.
        self.stores = Some(SendWrapper::new(stores));
        Ok(rows)
    }

    /// Await `requests`, compare what they read again with what was noted,
    /// and note the versions of the names they took.
    async fn answer(&mut self, requests: Requests) -> Result<Vec<MapRows>, MutationError> {
        let answered = requests.answer().await?;

        if let Some(again) = answered.again {
            match &self.noted {
                None => self.noted = Some(again),
                Some(noted) if *noted != again => return Err(MutationError::WriteConflict),
                Some(_) => {}
            }
        }

        let noted = self.noted.as_mut().expect("every call reads the epoch until one has noted it");
        noted.versions.extend(answered.taken);

        Ok(answered.rows)
    }
}

impl ReadStores {
    /// The object stores of a new readonly transaction over the maps.
    async fn begin(database: &Database) -> Result<Self, MutationError> {
        let connection = database.connection_with_map_stores().await?;
        let transaction = requests::transaction(&connection, &STORES, IdbTransactionMode::Readonly)?;

        Ok(Self { maps: requests::store(&transaction, MAPS)?, versions: requests::store(&transaction, LOCK_VERSIONS)? })
    }
}

/// One call's requests, made in one readonly transaction.
struct Requests {
    /// The reset epoch and any previously observed lock versions, read in a
    /// newly opened transaction.
    again: Option<VersionRequests>,

    /// The versions of the names the call takes, with each name.
    taken: Vec<(LockName, Request)>,

    reads: Vec<Asked>,
}

/// What one call's requests read.
struct Answered {
    again: Option<Noted>,
    taken: BTreeMap<LockName, u64>,
    rows: Vec<MapRows>,
}

impl Requests {
    /// Make the call's requests in `stores`, after `again`, made there
    /// already, if any: the versions of `locks`, and then `reads`.
    fn make(stores: &ReadStores, again: Option<VersionRequests>, locks: &[LockName], reads: &[MapRead]) -> Result<Self, MapError> {
        let taken = locks.iter().map(|name| version_request(&stores.versions, name)).collect::<Result<_, _>>()?;
        let reads = reads.iter().map(|read| Asked::make(&stores.maps, read)).collect::<Result<_, _>>()?;

        Ok(Self { again, taken, reads })
    }

    /// The answers to every request, in the order they were made.
    async fn answer(self) -> Result<Answered, MapError> {
        let again = match self.again {
            Some(again) => Some(again.answer().await?),
            None => None,
        };

        let taken = versions_read(self.taken).await?;
        let mut rows = Vec::with_capacity(self.reads.len());
        for asked in self.reads {
            rows.push(asked.answer().await?);
        }

        Ok(Answered { again, taken, rows })
    }
}
