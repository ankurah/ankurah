//! Creating, rebuilding, opening and deleting trees through the storage-engine adapter.

use std::collections::BTreeMap;
use std::sync::Arc;

use ankurah_core_types::key_spec::KeySpec;
use ankurah_digest_tree::{Created, DigestGroup, DigestTree, Encode, Member, MemberId, Payload, Row, RowKey};

use super::StorageCache;
use super::{EngineAdapter, Inner, StorageEngineTrx};
use crate::error::{MutationError, RetrievalError};
use crate::storage::{MapRead, MapSnapshot, MapWrite, StorageEngine, StorageTransaction};
use crate::sync::error::{change_failure, open_failure};
use crate::sync::storage_adapter::guard::PoisonGuard;
use crate::sync::storage_adapter::layout::entry;
use crate::sync::storage_adapter::layout::names::{trees_map, Incarnation};
use crate::sync::{Error, TreeId};

/// `trx` must belong to `storage`; the transaction does not expose its engine
/// identity for verification.
///
/// A new tree named `id`, keyed by `key_spec`, holding exactly `members`,
/// written through `trx`: it exists once `trx` commits, and then
/// [`Created::landed`] gives it.
///
/// It takes the tree's lock, and finds no tree under `id`, or fails with
/// [`Error::Exists`]; it refuses what the crate's creation
/// refuses. A creation that fails, or is dropped part way, leaves `trx`
/// unable to commit.
///
/// It takes the lock in its first request, then hashes every member with
/// `trx` open and idle: an engine that ends a transaction left idle too
/// long, as PostgreSQL does, ends a creation of many members, and since it
/// sends the tree's write with the commit, `create` answers `Ok` and the
/// commit fails.
pub async fn create<SE, K, I, L, G, M>(
    storage: Arc<SE>,
    cache: Arc<StorageCache>,
    trx: &mut SE::Transaction<'_>,
    id: TreeId,
    key_spec: KeySpec<K>,
    members: impl IntoIterator<Item = M>,
) -> Result<Created<K, I, L, G, EngineAdapter<SE, K, L, G>>, MutationError>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
    M: Member<K, Id = I, Payload = L>,
{
    // Collected before the first await, so that the caller's iterator is
    // never held across one.
    let members: Vec<M> = members.into_iter().collect();
    let mut trx = PoisonGuard::new(trx);

    let found = trx.map_lock_and_read(&[id.lock()], vec![entry::read([&id])]).await?;
    if entry::found_one(&id, found)?.is_some() {
        return Err(Error::Exists(id).into());
    }

    let store = EngineAdapter::new(Inner::new(storage, cache, id, Incarnation::mint()));
    let lent = StorageEngineTrx::new(store.clone(), &mut *trx, None, BTreeMap::new());

    match DigestTree::create_in(store, lent, key_spec, members).await {
        Ok(created) => Ok(trx.complete(created)),
        Err(error) => Err(change_failure(error)),
    }
}

/// Rebuild the tree named `id` to hold exactly `members`, or create it if it
/// is absent. An existing tree keeps its incarnation, key specification and
/// digest group; its rows change at the next generation, under its lock, in
/// `trx`. Other handles continue to read and change that same tree after the
/// commit. [`Created::landed`] gives the rebuilt tree.
///
/// A tree marked invalid can be rebuilt when its rows still decode. Damaged
/// entries or rows, or a different key specification or digest group, fail
/// the rebuild rather than silently discarding the tree's stored identity.
/// A failure or cancellation leaves `trx` unable to commit. Dropping the
/// transaction or a commit conflict preserves the old tree and its maps.
///
/// `trx` must belong to `storage`, as for [`create`]. The same limits on how
/// long a storage transaction may remain idle while hashing members apply.
pub async fn rebuild<SE, K, I, L, G, M>(
    storage: Arc<SE>,
    cache: Arc<StorageCache>,
    trx: &mut SE::Transaction<'_>,
    id: TreeId,
    key_spec: KeySpec<K>,
    members: impl IntoIterator<Item = M>,
) -> Result<Created<K, I, L, G, EngineAdapter<SE, K, L, G>>, MutationError>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + PartialEq + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
    M: Member<K, Id = I, Payload = L>,
{
    replace(storage, cache, trx, id, key_spec, members, false).await
}

/// Initialize a missing, invalid or legacy-key managed tree. A concurrent completed or
/// partial build wins; it must never be replaced by an earlier observation.
pub(in crate::sync) async fn initialize<SE, K, I, L, G, M>(
    storage: Arc<SE>,
    cache: Arc<StorageCache>,
    trx: &mut SE::Transaction<'_>,
    id: TreeId,
    key_spec: KeySpec<K>,
    members: impl IntoIterator<Item = M>,
) -> Result<Created<K, I, L, G, EngineAdapter<SE, K, L, G>>, MutationError>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + PartialEq + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
    M: Member<K, Id = I, Payload = L>,
{
    replace(storage, cache, trx, id, key_spec, members, true).await
}

async fn replace<SE, K, I, L, G, M>(
    storage: Arc<SE>,
    cache: Arc<StorageCache>,
    trx: &mut SE::Transaction<'_>,
    id: TreeId,
    key_spec: KeySpec<K>,
    members: impl IntoIterator<Item = M>,
    only_invalid: bool,
) -> Result<Created<K, I, L, G, EngineAdapter<SE, K, L, G>>, MutationError>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + PartialEq + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
    M: Member<K, Id = I, Payload = L>,
{
    let members: Vec<M> = members.into_iter().collect();
    let mut trx = PoisonGuard::new(trx);

    let found = trx.map_lock_and_read(&[id.lock()], vec![entry::read([&id])]).await?;
    let previous = entry::found_one(&id, found)?;
    if only_invalid {
        if let Some(entry) = previous.filter(|entry| entry.status != entry::Status::Invalid) {
            // Check the encoding while holding the same lock as replacement.
            // A concurrent builder that already migrated the tree wins.
            let found = trx.map_read(vec![MapRead::Get { map: entry.incarnation.rows(), keys: vec![RowKey::Meta.encode()] }]).await?;
            let bytes = super::rows::got(found, 1).map_err(|error| change_failure(error.into()))?.pop().flatten();
            let row = bytes
                .map(|bytes| super::rows::decode_row::<K, L, G>(&id, &bytes))
                .transpose()
                .map_err(|error| change_failure(error.into()))?;
            if !matches!(row, Some(Row::Meta(meta)) if meta.key_encoding() == 1) {
                return Err(MutationError::WriteConflict);
            }
        }
    }
    let incarnation = previous.map_or_else(Incarnation::mint, |entry| entry.incarnation);
    if previous.is_some() {
        // Replacement rows no longer represent the old member lookup or cursor.
        // Clear them in this commit, including if the manager stops before catch-up.
        trx.map_write(vec![MapWrite::Clear { map: incarnation.members() }]).await?;
    }
    let store = EngineAdapter::new(Inner::new(storage, cache, id.clone(), incarnation));
    let lent = StorageEngineTrx::new(store.clone(), &mut *trx, previous.map(|entry| entry.generation), BTreeMap::new());
    let rebuilding = match previous {
        Some(_) => DigestTree::rebuild_in(store, lent, key_spec, members).await,
        None => DigestTree::create_in(store, lent, key_spec, members).await,
    };

    match rebuilding {
        Ok(created) => {
            if previous.is_some() {
                // Rebuilding replaces a recovery boundary, even if its rows fit
                // the ordinary history budget. Existing captures must restart.
                trx.map_write(vec![MapWrite::Clear { map: incarnation.history() }]).await?;
            }
            if only_invalid {
                let found = trx.map_read(vec![entry::read([&id])]).await?;
                let entry = entry::found_one(&id, found)?.ok_or_else(|| Error::NoSuchTree(id.clone()))?;
                let entry = entry::TreeEntry { status: entry::Status::Building, ..entry };
                trx.map_write(vec![MapWrite::Put { map: trees_map(), key: id.entry_key(), value: entry.encode() }]).await?;
            }
            Ok(trx.complete(created))
        }
        Err(error) => Err(change_failure(error)),
    }
}

/// The tree named `id`, at its current incarnation, read in a snapshot of
/// its own: [`Error::NoSuchTree`] where there is none, and
/// [`Error::Invalid`] after refused maintenance, and [`Error::Building`] for
/// incomplete initial membership. Neither opens for ordinary reads.
/// A snapshot that fails retryably fails the opening with the
/// engine's own error.
pub async fn open<SE, K, I, L, G>(
    storage: Arc<SE>,
    cache: Arc<StorageCache>,
    id: &TreeId,
) -> Result<DigestTree<K, I, L, G, EngineAdapter<SE, K, L, G>>, RetrievalError>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
{
    // The entry's snapshot ends before the tree begins its own: holding
    // both, an opening could wait for ever for the second on an engine
    // that bounds the snapshots open, or queues a commit between them.
    let found = {
        let mut snapshot = storage.map_snapshot(&[id.lock()]).await?;
        snapshot.map_read(vec![entry::read([id])]).await?
    };

    let entry = entry::found_one(id, found)?.ok_or_else(|| Error::NoSuchTree(id.clone()))?;
    match entry.status {
        entry::Status::Ready => {}
        entry::Status::Invalid => return Err(Error::Invalid(id.clone()).into()),
        entry::Status::Building => return Err(Error::Building(id.clone()).into()),
    }

    let store = EngineAdapter::new(Inner::new(storage, cache, id.clone(), entry.incarnation));
    DigestTree::open(store).await.map_err(open_failure)
}

/// Attach a manager to Ready or resumable Building membership. Invalid trees
/// still require initialization. The caller must reconcile before publishing
/// the returned tree to readers.
pub(in crate::sync) async fn open_managed<SE, K, I, L, G>(
    storage: Arc<SE>,
    cache: Arc<StorageCache>,
    id: &TreeId,
    publication: Arc<std::sync::atomic::AtomicU64>,
) -> Result<DigestTree<K, I, L, G, EngineAdapter<SE, K, L, G>>, RetrievalError>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
{
    let found = {
        let mut snapshot = storage.map_snapshot(&[id.lock()]).await?;
        snapshot.map_read(vec![entry::read([id])]).await?
    };
    let entry = entry::found_one(id, found)?.ok_or_else(|| Error::NoSuchTree(id.clone()))?;
    if entry.status == entry::Status::Invalid {
        return Err(Error::Invalid(id.clone()).into());
    }
    let store = EngineAdapter::new(Inner::new(storage, cache, id.clone(), entry.incarnation));
    store.load_buffered(publication.clone()).await?;
    // Opening metadata is internal maintenance, before the manager has made
    // this version current for readers. Restore its publication guard after it opens.
    let tree = DigestTree::open(store.working()).await.map_err(open_failure)?;
    *tree.store().inner.publication.lock().unwrap() = Some(publication);
    Ok(tree)
}

/// Drop the tree named `id` through `trx`: its entry, rows, change log and member lookup
/// go when `trx` commits, and its open handles fail their next use.
///
/// It takes the tree's lock, and finds a tree under `id`, or fails with
/// [`Error::NoSuchTree`]. An entry that doesn't decode names no
/// maps: it is deleted, and the maps of the tree it was are left behind.
/// A drop that fails, or is dropped part way, leaves `trx` unable to
/// commit.
pub async fn drop(trx: &mut dyn StorageTransaction, id: &TreeId) -> Result<(), MutationError> {
    let mut trx = PoisonGuard::new(trx);

    let found = trx.map_lock_and_read(&[id.lock()], vec![entry::read([id])]).await?;
    let mut writes = vec![MapWrite::Delete { map: trees_map(), key: id.entry_key() }];

    match entry::found_one(id, found) {
        Ok(Some(entry)) => {
            writes.push(MapWrite::Clear { map: entry.incarnation.rows() });
            writes.push(MapWrite::Clear { map: entry.incarnation.log() });
            writes.push(MapWrite::Clear { map: entry.incarnation.history() });
            writes.push(MapWrite::Clear { map: entry.incarnation.members() });
        }

        Err(Error::Damaged { .. }) => {}
        Ok(None) => return Err(Error::NoSuchTree(id.clone()).into()),
        Err(misfit) => return Err(misfit.into()),
    }

    trx.map_write(writes).await?;
    trx.complete(());
    Ok(())
}
