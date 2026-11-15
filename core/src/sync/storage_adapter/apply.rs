//! Changing trees in a storage transaction: every tree's changes in one call,
//! their locks and reads in one request, and each tree's write made at once.

use std::collections::BTreeMap;
use std::sync::Arc;

use ankurah_digest_tree::{
    Applied, DigestGroup, DigestTree, Encode, Error as DigestTreeError, MemberId, Payload, Prepared, RowKey, StoreError, Transaction,
};

use super::guard::PoisonGuard;
use super::layout::entry::{self, TreeEntry};
use super::layout::log;
use super::layout::names::trees_map;
use super::{EngineAdapter, Error, StorageEngineTrx, TreeId};
use crate::error::MutationError;
use crate::storage::{LockName, MapRead, MapRows, MapWrite, StorageCommitResult, StorageEngine, StorageTransaction};
use crate::sync::error::refusal;

/// One tree's part in an apply.
pub enum TreeChange<SE: StorageEngine, K, I, L, G: DigestGroup> {
    /// Changes to the tree, to apply.
    Changes(Transaction<K, I, L, G, EngineAdapter<SE, K, L, G>>),

    /// A tree whose owner couldn't build its changes, for example because a
    /// member couldn't provide a key. Applying marks it invalid so future
    /// reads cannot use a tree whose maintenance was skipped.
    Refused(DigestTree<K, I, L, G, EngineAdapter<SE, K, L, G>>, DigestTreeError),
}

impl<SE, K, I, L, G> TreeChange<SE, K, I, L, G>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
{
    /// The store of the tree the change is to.
    fn store(&self) -> &EngineAdapter<SE, K, L, G> {
        match self {
            Self::Changes(transaction) => transaction.tree().store(),
            Self::Refused(tree, _) => tree.store(),
        }
    }
}

/// What an apply did to each tree it was given: applied its changes, left it
/// alone as retired, or refused them.
#[must_use = "the applied trees' rows land with the storage transaction, and then `landed` tells their stores"]
pub struct AppliedTrees<SE, K, I, L, G>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    L: Encode + Clone + Send + Sync + 'static,
    G: DigestGroup,
{
    applied: Vec<Applied<K, I, L, G, EngineAdapter<SE, K, L, G>>>,
    retired: Vec<TreeId>,
    refused: Vec<(TreeId, DigestTreeError)>,
}

impl<SE, K, I, L, G> AppliedTrees<SE, K, I, L, G>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    L: Encode + Clone + Send + Sync + 'static,
    G: DigestGroup,
{
    /// The storage transaction the changes were applied in has committed:
    /// each applied tree's store takes in what it kept of its rows.
    ///
    /// The owner calls this for that transaction's own commit, and for no
    /// other. The commit's result is asked for as a reminder of that, since
    /// only a commit that landed gives one
    /// ([`StorageCommitOutcome::committed`](crate::storage::StorageCommitOutcome::committed)
    /// answers a conflict with [`MutationError::WriteConflict`]), but a
    /// result doesn't show which transaction it came from. A landing after a
    /// commit that didn't land leaves the cache holding rows storage never
    /// took. The tree's next commit or read finds the cache past storage's
    /// generation and forgets them, unless other nodes' commits have brought
    /// storage to that generation meanwhile: then this node reads the tree
    /// wrongly from there on.
    pub fn landed(self, _committed: &StorageCommitResult) {
        for applied in self.applied {
            applied.landed();
        }
    }

    /// The trees nothing was written for, since each was dropped, created
    /// again or marked invalid after its handle was opened.
    pub fn retired(&self) -> &[TreeId] { &self.retired }

    /// The trees that refused their changes, each with the crate's error:
    /// each is marked invalid in the storage transaction, and nothing else of
    /// it is written. A tree whose entry doesn't decode is left as it is, with
    /// nothing written.
    pub fn refused(&self) -> &[(TreeId, DigestTreeError)] { &self.refused }
}

/// One tree's part, once its changes are prepared.
enum Planned<SE: StorageEngine, K, I, L, G: DigestGroup> {
    Changes {
        prepared: Prepared<K, I, L, G, EngineAdapter<SE, K, L, G>>,

        /// The generation the cache kept the tree's rows at as the apply
        /// began, if it kept any: the change log is read after it.
        cached: Option<u64>,
    },

    Refused(DigestTree<K, I, L, G, EngineAdapter<SE, K, L, G>>, DigestTreeError),
}

/// What the apply's first request found of one tree, besides its entry: for
/// a tree with changes, the leaves its commit checks, by key, and the change
/// log after the generation its rows were cached at, with that generation.
struct Found {
    leaves: BTreeMap<RowKey, Option<Vec<u8>>>,
    log: Option<(u64, MapRows)>,
}

/// What became of one tree's changes that didn't fail the call.
enum Outcome<SE: StorageEngine + 'static, K, I, L, G>
where
    K: Encode + Clone + Send + Sync + 'static,
    L: Encode + Clone + Send + Sync + 'static,
    G: DigestGroup,
{
    /// Applied and written, to land with the storage transaction.
    Applied(Applied<K, I, L, G, EngineAdapter<SE, K, L, G>>),

    /// Refused by the tree, with nothing written.
    Refused(DigestTreeError),
}

/// Apply every tree's changes through `trx`, one storage transaction:
/// they land when it commits, with its entity records, all of them or
/// none. Once it has committed, the owner calls
/// [`AppliedTrees::landed`] with its result.
/// `trx` must belong to `storage`; its engine identity cannot be checked.
///
/// The trees are taken in the order of their ids. Their locks, their
/// entries, the leaves their commits check and their change logs since
/// the generations the cache keeps their rows at are read in one request,
/// and then each tree's cached rows are brought up to date and its
/// changes applied and written. A tree dropped, created again or marked
/// invalid since its handle was opened is retired: nothing is written for
/// it. A tree that refuses its changes, or that the owner reports
/// refused, is marked invalid, in `trx`, and nothing else of it is
/// written. A tree whose entry doesn't decode refuses them too, and
/// nothing of it is written: it is out of step until it is dropped. An
/// apply given no changes makes no map call, so that `trx` commits as one
/// that touched no map.
///
/// A change to a tree from another storage engine fails the call with
/// [`Error::ForeignTree`], two changes for one tree with
/// [`Error::ChangedTwice`], and a lock out of order with those
/// `trx` took before fails it too. Any failure, or dropping the call part
/// way, leaves `trx` unable to commit. An engine's own error leaves as
/// itself, a [`MutationError::WriteConflict`] as one.
pub async fn apply<SE, K, I, L, G>(
    storage: &Arc<SE>,
    trx: &mut SE::Transaction<'_>,
    mut changes: Vec<TreeChange<SE, K, I, L, G>>,
) -> Result<AppliedTrees<SE, K, I, L, G>, MutationError>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
{
    let mut applied = AppliedTrees { applied: Vec::new(), retired: Vec::new(), refused: Vec::new() };
    if changes.is_empty() {
        return Ok(applied);
    }

    let mut trx = PoisonGuard::new(trx);
    if let Some(foreign) = changes.iter().find(|change| !change.store().uses_storage(storage)) {
        return Err(Error::ForeignTree(foreign.store().inner().id.clone()).into());
    }

    changes.sort_by(|a, b| a.store().inner().id.cmp(&b.store().inner().id));
    if let Some(pair) = changes.windows(2).find(|pair| pair[0].store().inner().id == pair[1].store().inner().id) {
        return Err(Error::ChangedTwice(pair[0].store().inner().id.clone()).into());
    }

    let mut planned = Vec::with_capacity(changes.len());
    for change in changes {
        planned.push(match change {
            TreeChange::Changes(transaction) => {
                let cached = transaction.tree().store().inner().cached_generation();
                Planned::Changes { prepared: transaction.prepare().await, cached }
            }

            TreeChange::Refused(tree, error) => Planned::Refused(tree, error),
        });
    }

    let ids: Vec<&TreeId> = planned.iter().map(|planned| &planned.store().inner().id).collect();
    let locks: Vec<LockName> = planned.iter().map(|planned| planned.store().inner().lock.clone()).collect();
    let reads = [entry::read(ids.iter().copied())].into_iter().chain(planned.iter().flat_map(Planned::reads)).collect();

    let mut answers = trx.map_lock_and_read(&locks, reads).await?.into_iter();
    let entries = entry::found(&ids, answers.next())?;
    let found = planned.iter().map(|planned| planned.found(&mut answers)).collect::<Result<Vec<_>, _>>()?;

    for ((planned, entry), found) in planned.into_iter().zip(entries).zip(found) {
        let store = planned.store().clone();
        let entry = match entry {
            Ok(entry) => entry,

            // An entry that doesn't decode is left as it is: an invalid
            // mark would need the incarnation and generation it lost.
            Err(damaged) => {
                applied.refused.push((store.inner().id.clone(), DigestTreeError::Store(StoreError::permanent(damaged))));
                continue;
            }
        };

        let Some(entry) = TreeEntry::current(entry, store.inner().incarnation) else {
            applied.retired.push(store.inner().id.clone());
            continue;
        };

        let refused = match planned {
            Planned::Refused(_, error) => error,

            Planned::Changes { prepared, .. } => match apply_changes(&mut *trx, prepared, entry.generation, found).await? {
                Outcome::Applied(done) => {
                    applied.applied.push(done);
                    continue;
                }

                Outcome::Refused(refused) => refused,
            },
        };

        let invalid = TreeEntry { valid: false, ..entry };
        trx.map_write(vec![MapWrite::Put { map: trees_map(), key: store.inner().id.entry_key(), value: invalid.encode() }]).await?;
        applied.refused.push((store.inner().id.clone(), refused));
    }

    Ok(trx.complete(applied))
}

impl<SE, K, I, L, G> Planned<SE, K, I, L, G>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
{
    fn store(&self) -> &EngineAdapter<SE, K, L, G> {
        match self {
            Self::Changes { prepared, .. } => prepared.tree().store(),
            Self::Refused(tree, _) => tree.store(),
        }
    }

    /// The reads of the tree the apply's first request makes besides its
    /// entry: for a tree with changes, the leaves its commit checks, and the
    /// change log after the generation its rows are cached at.
    fn reads(&self) -> Vec<MapRead> {
        let Self::Changes { prepared, cached } = self else { return Vec::new() };
        let tree = prepared.tree().store().inner();

        let leaves = MapRead::Get { map: tree.rows.clone(), keys: prepared.leaves_checked().iter().map(Encode::encode).collect() };
        [leaves].into_iter().chain(cached.map(|from| log::read_after(tree.log.clone(), from))).collect()
    }

    /// What the next of `answers` hold of the tree, in the order of
    /// [`reads`](Self::reads), which every tree with changes made, retired
    /// or not.
    fn found(&self, answers: &mut impl Iterator<Item = MapRows>) -> Result<Found, Error> {
        let Self::Changes { prepared, cached } = self else { return Ok(Found { leaves: BTreeMap::new(), log: None }) };
        let keys = prepared.leaves_checked();

        let leaves = match answers.next() {
            Some(MapRows::Got(values)) if values.len() == keys.len() => keys.iter().cloned().zip(values).collect(),
            _ => return Err(Error::AnswerMisfit),
        };

        let log = match cached {
            Some(from) => Some((*from, answers.next().ok_or(Error::AnswerMisfit)?)),
            None => None,
        };

        Ok(Found { leaves, log })
    }
}

/// Apply one tree's prepared changes through `trx`, whose lock found the
/// tree's rows at generation `locked`, with what the first request `found`:
/// the cached rows brought up to that generation, then the commit applied and
/// written, or the tree's refusal, which writes nothing.
async fn apply_changes<SE, K, I, L, G>(
    trx: &mut dyn StorageTransaction,
    prepared: Prepared<K, I, L, G, EngineAdapter<SE, K, L, G>>,
    locked: u64,
    found: Found,
) -> Result<Outcome<SE, K, I, L, G>, MutationError>
where
    SE: StorageEngine + 'static,
    K: Encode + Clone + Send + Sync + 'static,
    I: MemberId,
    L: Encode + Payload,
    G: DigestGroup,
{
    let store = prepared.tree().store().clone();
    if let Some((from, log)) = found.log {
        store.inner().catch_up(from, locked, Some(log))?;
    }

    match prepared.apply(StorageEngineTrx::new(store, trx, Some(locked), found.leaves)).await {
        Ok(applied) => Ok(Outcome::Applied(applied)),
        Err(error) => refusal(error).map(Outcome::Refused),
    }
}
