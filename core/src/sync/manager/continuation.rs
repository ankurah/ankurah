//! Reconcile exact Selection membership at a complete storage read boundary.

use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;

use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::{DigestGroup, Leaf};
use ankurah_proto::{Attested, EntityState};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures::TryStreamExt;
use sha2::{Digest, Sha256};

use super::{selection_key, DigestTree, TreeManager};
use crate::entity::TemporaryEntity;
use crate::error::{MutationError, RetrievalError};
use crate::selection::filter::{evaluate_predicate, membership_match, Error as FilterError};
use crate::storage::{StateScan, StateSnapshot, StorageEngine, StorageTransaction};
use crate::sync::error::open_failure;
use crate::sync::member::EntityMember;
use crate::sync::{storage_adapter, EntityHead, EntityMemberId, TreeId};

const PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(256).unwrap();
const ATTEMPTS: usize = 3;

impl<SE: StorageEngine + 'static, G: DigestGroup> TreeManager<SE, G> {
    pub async fn maintain(&self) -> Result<(), RetrievalError> {
        let _publication = self.publication.lock().await;
        self.maintain_locked(false).await
    }

    /// Catch up and persist all managed trees before returning.
    pub async fn flush(&self) -> Result<(), RetrievalError> {
        let _publication = self.publication.lock().await;
        self.maintain_locked(true).await
    }

    pub(super) async fn maintain_locked(&self, force_flush: bool) -> Result<(), RetrievalError> {
        let trees = Self::live(&mut self.trees.lock().unwrap());
        let mut first_error = None;
        for (selection, tree) in trees {
            if let Err(error) = self.reconcile(&tree, &selection, false, force_flush).await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(super) async fn open_locked(&self, selection: Selection<Resolved>, rebuild: bool) -> Result<DigestTree<SE, G>, RetrievalError> {
        let key = selection_key(&selection)?;
        if let Some(tree) = self.tree(&key) {
            self.reconcile(&tree, &selection, rebuild, false).await?;
            return Ok(tree);
        }
        let id = TreeId::new(format!("selection-v2-{}-{}", G::ROW_VERSION, URL_SAFE_NO_PAD.encode(Sha256::digest(&key))));
        let tree = self.open_backing(id, &selection).await?;
        self.reconcile(&tree, &selection, rebuild, false).await?;
        self.remember(selection, key, tree.clone());
        Ok(tree)
    }

    async fn open_backing(&self, id: TreeId, selection: &Selection<Resolved>) -> Result<DigestTree<SE, G>, RetrievalError> {
        match storage_adapter::open_managed(self.storage.clone(), self.cache.clone(), &id, self.epoch.clone()).await {
            Ok(tree) => Ok(tree),
            Err(error) if missing_or_invalid(&error) => {
                let key_spec = self.key_spec(selection).await?;
                let mut trx = self.storage.transaction();
                let created = storage_adapter::initialize::<_, _, _, _, G, _>(
                    self.storage.clone(),
                    self.cache.clone(),
                    &mut trx,
                    id.clone(),
                    key_spec,
                    std::iter::empty::<EntityMember<'_>>(),
                )
                .await;
                match created {
                    Ok(created) => {
                        trx.commit().await?.committed()?;
                        drop(created.landed());
                    }
                    Err(MutationError::WriteConflict) => drop(trx),
                    Err(error) => return Err(error.into()),
                }
                storage_adapter::open_managed(self.storage.clone(), self.cache.clone(), &id, self.epoch.clone()).await.map_err(|error| {
                    // The entry existed when initialization completed or lost
                    // to another opener. Losing it now is a concurrent reset.
                    if missing_or_invalid(&error) {
                        MutationError::WriteConflict.into()
                    } else {
                        error
                    }
                })
            }
            Err(error) => Err(error),
        }
    }

    async fn reconcile(
        &self,
        tree: &DigestTree<SE, G>,
        selection: &Selection<Resolved>,
        rebuild: bool,
        force_flush: bool,
    ) -> Result<(), RetrievalError> {
        for attempt in 0..ATTEMPTS {
            match self.reconcile_once(tree, selection, rebuild, force_flush).await {
                Ok(()) => return Ok(()),
                Err(error) if retryable(&error) && attempt + 1 < ATTEMPTS => {
                    let _ = self.open_backing(tree.store().id().clone(), selection).await?;
                    tree.store().load_buffered(self.epoch.clone()).await?;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!()
    }

    async fn reconcile_once(
        &self,
        tree: &DigestTree<SE, G>,
        selection: &Selection<Resolved>,
        rebuild: bool,
        force_flush: bool,
    ) -> Result<(), RetrievalError> {
        let (working, mut snapshot, full, initial) = self.begin_reconciliation(tree, selection, rebuild).await?;
        let changed = self.apply_snapshot(&working, selection, &mut *snapshot, initial).await?;
        let cursor = snapshot.finish().await?;
        working.store().advance(cursor, self.epoch.load(Ordering::Acquire), changed);
        let version = working.store().buffered();
        let policy = *self.write_behind.lock().unwrap();
        if full
            || force_flush
            || version.dirty_since.is_some_and(|since| since.elapsed() >= policy.max_delay || version.changes >= policy.max_changes)
        {
            if version.dirty_since.is_some() {
                working.store().flush().await?;
            }
        }
        tree.store().publish(working.store().buffered());
        Ok(())
    }

    pub(super) async fn begin_reconciliation(
        &self,
        tree: &DigestTree<SE, G>,
        selection: &Selection<Resolved>,
        rebuild: bool,
    ) -> Result<(DigestTree<SE, G>, Box<dyn StateSnapshot>, bool, bool), RetrievalError> {
        let store = tree.store().working();
        // Capture/check the durable base before beginning the entity snapshot.
        // Reversing that order can replace a newer tree with an older snapshot.
        // Even a rebuild must validate the base entry, but it need not decode
        // the old metadata or tree records before replacing them.
        let read = store.buffered_transaction().await.map_err(|error| open_failure(error.into()))?;
        drop(read);
        let base = store.buffered();
        let initial = base.cursor.is_none();
        let mut full = rebuild || initial;
        // Only an explicit Building entry promises resumable member lookup.
        // A Ready tree rebuilt through the public adapter may contain arbitrary
        // members without their lookup/cursor, so it needs full replacement.
        let mut clear = rebuild || (initial && !base.is_building());
        let scan = if selection.limit.is_some() {
            StateScan::Selection(selection)
        } else {
            StateScan::Changes(if full { None } else { base.cursor.as_ref() })
        };
        let snapshot = match self.storage.state_snapshot(scan).await {
            Err(RetrievalError::InvalidStateCursor) => {
                full = true;
                clear = true;
                let scan = match scan {
                    StateScan::Changes(_) => StateScan::Changes(None),
                    selection => selection,
                };
                self.storage.state_snapshot(scan).await?
            }
            result => result?,
        };
        // A Building tree keeps its completed batches. The full scan corrects
        // changed keys and membership in place; physical deletion only occurs
        // through reset, which also retires this tree's base.
        let working = if clear {
            let key_spec = tree.key_spec().clone();
            store.clear_buffered();
            let trx = store.buffered_transaction().await.map_err(|error| open_failure(error.into()))?;
            let created =
                DigestTree::create_in(store.clone(), trx, key_spec, std::iter::empty::<EntityMember<'_>>()).await.map_err(open_failure)?;
            created.landed()
        } else {
            DigestTree::open(store).await.map_err(open_failure)?
        };
        Ok((working, snapshot, full, initial))
    }

    pub(super) async fn apply_snapshot(
        &self,
        tree: &DigestTree<SE, G>,
        selection: &Selection<Resolved>,
        snapshot: &mut dyn StateSnapshot,
        initial: bool,
    ) -> Result<usize, RetrievalError> {
        let mut page = snapshot.next_page(PAGE_SIZE).await?;
        if selection.limit.is_some() {
            let mut selected = Vec::new();
            while !page.is_empty() {
                selected.extend(page);
                page = snapshot.next_page(PAGE_SIZE).await?;
            }
            // A complete selected set includes unchanged candidates that refill
            // the boundary. Keep unchanged leaves, and remove only former members.
            let selected_ids: std::collections::HashSet<_> = selected.iter().map(|state| state.payload.entity_id).collect();
            let held: Vec<_> = tree.view().range(..).map_err(open_failure)?.try_collect().await.map_err(open_failure)?;
            let mut changes = tree.transaction();
            let mut removed = Vec::new();
            for leaf in held {
                if !selected_ids.contains(&leaf.id.0) {
                    removed.push((leaf.id.0.to_bytes().to_vec(), None));
                    changes.remove_leaf(leaf).map_err(open_failure)?;
                }
            }
            let count = removed.len();
            let prepared = changes.prepare().await;
            let trx = tree.store().buffered_transaction().await.map_err(|error| open_failure(error.into()))?;
            prepared.apply(trx).await.map_err(open_failure)?.landed();
            tree.store().set_member_rows(removed);
            return Ok(count + self.apply_states(tree, selection, selected).await?);
        }
        let mut count = 0;
        while !page.is_empty() {
            count += page.len();
            self.apply_states(tree, selection, page).await?;
            if initial && tree.store().buffered().has_writes() {
                // No completed backing tree exists yet. Persist bounded build
                // batches as Building, with no recovery cursor. Another builder
                // resumes them through an idempotent full scan. Only finish's
                // successful validation permits the final Ready publication.
                tree.store().flush().await?;
            }
            page = snapshot.next_page(PAGE_SIZE).await?;
        }
        Ok(count)
    }

    async fn apply_states(
        &self,
        tree: &DigestTree<SE, G>,
        selection: &Selection<Resolved>,
        states: Vec<Attested<EntityState>>,
    ) -> Result<usize, RetrievalError> {
        let ids: Vec<_> = states.iter().map(|state| state.payload.entity_id).collect();
        let keys: Vec<_> = ids.iter().map(|id| id.to_bytes().to_vec()).collect();
        let held = tree.store().member_rows(&keys).await?;
        let mut changes = tree.transaction();
        let mut members = Vec::new();
        for ((state, id), old) in states.into_iter().zip(ids).zip(held) {
            let old = old
                .map(|bytes| bincode::deserialize::<(Vec<u8>, ankurah_proto::Clock)>(&bytes))
                .transpose()?
                .map(|(key, head)| Leaf { key, id: EntityMemberId(id), payload: EntityHead(head) });
            // Membership can exclude an unrelated model without decoding its
            // property backends. A former member still needs its old leaf removed.
            if membership_match(&selection.predicate, &|model| state.payload.state.memberships.contains(model)) == Some(false) {
                if let Some(old) = old {
                    changes.remove_leaf(old).map_err(open_failure)?;
                    members.push((id.to_bytes().to_vec(), None));
                }
                continue;
            }
            let entity = TemporaryEntity::new(id, &state.payload.state)?;
            let matches = if selection.limit.is_some() {
                true
            } else {
                match evaluate_predicate(&entity, &selection.predicate) {
                    Ok(value) => value,
                    Err(FilterError::PropertyNotFound(_)) => false,
                    Err(error) => return Err(error.into()),
                }
            };
            let new = if matches { Some(tree.leaf(&EntityMember(&entity)).map_err(open_failure)?) } else { None };
            if old == new {
                continue;
            }
            match (&old, &new) {
                (Some(old), Some(_)) => changes.update_leaf(old.clone(), EntityMember(&entity)).map_err(open_failure)?,
                (Some(old), None) => changes.remove_leaf(old.clone()).map_err(open_failure)?,
                (None, Some(_)) => changes.insert(EntityMember(&entity)).map_err(open_failure)?,
                (None, None) => continue,
            }
            members.push((id.to_bytes().to_vec(), new.map(|leaf| bincode::serialize(&(leaf.key, leaf.payload.0))).transpose()?));
        }
        let prepared = changes.prepare().await;
        let trx = tree.store().buffered_transaction().await.map_err(|error| open_failure(error.into()))?;
        prepared.apply(trx).await.map_err(open_failure)?.landed();
        let count = members.len();
        tree.store().set_member_rows(members);
        Ok(count)
    }
}

fn missing_or_invalid(error: &RetrievalError) -> bool {
    match error {
        RetrievalError::StorageError(error) => error
            .downcast_ref::<crate::sync::Error>()
            .is_some_and(|error| matches!(error, crate::sync::Error::NoSuchTree(_) | crate::sync::Error::Invalid(_))),
        _ => false,
    }
}

pub(super) fn retryable(error: &RetrievalError) -> bool {
    matches!(error, RetrievalError::MutationError(error) if matches!(**error, MutationError::WriteConflict))
}
