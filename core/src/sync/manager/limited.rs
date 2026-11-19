//! Maintain a limited selection in its tree and refill only an incomplete tail.

use std::{collections::HashSet, num::NonZeroUsize};

use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::{leaf_address, DigestGroup, ScanRange};
use futures::{StreamExt, TryStreamExt};

use super::{continuation::PAGE_SIZE, DigestTree, TreeManager};
use crate::{
    entity::TemporaryEntity,
    error::RetrievalError,
    selection::{filter::membership_match, ordering::OrderBoundary},
    storage::{StateSnapshot, StorageEngine},
    sync::{error::open_failure, member::EntityMember},
};

impl<SE: StorageEngine + 'static, G: DigestGroup> TreeManager<SE, G> {
    pub(super) async fn apply_limited_changes(
        &self,
        tree: &DigestTree<SE, G>,
        selection: &Selection<Resolved>,
        snapshot: &mut dyn StateSnapshot,
        limit: u64,
    ) -> Result<usize, RetrievalError> {
        let view = tree.view();
        let count = view.count(..).await.map_err(open_failure)?;
        // Below LIMIT the previous tree held the entire selection. Otherwise
        // its last address bounds a complete prefix. Unchanged outsiders could
        // not have preceded it; the change scan accounts for every moved/new ID.
        let mut through = if count < limit || limit == 0 {
            None
        } else {
            view.range(ScanRange::reverse(..))
                .map_err(open_failure)?
                .try_next()
                .await
                .map_err(open_failure)?
                .map(|leaf| leaf_address(&leaf.key, &leaf.id))
        };
        let mut changed = 0;
        loop {
            let states = snapshot.next_page(PAGE_SIZE).await?;
            if states.is_empty() {
                break;
            }
            if limit == 0 {
                continue;
            }
            let candidates: Vec<_> = states
                .iter()
                .filter(|state| {
                    membership_match(&selection.predicate, &|model| state.payload.state.memberships.contains(model)) != Some(false)
                })
                .map(|state| state.payload.entity_id)
                .collect();
            let matching: HashSet<_> =
                if candidates.is_empty() { HashSet::new() } else { snapshot.matching_ids(&candidates).await?.into_iter().collect() };
            changed += self.apply_states(tree, selection, states, Some(&matching), through.as_deref()).await?;
            if let Some((last, removed)) = self.trim_limited(tree, limit).await? {
                changed += removed;
                // Bound temporary tree growth to LIMIT plus one change page.
                // Pruning establishes a shorter complete prefix; later changes
                // must not admit candidates beyond it without the final refill.
                through = Some(last);
            }
        }
        let count = tree.view().count(..).await.map_err(open_failure)?;
        if limit == 0 || count >= limit || through.is_none() {
            return Ok(changed);
        }

        let last = tree.view().range(ScanRange::reverse(..)).map_err(open_failure)?.try_next().await.map_err(open_failure)?;
        let after = if let Some(leaf) = last {
            let states = snapshot.get_states(&[leaf.id.0]).await?;
            let [Some(state)] = states.as_slice() else {
                return Err(RetrievalError::InvalidState("tree boundary is absent from its canonical snapshot".into()));
            };
            let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
            if tree.leaf(&EntityMember(&entity)).map_err(open_failure)? != leaf {
                return Err(RetrievalError::InvalidState("tree boundary differs from its canonical snapshot".into()));
            }
            Some(OrderBoundary::from_item(selection, &entity))
        } else {
            None
        };
        let needed = usize::try_from(limit - count).map_err(|_| RetrievalError::Other("selection LIMIT exceeds this platform".into()))?;
        let states = snapshot.select_after(after.as_ref(), NonZeroUsize::new(needed).unwrap()).await?;
        if states.len() > needed {
            return Err(crate::sync::Error::AnswerMisfit.into());
        }
        changed += self.apply_states(tree, selection, states, None, None).await?;
        Ok(changed)
    }

    /// Drop a bounded excess and return the address through which membership
    /// remains complete. The caller supplies a positive limit.
    async fn trim_limited(&self, tree: &DigestTree<SE, G>, limit: u64) -> Result<Option<(Vec<u8>, usize)>, RetrievalError> {
        let count = tree.view().count(..).await.map_err(open_failure)?;
        if count <= limit {
            return Ok(None);
        }
        let excess = usize::try_from(count - limit).map_err(|_| RetrievalError::Other("tree size exceeds this platform".into()))?;
        let leaves: Vec<_> =
            tree.view().range(ScanRange::reverse(..)).map_err(open_failure)?.take(excess).try_collect().await.map_err(open_failure)?;
        let mut changes = tree.transaction();
        let mut members = Vec::with_capacity(leaves.len());
        for leaf in leaves {
            members.push((leaf.id.0.to_bytes().to_vec(), None));
            changes.remove_leaf(leaf).map_err(open_failure)?;
        }
        let prepared = changes.prepare().await;
        let trx = tree.store().buffered_transaction().await.map_err(|error| open_failure(error.into()))?;
        prepared.apply(trx).await.map_err(open_failure)?.landed();
        tree.store().set_member_rows(members);
        let last = tree
            .view()
            .range(ScanRange::reverse(..))
            .map_err(open_failure)?
            .try_next()
            .await
            .map_err(open_failure)?
            .ok_or_else(|| RetrievalError::InvalidState("a positive limited prefix is empty".into()))?;
        Ok(Some((leaf_address(&last.key, &last.id), excess)))
    }
}
