//! Capture an authorized tree version for query comparison.
use std::sync::atomic::Ordering;

use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::Ristretto255;
use ankurah_proto::{EntityId, State};
use futures::TryStreamExt;

use super::{continuation::retryable, DigestTree, TreeManager};
use crate::error::{MutationError, RetrievalError};
use crate::storage::StorageEngine;
use crate::sync::error::open_failure;

impl<SE: StorageEngine + 'static> TreeManager<SE, Ristretto255> {
    /// Return the original managed owner and a private authorized capture.
    /// Both are tree handles so callers can read their views and key specs.
    /// Keep the owner while using the capture: only the original tree belongs
    /// to the manager's maintenance set.
    ///
    /// `None` means the caller has proved every selected member is authorized.
    /// Otherwise each member is checked against the same canonical snapshot
    /// used for reconciliation. Denied leaves are removed from a private
    /// overlay, including their trie structure; selecting spans alone would
    /// still expose denied members' prefixes in page subdivisions. Filtering
    /// never refills a limited Selection or changes the managed tree.
    pub(crate) async fn capture_for_comparison(
        &self,
        selection: Selection<Resolved>,
        mut authorize: Option<&mut (dyn FnMut(EntityId, &State) -> Result<bool, RetrievalError> + Send)>,
    ) -> Result<(DigestTree<SE, Ristretto255>, DigestTree<SE, Ristretto255>), RetrievalError> {
        let _publication = self.publication.lock().await;
        let tree = self.open_locked(selection.clone(), false).await?;
        for attempt in 0..3 {
            let result = async {
                let (working, mut snapshot, full, initial) = self.begin_reconciliation(&tree, &selection, false).await?;
                let changed = self.apply_snapshot(&working, &selection, &mut *snapshot, full, initial).await?;
                let authorized = match authorize.as_deref_mut() {
                    None => working.clone(),
                    Some(authorize) => {
                        let authorized = DigestTree::open(working.store().working()).await.map_err(open_failure)?;
                        let mut stream = working.view().range(..).map_err(open_failure)?;
                        loop {
                            let mut leaves = Vec::new();
                            while leaves.len() < 256 {
                                let Some(leaf) = stream.try_next().await.map_err(open_failure)? else { break };
                                leaves.push(leaf);
                            }
                            if leaves.is_empty() {
                                break;
                            }
                            let ids: Vec<_> = leaves.iter().map(|leaf| leaf.id.0).collect();
                            let states = snapshot.get_states(&ids).await?;
                            if states.len() != leaves.len() {
                                return Err(crate::sync::Error::AnswerMisfit.into());
                            }
                            let mut removals = None;
                            for (leaf, state) in leaves.into_iter().zip(states) {
                                let state = state
                                    .ok_or_else(|| RetrievalError::InvalidState("tree member absent from its canonical snapshot".into()))?;
                                if state.payload.state.head != leaf.payload.0 {
                                    return Err(RetrievalError::InvalidState("tree head differs from its canonical snapshot".into()));
                                }
                                if !authorize(leaf.id.0, &state.payload.state)? {
                                    removals.get_or_insert_with(|| authorized.transaction()).remove_leaf(leaf).map_err(open_failure)?;
                                }
                            }
                            if let Some(removals) = removals {
                                removals
                                    .prepare()
                                    .await
                                    .apply(authorized.store().buffered_transaction().await.map_err(|error| open_failure(error.into()))?)
                                    .await
                                    .map_err(open_failure)?
                                    .landed();
                            }
                        }
                        authorized
                    }
                };
                let cursor = snapshot.finish().await?;
                working.store().advance(cursor, self.epoch.load(Ordering::Acquire), changed);
                tree.store().publish(working.store().buffered());
                let captured = DigestTree::open(authorized.store().capture()).await.map_err(open_failure)?;
                Ok((tree.clone(), captured))
            }
            .await;
            match result {
                Err(error) if retryable(&error) && attempt < 2 => tree.store().load_buffered(self.epoch.clone()).await?,
                result => return result,
            }
        }
        Err(MutationError::WriteConflict.into())
    }
}
