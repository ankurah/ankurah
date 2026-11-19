//! Prepare both request summaries from one authorized tree version.

use std::ops::Bound;
use std::sync::atomic::Ordering;

use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::{leaf_address, DigestTreeView, Ristretto255, Span};
use ankurah_proto::{EntityId, KnownEntity, KnownState, State};
use futures::TryStreamExt;

use super::{continuation::retryable, TreeManager};
use crate::error::{MutationError, RetrievalError};
use crate::storage::StorageEngine;
use crate::sync::error::open_failure;
use crate::sync::{EngineAdapter, EntityHead, EntityMemberId, TreeProperty};

impl<SE: StorageEngine + 'static> TreeManager<SE, Ristretto255> {
    /// Calculate the legacy inventory and digest together. Authorization reads
    /// canonical states from the same snapshot as maintenance, never mutable
    /// resident Entity handles. The returned whole-tree view is a lifetime
    /// reference, not an authorized result view. Only the two summaries contain
    /// the authorized subset; each subsequent request reevaluates authorization.
    pub(crate) async fn prepare_known(
        &self,
        selection: Selection<Resolved>,
        mut authorize: impl FnMut(EntityId, &State) -> Result<bool, RetrievalError> + Send,
    ) -> Result<
        (
            DigestTreeView<
                TreeProperty,
                EntityMemberId,
                EntityHead,
                Ristretto255,
                EngineAdapter<SE, TreeProperty, EntityHead, Ristretto255>,
            >,
            KnownState,
            Vec<KnownEntity>,
        ),
        RetrievalError,
    > {
        let _publication = self.publication.lock().await;
        let tree = self.open_locked(selection.clone(), false).await?;
        for attempt in 0..3 {
            let result = async {
                let (working, mut snapshot, full, initial) = self.begin_reconciliation(&tree, false).await?;
                let changed = self.apply_snapshot(&working, &selection, &mut *snapshot, full, initial).await?;
                let mut stream = working.view().range(..).map_err(open_failure)?;
                let mut known_matches = Vec::new();
                let mut spans = Vec::new();
                let mut run: Option<(Vec<u8>, Vec<u8>)> = None;
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
                    for (leaf, state) in leaves.into_iter().zip(states) {
                        let state =
                            state.ok_or_else(|| RetrievalError::InvalidState("tree member absent from its canonical snapshot".into()))?;
                        if state.payload.state.head != leaf.payload.0 {
                            return Err(RetrievalError::InvalidState("tree head differs from its canonical snapshot".into()));
                        }
                        if authorize(leaf.id.0, &state.payload.state)? {
                            let address = leaf_address(&leaf.key, &leaf.id);
                            match &mut run {
                                Some((_, end)) => *end = address,
                                None => run = Some((address.clone(), address)),
                            }
                            known_matches.push(KnownEntity { entity_id: leaf.id.0, head: leaf.payload.0 });
                        } else if let Some((start, end)) = run.take() {
                            spans.push(Span::new(Bound::Included(start), Bound::Included(end)));
                        }
                    }
                }
                if let Some((start, end)) = run {
                    spans.push(Span::new(Bound::Included(start), Bound::Included(end)));
                }
                let cursor = snapshot.finish().await?;
                let view = working.select(spans).map_err(open_failure)?;
                let digest = view.span_digest(..).await.map_err(open_failure)?.digest;
                working.store().advance(cursor, self.epoch.load(Ordering::Acquire), changed);
                tree.store().publish(working.store().buffered());
                Ok((tree.view(), KnownState { digest }, known_matches))
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
