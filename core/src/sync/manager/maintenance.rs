//! Convert stable before/after entity versions into each tree's changes.

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_digest_tree::{DigestGroup, Error};

use super::{DigestTree, TreeManager};
use crate::error::MutationError;
use crate::selection::filter::{evaluate_predicate, Error as FilterError};
use crate::storage::{StorageEngine, StorageTransaction};
use crate::sync::member::{EntityHead, EntityMember, EntityMemberId, EntityVersion, TreeProperty};
use crate::sync::{storage_adapter, AppliedTrees, TreeChange};

impl<SE: StorageEngine + 'static, G: DigestGroup> TreeManager<SE, G> {
    /// Apply before/after entity versions to every resident's full predicate
    /// membership. `None` before means insertion; `None` after means deletion.
    /// All tree changes use the caller's storage transaction and land with it.
    /// Applied changes keep their trees alive until the caller reports the commit landed.
    /// Hold the publication lock supplied to `new` from before this call through
    /// the storage commit, landing and entity publication. Do not call `open`
    /// while holding that lock.
    pub async fn apply<E: EntityVersion>(
        &self,
        trx: &mut SE::Transaction<'_>,
        changes: &[(Option<E>, Option<E>)],
    ) -> Result<AppliedTrees<SE, TreeProperty, EntityMemberId, EntityHead, G>, MutationError> {
        let trees = Self::live(&mut self.trees.lock().unwrap());
        let planned = match build_changes(&trees, changes) {
            Ok(planned) => planned,
            Err(error) => {
                trx.poison();
                return Err(error);
            }
        };
        storage_adapter::apply(&self.storage, trx, planned).await
    }
}

/// Missing properties and paths make a comparison UNKNOWN, not true. Invalid
/// or unsupported expressions remain errors rather than silently excluding data.
fn matches<E: EntityVersion>(entity: &E, predicate: &Predicate<Resolved>) -> Result<bool, MutationError> {
    match evaluate_predicate(entity, predicate) {
        Ok(included) => Ok(included),
        Err(FilterError::PropertyNotFound(_)) => Ok(false),
        Err(error) => Err(MutationError::General(Box::new(error))),
    }
}

fn build_changes<SE: StorageEngine + 'static, G: DigestGroup, E: EntityVersion>(
    trees: &[(Selection<Resolved>, DigestTree<SE, G>)],
    changes: &[(Option<E>, Option<E>)],
) -> Result<Vec<TreeChange<SE, TreeProperty, EntityMemberId, EntityHead, G>>, MutationError> {
    for (before, after) in changes {
        if let (Some(before), Some(after)) = (before, after) {
            if before.id() != after.id() {
                return Err(MutationError::General(Box::new(Error::IdChanged {
                    old: before.id().to_bytes().to_vec(),
                    new: after.id().to_bytes().to_vec(),
                })));
            }
        }
    }
    let mut planned = Vec::with_capacity(trees.len());
    for (selection, tree) in trees {
        let mut transaction = tree.transaction();
        let mut refused = None;
        let mut touched = false;
        for (before, after) in changes {
            let includes = |entity: &E| matches(entity, &selection.predicate);
            let before = match before {
                Some(entity) if includes(entity)? => Some(EntityMember(entity)),
                _ => None,
            };
            let after = match after {
                Some(entity) if includes(entity)? => Some(EntityMember(entity)),
                _ => None,
            };
            let result = match (before, after) {
                (Some(before), Some(after)) => transaction.update(before, after),
                (Some(before), None) => transaction.remove(before),
                (None, Some(after)) => transaction.insert(after),
                (None, None) => continue,
            };
            touched = true;
            if let Err(error) = result {
                refused = Some(error);
                break;
            }
        }
        if touched {
            planned.push(match refused {
                Some(error) => TreeChange::Refused(tree.clone(), error),
                None => TreeChange::Changes(transaction),
            });
        }
    }
    Ok(planned)
}
