//! Whether the requester's copy of an entity covers a head the responder
//! holds: the same head, or a causally newer one.

use crate::{
    error::RetrievalError,
    event_dag::{compare, AbstractCausalRelation},
    node::Node,
    policy::PolicyAgent,
    retrieval::LocalEventGetter,
    storage::{GetStateResult, StorageEngine},
};
use ankurah_proto::{Clock, EntityId};
use std::collections::HashMap;

/// Events the check may walk to decide whether a local head covers a
/// responder's. A copy further ahead counts as uncovered until a repair
/// reports the responder's newer head.
const COVERAGE_BUDGET: usize = 256;

/// Whether `local` covers `target`: it is that head or descends from it.
pub(super) async fn covers<SE, PA>(node: &Node<SE, PA>, local: &Clock, target: &Clock) -> anyhow::Result<bool>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    if local == target || target.entries().iter().all(|entry| local.entries().contains(entry)) {
        return Ok(true);
    }
    if local.is_empty() {
        return Ok(false);
    }
    let relation = match compare(LocalEventGetter::new(node.storage.clone(), node.durable), local, target, COVERAGE_BUDGET).await {
        Ok(relation) => relation,
        // A newer state can arrive before the events behind it. Missing local
        // history proves neither coverage nor an invalid account.
        Err(RetrievalError::EventNotFound(_)) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    Ok(matches!(relation.relation, AbstractCausalRelation::Equal | AbstractCausalRelation::StrictDescends { .. }))
}

/// This node's head of each of `ids` it holds: a resident entity's, or else
/// the stored state's, read from storage in one read.
pub(super) async fn local_heads<SE, PA>(
    node: &Node<SE, PA>,
    ids: impl IntoIterator<Item = EntityId>,
) -> anyhow::Result<HashMap<EntityId, Clock>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut heads = HashMap::new();
    let mut stored = Vec::new();
    for id in ids {
        match node.entities.get(&id) {
            Some(entity) => {
                heads.insert(id, entity.head());
            }
            None => stored.push(id),
        }
    }
    if !stored.is_empty() {
        for result in node.storage.get_states(stored, &ankql::ast::Predicate::True).await? {
            if let GetStateResult::Found(state) = result {
                heads.insert(state.payload.entity_id, state.payload.state.head);
            }
        }
    }
    Ok(heads)
}
