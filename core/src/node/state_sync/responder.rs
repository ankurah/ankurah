//! The responder's side of a state comparison. It compares a requester's offer
//! with a snapshot of its own result, answers with its own account of that
//! snapshot, which is the requester's target, and pushes on the update channel
//! what the requester lacks. Later it answers the requester's span questions
//! from a fresh snapshot each time, and pushes the entities a repair names. It
//! keeps nothing between requests.

mod comparison;
mod push;
mod repair;
mod size;

#[cfg(test)]
mod tests;

use self::{
    push::{spawn_push, Push},
    repair::{current_state_repairs, CurrentStateRepair},
};
use super::tree::{entity_id_spec, serves};
use crate::{
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    schema::is_catalog_read,
    storage::StorageEngine,
    util::cancel_flag::CancelFlag,
};
use ankql::ast::{Resolved, Selection};
use ankurah_proto::{self as proto, KnownState, SpanAnswer, SpanQuestion};

/// The request an offer arrived in, which decides the tag on the items its
/// push carries and what may stop that push before it finishes.
pub(crate) enum OfferedIn {
    /// A Fetch: its pushed items belong to no standing query, and its push ends
    /// only with its connection.
    Fetch,
    /// A SubscribeQuery for `query_id`: its pushed items are that query's
    /// initial data, and `cancel` stops its push when a later version replaces
    /// the query or the peer removes it.
    SubscribeQuery { query_id: proto::QueryId, cancel: CancelFlag },
}

/// Answer a requester's offer of `selection`'s result, which a Fetch or a
/// SubscribeQuery carries: compare it with a snapshot of the resident tree that
/// serves `selection` for `credentials`, push `peer` every entity the
/// comparison finds it lacks, and return the snapshot's own account. An offer
/// under a spec no tree here serves, as when two catalogs type a key part
/// differently, is answered under the entity-id spec and treated as unknown,
/// so the requester still gets every member.
pub(crate) async fn answer_offer<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    selection: Selection<Resolved>,
    known: KnownState,
    credentials: Vec<PA::ContextData>,
    offered_in: OfferedIn,
) -> anyhow::Result<KnownState>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let catalog = is_catalog_read(&selection.predicate);
    let known = if serves(&known.spec, &selection, &node.catalog) {
        // A malformed offer is refused before any snapshot is taken for it.
        known.validate()?;
        known
    } else {
        KnownState { spec: entity_id_spec(&selection), spans: Vec::new() }
    };
    let spec = known.spec.clone();
    let mut snapshot = node.resident_trees.snapshot(node, selection, Some(spec), credentials.clone()).await?.snapshot;
    let lacking = comparison::lacking(&snapshot, known);
    let target = snapshot.offer().await?;
    // The push reads current states, never the snapshot: holding it would copy
    // the tree's entries at its next commit.
    drop(snapshot);
    let (relevance, cancel) = match offered_in {
        OfferedIn::Fetch => (Vec::new(), CancelFlag::default()),
        OfferedIn::SubscribeQuery { query_id, cancel } => (vec![(query_id, proto::MembershipChange::Initial)], cancel),
    };
    spawn_push(node, lacking, credentials, catalog, Push::new(node, peer, relevance, cancel));
    Ok(target)
}

/// Answer each of `questions` about `selection`'s result in a tree under `spec`,
/// from a fresh snapshot of the resident tree that serves it for `credentials`.
pub(crate) async fn compare_spans<SE, PA>(
    node: &Node<SE, PA>,
    selection: Selection<Resolved>,
    spec: proto::TreeSpec,
    questions: Vec<SpanQuestion>,
    credentials: Vec<PA::ContextData>,
) -> anyhow::Result<proto::NodeResponseBody>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    SpanQuestion::validate_all(&questions)?;
    let mut snapshot = node.resident_trees.snapshot(node, selection, Some(spec), credentials).await?.snapshot;
    let mut answers = Vec::with_capacity(questions.len());
    for question in questions {
        answers.push(match question {
            SpanQuestion::Digest(span) => SpanAnswer::Digest(snapshot.span_digest(span).await?),
            SpanQuestion::Heads(span) => SpanAnswer::Heads(snapshot.page(span)?),
        });
    }
    Ok(proto::NodeResponseBody::SpansCompared { answers })
}

/// Push `peer` each of `entities` from the head it holds, and answer with the
/// head this node holds of each it may send and the entities it holds no
/// readable state of. `selection` says whether the request bypassed the read
/// policy as a catalog read, which confines it to catalog entities.
pub(crate) async fn sync_entities<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    selection: Selection<Resolved>,
    entities: Vec<proto::EntityHead>,
    credentials: Vec<PA::ContextData>,
) -> anyhow::Result<proto::NodeResponseBody>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    proto::EntityHead::validate_sync_request(&entities)?;
    let entities: Vec<_> =
        entities.into_iter().map(|entity| (entity.entity_id, (!entity.head.is_empty()).then_some(entity.head))).collect();
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    let repairs = current_state_repairs(node, &policy, &entities, is_catalog_read(&selection.predicate)).await?;
    let mut push = Push::new(node, peer, Vec::new(), CancelFlag::default());
    let (mut held, mut unavailable) = (Vec::new(), Vec::new());
    for ((entity_id, _), repair) in entities.into_iter().zip(repairs) {
        match repair {
            CurrentStateRepair::Unavailable => unavailable.push(entity_id),
            CurrentStateRepair::Current { head } => held.push(proto::EntityHead { entity_id, head }),
            CurrentStateRepair::Lacking { head, content } => {
                held.push(proto::EntityHead { entity_id, head });
                push.add(node, entity_id, content);
            }
        }
    }
    push.send(node);
    Ok(proto::NodeResponseBody::EntitiesSynced { held, unavailable })
}
