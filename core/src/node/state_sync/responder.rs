//! The responder's side of a state comparison. It compares a requester's offer
//! with a snapshot of its own result, answers with its own account of that
//! snapshot, which is the requester's target, and pushes on the update channel
//! what the requester lacks. Later it answers the requester's span questions
//! from a fresh snapshot each time, and pushes the entities a repair names. It
//! keeps nothing between requests.

mod comparison;
mod push;
mod repair;

use self::{
    comparison::OfferComparison,
    push::{spawn_push, Push},
    repair::{current_state_repairs, CurrentStateRepair},
};
use super::tree::{accept_peer_spec, entity_id_spec};
use crate::{
    node::Node,
    policy::{ContextPolicy, PolicyAgent},
    schema::is_catalog_read,
    storage::StorageEngine,
};
use ankql::ast::{Resolved, Selection};
use ankurah_proto::{self as proto, state_sync::limits::MAX_PAGE_HEADS, KnownState, SpanAnswer, SpanQuestion};
use anyhow::ensure;
use std::collections::HashSet;

/// Answer a requester's offer of `selection`'s result, which a Fetch or a
/// SubscribeQuery carries: compare it with a snapshot of the resident that
/// serves `selection` for `credentials`, push `peer` every entity the
/// comparison finds it lacks, as items carrying `relevance`, and return the
/// snapshot's own account. An offer under a spec no tree here serves, as when
/// two catalogs type a key part differently, is answered under the entity-id
/// spec and treated as unknown, so the requester still gets every member.
pub(crate) async fn answer_offer<SE, PA>(
    node: &Node<SE, PA>,
    peer: proto::EntityId,
    selection: Selection<Resolved>,
    known: KnownState,
    credentials: Vec<PA::ContextData>,
    relevance: Vec<(proto::QueryId, proto::MembershipChange)>,
) -> anyhow::Result<KnownState>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let catalog = is_catalog_read(&selection.predicate);
    let (spec, known) = match accept_peer_spec(&selection, &node.catalog, &known) {
        Ok(spec) => (spec, known),
        Err(_) => {
            let spec = entity_id_spec(&selection);
            (spec.clone(), KnownState { spec, spans: Vec::new() })
        }
    };
    let mut snapshot = node.resident_trees.snapshot(node, selection, Some(spec), credentials.clone()).await?.snapshot;
    let comparison = OfferComparison::new(&mut snapshot, known).await?;
    let target = snapshot.offer().await?;
    // The push reads current states, never the snapshot: holding it would copy
    // the resident's entries at its next commit.
    drop(snapshot);
    spawn_push(node, comparison.lacking, credentials, catalog, Push::new(peer, relevance));
    Ok(target)
}

/// Answer each of `questions` about `selection`'s result in a tree under `spec`,
/// from a fresh snapshot of the resident that serves it for `credentials`.
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
    entities: Vec<proto::KnownEntity>,
    credentials: Vec<PA::ContextData>,
) -> anyhow::Result<proto::NodeResponseBody>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    ensure!(entities.len() <= MAX_PAGE_HEADS, "too many entities to sync");
    let mut named = HashSet::with_capacity(entities.len());
    ensure!(entities.iter().all(|entity| named.insert(entity.entity_id)), "an entity to sync is named twice");
    let entities: Vec<_> =
        entities.into_iter().map(|entity| (entity.entity_id, (!entity.head.is_empty()).then_some(entity.head))).collect();
    let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
    let repairs = current_state_repairs(node, &policy, &entities, &credentials, is_catalog_read(&selection.predicate)).await?;
    let mut push = Push::new(peer, Vec::new());
    let (mut held, mut unavailable) = (Vec::new(), Vec::new());
    for ((entity_id, known), repair) in entities.into_iter().zip(repairs) {
        match repair {
            CurrentStateRepair::Denied => unavailable.push(entity_id),
            CurrentStateRepair::Current => held.push(proto::KnownEntity { entity_id, head: known.unwrap_or_default() }),
            CurrentStateRepair::Delta { head, delta } => {
                held.push(proto::KnownEntity { entity_id, head });
                push.add(node, delta)?;
            }
        }
    }
    push.send(node);
    Ok(proto::NodeResponseBody::EntitiesSynced { held, unavailable })
}
