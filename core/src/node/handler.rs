use super::Node;
use crate::{
    error::{MutationError, RetrievalError},
    policy::{AccessDenied, ContextPolicy, PolicyAgent},
    remote_transaction::RemoteTransaction,
    storage::StorageEngine,
    util::{retry::retry_on, Iterable},
};
use ankql::ast::Predicate;
use ankurah_proto::{Attested, Event, EventId, NodeResponseBody, TransactionId};
use anyhow::anyhow;
use itertools::Itertools;
use std::collections::BTreeSet;
use tracing::debug;

/// Commit received events under one authenticated principal, retrying storage conflicts.
pub async fn commit_transaction<SE, PA, C>(
    node: &Node<SE, PA>,
    cdata: &C,
    id: TransactionId,
    events: Vec<Attested<Event>>,
) -> anyhow::Result<NodeResponseBody>
where
    SE: StorageEngine + 'static,
    PA: PolicyAgent,
    C: Iterable<PA::ContextData>,
{
    let cdata = cdata.iterable().exactly_one().map_err(|_| anyhow!("Only one cdata is permitted for CommitTransaction"))?;
    node.system.require_system_ready()?;

    debug!("{node} commiting transaction {id} with {} events", events.len());
    let policy = ContextPolicy::from_credentials(&node.policy_agent, cdata);
    retry_on!(MutationError::WriteConflict, {
        let mut trx = RemoteTransaction::new(node, &policy);
        for event in &events {
            trx.add_event(event).await?;
        }
        trx.commit().await
    })?;
    Ok(NodeResponseBody::CommitComplete { id })
}

/// Retrieve visible events, then apply event-specific checks without blocking catalog bootstrap reads.
pub(super) async fn get_events<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent, C: Iterable<PA::ContextData>>(
    node: &Node<SE, PA>,
    credentials: &C,
    event_ids: Vec<EventId>,
) -> Result<NodeResponseBody, RetrievalError> {
    let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
    let events = node.storage.get_events(event_ids, &policy.retrieval_predicate()).await?;
    let ids = events.iter().map(|event| event.payload.entity_id).collect::<BTreeSet<_>>();
    let catalog = crate::schema::CATALOG_MODELS.into_iter().map(Predicate::MemberOf)
        .reduce(|left, right| Predicate::Or(Box::new(left), Box::new(right))).unwrap();
    let exempt: BTreeSet<_> = node.storage.filter_entity_ids(&ids.into_iter().collect::<Vec<_>>(), &catalog).await?.into_iter().collect();
    let mut accepted = Vec::new();
    for event in events {
        if !exempt.contains(&event.payload.entity_id) {
            match node.policy_agent.check_read_event(credentials, &event) {
                Ok(()) => {}
                Err(AccessDenied::ByPolicy(_) | AccessDenied::ModelDenied(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        accepted.push(event);
    }
    Ok(NodeResponseBody::GetEvents(accepted))
}
