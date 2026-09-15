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
