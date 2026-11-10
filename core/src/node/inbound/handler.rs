use super::schema_registration::WireRegistrant;
use crate::node::{Node, PeerState};
use crate::{
    error::{MutationError, RetrievalError},
    policy::{AccessDenied, ContextPolicy, PolicyAgent},
    schema::{catalog::register::RegistrationAuth, registration::RegistrationError},
    storage::{GetStateResult, StorageEngine},
    transaction::remote::RemoteTransaction,
    util::{retry::retry_on, Iterable},
};
use ankql::ast::Predicate;
use ankurah_proto::{self as proto, Attested, EntityId, Event, EventId, NodeResponseBody, State, TransactionId};
use anyhow::anyhow;
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};
use tracing::debug;
#[cfg(feature = "instrument")]
use tracing::instrument;

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
    node.lifecycle.check_not_halted()?;

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

/// Return the requested events these credentials may read, checking each against its entity's state.
pub(super) async fn get_events<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent, C: Iterable<PA::ContextData>>(
    node: &Node<SE, PA>,
    credentials: &C,
    event_ids: Vec<EventId>,
) -> Result<NodeResponseBody, RetrievalError> {
    let policy = ContextPolicy::from_credentials(&node.policy_agent, credentials);
    let events = node.storage.get_events(event_ids).await?;
    let states = entity_states(node, events.iter().map(|event| event.payload.entity_id)).await?;

    let mut accepted = Vec::new();
    for event in events {
        // Without its entity's state there is nothing to check an event against, so it is withheld.
        let Some(state) = states.get(&event.payload.entity_id) else { continue };
        match policy.check_read_event(&event, state) {
            Ok(()) => accepted.push(event),
            Err(AccessDenied::ByPolicy(_) | AccessDenied::ModelDenied(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(NodeResponseBody::GetEvents(accepted))
}

/// Each entity's current state, read once: from its resident instance when loaded, else from storage.
async fn entity_states<SE: StorageEngine + Send + Sync + 'static, PA: PolicyAgent>(
    node: &Node<SE, PA>,
    ids: impl IntoIterator<Item = EntityId>,
) -> Result<BTreeMap<EntityId, State>, RetrievalError> {
    let mut states = BTreeMap::new();
    let mut unloaded = Vec::new();
    for id in ids.into_iter().collect::<BTreeSet<_>>() {
        match node.entities.get(&id) {
            Some(entity) => {
                states.insert(id, entity.to_state()?);
            }
            None => unloaded.push(id),
        }
    }
    for result in node.storage.get_states(unloaded, &Predicate::True).await? {
        if let GetStateResult::Found(state) = result {
            states.insert(state.entity_id, state.state);
        }
    }
    Ok(states)
}

/// Answer `body`, a request that arrived on `connection`.
#[cfg_attr(feature = "instrument", instrument(level = "debug", skip_all, fields(body = %body)))]
pub(super) async fn handle_request<SE, PA, C>(
    node: &Node<SE, PA>,
    cdata: &C,
    connection: &PeerState<PA::ContextData>,
    body: proto::NodeRequestBody,
) -> anyhow::Result<proto::NodeResponseBody>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    C: Iterable<PA::ContextData>,
{
    match body {
        proto::NodeRequestBody::CommitTransaction { id, events } => commit_transaction(node, cdata, id, events).await,
        proto::NodeRequestBody::RegisterSchema { model } => {
            let cdata = cdata.iterable().exactly_one().map_err(|_| anyhow!("Only one cdata is permitted for RegisterSchema"))?;
            if !node.durable {
                return Ok(proto::NodeResponseBody::Error(RegistrationError::NotDurable.to_string()));
            }
            let mut registrant = WireRegistrant::new(model);
            match node.catalog.resolve_or_register(node, &mut registrant, RegistrationAuth::Credential(cdata.clone())).await {
                Ok(()) => Ok(proto::NodeResponseBody::SchemaRegistered {
                    model: registrant.into_response().expect("successful registration must bind its response"),
                }),
                Err(e) => Ok(proto::NodeResponseBody::Error(e.to_string())),
            }
        }
        proto::NodeRequestBody::Fetch { mut selection, known_matches } => {
            let policy = ContextPolicy::from_credentials(&node.policy_agent, cdata);
            selection.predicate = policy.filter_predicate(selection.predicate)?;

            let expanded_states = crate::util::expand_states::expand_states(
                node.storage.fetch_states(&selection).await?,
                known_matches.iter().map(|k| k.entity_id).collect::<Vec<_>>(),
                node.storage.as_ref(),
            )
            .await?;

            let known_map: std::collections::HashMap<_, _> = known_matches.into_iter().map(|k| (k.entity_id, k.head)).collect();

            let mut deltas = Vec::new();
            for state in expanded_states {
                if policy.check_read(&state.entity_id, &state.state).is_err() {
                    continue;
                }

                if let Some(delta) = crate::node::inbound::delta::generate_entity_delta(node, &known_map, state, cdata).await? {
                    deltas.push(delta);
                }
            }
            Ok(proto::NodeResponseBody::Fetch(deltas))
        }
        proto::NodeRequestBody::Get { ids } => {
            let policy = ContextPolicy::from_credentials(&node.policy_agent, cdata);
            let results = node.storage.get_states(ids, &policy.retrieval_predicate()).await?;
            let denied = policy.check_reads(&results);
            let results = results
                .into_iter()
                .map(|result| match result {
                    GetStateResult::Found(state) if denied.contains(&state.entity_id) => proto::GetResult::AccessDenied(state.entity_id),
                    GetStateResult::Found(state) => proto::GetResult::Found(crate::node::identity::attestation::served_state(node, state)),
                    GetStateResult::NotFound(id) => proto::GetResult::NotFound(id),
                    GetStateResult::PredicateMismatch(id) => proto::GetResult::AccessDenied(id),
                })
                .collect();
            Ok(proto::NodeResponseBody::Get(results))
        }
        proto::NodeRequestBody::GetEvents { event_ids } => Ok(get_events(node, cdata, event_ids).await?),
        proto::NodeRequestBody::SubscribeQuery { query_id, selection, version, known_matches } => {
            // Catalog subscriptions carry no credential; others carry exactly one.
            let cdata = match cdata.iterable().at_most_one() {
                Ok(cdata) => cdata,
                Err(_) => {
                    return Err(anyhow!(
                        "SubscribeQuery currently requires at most one cdata (this server does not yet accept several per subscribe)"
                    ));
                }
            };
            connection.subscription_handler.subscribe_query(node, query_id, selection, cdata, version, known_matches).await
        }
    }
}
