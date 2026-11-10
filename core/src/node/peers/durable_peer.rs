//! The work a node asks a durable peer to do.

use crate::internal::prelude::*;
use crate::node::{inbound::applier, Node};
use crate::retrieval::{CachedEventGetter, LocalStateGetter};
use ankql::ast::Resolved;
use ankurah_proto::Attested;
use tracing::debug;

pub(crate) async fn relay_to_required_peers<SE, PA>(
    node: &Node<SE, PA>,
    cdata: &PA::ContextData,
    id: proto::TransactionId,
    events: &[Attested<proto::Event>],
) -> Result<(), MutationError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    // TODO determine how many durable peers need to respond before we can proceed. The others should continue in the background.
    // as of this writing, we only have one durable peer, so we can just await the response from "all" of them
    for peer_id in node.get_durable_peers() {
        let body = proto::NodeRequestBody::CommitTransaction { id: id.clone(), events: events.to_vec() };
        match node.request_durable(peer_id, cdata, body).await {
            Ok(proto::NodeResponseBody::CommitComplete { .. }) => (),
            Err(error) => return Err(RetrievalError::from(error).into()),
            Ok(proto::NodeResponseBody::Error(e)) => {
                return Err(MutationError::General(Box::new(std::io::Error::other(format!("Peer {} rejected: {}", peer_id, e)))));
            }
            _ => {
                return Err(MutationError::General(Box::new(std::io::Error::other(format!(
                    "Peer {} returned unexpected response",
                    peer_id
                )))));
            }
        }
    }
    Ok(())
}

/// Retrieve peer states and persist them in this node.
pub(crate) async fn get_from_peer<SE, PA>(
    node: &Node<SE, PA>,
    ids: Vec<proto::EntityId>,
    cdata: &Vec<PA::ContextData>,
) -> Result<(), RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node.lifecycle.check_not_halted()?;
    let peer_id = node.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

    match node.request_durable(peer_id, cdata, proto::NodeRequestBody::Get { ids: ids.clone() }).await? {
        proto::NodeResponseBody::Get(results) => {
            if results.len() != ids.len() || results.iter().zip(&ids).any(|(result, id)| result.entity_id() != *id) {
                return Err(RetrievalError::Other("Peer Get did not answer the requested identities".into()));
            }
            let deltas = results.into_iter().map(proto::EntityDelta::try_from).collect::<Result<Vec<_>, _>>()?;
            let event_getter = CachedEventGetter::new(node, cdata);
            applier::NodeApplier::apply_deltas(node, &peer_id, deltas, &event_getter, node.storage.as_ref()).await?;
            Ok(())
        }
        proto::NodeResponseBody::Error(e) => {
            debug!("Error from peer fetch: {}", e);
            Err(RetrievalError::Other(format!("{:?}", e)))
        }
        _ => {
            debug!("Unexpected response type from peer get");
            Err(RetrievalError::Other("Unexpected response type".to_string()))
        }
    }
}

/// Fetch matching entities from a durable peer using local heads as known matches.
/// Apply its deltas locally before returning.
pub(crate) async fn fetch_from_peer<SE, PA>(
    node: &Node<SE, PA>,
    selection: ankql::ast::Selection<Resolved>,
    cdata: &Vec<PA::ContextData>,
) -> Result<Vec<Entity>, RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node.lifecycle.check_not_halted()?;
    let peer_id = node.get_durable_peer_random().ok_or(RetrievalError::NoDurablePeers)?;

    let known_matched_entities = node.fetch_entities_from_local(&selection).await?;

    let known_matches =
        known_matched_entities.iter().map(|entity| proto::KnownEntity { entity_id: entity.id(), head: entity.head().clone() }).collect();

    let selection_clone = selection.clone();
    match node.request_durable(peer_id, cdata, proto::NodeRequestBody::Fetch { selection, known_matches }).await? {
        proto::NodeResponseBody::Fetch(deltas) => {
            let event_getter = CachedEventGetter::new(node, cdata);
            let state_getter = LocalStateGetter::new(node.storage.clone());

            applier::NodeApplier::apply_deltas(node, &peer_id, deltas, &event_getter, &state_getter).await?;
            // ARCHITECTURAL QUESTION: Optimize in-place mutation vs re-fetching for remote-peer-assisted operations https://github.com/ankurah/ankurah/issues/145

            node.fetch_entities_from_local(&selection_clone).await
        }
        proto::NodeResponseBody::Error(e) => {
            debug!("Error from peer fetch: {}", e);
            Err(RetrievalError::Other(format!("{:?}", e)))
        }
        _ => {
            debug!("Unexpected response type from peer fetch");
            Err(RetrievalError::Other("Unexpected response type".to_string()))
        }
    }
}
