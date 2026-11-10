//! What a node does with a frame from a registered peer.

pub mod applier;
pub(crate) mod handler;
mod schema_registration;

use super::{ConnectionRegistration, Node};
use crate::error::RequestError;
use crate::internal::prelude::*;
use anyhow::anyhow;
use tracing::{debug, warn};

/// Handle a frame that arrived on the connection `registration` registered; see [`Node::handle_message`].
pub(super) async fn handle_message<SE, PA>(
    node: &Node<SE, PA>,
    registration: ConnectionRegistration,
    message: proto::NodeMessage,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node.lifecycle.check_not_halted()?;
    let sender_named = sender_of(&message);
    if sender_named != registration.peer() {
        warn!("Node({}) refusing a frame from peer {} that names {} as its sender", node.id, registration.peer(), sender_named);
        return Ok(());
    }
    let Some(connection) = node.peers.registered(&registration) else {
        warn!("Node({}) refusing a frame from peer {} on a connection no longer registered", node.id, registration.peer());
        return Ok(());
    };
    match message {
        proto::NodeMessage::Update(update) => {
            debug!("Node({}) received update {}", node.id, update);
            if update.to != node.id {
                warn!("{} received message from {} but is not the intended recipient", node.id, update.from);
                return Ok(());
            }

            // take down the return address
            let id = update.id.clone();
            let to = update.from;
            let from = node.id;

            let body = match update.body.decode() {
                Ok(body) => match handle_update(node, update.from, body).await {
                    Ok(_) => proto::NodeUpdateAckBody::Success,
                    Err(e) => proto::NodeUpdateAckBody::Error(e.to_string()),
                },
                Err(unreadable) => {
                    debug!("Node({}) cannot read update {} from {}: {}", node.id, id, update.from, unreadable);
                    proto::NodeUpdateAckBody::Unsupported
                }
            };

            connection.send_message(proto::NodeMessage::UpdateAck(proto::NodeUpdateAck { id, from, to, body }))?;
        }
        proto::NodeMessage::UpdateAck(ack) => {
            debug!("Node({}) received ack notification {} {}", node.id, ack.id, ack.body);
            // let connection = node.peers.connection(&ack.from).ok_or(RequestError::PeerNotConnected)?;
            // if let Some(tx) = connection.pending_updates.remove(&ack.id) {
            //     tx.send(Ok(proto::NodeResponseBody::Success)).unwrap();
            // }
        }
        proto::NodeMessage::Request { auth, request } => {
            debug!("Node({}) received request {}", node.id, request);
            // TODO: Should we spawn a task here and make handle_message synchronous?
            // I think this depends on how we want to handle timeouts.
            // I think we want timeouts to be handled by the node, not the connector,
            // which would lend itself to spawning a task here and making this function synchronous.

            let from = request.from;
            let request_id = request.id.clone();
            if request.to != node.id {
                warn!("{} received message from {} but is not the intended recipient", node.id, request.from);
                return Ok(());
            }

            let body = match request.body.decode() {
                Ok(body) => {
                    // Temporary credentialless catalog reads. Remove this bypass when the #425 catalog
                    // follow-on uses #426's node session union for catalog queries.
                    let cdata = if crate::schema::request_bypasses_policy(&body) {
                        Ok(Vec::new())
                    } else {
                        node.policy_agent.check_request(node, &auth, &request).await
                    };
                    match cdata {
                        Ok(cdata) => match handler::handle_request(node, &cdata, from, body).await {
                            Ok(result) => result,
                            Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                        },
                        Err(e) => proto::NodeResponseBody::Error(e.to_string()),
                    }
                }
                Err(unreadable) => {
                    debug!("Node({}) cannot read request {} from {}: {}", node.id, request_id, from, unreadable);
                    proto::NodeResponseBody::Unsupported
                }
            };
            let _result = connection.send_message(proto::NodeMessage::Response(proto::NodeResponse {
                request_id,
                from: node.id,
                to: from,
                body: proto::CarriedBody::encode(&body),
            }));
        }
        proto::NodeMessage::Response(response) => {
            debug!("Node {} received response {}", node.id, response);
            if let Some(tx) = connection.pending_requests.remove(&response.request_id) {
                let body = match response.body.decode() {
                    Ok(proto::NodeResponseBody::Unsupported) => Err(RequestError::UnsupportedByPeer(response.from)),
                    Ok(body) => Ok(body),
                    Err(unreadable) => Err(RequestError::UnreadableResponse(unreadable)),
                };
                tx.send(body).map_err(|e| anyhow!("Failed to send response: {:?}", e))?;
            }
        }
        proto::NodeMessage::UnsubscribeQuery { query_id, .. } => {
            // Remove predicate from the peer's subscription
            connection.subscription_handler.remove_predicate(query_id).await?;
        }
    }
    Ok(())
}

async fn handle_update<SE, PA>(node: &Node<SE, PA>, from: proto::NodeId, body: proto::NodeUpdateBody) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    match body {
        proto::NodeUpdateBody::SubscriptionUpdate { items } => {
            tracing::debug!("Node({}) received subscription update from peer {}", node.id, from);
            applier::NodeApplier::apply_updates(node, &from, items).await?;
            Ok(())
        }
    }
}

/// The node a frame names as its sender.
fn sender_of(message: &proto::NodeMessage) -> proto::NodeId {
    match message {
        proto::NodeMessage::Request { request, .. } => request.from,
        proto::NodeMessage::Response(response) => response.from,
        proto::NodeMessage::Update(update) => update.from,
        proto::NodeMessage::UpdateAck(ack) => ack.from,
        proto::NodeMessage::UnsubscribeQuery { from, .. } => *from,
    }
}
