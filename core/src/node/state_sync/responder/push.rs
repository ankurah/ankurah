//! A responder's pushes on the update channel: each repair becomes an ordinary
//! `SubscriptionUpdateItem`, sent on the connection the push began on in
//! updates bounded by count and size.

use super::{
    repair::{current_state_repairs, CurrentStateRepair},
    size::data_bytes,
};
use crate::{
    node::{Node, PeerState, WeakNode},
    policy::{ContextPolicy, PolicyAgent},
    session::ContextData,
    storage::StorageEngine,
    util::cancel_flag::CancelFlag,
};
use ankurah_proto::{self as proto, state_sync::limits::MAX_PAGE_HEADS};
use std::sync::{Arc, Weak};

#[cfg(test)]
mod tests;

/// About how much item data one update carries: a count alone would let large
/// entities build messages past a websocket's message limit. A responder's own
/// bound, not a protocol limit.
const MAX_UPDATE_BYTES: usize = 1024 * 1024;

/// The items a push sends one peer on the connection it began on, gathered into
/// updates of at most `MAX_PAGE_HEADS` items and about `MAX_UPDATE_BYTES` of item
/// data, at least one item each. The push ends with that connection: a peer that
/// reconnects compares afresh, and the old push's remaining items would reach it
/// beside the new push's. It also ends once `cancel` is raised.
pub(super) struct Push<CD: ContextData> {
    peer: proto::EntityId,
    connection: Weak<PeerState<CD>>,
    cancel: CancelFlag,
    relevance: Vec<(proto::QueryId, proto::MembershipChange)>,
    items: Vec<proto::SubscriptionUpdateItem>,
    bytes: usize,
}

impl<CD: ContextData> Push<CD> {
    /// A push to `peer`, on its current connection, whose items carry
    /// `relevance`: a subscription's initial tag, or none for data no standing
    /// query of the peer's asked for.
    pub(super) fn new<SE, PA>(
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        relevance: Vec<(proto::QueryId, proto::MembershipChange)>,
        cancel: CancelFlag,
    ) -> Self
    where
        PA: PolicyAgent<ContextData = CD>,
    {
        let connection = node.peer_connections.get(&peer).map(|connection| Arc::downgrade(&connection)).unwrap_or_default();
        Self { peer, connection, cancel, relevance, items: Vec::new(), bytes: 0 }
    }

    /// Whether the push has ended: its connection closed, or `cancel` was raised.
    fn stopped(&self) -> bool { self.cancel.canceled() || self.connection.strong_count() == 0 }

    /// Add the item that carries `content` for `entity_id`, first sending the
    /// gathered items when it would overfill their update.
    pub(super) fn add<SE, PA>(&mut self, node: &Node<SE, PA>, entity_id: proto::EntityId, content: proto::UpdateContent)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let bytes = data_bytes(&content);
        if !self.items.is_empty() && (self.items.len() == MAX_PAGE_HEADS || self.bytes + bytes > MAX_UPDATE_BYTES) {
            self.send(node);
        }
        self.items.push(proto::SubscriptionUpdateItem { entity_id, content, predicate_relevance: self.relevance.clone() });
        self.bytes += bytes;
    }

    /// Send the gathered items, if any, unless the push has ended.
    pub(super) fn send<SE, PA>(&mut self, node: &Node<SE, PA>)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        if self.items.is_empty() {
            return;
        }
        self.bytes = 0;
        let items = std::mem::take(&mut self.items);
        if self.cancel.canceled() {
            return;
        }
        if let Some(connection) = self.connection.upgrade() {
            node.send_update_on(&connection, self.peer, proto::NodeUpdateBody::SubscriptionUpdate { items });
        }
    }
}

/// Push `peer` each entity in `lacking` from the head listed beside it, from a
/// task of its own that reads a page of states at a time as it sends. The
/// requester learns of entities this node holds no readable state of, which
/// this skips, through its own repair.
pub(super) fn spawn_push<SE, PA>(
    node: &Node<SE, PA>,
    lacking: Vec<(proto::EntityId, Option<proto::Clock>)>,
    credentials: Vec<PA::ContextData>,
    catalog: bool,
    push: Push<PA::ContextData>,
) where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let node = node.weak();
    crate::task::spawn(async move {
        let peer = push.peer;
        if let Err(error) = push_lacking(node, lacking, credentials, catalog, push).await {
            tracing::warn!(%peer, %error, "stopped pushing a comparison's data");
        }
    });
}

async fn push_lacking<SE, PA>(
    node: WeakNode<SE, PA>,
    lacking: Vec<(proto::EntityId, Option<proto::Clock>)>,
    credentials: Vec<PA::ContextData>,
    catalog: bool,
    mut push: Push<PA::ContextData>,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    for page in lacking.chunks(MAX_PAGE_HEADS) {
        // A dropped node, a closed connection or a cancel stops the push between pages.
        let Some(node) = node.upgrade() else { return Ok(()) };
        if push.stopped() {
            return Ok(());
        }
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
        for ((entity_id, _), repair) in page.iter().zip(current_state_repairs(&node, &policy, page, catalog).await?) {
            if let CurrentStateRepair::Lacking { content, .. } = repair {
                push.add(&node, *entity_id, content);
            }
        }
        push.send(&node);
    }
    Ok(())
}
