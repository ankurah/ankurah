//! A responder's pushes on the update channel: each repair becomes an ordinary
//! `SubscriptionUpdateItem`, sent through `Node::send_update` in updates
//! bounded by count and size.

use super::repair::{current_state_repairs, CurrentStateRepair};
use crate::{
    node::{Node, WeakNode},
    policy::{ContextPolicy, PolicyAgent},
    storage::StorageEngine,
};
use ankurah_proto::{self as proto, state_sync::limits::MAX_PAGE_HEADS};

/// The item data one update carries, about: a count alone would let large
/// entities build messages past a websocket's message limit. A responder's own
/// bound, not a protocol limit.
const MAX_UPDATE_BYTES: usize = 1024 * 1024;

/// The items a push sends one peer, gathered into updates of at most
/// `MAX_PAGE_HEADS` items and about `MAX_UPDATE_BYTES` of item data, at least
/// one item each.
pub(super) struct Push {
    peer: proto::EntityId,
    relevance: Vec<(proto::QueryId, proto::MembershipChange)>,
    items: Vec<proto::SubscriptionUpdateItem>,
    bytes: usize,
}

impl Push {
    /// A push to `peer` whose items carry `relevance`: a subscription's initial
    /// tag, or none for data no standing query of the peer's asked for.
    pub(super) fn new(peer: proto::EntityId, relevance: Vec<(proto::QueryId, proto::MembershipChange)>) -> Self {
        Self { peer, relevance, items: Vec::new(), bytes: 0 }
    }

    /// Add the item that carries `delta`, first sending the gathered items when
    /// it would overfill their update.
    pub(super) fn add<SE, PA>(&mut self, node: &Node<SE, PA>, delta: proto::EntityDelta) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let content = match delta.content {
            proto::DeltaContent::StateSnapshot { state } => proto::UpdateContent::StateAndEvent(state, vec![]),
            proto::DeltaContent::EventBridge { events } => proto::UpdateContent::EventOnly(events),
            proto::DeltaContent::StateAndRelation { .. } => anyhow::bail!("a state-and-relation delta has no update content"),
        };
        let bytes = data_bytes(&content);
        let item = proto::SubscriptionUpdateItem { entity_id: delta.entity_id, content, predicate_relevance: self.relevance.clone() };
        if !self.items.is_empty() && (self.items.len() == MAX_PAGE_HEADS || self.bytes + bytes > MAX_UPDATE_BYTES) {
            self.send(node);
        }
        self.items.push(item);
        self.bytes += bytes;
        Ok(())
    }

    /// Send the gathered items, if any.
    pub(super) fn send<SE, PA>(&mut self, node: &Node<SE, PA>)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if self.items.is_empty() {
            return;
        }
        self.bytes = 0;
        node.send_update(self.peer, proto::NodeUpdateBody::SubscriptionUpdate { items: std::mem::take(&mut self.items) });
    }
}

/// The bytes of `content`'s own buffers: its state's buffers, its events'
/// operation diffs, and the attestations of both. These grow with an entity
/// while the rest of an item stays small, so they bound an update's size
/// without an encoding beyond the one the connector makes to send it.
fn data_bytes(content: &proto::UpdateContent) -> usize {
    let attestations = |set: &proto::AttestationSet| set.iter().map(|attestation| attestation.0.len()).sum::<usize>();
    let event = |event: &proto::EventFragment| {
        let (proto::EventBody::Genesis { operations, .. } | proto::EventBody::Update { operations, .. }) = &event.body;
        let diffs = operations.0.iter().map(|operation| match operation {
            proto::Operation::Backend { operations: diffs, .. } => diffs.iter().map(|diff| diff.diff.len()).sum(),
            proto::Operation::Membership(_) => 0,
        });
        diffs.sum::<usize>() + attestations(&event.attestations)
    };
    match content {
        proto::UpdateContent::EventOnly(events) => events.iter().map(event).sum(),
        proto::UpdateContent::StateAndEvent(state, events) => {
            let buffers = state.state.state_buffers.values().map(Vec::len).sum::<usize>();
            buffers + attestations(&state.attestations) + events.iter().map(event).sum::<usize>()
        }
    }
}

/// Push `peer` each entity in `lacking` from the head listed beside it, from a
/// task of its own that reads a page of states at a time as it sends. The
/// requester learns of entities the read policy denies, which this skips,
/// through its own repair.
pub(super) fn spawn_push<SE, PA>(
    node: &Node<SE, PA>,
    lacking: Vec<(proto::EntityId, Option<proto::Clock>)>,
    credentials: Vec<PA::ContextData>,
    catalog: bool,
    push: Push,
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
    mut push: Push,
) -> anyhow::Result<()>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    for page in lacking.chunks(MAX_PAGE_HEADS) {
        // A dropped node stops the push between pages.
        let Some(node) = node.upgrade() else { return Ok(()) };
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
        for repair in current_state_repairs(&node, &policy, page, &credentials, catalog).await? {
            if let CurrentStateRepair::Delta { delta, .. } = repair {
                push.add(&node, delta)?;
            }
        }
        push.send(&node);
    }
    Ok(())
}
