//! Hold live updates until initial state has been applied and its query installed.
use crate::{node::Node, policy::PolicyAgent, storage::StorageEngine};
use ankurah_proto as proto;
use std::collections::{HashMap, VecDeque};

#[derive(Default)]
pub(super) struct Delivery {
    pending: HashMap<proto::QueryId, (u32, Option<proto::ComparisonId>)>,
    updates: VecDeque<proto::NodeUpdateBody>,
    bytes: usize,
}

impl Delivery {
    pub fn begin(&mut self, query: proto::QueryId, version: u32) { self.pending.insert(query, (version, None)); }
    pub fn bind(&mut self, query: proto::QueryId, version: u32, comparison: proto::ComparisonId) -> bool {
        let Some(pending) = self.pending.get_mut(&query).filter(|pending| pending.0 == version) else { return false };
        pending.1 = Some(comparison);
        true
    }
    pub fn matches(&self, query: proto::QueryId, version: u32, comparison: proto::ComparisonId) -> bool {
        self.pending.get(&query) == Some(&(version, Some(comparison)))
    }
    pub fn send<SE, PA>(&mut self, node: &Node<SE, PA>, peer: proto::EntityId, update: proto::NodeUpdateBody) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if self.pending.is_empty() {
            node.send_update(peer, update);
        } else {
            let size = bincode::serialized_size(&update)? as usize;
            if self.bytes.saturating_add(size) > 8 * 1024 * 1024 {
                // Resume live delivery without dropping events. Every interrupted
                // comparison must restart, even if its final reply was in flight.
                if let Some(connection) = node.peer_connections.get(&peer) {
                    for query in self.pending.keys() {
                        connection.comparisons.invalidate_query(*query);
                    }
                }
                self.pending.clear();
                self.updates.push_back(update);
                self.flush(node, peer);
            } else {
                self.bytes += size;
                self.updates.push_back(update);
            }
        }
        Ok(())
    }
    pub fn release<SE, PA>(&mut self, node: &Node<SE, PA>, peer: proto::EntityId, query: proto::QueryId)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        self.pending.remove(&query);
        if self.pending.is_empty() {
            self.flush(node, peer);
        }
    }
    fn flush<SE, PA>(&mut self, node: &Node<SE, PA>, peer: proto::EntityId)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // A single batch avoids flooding the connector queue on handoff. Keep
        // every connecting event with the latest captured state of each entity.
        let mut combined: indexmap::IndexMap<proto::EntityId, proto::SubscriptionUpdateItem> = indexmap::IndexMap::new();
        while let Some(proto::NodeUpdateBody::SubscriptionUpdate { items }) = self.updates.pop_front() {
            for mut item in items {
                if let Some(previous) = combined.get_mut(&item.entity_id) {
                    match (&mut previous.content, &mut item.content) {
                        (proto::UpdateContent::StateAndEvent(state, events), proto::UpdateContent::StateAndEvent(next, more)) => {
                            *state = next.clone();
                            events.append(more);
                        }
                        // SubscriptionHandler currently freezes every update as
                        // StateAndEvent; event-only updates can still append safely.
                        (proto::UpdateContent::StateAndEvent(_, events), proto::UpdateContent::EventOnly(more)) => events.append(more),
                        (proto::UpdateContent::EventOnly(events), proto::UpdateContent::EventOnly(more)) => events.append(more),
                        (proto::UpdateContent::EventOnly(events), proto::UpdateContent::StateAndEvent(state, more)) => {
                            events.append(more);
                            previous.content = proto::UpdateContent::StateAndEvent(state.clone(), std::mem::take(events));
                        }
                    }
                    previous.predicate_relevance.append(&mut item.predicate_relevance);
                } else {
                    combined.insert(item.entity_id, item);
                }
            }
        }
        self.bytes = 0;
        if !combined.is_empty() {
            node.send_update(peer, proto::NodeUpdateBody::SubscriptionUpdate { items: combined.into_values().collect() });
        }
    }
}
