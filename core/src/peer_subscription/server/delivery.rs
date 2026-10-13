//! Keep each query generation on its comparison's numbered live stream.
use crate::{node::Node, policy::PolicyAgent, storage::StorageEngine};
use ankurah_proto as proto;
use std::collections::{HashMap, HashSet, VecDeque};

const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("live stream {0} rejected its queued update")]
pub(super) struct LiveStreamFailure(pub proto::ComparisonId);

#[derive(Default)]
pub(super) struct Delivery {
    routes: HashMap<proto::QueryId, Route>,
    bytes: usize,
}

struct Route {
    version: u32,
    comparison: Option<proto::ComparisonId>,
    members: HashSet<proto::EntityId>,
    pending: VecDeque<(proto::SubscriptionUpdateItem, usize)>,
    bytes: usize,
}

impl Route {
    fn update_members(&mut self, item: &proto::SubscriptionUpdateItem) {
        for (_, change) in &item.predicate_relevance {
            match change {
                proto::MembershipChange::Initial | proto::MembershipChange::Add => {
                    self.members.insert(item.delta.entity_id);
                }
                proto::MembershipChange::Remove => {
                    self.members.remove(&item.delta.entity_id);
                }
            }
        }
    }

    fn admits(&mut self, item: &proto::SubscriptionUpdateItem) -> bool {
        let was_member = self.members.contains(&item.delta.entity_id);
        self.update_members(item);
        was_member || self.members.contains(&item.delta.entity_id) || !item.predicate_relevance.is_empty()
    }
}

impl Delivery {
    pub fn begin(&mut self, query: proto::QueryId, version: u32) {
        self.remove(query);
        self.routes.insert(query, Route { version, comparison: None, members: HashSet::new(), pending: VecDeque::new(), bytes: 0 });
    }

    pub fn bind<SE, PA>(
        &mut self,
        query: proto::QueryId,
        version: u32,
        comparison: proto::ComparisonId,
        members: impl IntoIterator<Item = proto::EntityId>,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
    ) -> anyhow::Result<bool>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if !self.bind_route(query, version, comparison, members) {
            return Ok(false);
        }
        // The comparison owns bounded framing and holds emission until Applied.
        // Preserve every connecting event batch instead of coalescing snapshots.
        while let Some(item) = self.next_pending(query) {
            node.queue_live(peer, comparison, vec![item]).map_err(|error| error.context(LiveStreamFailure(comparison)))?;
        }
        Ok(true)
    }

    fn bind_route(
        &mut self,
        query: proto::QueryId,
        version: u32,
        comparison: proto::ComparisonId,
        members: impl IntoIterator<Item = proto::EntityId>,
    ) -> bool {
        let Some(route) = self.routes.get_mut(&query).filter(|route| route.version == version && route.comparison.is_none()) else {
            return false;
        };
        route.comparison = Some(comparison);
        route.members = members.into_iter().collect();
        // Capture may occur between queued notifications. First derive the
        // final membership, then rewind to the beginning of that queue.
        for (item, _) in &route.pending {
            for (_, change) in &item.predicate_relevance {
                match change {
                    proto::MembershipChange::Initial | proto::MembershipChange::Add => {
                        route.members.insert(item.delta.entity_id);
                    }
                    proto::MembershipChange::Remove => {
                        route.members.remove(&item.delta.entity_id);
                    }
                }
            }
        }
        for (item, _) in route.pending.iter().rev() {
            for (_, change) in item.predicate_relevance.iter().rev() {
                match change {
                    proto::MembershipChange::Initial | proto::MembershipChange::Add => {
                        route.members.remove(&item.delta.entity_id);
                    }
                    proto::MembershipChange::Remove => {
                        route.members.insert(item.delta.entity_id);
                    }
                }
            }
        }
        true
    }

    fn next_pending(&mut self, query: proto::QueryId) -> Option<proto::SubscriptionUpdateItem> {
        let route = self.routes.get_mut(&query)?;
        while let Some((item, size)) = route.pending.pop_front() {
            route.bytes -= size;
            self.bytes -= size;
            if route.admits(&item) {
                return Some(item);
            }
        }
        None
    }

    pub fn matches(&self, query: proto::QueryId, version: u32, comparison: proto::ComparisonId) -> bool {
        self.routes.get(&query).is_some_and(|route| route.version == version && route.comparison == Some(comparison))
    }

    pub fn send<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        items: Vec<proto::SubscriptionUpdateItem>,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        if self.routes.is_empty() {
            node.send_update(peer, proto::NodeUpdateBody::SubscriptionUpdate { items });
            return Ok(());
        }
        self.route_updates(items, |comparison, items| {
            node.queue_live(peer, comparison, items).map_err(|error| error.context(LiveStreamFailure(comparison)))
        })
    }

    fn route_updates(
        &mut self,
        items: Vec<proto::SubscriptionUpdateItem>,
        mut queue: impl FnMut(proto::ComparisonId, Vec<proto::SubscriptionUpdateItem>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        for item in items {
            for (query, route) in &mut self.routes {
                if route.comparison.is_some()
                    && !route.members.contains(&item.delta.entity_id)
                    && !item.predicate_relevance.iter().any(|(id, _)| id == query)
                {
                    continue;
                }
                let mut item = item.clone();
                item.predicate_relevance.retain(|(id, _)| id == query);
                if let Some(comparison) = route.comparison {
                    if route.admits(&item) {
                        queue(comparison, vec![item])?;
                    }
                } else {
                    let size = usize::try_from(bincode::serialized_size(&item)?)?;
                    anyhow::ensure!(
                        self.bytes.saturating_add(size) <= MAX_PENDING_BYTES,
                        "subscription initialization live queue overflow"
                    );
                    self.bytes += size;
                    route.bytes += size;
                    route.pending.push_back((item, size));
                }
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, query: proto::QueryId) {
        if let Some(route) = self.routes.remove(&query) {
            self.bytes -= route.bytes;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(entity: proto::EntityId, relevance: Vec<(proto::QueryId, proto::MembershipChange)>) -> proto::SubscriptionUpdateItem {
        let event = proto::Event::update(entity, proto::Clock::default(), proto::AuthorId::Unknown, proto::OperationSet::default());
        proto::SubscriptionUpdateItem {
            delta: proto::EntityDelta {
                entity_id: entity,
                content: proto::DeltaContent::StateAndEvents {
                    state: proto::StateFragment { state: Default::default(), attestations: Default::default() },
                    events: vec![proto::Attested::opt(event, None).into()],
                },
            },
            predicate_relevance: relevance,
        }
    }

    #[test]
    fn initializing_query_does_not_gate_bound_query_and_relevance_is_partitioned() -> anyhow::Result<()> {
        let warm = proto::QueryId::new();
        let cold = proto::QueryId::new();
        let warm_stream = proto::ComparisonId::new();
        let cold_stream = proto::ComparisonId::new();
        let entity = proto::EntityId::random();
        let mut delivery = Delivery::default();
        delivery.begin(warm, 1);
        assert!(delivery.bind_route(warm, 1, warm_stream, [entity]));
        delivery.begin(cold, 1);
        let mut sent = Vec::new();
        delivery.route_updates(
            vec![item(entity, vec![(warm, proto::MembershipChange::Remove), (cold, proto::MembershipChange::Add)])],
            |stream, items| {
                sent.push((stream, items));
                Ok(())
            },
        )?;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, warm_stream);
        assert_eq!(sent[0].1[0].predicate_relevance, vec![(warm, proto::MembershipChange::Remove)]);
        assert!(delivery.bytes > 0);
        assert!(delivery.bind_route(cold, 1, cold_stream, [entity]));
        let pending = delivery.next_pending(cold).unwrap();
        assert_eq!(pending.predicate_relevance, vec![(cold, proto::MembershipChange::Add)]);
        assert!(delivery.next_pending(cold).is_none());
        assert_eq!(delivery.bytes, 0);
        assert!(delivery.matches(warm, 1, warm_stream));
        assert!(delivery.matches(cold, 1, cold_stream));
        sent.clear();
        delivery.route_updates(vec![item(entity, vec![])], |stream, items| {
            sent.push((stream, items));
            Ok(())
        })?;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, cold_stream, "ordinary changes follow current membership even without relevance");
        Ok(())
    }

    #[test]
    fn removal_during_capture_preserves_all_connecting_events() -> anyhow::Result<()> {
        let query = proto::QueryId::new();
        let stream = proto::ComparisonId::new();
        let entity = proto::EntityId::random();
        let first = item(entity, vec![]);
        let remove = item(entity, vec![(query, proto::MembershipChange::Remove)]);
        let outsider = item(proto::EntityId::random(), vec![]);
        let mut delivery = Delivery::default();
        delivery.begin(query, 1);
        delivery.route_updates(vec![first.clone(), remove.clone(), outsider], |_, _| panic!("unbound updates must wait"))?;
        assert!(delivery.bind_route(query, 1, stream, []));
        let received = [delivery.next_pending(query).unwrap(), delivery.next_pending(query).unwrap()];
        assert_eq!(bincode::serialize(&received)?, bincode::serialize(&[first, remove])?);
        assert!(delivery.next_pending(query).is_none());
        assert_eq!(delivery.bytes, 0);
        assert!(delivery.routes[&query].members.is_empty());
        Ok(())
    }

    #[test]
    fn replacement_discards_only_that_query_generation() -> anyhow::Result<()> {
        let query = proto::QueryId::new();
        let other = proto::QueryId::new();
        let old = proto::ComparisonId::new();
        let active = proto::ComparisonId::new();
        let mut delivery = Delivery::default();
        delivery.begin(other, 1);
        assert!(delivery.bind_route(other, 1, active, []));
        delivery.begin(query, 1);
        delivery.route_updates(vec![item(proto::EntityId::random(), vec![])], |_, _| Ok(()))?;
        assert!(delivery.bytes > 0);
        delivery.begin(query, 2);
        assert_eq!(delivery.bytes, 0);
        assert!(!delivery.bind_route(query, 1, old, []));
        let replacement = proto::ComparisonId::new();
        assert!(delivery.bind_route(query, 2, replacement, []));
        assert!(!delivery.matches(query, 1, old));
        assert!(delivery.matches(query, 2, replacement));
        delivery.remove(query);
        assert!(delivery.matches(other, 1, active));
        Ok(())
    }

    #[test]
    fn pending_overflow_fails_without_releasing_or_flushing_routes() {
        let query = proto::QueryId::new();
        let mut delivery = Delivery::default();
        delivery.begin(query, 1);
        let mut oversized = item(proto::EntityId::random(), vec![]);
        let proto::DeltaContent::StateAndEvents { state, .. } = &mut oversized.delta.content else { unreachable!() };
        state.state.state_buffers.0.insert("large".to_owned(), vec![0; MAX_PENDING_BYTES]);
        assert!(delivery.route_updates(vec![oversized], |_, _| panic!("overflow must never flush unsequenced updates")).is_err());
        assert!(delivery.routes.contains_key(&query));
        assert!(delivery.routes[&query].comparison.is_none());
        assert_eq!(delivery.bytes, 0);
    }
}
