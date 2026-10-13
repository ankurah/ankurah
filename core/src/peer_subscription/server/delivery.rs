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
            crate::node::state_sync::queue_live(node, peer, comparison, vec![item])
                .map_err(|error| error.context(LiveStreamFailure(comparison)))?;
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
        self.route_updates(items, |comparison, items| match crate::node::state_sync::queue_live(node, peer, comparison, items) {
            Ok(()) => Ok(()),
            Err(error) => {
                if node.peer_connections.get(&peer).is_some_and(|peer| peer.comparisons.get(&comparison).is_some()) {
                    Err(error)
                } else {
                    // Retain the binding for exact-generation cleanup by its
                    // owner; removing it here would hide the standing query.
                    Ok(())
                }
            }
        })
    }

    fn route_updates(
        &mut self,
        items: Vec<proto::SubscriptionUpdateItem>,
        mut queue: impl FnMut(proto::ComparisonId, Vec<proto::SubscriptionUpdateItem>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut failed_queries = HashSet::new();
        let mut stream_failure = None;
        let mut pending_failure = None;
        for item in items {
            for (query, route) in &mut self.routes {
                if failed_queries.contains(query) {
                    continue;
                }
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
                        if let Err(error) = queue(comparison, vec![item]) {
                            failed_queries.insert(*query);
                            stream_failure.get_or_insert_with(|| error.context(LiveStreamFailure(comparison)));
                        }
                    }
                } else {
                    let result: anyhow::Result<()> = (|| {
                        let size = usize::try_from(bincode::serialized_size(&item)?)?;
                        anyhow::ensure!(
                            self.bytes.saturating_add(size) <= MAX_PENDING_BYTES,
                            "subscription initialization live queue overflow"
                        );
                        self.bytes += size;
                        route.bytes += size;
                        route.pending.push_back((item, size));
                        Ok(())
                    })();
                    if let Err(error) = result {
                        failed_queries.insert(*query);
                        pending_failure.get_or_insert(error);
                    }
                }
            }
        }
        // Every healthy bound route receives the whole batch. Initialization
        // failures still require disconnect and must not be hidden by a stopped stream.
        pending_failure.or(stream_failure).map_or(Ok(()), Err)
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
    use crate::{
        connector::{PeerSender, SendError},
        policy::PermissiveAgent,
        test_utils::TestStorage,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct CaptureSender {
        peer: proto::EntityId,
        messages: Arc<Mutex<Vec<proto::NodeMessage>>>,
    }
    impl PeerSender for CaptureSender {
        fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
            self.messages.lock().unwrap().push(message);
            Ok(())
        }
        fn recipient_node_id(&self) -> proto::EntityId { self.peer }
        fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
    }

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

    #[test]
    fn failed_bound_stream_does_not_skip_other_streams_or_later_items() -> anyhow::Result<()> {
        let failed_query = proto::QueryId::new();
        let healthy_query = proto::QueryId::new();
        let failed = proto::ComparisonId::new();
        let healthy = proto::ComparisonId::new();
        let failed_entity = proto::EntityId::random();
        let healthy_entity = proto::EntityId::random();
        let mut delivery = Delivery::default();
        delivery.begin(failed_query, 1);
        delivery.bind_route(failed_query, 1, failed, [failed_entity]);
        delivery.begin(healthy_query, 1);
        delivery.bind_route(healthy_query, 1, healthy, [healthy_entity]);
        let changes = vec![item(healthy_entity, vec![]), item(healthy_entity, vec![])];
        let mut batch = vec![item(failed_entity, vec![])];
        batch.extend(changes.clone());
        let mut received = Vec::new();
        let failure = delivery
            .route_updates(batch, |stream, items| {
                if stream == failed {
                    anyhow::bail!("stopped stream");
                }
                assert_eq!(stream, healthy);
                received.extend(items);
                Ok(())
            })
            .unwrap_err();
        assert_eq!(failure.downcast_ref::<LiveStreamFailure>().unwrap().0, failed);
        assert_eq!(bincode::serialize(&received)?, bincode::serialize(&changes)?);
        assert!(delivery.matches(healthy_query, 1, healthy));
        Ok(())
    }

    #[test]
    fn failed_stream_does_not_hide_initialization_overflow() {
        let failed_query = proto::QueryId::new();
        let initializing_query = proto::QueryId::new();
        let failed = proto::ComparisonId::new();
        let entity = proto::EntityId::random();
        let mut delivery = Delivery::default();
        delivery.begin(failed_query, 1);
        delivery.bind_route(failed_query, 1, failed, [entity]);
        delivery.begin(initializing_query, 1);
        let mut oversized = item(proto::EntityId::random(), vec![]);
        let proto::DeltaContent::StateAndEvents { state, .. } = &mut oversized.delta.content else { unreachable!() };
        state.state.state_buffers.0.insert("large".to_owned(), vec![0; MAX_PENDING_BYTES]);
        let error = delivery.route_updates(vec![item(entity, vec![]), oversized], |_, _| anyhow::bail!("stopped stream")).unwrap_err();
        assert!(error.downcast_ref::<LiveStreamFailure>().is_none(), "initialization failure must still require connection recovery");
        assert!(error.to_string().contains("initialization live queue overflow"));
    }

    #[tokio::test]
    async fn removed_last_route_drops_late_notifications() -> anyhow::Result<()> {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let peer = proto::EntityId::random();
        let messages = Arc::new(Mutex::new(Vec::new()));
        node.register_peer(
            proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
            Box::new(CaptureSender { peer, messages: messages.clone() }),
        )
        .await?;
        let query = proto::QueryId::new();
        let entity = proto::EntityId::random();
        let mut delivery = Delivery::default();
        delivery.begin(query, 1);
        assert!(delivery.bind_route(query, 1, proto::ComparisonId::new(), [entity]));
        delivery.remove(query);
        messages.lock().unwrap().clear();

        delivery.send(&node, peer, vec![item(entity, vec![(query, proto::MembershipChange::Remove)])])?;
        assert!(messages.lock().unwrap().is_empty(), "late notifications must not escape on an unsequenced stream");
        assert_eq!(delivery.bytes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn removed_comparison_route_is_stale_and_healthy_stream_still_receives() -> anyhow::Result<()> {
        let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
        let peer = proto::EntityId::random();
        let messages = Arc::new(Mutex::new(Vec::new()));
        node.register_peer(
            proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION },
            Box::new(CaptureSender { peer, messages: messages.clone() }),
        )
        .await?;
        let stale_query = proto::QueryId::new();
        let healthy_query = proto::QueryId::new();
        let stale_entity = proto::EntityId::random();
        let healthy_entity = proto::EntityId::random();
        let mut delivery = Delivery::default();
        let mut streams = Vec::new();
        for (query, entity) in [(stale_query, stale_entity), (healthy_query, healthy_entity)] {
            let spec = proto::TreeSpec::new(ankurah_core_types::KeySpec::new(vec![]), ankql::ast::Predicate::True);
            let snapshot = crate::node::state_sync::QueryTree::default().refresh(spec, vec![]).await?;
            let initial = crate::node::state_sync::start_comparison(
                &node,
                peer,
                proto::RequestId::new(),
                vec![],
                vec![],
                Some((query, 1)),
                false,
                snapshot,
                None,
            )
            .await?;
            delivery.begin(query, 1);
            assert!(delivery.bind_route(query, 1, initial.frame.comparison, [entity]));
            streams.push(initial.frame.comparison);
        }
        // Reproduce the cleanup window: comparison is gone, route is still bound.
        crate::node::state_sync::close_state(&node, peer, streams[0], proto::ComparisonOutcome::Abandoned, vec![]).await?;
        crate::node::state_sync::close_state(&node, peer, streams[1], proto::ComparisonOutcome::Applied { processed_through: 0 }, vec![])
            .await?;
        delivery.send(&node, peer, vec![item(stale_entity, vec![]), item(healthy_entity, vec![])])?;
        assert!(node.peer_connections.get(&peer).is_some(), "retiring one stream must preserve the peer");
        assert!(delivery.matches(stale_query, 1, streams[0]), "retain the binding for its owner's standing-query cleanup");
        assert!(delivery.matches(healthy_query, 1, streams[1]));
        crate::node::state_sync::pump_comparison(&node, peer, streams[1]).await?;
        assert!(messages.lock().unwrap().iter().any(|message| matches!(
            message,
            proto::NodeMessage::Update(proto::NodeUpdate {
                body: proto::NodeUpdateBody::StateFrame {
                    frame: proto::StateFrame { comparison, body: proto::StateFrameBody::Live(items), .. }, ..
                }, ..
            }) if *comparison == streams[1] && items.iter().any(|item| item.delta.entity_id == healthy_entity)
        )));
        let replacement = proto::ComparisonId::new();
        delivery.begin(stale_query, 2);
        assert!(delivery.bind_route(stale_query, 2, replacement, []));
        assert!(!delivery.matches(stale_query, 1, streams[0]), "old cleanup must not match the replacement");
        assert!(delivery.matches(stale_query, 2, replacement));
        Ok(())
    }
}
