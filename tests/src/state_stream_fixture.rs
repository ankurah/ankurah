//! A malicious peer on a real registered query stream. Only the transport is
//! scripted: admission, frame ordering, entity application and failure remain
//! production receiver behavior.
use ankurah::{
    core::connector::{PeerSender, SendError},
    proto, LiveQuery, Node, PermissiveAgent, View,
};
use ankurah_storage_sled::SledStorageEngine;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
type TestNode = Node<SledStorageEngine, PermissiveAgent>;

#[derive(Clone)]
struct Sender(mpsc::Sender<proto::NodeMessage>, proto::EntityId);
#[async_trait::async_trait]
impl PeerSender for Sender {
    fn recipient_node_id(&self) -> proto::EntityId { self.1 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.0.try_send(message).map_err(|_| SendError::ConnectionClosed)
    }
}
#[derive(Clone)]
struct Stream {
    request: proto::RequestId,
    comparison: proto::StateStreamId,
    query: proto::QueryId,
    next_sequence: u64,
    next_revision: u64,
    applied: Option<u64>,
    scripted: bool,
    abandoned: bool,
}
type Streams = Arc<Mutex<HashMap<proto::StateStreamId, Stream>>>;

pub struct ScriptedConnection {
    source: TestNode,
    client: TestNode,
    streams: Streams,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl ScriptedConnection {
    pub async fn new(source: &TestNode, client: &TestNode) -> anyhow::Result<Self> {
        let (source_tx, mut source_rx) = mpsc::channel(1024);
        let (client_tx, mut client_rx) = mpsc::channel(1024);
        client.register_peer(source.presence().await?, Box::new(Sender(source_tx, source.id))).await?;
        source.register_peer(client.presence().await?, Box::new(Sender(client_tx, client.id))).await?;
        let streams: Streams = Arc::new(Mutex::new(HashMap::new()));
        let (server, receiver, routes) = (source.clone(), client.clone(), streams.clone());
        let source_task = tokio::spawn(async move {
            while let Some(message) = source_rx.recv().await {
                let response = if let proto::NodeMessage::Request { request, .. } = &message {
                    let comparison = match &request.body {
                        proto::NodeRequestBody::ContinueState { comparison, .. }
                        | proto::NodeRequestBody::CloseComparison { comparison, .. } => Some(*comparison),
                        _ => None,
                    };
                    let mut routes = routes.lock().unwrap();
                    comparison.and_then(|id| routes.get_mut(&id)).filter(|stream| stream.scripted).map(|stream| {
                        if matches!(
                            request.body,
                            proto::NodeRequestBody::CloseComparison { outcome: proto::ComparisonOutcome::Abandoned, .. }
                        ) {
                            stream.abandoned = true;
                        }
                        if let proto::NodeRequestBody::ContinueState { applied_through: Some(revision), .. } = request.body {
                            stream.applied = Some(revision);
                        }
                        proto::NodeMessage::Response(proto::NodeResponse {
                            request_id: request.id.clone(),
                            from: request.to,
                            to: request.from,
                            body: proto::NodeResponseBody::Success,
                        })
                    })
                } else {
                    None
                };
                let node = if response.is_some() { receiver.clone() } else { server.clone() };
                tokio::spawn(async move {
                    let _ = node.handle_message(response.unwrap_or(message)).await;
                });
            }
        });
        let (receiver, routes) = (client.clone(), streams.clone());
        let client_task = tokio::spawn(async move {
            while let Some(message) = client_rx.recv().await {
                let discard = {
                    let mut routes = routes.lock().unwrap();
                    match &message {
                        proto::NodeMessage::Response(response) => {
                            if let proto::NodeResponseBody::QuerySubscribed { query_id, state } = &response.body {
                                assert!(state.spec.key_spec().keyparts.is_empty(), "script fixture requires the entity-ID query key");
                                routes.insert(
                                    state.frame.comparison,
                                    Stream {
                                        request: response.request_id.clone(),
                                        comparison: state.frame.comparison,
                                        query: *query_id,
                                        next_sequence: state.frame.sequence + 1,
                                        next_revision: state.round.revision + 1,
                                        applied: None,
                                        scripted: false,
                                        abandoned: false,
                                    },
                                );
                            }
                            false
                        }
                        proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. }) => {
                            if let Some(stream) = routes.get_mut(&frame.comparison) {
                                if stream.scripted {
                                    true
                                } else {
                                    stream.next_sequence = stream.next_sequence.max(frame.sequence + 1);
                                    if let proto::StateFrameBody::Live { revision, .. }
                                    | proto::StateFrameBody::LiveChunk { revision, .. } = frame.body
                                    {
                                        stream.next_revision = stream.next_revision.max(revision + 1);
                                    }
                                    false
                                }
                            } else {
                                false
                            }
                        }
                        _ => false,
                    }
                };
                if !discard {
                    let node = receiver.clone();
                    tokio::spawn(async move {
                        let _ = node.handle_message(message).await;
                    });
                }
            }
        });
        Ok(Self { source: source.clone(), client: client.clone(), streams, tasks: vec![source_task, client_task] })
    }

    /// Deliver one publication at a time, observing its application boundary
    /// before the next. After a malformed delta, later frames retain the same
    /// failed origin and cannot silently establish a replacement stream.
    /// Returns true when the production receiver reports stream failure.
    pub async fn deliver<R: View>(&self, query: &LiveQuery<R>, deltas: Vec<proto::EntityDelta>) -> anyhow::Result<bool> {
        let mut failed = query.error().value().is_some();
        let comparison = {
            let routes = self.streams.lock().unwrap();
            let matching: Vec<_> = routes.values().filter(|stream| stream.query == query.query_id()).collect();
            anyhow::ensure!(matching.len() == 1, "fixture requires one unambiguous captured query origin");
            matching[0].comparison
        };
        for delta in deltas {
            let (frame, request, revision, comparison) = {
                let mut routes = self.streams.lock().unwrap();
                let stream = routes.get_mut(&comparison).ok_or_else(|| anyhow::anyhow!("captured query origin disappeared"))?;
                stream.scripted = true;
                let revision = stream.next_revision;
                let frame = proto::StateFrame {
                    comparison: stream.comparison,
                    sequence: stream.next_sequence,
                    body: proto::StateFrameBody::Live { revision, items: vec![change(delta)], complete: true },
                };
                stream.next_sequence += 1;
                stream.next_revision += 1;
                (frame, stream.request.clone(), revision, stream.comparison)
            };
            self.client
                .handle_message(proto::NodeMessage::Update(proto::NodeUpdate {
                    id: proto::UpdateId::new(),
                    from: self.source.id,
                    to: self.client.id,
                    body: proto::NodeUpdateBody::StateFrame { request_id: request, frame },
                }))
                .await?;
            if failed {
                continue;
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if query.error().value().is_some()
                        || self.streams.lock().unwrap().get(&comparison).is_some_and(|stream| stream.abandoned)
                    {
                        failed = true;
                        break;
                    }
                    if self
                        .streams
                        .lock()
                        .unwrap()
                        .get(&comparison)
                        .and_then(|stream| stream.applied)
                        .is_some_and(|applied| applied >= revision)
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(|_| anyhow::anyhow!("scripted publication neither applied nor failed"))?;
        }
        Ok(failed)
    }
}
impl Drop for ScriptedConnection {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        self.client.deregister_peer(self.source.id);
        self.source.deregister_peer(self.client.id);
    }
}

/// Explicit target metadata for a scripted entity-ID membership decision.
/// Parent IDs supplied in the bridge are removed from its tips even when the
/// fragment order is deliberately child-first.
pub fn change(delta: proto::EntityDelta) -> proto::QueryChange {
    let head = match &delta.content {
        proto::DeltaContent::EventBridge { events } => {
            let mut head = proto::Clock::new(
                events
                    .iter()
                    .map(|fragment| {
                        let event: proto::Attested<proto::Event> = (delta.entity_id, fragment.clone()).into();
                        (event.payload.generation(), event.payload.id())
                    })
                    .collect::<Vec<_>>(),
            )
            .expect("normalized event tips");
            for event in events {
                for parent in event.parent.ids() {
                    head.remove(parent);
                }
            }
            head
        }
        proto::DeltaContent::StateSnapshot { state }
        | proto::DeltaContent::StateAndEvents { state, .. }
        | proto::DeltaContent::StateAndRelation { state, .. } => state.state.head.clone(),
    };
    proto::QueryChange { entity_id: delta.entity_id, member: Some(proto::QueryMember { key: vec![], head }), delta: Some(delta.content) }
}
