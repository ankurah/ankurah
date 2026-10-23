//! The virtual transport seam.
//!
//! `SimSender` implements the production `PeerSender` trait, so a real Node
//! sends through it exactly as it would through the local-process connector.
//! But instead of delivering, it captures every outbound `NodeMessage` into a
//! shared `Captured` queue tagged with the sender's logical node index. The
//! scheduler is the sole owner of delivery: nothing crosses the wire except
//! when the scheduler decides to, which is what makes reorder / delay /
//! duplication / drop / partition first-class and the schedule a pure function
//! of the seed.
//!
//! This replaces `LocalProcessConnection`, whose receiver task
//! `tokio::spawn`s a fresh task per message; that spawn makes intra-run poll
//! order (and therefore delivery order) nondeterministic, which is exactly the
//! hole a deterministic harness must close.

use ankurah::core::connector::{PeerSender, SendError};
use ankurah::proto;
use std::sync::{Arc, Mutex};

/// A message a node emitted, tagged with the logical index of its sender.
/// (`NodeMessage` already carries the logical-free routing ids; the harness
/// keys everything on `src`/`dst` logical indices instead.)
pub struct Outbound {
    pub src: usize,
    pub message: proto::NodeMessage,
}

/// Shared sink plus captured standing-route metadata for malicious-wire scenarios.
#[derive(Clone, Default)]
pub struct Captured {
    queue: Arc<Mutex<Vec<Outbound>>>,
    streams: Arc<Mutex<std::collections::HashMap<proto::StateStreamId, ScriptedRoute>>>,
}
struct ScriptedRoute {
    request: proto::RequestId,
    query: proto::QueryId,
    from: proto::EntityId,
    to: proto::EntityId,
    source_index: usize,
    comparison: proto::StateStreamId,
    next_sequence: u64,
    next_revision: u64,
    scripted: bool,
    abandoned: bool,
}
impl Captured {
    pub fn new() -> Self { Self::default() }
    fn push(&self, out: Outbound) {
        let mut streams = self.streams.lock().unwrap();
        match &out.message {
            proto::NodeMessage::Response(response) => {
                if let proto::NodeResponseBody::QuerySubscribed { query_id, state } = &response.body {
                    streams.insert(
                        state.frame.stream,
                        ScriptedRoute {
                            request: response.request_id.clone(),
                            query: *query_id,
                            from: response.from,
                            to: response.to,
                            source_index: out.src,
                            comparison: state.frame.stream,
                            next_sequence: state.frame.sequence + 1,
                            next_revision: state.round.revision + 1,
                            scripted: false,
                            abandoned: false,
                        },
                    );
                }
            }
            proto::NodeMessage::Update(proto::NodeUpdate { body: proto::NodeUpdateBody::StateFrame { frame, .. }, .. }) => {
                if let Some(stream) = streams.get_mut(&frame.stream) {
                    if stream.scripted {
                        return;
                    }
                    stream.next_sequence = stream.next_sequence.max(frame.sequence + 1);
                    if let proto::StateFrameBody::Live { revision, .. } | proto::StateFrameBody::LiveChunk { revision, .. } = frame.body {
                        stream.next_revision = stream.next_revision.max(revision + 1);
                    }
                }
            }
            proto::NodeMessage::AbandonStateStream { stream, .. } => {
                if let Some(route) = streams.get_mut(stream).filter(|route| route.scripted) {
                    route.abandoned = true;
                    return; // One-way control: the scripted peer sends no response.
                }
            }
            proto::NodeMessage::Request { request, .. } => {
                let comparison = match request.body {
                    proto::NodeRequestBody::StateFeedback { stream: comparison, .. }
                    | proto::NodeRequestBody::FinishStateRound { stream: comparison, .. } => Some(comparison),
                    _ => None,
                };
                if let Some(stream) = comparison.and_then(|id| streams.get_mut(&id)).filter(|stream| stream.scripted) {
                    // This scenario has become a malicious source. Its control
                    // responses still cross the same seeded transport scheduler.
                    self.queue.lock().unwrap().push(Outbound {
                        src: stream.source_index,
                        message: proto::NodeMessage::Response(proto::NodeResponse {
                            request_id: request.id.clone(),
                            from: request.to,
                            to: request.from,
                            body: proto::NodeResponseBody::Success,
                        }),
                    });
                    return;
                }
            }
            _ => {}
        }
        self.queue.lock().unwrap().push(out);
    }
    pub fn drain(&self) -> Vec<Outbound> { std::mem::take(&mut *self.queue.lock().unwrap()) }
    pub fn is_empty(&self) -> bool { self.queue.lock().unwrap().is_empty() }
    /// Select an actual captured origin once; callers retain this comparison
    /// across failure, never selecting a replacement query generation.
    pub fn captured_origin(&self, query: proto::QueryId, to: proto::EntityId) -> proto::StateStreamId {
        let streams = self.streams.lock().unwrap();
        let found: Vec<_> = streams.values().filter(|stream| stream.query == query && stream.to == to).collect();
        assert_eq!(found.len(), 1, "one unambiguous standing query origin");
        found[0].comparison
    }
    pub fn origin_failed(&self, comparison: proto::StateStreamId) -> bool {
        self.streams.lock().unwrap().get(&comparison).is_some_and(|stream| stream.abandoned)
    }
    pub fn begin_scripted(&self, comparison: proto::StateStreamId) {
        self.streams.lock().unwrap().get_mut(&comparison).expect("captured origin").scripted = true;
    }
    pub fn scripted_publication(&self, comparison: proto::StateStreamId, delta: proto::EntityDelta) -> proto::NodeMessage {
        let mut streams = self.streams.lock().unwrap();
        let stream = streams.get_mut(&comparison).expect("captured standing origin");
        stream.scripted = true;
        let frame = proto::StateFrame {
            stream: stream.comparison,
            sequence: stream.next_sequence,
            body: proto::StateFrameBody::Live {
                revision: stream.next_revision,
                items: vec![crate::state_stream_fixture::change(delta)],
                complete: true,
            },
        };
        stream.next_sequence += 1;
        stream.next_revision += 1;
        proto::NodeMessage::Update(proto::NodeUpdate {
            id: proto::UpdateId::new(),
            from: stream.from,
            to: stream.to,
            body: proto::NodeUpdateBody::StateFrame { request_id: stream.request.clone(), frame },
        })
    }
}

/// `PeerSender` bound to one logical source node, feeding `Captured`.
#[derive(Clone)]
pub struct SimSender {
    src: usize,
    /// The recipient's real node id, required by the `PeerSender` contract.
    recipient: proto::EntityId,
    captured: Captured,
}

impl SimSender {
    pub fn new(src: usize, recipient: proto::EntityId, captured: Captured) -> Self { Self { src, recipient, captured } }
}

#[async_trait::async_trait]
impl PeerSender for SimSender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.captured.push(Outbound { src: self.src, message });
        Ok(())
    }

    fn recipient_node_id(&self) -> proto::EntityId { self.recipient }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// A stable *semantic* digest of a `NodeMessage` for the trace.
///
/// This deliberately does NOT hash the whole message: `NodeMessage` carries
/// random ULID correlation ids (`RequestId`, `TransactionId`, `UpdateId`,
/// `QueryId`) minted by `::new()` on every send, including by the Node
/// internally for messages the harness captures. Those ids never affect
/// scheduling (the scheduler keys on queue position and the seeded RNG, and a
/// Node's internal correlation is deterministic within a run), but hashing them
/// would make two runs of one seed produce different digests and defeat the
/// determinism audit. So the digest is over the semantically meaningful,
/// harness-deterministic content: the message kind plus the entity/event ids
/// and collections it carries. Two deliveries of "the same CommitTransaction
/// for the same events" therefore share a digest across runs, which is exactly
/// what the audit needs.
pub fn message_digest(message: &proto::NodeMessage) -> String {
    use base64::{engine::general_purpose, Engine as _};
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(semantic_descriptor(message).as_bytes());
    let full: [u8; 32] = hasher.finalize().into();
    general_purpose::URL_SAFE_NO_PAD.encode(&full[..8])
}

/// Canonical, correlation-id-free description of a message's payload.
fn semantic_descriptor(message: &proto::NodeMessage) -> String {
    match message {
        proto::NodeMessage::Request { request, .. } => {
            format!("REQ {}", request_descriptor(&request.body))
        }
        proto::NodeMessage::Response(response) => {
            format!("RESP {}", response_descriptor(&response.body))
        }
        proto::NodeMessage::Update(update) => match &update.body {
            proto::NodeUpdateBody::StateFrame { frame, .. } => format!("FRAME {}", frame_descriptor(frame)),
        },
        proto::NodeMessage::UpdateAck(ack) => format!("ACK {}", matches!(ack.body, proto::NodeUpdateAckBody::Success)),
        proto::NodeMessage::UnsubscribeQuery { .. } => "UNSUB".to_string(),
        proto::NodeMessage::AbandonStateStream { .. } => "ABANDON".to_string(),
    }
}

fn event_ids(events: &[proto::Attested<proto::Event>]) -> String {
    let mut ids: Vec<String> = events.iter().map(|e| e.payload.id().to_base64_short()).collect();
    ids.sort();
    ids.join("+")
}

fn request_descriptor(body: &proto::NodeRequestBody) -> String {
    match body {
        proto::NodeRequestBody::CommitTransaction { events, .. } => format!("commit {}", event_ids(events)),
        proto::NodeRequestBody::Get { ids } => {
            let mut ss: Vec<String> = ids.iter().map(|i| i.to_base64_short()).collect();
            ss.sort();
            format!("get {}", ss.join("+"))
        }
        proto::NodeRequestBody::GetEvents { event_ids } => {
            let mut ss: Vec<String> = event_ids.iter().map(|i| i.to_base64_short()).collect();
            ss.sort();
            format!("getevents {}", ss.join("+"))
        }
        proto::NodeRequestBody::Fetch { selection, .. } => format!("fetch {}", selection),
        proto::NodeRequestBody::SubscribeQuery { selection, .. } => format!("subscribe {}", selection),
        proto::NodeRequestBody::StateFeedback { round, processed_through, applied_through, answers, .. } => {
            format!("continue {}", payload_digest(&(round, processed_through, applied_through, answers)))
        }
        proto::NodeRequestBody::RefreshState { round, known_state, .. } => format!("refresh {}", payload_digest(&(round, known_state))),
        proto::NodeRequestBody::FinishStateRound { round, outcome, .. } => format!("finish round {round} {outcome:?}"),
        proto::NodeRequestBody::RegisterSchema { model } => {
            format!("registerschema {} {}p", model.label, model.properties.len())
        }
    }
}

fn response_descriptor(body: &proto::NodeResponseBody) -> String {
    match body {
        proto::NodeResponseBody::CommitComplete { .. } => "commitcomplete".to_string(),
        proto::NodeResponseBody::SchemaRegistered { model } => {
            format!("schemaregistered {} {}p", model.label, model.properties.len())
        }
        proto::NodeResponseBody::Fetch(state) => format!("fetch {}", state_descriptor(state)),
        proto::NodeResponseBody::Get(results) => {
            let mut ss: Vec<String> = results
                .iter()
                .map(|result| match result {
                    proto::GetResult::Found(state) => state.payload.entity_id.to_base64_short(),
                    proto::GetResult::NotFound(id) => format!("{}:missing", id.to_base64_short()),
                    proto::GetResult::AccessDenied(id) => format!("{}:denied", id.to_base64_short()),
                })
                .collect();
            ss.sort();
            format!("get [{}]", ss.join("+"))
        }
        proto::NodeResponseBody::GetEvents(events) => format!("getevents {}", event_ids(events)),
        proto::NodeResponseBody::QuerySubscribed { state, .. } => format!("subscribed {}", state_descriptor(state)),
        proto::NodeResponseBody::StateStreamUnavailable { .. } => "state stream unavailable".into(),
        proto::NodeResponseBody::StateSnapshotBusy => "state snapshot busy".into(),
        proto::NodeResponseBody::Success => "success".to_string(),
        // Include the error text so two distinct rejections are distinguishable
        // in the trace (advisory path, but keeps the digest faithful).
        proto::NodeResponseBody::Error(e) => format!("error:{e}"),
    }
}

fn state_descriptor(state: &proto::StateReply) -> String {
    format!("{} {}", payload_digest(&(&state.spec, state.round)), frame_descriptor(&state.frame))
}

fn frame_descriptor(frame: &proto::StateFrame) -> String {
    let body = match &frame.body {
        proto::StateFrameBody::BeginRound(round) => format!("begin {}", payload_digest(round)),
        proto::StateFrameBody::Round { round, items, progress } => format!("state {}", payload_digest(&(round, items, progress))),
        proto::StateFrameBody::Live { revision, items, complete } => format!("live {}", payload_digest(&(revision, items, complete))),
        proto::StateFrameBody::LiveChunk { revision, chunk, member, complete } => {
            format!("livechunk {}", payload_digest(&(revision, chunk, member, complete)))
        }
        proto::StateFrameBody::LiveRepair { items } => format!("liverepair {}", payload_digest(items)),
        proto::StateFrameBody::RestartRequired => "restart".into(),
        proto::StateFrameBody::SourceUnavailable => "source unavailable".into(),
    };
    format!("{} {body}", frame.sequence)
}

/// Compare actual comparison payloads, excluding their random session id.
/// Counts alone would hide different repairs with the same number of items.
fn payload_digest(payload: &impl serde::Serialize) -> String {
    use base64::{engine::general_purpose, Engine as _};
    use sha2::{Digest, Sha256};
    general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(bincode::serialize(payload).expect("comparison payload serializes")))
}
