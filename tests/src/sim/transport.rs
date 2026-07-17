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

/// Shared sink for node-emitted messages, drained by the scheduler each step.
#[derive(Clone, Default)]
pub struct Captured(Arc<Mutex<Vec<Outbound>>>);

impl Captured {
    pub fn new() -> Self { Self::default() }

    fn push(&self, out: Outbound) { self.0.lock().unwrap().push(out); }

    /// Remove and return everything captured since the last drain, preserving
    /// insertion order. The scheduler decides what to do with them.
    pub fn drain(&self) -> Vec<Outbound> { std::mem::take(&mut *self.0.lock().unwrap()) }

    pub fn is_empty(&self) -> bool { self.0.lock().unwrap().is_empty() }
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
        proto::NodeMessage::Update(update) => {
            let proto::NodeUpdateBody::SubscriptionUpdate { items } = &update.body;
            let mut parts: Vec<String> = items.iter().map(update_item_descriptor).collect();
            parts.sort();
            format!("UPD [{}]", parts.join(","))
        }
        proto::NodeMessage::UpdateAck(ack) => format!("ACK {}", matches!(ack.body, proto::NodeUpdateAckBody::Success)),
        proto::NodeMessage::UnsubscribeQuery { .. } => "UNSUB".to_string(),
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
        proto::NodeRequestBody::Get { collection, ids } => {
            let mut ss: Vec<String> = ids.iter().map(|i| i.to_base64_short()).collect();
            ss.sort();
            format!("get {} {}", collection, ss.join("+"))
        }
        proto::NodeRequestBody::GetEvents { collection, event_ids } => {
            let mut ss: Vec<String> = event_ids.iter().map(|i| i.to_base64_short()).collect();
            ss.sort();
            format!("getevents {} {}", collection, ss.join("+"))
        }
        proto::NodeRequestBody::Fetch { collection, .. } => format!("fetch {}", collection),
        proto::NodeRequestBody::SubscribeQuery { collection, .. } => format!("subscribe {}", collection),
        proto::NodeRequestBody::RegisterSchema { models, properties, memberships } => {
            let mut cols: Vec<String> = models.iter().map(|m| m.collection.to_string()).collect();
            cols.sort();
            format!("registerschema [{}] p{} ms{}", cols.join("+"), properties.len(), memberships.len())
        }
    }
}

fn response_descriptor(body: &proto::NodeResponseBody) -> String {
    match body {
        proto::NodeResponseBody::CommitComplete { .. } => "commitcomplete".to_string(),
        proto::NodeResponseBody::Fetch(deltas) => format!("fetch {}", deltas.len()),
        proto::NodeResponseBody::Get(states) => {
            let mut ss: Vec<String> = states.iter().map(|s| s.payload.entity_id.to_base64_short()).collect();
            ss.sort();
            format!("get [{}]", ss.join("+"))
        }
        proto::NodeResponseBody::GetEvents(events) => format!("getevents {}", event_ids(events)),
        proto::NodeResponseBody::QuerySubscribed { deltas, .. } => format!("subscribed {}", deltas.len()),
        // Counts only: allocated ids are ULIDs and would perturb the
        // determinism digest without adding discriminating power.
        proto::NodeResponseBody::SchemaRegistered { models, properties, memberships } => {
            format!("schemaregistered m{} p{} ms{}", models.len(), properties.len(), memberships.len())
        }
        proto::NodeResponseBody::Success => "success".to_string(),
        // Include the error text so two distinct rejections are distinguishable
        // in the trace (advisory path, but keeps the digest faithful).
        proto::NodeResponseBody::Error(e) => format!("error:{e}"),
    }
}

fn update_item_descriptor(item: &proto::SubscriptionUpdateItem) -> String {
    let entity = item.entity_id;
    let kind = match &item.content {
        proto::UpdateContent::EventOnly(fragments) => format!("eventonly:{}", fragment_ids(entity, fragments)),
        proto::UpdateContent::StateAndEvent(state, fragments) => {
            // The state fragment's head clock identifies the snapshot content
            // deterministically (state buffers are a BTreeMap, the head is
            // sorted), so hashing the head plus the event ids distinguishes two
            // batches that differ only in payload.
            format!("stateandevent:head={}:{}", state.state.head.to_base64_short(), fragment_ids(entity, fragments))
        }
    };
    format!("{}/{}", entity.to_base64_short(), kind)
}

/// Sorted, joined content-derived event ids for a set of `EventFragment`s.
/// `EventFragment` omits the event id, but it is a pure hash of
/// `(entity_id, operations, parent)`, so we recompute it: this makes the digest
/// faithful to the batch's actual events, not merely their count. Keying on the
/// count alone would let two different batches for one entity share a digest and
/// false-pass the determinism audit.
fn fragment_ids(entity: proto::EntityId, fragments: &[proto::EventFragment]) -> String {
    let mut ids: Vec<String> =
        fragments.iter().map(|f| proto::EventId::from_parts(&entity, &f.operations, &f.parent).to_base64_short()).collect();
    ids.sort();
    ids.join("+")
}
