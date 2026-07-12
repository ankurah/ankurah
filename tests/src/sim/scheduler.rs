//! The deterministic scheduler: the sole owner of message delivery.
//!
//! Real Nodes emit messages into `Captured` via their `SimSender`s; the harness
//! also enqueues change-propagation requests directly. The scheduler holds all
//! in-flight messages and, on each step, decides (from the one seeded RNG) which
//! to deliver, reorder, duplicate, delay, or drop, subject to the current
//! partition matrix. It delivers by awaiting the recipient's real
//! connection-bound message ingress, so the production applier runs. Every decision is recorded
//! in the `Trace`.
//!
//! Quiescence is defined by message drain, not by tokio's paused-time
//! auto-advance: the Node spawns background tasks (system-catalog load,
//! subscription retry with a `futures_timer`) that never settle a paused clock,
//! and sled's `spawn_blocking` further muddies any all-tasks-idle signal.
//! Draining is robust to all of that: the system is quiescent when a full
//! delivery round moves no message and no load-bearing message is still
//! awaiting acceptance.
//!
//! Load-bearing propagation is delivered under an *acceptance check*: a
//! `CommitTransaction` for a single event whose parents the receiver has not yet
//! seen is correctly rejected by the empty-head guard (the V6 semantics), and
//! message ingress still returns `Ok` because the request handler turns the
//! apply error into an error *response*. So the scheduler cannot read acceptance
//! from the return value; instead it verifies the event landed in the
//! receiver's storage and, if not, redelivers it in a later round. This models a
//! transport that retries until delivery and lets any causal order converge
//! (an edit that arrives before its create is retried until the create lands),
//! without the scheduler ever having to understand causality itself.

use ankurah::proto;

use super::faults::FaultConfig;
use super::node::SimNode;
use super::rng::SimRng;
use super::trace::{Trace, TraceEvent};
use super::transport::{message_digest, Captured};

/// One message the scheduler is holding.
struct InFlight {
    src: usize,
    dst: usize,
    message: proto::SignedPeerMessage,
    digest: String,
    /// Advisory messages (acks, responses) may be dropped outright under the
    /// drop fault; load-bearing propagation may only be delayed, never lost, so
    /// quiescence can still converge. This flag marks the droppable ones.
    droppable: bool,
    /// For load-bearing single-event propagation, the (entity, event) the
    /// receiver must end up holding for the delivery to count as accepted. If
    /// absent after delivery, the message is redelivered. `None` for advisory
    /// traffic and for messages with no single acceptance target.
    accept: Option<(proto::EntityId, proto::EventId)>,
}

/// How many times to yield to the executor when the queue looks empty, letting
/// pending spawned tasks emit before declaring quiescence. Small: on a
/// current-thread runtime a spawned task that is going to emit does so within a
/// yield or two of becoming ready; a larger budget only delays detection of
/// genuine idleness.
const YIELD_BUDGET: usize = 8;

/// Unordered node-pair key for the partition matrix.
fn pair(a: usize, b: usize) -> (usize, usize) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

pub struct Scheduler {
    inflight: Vec<InFlight>,
    /// Cut links (heals restore delivery).
    /// Cut links. A `BTreeSet` (not `HashSet`): its iteration and `drain` order
    /// is sorted and deterministic, so the order in which partitions are healed,
    /// and therefore the trace, does not depend on hash-map iteration order. A
    /// `HashSet` here leaks per-process randomized iteration into the trace and
    /// breaks the determinism audit (found by that audit at scale).
    partitions: std::collections::BTreeSet<(usize, usize)>,
    captured: Captured,
    faults: FaultConfig,
    /// Node id per logical index, used to resolve the recipient index of
    /// captured (node-emitted) messages whose routing carries only the id.
    node_ids: Vec<proto::NodeId>,
    /// Cap on delivery rounds so a harness bug (e.g. an unhealed partition
    /// starving a load-bearing message) surfaces as a loud failure rather than
    /// an infinite loop.
    max_rounds: usize,
}

impl Scheduler {
    pub fn new(captured: Captured, faults: FaultConfig, node_ids: Vec<proto::NodeId>) -> Self {
        Self { inflight: Vec::new(), partitions: std::collections::BTreeSet::new(), captured, faults, node_ids, max_rounds: 100_000 }
    }

    fn node_count(&self) -> usize { self.node_ids.len() }

    /// Logical index of the node whose id is `id`, if any.
    fn index_of(&self, id: &proto::NodeId) -> Option<usize> { self.node_ids.iter().position(|nid| nid == id) }

    pub fn faults(&self) -> FaultConfig { self.faults }

    /// Enqueue a load-bearing propagation of a single event, redelivered until
    /// the receiver holds `event`. Never dropped outright.
    pub fn enqueue_event(
        &mut self,
        src: usize,
        dst: usize,
        entity: proto::EntityId,
        event: proto::EventId,
        message: proto::SignedPeerMessage,
    ) {
        let digest = message_digest(&message.message);
        self.inflight.push(InFlight { src, dst, message, digest, droppable: false, accept: Some((entity, event)) });
    }

    /// Enqueue a load-bearing message with no single acceptance target (e.g. a
    /// harness-built adversarial batch delivered once for its own sake). Not
    /// dropped, not acceptance-retried; may be reordered/delayed/duplicated.
    pub fn enqueue(&mut self, src: usize, dst: usize, message: proto::SignedPeerMessage) {
        let digest = message_digest(&message.message);
        self.inflight.push(InFlight { src, dst, message, digest, droppable: false, accept: None });
    }

    fn link_up(&self, a: usize, b: usize) -> bool { !self.partitions.contains(&pair(a, b)) }

    /// Pull everything nodes have emitted since the last pump into the in-flight
    /// queue. Node-emitted messages (acks, responses, relay traffic) are
    /// droppable; losing one only costs a retry, never convergence.
    fn absorb_captured(&mut self) {
        for out in self.captured.drain() {
            let Some(dst) = recipient_id(&out.message.message).and_then(|id| self.index_of(&id)) else { continue };
            let digest = message_digest(&out.message.message);
            self.inflight.push(InFlight { src: out.src, dst, message: out.message, digest, droppable: true, accept: None });
        }
    }

    /// Toggle partitions at a scheduling boundary if the partition fault is
    /// live. Deterministic in the seed.
    fn maybe_toggle_partitions(&mut self, rng: &mut SimRng, trace: &mut Trace) {
        let n = self.node_count();
        if !self.faults.partition || n < 2 {
            return;
        }
        if !rng.chance(self.faults.partition_toggle_p) {
            return;
        }
        let a = rng.below(n);
        let mut b = rng.below(n);
        if a == b {
            b = (b + 1) % n;
        }
        let key = pair(a, b);
        if self.partitions.remove(&key) {
            trace.record(TraceEvent::Partition { a: key.0, b: key.1, up: true });
        } else {
            self.partitions.insert(key);
            trace.record(TraceEvent::Partition { a: key.0, b: key.1, up: false });
        }
    }

    /// Deliver one message to its recipient's production, connection-bound
    /// ingress, recording the delivery. Ingress errors are swallowed: a node rejecting a
    /// message (a dangling parent, a V6 unknown-entity item) is a legitimate
    /// outcome the invariants check for, not a harness error. Returns whether
    /// the message's acceptance target (if any) is now satisfied.
    async fn deliver(&self, item: &InFlight, nodes: &[SimNode], trace: &mut Trace, duplicate: bool) -> bool {
        trace.record(TraceEvent::Deliver { src: item.src, dst: item.dst, digest: item.digest.clone(), duplicate });
        let authenticated_peer = nodes[item.src].id();
        let _ = nodes[item.dst].node.handle_peer_message(authenticated_peer, clone_message(&item.message)).await;
        match &item.accept {
            None => true,
            Some((entity, event)) => nodes[item.dst].stored_event_ids(*entity).await.contains(event),
        }
    }

    /// Run delivery until the network is quiescent: repeatedly absorb captured
    /// messages and deliver the in-flight set, healing partitions when only
    /// blocked or unaccepted load-bearing traffic remains, until nothing is left
    /// to deliver and every acceptance target is satisfied. Faults apply to
    /// every round before the final heal-and-flush.
    pub async fn run_to_quiescence(&mut self, nodes: &[SimNode], rng: &mut SimRng, trace: &mut Trace) {
        let mut rounds = 0;
        loop {
            rounds += 1;
            assert!(
                rounds <= self.max_rounds,
                "scheduler exceeded {} rounds without quiescing (harness bug or non-convergence)",
                self.max_rounds
            );

            self.absorb_captured();

            if self.inflight.is_empty() {
                // Nothing queued right now, but a node may have spawned a
                // background task (subscription setup, reactor gap-fill) that
                // has not yet run and will emit a message. Yield to the
                // current-thread executor a bounded number of times so those
                // tasks make progress, re-absorbing after each; only declare
                // quiescence once a full yield budget produces no new traffic.
                // yield_now is deterministic on a single-threaded runtime.
                let mut settled = true;
                for _ in 0..YIELD_BUDGET {
                    tokio::task::yield_now().await;
                    self.absorb_captured();
                    if !self.inflight.is_empty() {
                        settled = false;
                        break;
                    }
                }
                if settled {
                    break;
                }
            }

            self.maybe_toggle_partitions(rng, trace);

            // If nothing is deliverable this round (everything is behind a cut
            // link), heal all partitions so load-bearing traffic can flow. This
            // is the "faults healed before the quiescence barrier" discipline
            // the convergence invariant requires.
            let deliverable_now = self.inflight.iter().any(|m| self.link_up(m.src, m.dst));
            if !deliverable_now && !self.partitions.is_empty() {
                // Sorted iteration (BTreeSet via mem::take) keeps the heal order,
                // and thus the trace, deterministic.
                let healed = std::mem::take(&mut self.partitions);
                for key in healed {
                    trace.record(TraceEvent::Partition { a: key.0, b: key.1, up: true });
                }
            }

            // Split into deliverable vs blocked (behind a cut link).
            let mut deliverable: Vec<InFlight> = Vec::new();
            let mut blocked: Vec<InFlight> = Vec::new();
            for item in std::mem::take(&mut self.inflight) {
                if self.link_up(item.src, item.dst) {
                    deliverable.push(item);
                } else {
                    blocked.push(item);
                }
            }

            if self.faults.reorder {
                rng.shuffle(&mut deliverable);
            }

            let mut requeue: Vec<InFlight> = Vec::new();
            for item in deliverable {
                // Drop (advisory messages only).
                if self.faults.drop && item.droppable && rng.chance(self.faults.drop_p) {
                    trace.record(TraceEvent::Drop { src: item.src, dst: item.dst, digest: item.digest.clone() });
                    continue;
                }
                // Delay: hold for a later round.
                if self.faults.delay && rng.chance(self.faults.delay_p) {
                    requeue.push(item);
                    continue;
                }
                // Deliver, optionally an extra time (duplication).
                let dup = self.faults.duplicate && rng.chance(self.faults.duplicate_p);
                let accepted = self.deliver(&item, nodes, trace, false).await;
                if dup {
                    let _ = self.deliver(&item, nodes, trace, true).await;
                }
                // A load-bearing message not yet accepted (e.g. its parent has
                // not arrived) is redelivered in a later round.
                if !accepted {
                    requeue.push(item);
                }
            }

            self.inflight.append(&mut blocked);
            self.inflight.append(&mut requeue);

            // Termination: if every remaining message is an unaccepted
            // load-bearing one whose delivery made no progress this round and no
            // new traffic was captured, the only reason can be an unhealed
            // partition (handled above) or genuine non-convergence (caught by
            // max_rounds). Delay is probabilistic so it cannot livelock alone.
        }
        trace.record(TraceEvent::Quiesced { rounds });
    }
}

/// The recipient node id carried by a `NodeMessage`, if it targets a specific
/// node. Used to route node-emitted (captured) traffic to a logical index.
fn recipient_id(message: &proto::NodeMessage) -> Option<proto::NodeId> {
    match message {
        proto::NodeMessage::Request { request, .. } => Some(request.to),
        proto::NodeMessage::Response(response) => Some(response.to),
        proto::NodeMessage::Update(update) => Some(update.to),
        proto::NodeMessage::UpdateAck(ack) => Some(ack.to),
        proto::NodeMessage::UnsubscribeQuery { .. } => None,
    }
}

/// Deep-clone a `NodeMessage` by round-tripping through bincode. `NodeMessage`
/// is not `Clone`, but duplication and blocked-message requeue need copies. The
/// round-trip is exact (bincode is canonical here), so a duplicate is
/// byte-identical to the original.
fn clone_message(message: &proto::SignedPeerMessage) -> proto::SignedPeerMessage {
    let bytes = bincode::serialize(message).expect("SignedPeerMessage serializes");
    bincode::deserialize(&bytes).expect("SignedPeerMessage round-trips")
}
