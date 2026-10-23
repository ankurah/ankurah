//! The simulated connection between the server and the client. Each message is serialized with
//! bincode, waits for its direction's serializer at the configured bandwidth, travels for half the
//! round trip, and is decoded and handled by the receiving node. The link counts the traffic of
//! the measured interval and the subscription responses the nodes handle.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use ankurah::{
    core::{
        connector::{PeerSender, SendError},
        node::WeakNode,
    },
    proto, PermissiveAgent,
};
use async_trait::async_trait;
use serde::Serialize;

use crate::{storage::CountingStorage, BenchNode};

/// How long the link must stay quiet before it counts as settled.
const QUIET: Duration = Duration::from_millis(2);

/// The messages and bytes each node sent during the measured interval, serialized and without
/// transport framing.
#[derive(Clone, Default, Serialize)]
pub struct Traffic {
    pub requester_messages: usize,
    pub responder_messages: usize,
    pub requester_bytes: usize,
    pub responder_bytes: usize,
    pub max_message_bytes: usize,
}

/// A full-duplex link between the server and the client.
#[derive(Clone)]
pub struct Link {
    state: Arc<Mutex<LinkState>>,
    /// Propagation delay in each direction: half the round trip.
    one_way: Duration,
    /// Serialization rate in each direction; zero is unlimited.
    bytes_per_second: u64,
}

#[derive(Default)]
struct LinkState {
    connected: bool,
    /// Counts connections, so a message sent on an earlier connection is dropped on arrival.
    generation: usize,
    /// Messages sent and not yet handled or dropped.
    in_flight: usize,
    /// When a message was last sent, handled or dropped.
    last_activity: Option<Instant>,
    /// When each direction's serializer finishes the messages queued on it.
    requester_free_at: Option<Instant>,
    responder_free_at: Option<Instant>,
    /// `QuerySubscribed` responses the nodes handled: in total, and by query.
    subscriptions_handled: usize,
    query_subscriptions_handled: HashMap<proto::QueryId, usize>,
    /// Whether sends count toward `traffic`.
    measuring: bool,
    traffic: Traffic,
    /// Errors the nodes returned for delivered messages.
    errors: Vec<String>,
}

/// Which node sends on a lane: the client requests and the server responds.
#[derive(Clone, Copy)]
enum Direction {
    Requester,
    Responder,
}

impl Link {
    pub fn new(rtt_ms: u64, bytes_per_second: u64) -> Self {
        Self { state: Arc::default(), one_way: Duration::from_micros(rtt_ms * 500), bytes_per_second }
    }

    /// Opens a new connection: each node registers the other, then the client waits until it is
    /// ready.
    pub async fn connect(&self, server: &BenchNode, client: &BenchNode) -> anyhow::Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            state.connected = true;
            state.generation += 1;
            state.requester_free_at = None;
            state.responder_free_at = None;
        }
        // The server registers the client first: the client sends requests as soon as it registers
        // the server, and the server drops requests from a peer it has not registered.
        server.register_peer(client.presence().await?, Box::new(self.sender(client, Direction::Responder))).await?;
        client.register_peer(server.presence().await?, Box::new(self.sender(server, Direction::Requester))).await?;
        client.wait_ready().await?;
        Ok(())
    }

    /// Closes the connection. Messages still in flight are dropped on arrival.
    pub fn disconnect(&self, server: &BenchNode, client: &BenchNode) {
        self.state.lock().unwrap().connected = false;
        server.deregister_peer(client.id);
        client.deregister_peer(server.id);
    }

    /// Waits until no message is in flight, the nodes have handled at least `subscriptions`
    /// `QuerySubscribed` responses (and `query_subscription.1` of them for query
    /// `query_subscription.0`), and the link has been quiet for 2 ms. Returns the time of the last
    /// link activity, which leaves out the 2 ms confirmation.
    pub async fn settle(&self, subscriptions: usize, query_subscription: Option<(proto::QueryId, usize)>) -> anyhow::Result<Instant> {
        let waiting_since = Instant::now();
        loop {
            {
                let state = self.state.lock().unwrap();
                state.check_errors()?;
                let query_handled = query_subscription.is_none_or(|(query, count)| state.query_subscriptions_handled(query) >= count);
                if state.in_flight == 0 && state.subscriptions_handled >= subscriptions && query_handled {
                    let last = state.last_activity.unwrap_or_else(Instant::now);
                    if last.max(waiting_since).elapsed() >= QUIET {
                        return Ok(last);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// How many `QuerySubscribed` responses the nodes have handled: in total, and for `query`.
    pub fn subscriptions_handled(&self, query: Option<proto::QueryId>) -> (usize, Option<usize>) {
        let state = self.state.lock().unwrap();
        (state.subscriptions_handled, query.map(|query| state.query_subscriptions_handled(query)))
    }

    /// Starts counting the measured interval's traffic. Fails if a node already reported an error.
    pub fn start_measuring(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        anyhow::ensure!(state.errors.is_empty(), "setup peer errors: {:?}", state.errors);
        state.traffic = Traffic::default();
        state.measuring = true;
        Ok(())
    }

    /// The traffic counted so far in the measured interval.
    pub fn traffic(&self) -> Traffic { self.state.lock().unwrap().traffic.clone() }

    /// Stops counting traffic and returns what the measured interval sent. Fails if a node
    /// reported an error.
    pub fn stop_measuring(&self) -> anyhow::Result<Traffic> {
        let mut state = self.state.lock().unwrap();
        state.measuring = false;
        anyhow::ensure!(state.errors.is_empty(), "measured peer errors: {:?}", state.errors);
        Ok(std::mem::take(&mut state.traffic))
    }

    /// Fails if a node returned an error for a delivered message.
    pub fn check_errors(&self) -> anyhow::Result<()> { self.state.lock().unwrap().check_errors() }

    /// Runs `f` while the link neither accepts nor starts delivering a message. A delivery a node
    /// is already handling continues.
    pub fn paused<T>(&self, f: impl FnOnce() -> T) -> T {
        let _paused = self.state.lock().unwrap();
        f()
    }

    fn sender(&self, target: &BenchNode, direction: Direction) -> Sender {
        Sender { link: self.clone(), direction, target: target.weak(), recipient: target.id }
    }

    /// Queues `bytes` on `direction`'s serializer. Returns the connection the message was sent on and
    /// when it arrives.
    fn send(&self, direction: Direction, bytes: usize) -> Result<(usize, Instant), SendError> {
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        if !state.connected {
            return Err(SendError::ConnectionClosed);
        }
        let now = Instant::now();
        let free_at = match direction {
            Direction::Requester => &mut state.requester_free_at,
            Direction::Responder => &mut state.responder_free_at,
        };
        // Each direction serializes one message at a time; a message's propagation overlaps the
        // serialization of the messages behind it.
        let departure = free_at.unwrap_or(now).max(now) + self.transmission(bytes);
        *free_at = Some(departure);
        state.in_flight += 1;
        state.last_activity = Some(now);
        if state.measuring {
            state.traffic.record(direction, bytes);
        }
        Ok((state.generation, departure + self.one_way))
    }

    fn transmission(&self, bytes: usize) -> Duration {
        if self.bytes_per_second == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(bytes as f64 / self.bytes_per_second as f64)
        }
    }

    /// Whether a message sent on connection `generation` may still be delivered.
    fn is_current(&self, generation: usize) -> bool {
        let state = self.state.lock().unwrap();
        state.connected && state.generation == generation
    }

    /// Ends a delivery. `handled` holds the node's result, and the query a handled
    /// `QuerySubscribed` response answered, unless the message was dropped.
    fn finish_delivery(&self, generation: usize, handled: Option<(anyhow::Result<()>, Option<proto::QueryId>)>) {
        let mut state = self.state.lock().unwrap();
        if let Some((result, subscribed)) = handled {
            if state.connected && state.generation == generation {
                match result {
                    Err(error) => state.errors.push(error.to_string()),
                    Ok(()) => {
                        if let Some(query) = subscribed {
                            state.subscriptions_handled += 1;
                            *state.query_subscriptions_handled.entry(query).or_default() += 1;
                        }
                    }
                }
            }
        }
        state.in_flight -= 1;
        state.last_activity = Some(Instant::now());
    }
}

impl LinkState {
    fn check_errors(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.errors.is_empty(), "peer errors: {:?}", self.errors);
        Ok(())
    }

    fn query_subscriptions_handled(&self, query: proto::QueryId) -> usize {
        self.query_subscriptions_handled.get(&query).copied().unwrap_or(0)
    }
}

impl Traffic {
    fn record(&mut self, direction: Direction, bytes: usize) {
        match direction {
            Direction::Requester => {
                self.requester_messages += 1;
                self.requester_bytes += bytes;
            }
            Direction::Responder => {
                self.responder_messages += 1;
                self.responder_bytes += bytes;
            }
        }
        self.max_message_bytes = self.max_message_bytes.max(bytes);
    }
}

/// One end of the link, which sends to the node at the other end.
#[derive(Clone)]
struct Sender {
    link: Link,
    direction: Direction,
    target: WeakNode<CountingStorage, PermissiveAgent>,
    recipient: proto::EntityId,
}

#[async_trait]
impl PeerSender for Sender {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        let node = self.target.upgrade().ok_or(SendError::ConnectionClosed)?;
        let bytes = bincode::serialize(&message).expect("encode node message");
        let (generation, arrival) = self.link.send(self.direction, bytes.len())?;
        let link = self.link.clone();
        tokio::spawn(async move {
            if arrival > Instant::now() {
                tokio::time::sleep_until(tokio::time::Instant::from_std(arrival)).await;
            }
            let handled = if link.is_current(generation) {
                let message = bincode::deserialize(&bytes).expect("decode node message");
                let subscribed = subscribed_query(&message);
                Some((node.handle_message(message).await, subscribed))
            } else {
                None
            };
            link.finish_delivery(generation, handled);
        });
        Ok(())
    }

    fn recipient_node_id(&self) -> proto::EntityId { self.recipient }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// The query a `QuerySubscribed` response answers.
fn subscribed_query(message: &proto::NodeMessage) -> Option<proto::QueryId> {
    match message {
        proto::NodeMessage::Response(proto::NodeResponse { body: proto::NodeResponseBody::QuerySubscribed { query_id, .. }, .. }) => {
            Some(*query_id)
        }
        _ => None,
    }
}
