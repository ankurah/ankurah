//! A proxy between a node and its database that counts the node's round
//! trips: each time the node sends having heard every answer it waited for.
//! A node that sends several statements before it waits for any answer makes
//! one round trip for them all. A connection's opening, its startup and
//! authentication, counts as one.
//!
//! A count alone can't tell a node that sent its statements together from
//! one whose database answered the first before the rest arrived, so the
//! proxy is also a gate: a test tells it how many statements each round trip
//! it expects holds, and the proxy withholds the database's answers until all
//! of a round trip's statements have arrived. A node that waits for an answer
//! part way through a round trip never hears one; past [`HANG`] the gate
//! gives up, and the count fails the test.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use ankurah::core::test_helpers::map_conformance::fixtures::HANG;

/// The tags of the messages a node's statements end with, `Sync` and a
/// simple `Query`, each of which the database answers last with a
/// `ReadyForQuery`; and of `Terminate`, which it doesn't answer.
const SYNC: u8 = b'S';
const QUERY: u8 = b'Q';
const READY_FOR_QUERY: u8 = b'Z';
const TERMINATE: u8 = b'X';

/// A proxy that relays every connection to the database, counting round
/// trips over them all.
pub struct Proxy {
    port: u16,
    shared: Arc<Shared>,
    accepting: JoinHandle<()>,
}

/// What every relayed connection shares: the counts, and the gate.
#[derive(Default)]
struct Shared {
    counts: Mutex<Counts>,

    /// Woken as each statement arrives, for the answers the gate holds.
    arrived: Notify,
}

#[derive(Default)]
struct Counts {
    round_trips: u64,

    /// The statements nodes have sent since the test last said what it
    /// expects.
    sent: u64,

    /// Where each round trip the test expects ends, in statements sent.
    ends: Vec<u64>,

    /// Why the gate gave up holding answers, if it did.
    gave_up: Option<String>,
}

/// One relayed connection.
struct Relayed {
    shared: Arc<Shared>,
    awaiting: Mutex<Awaiting>,
}

/// Whether a round trip is under way on a connection, from the node's first
/// message until the database has answered every statement since, and how
/// many of them it has yet to answer.
#[derive(Default)]
struct Awaiting {
    under_way: bool,
    statements: usize,
}

impl Proxy {
    /// A proxy to the database at `host` and `port`.
    pub async fn start(host: &str, port: u16) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let proxy_port = listener.local_addr()?.port();
        let shared = Arc::new(Shared::default());

        let target = format!("{host}:{port}");
        let relaying = Arc::clone(&shared);
        let accepting = tokio::spawn(async move {
            while let Ok((node, _)) = listener.accept().await {
                let Ok(database) = TcpStream::connect(&target).await else { return };
                let ((from_node, to_node), (from_database, to_database)) = (node.into_split(), database.into_split());

                let relayed = Arc::new(Relayed { shared: Arc::clone(&relaying), awaiting: Mutex::default() });
                tokio::spawn(relay_to_database(from_node, to_database, Arc::clone(&relayed)));
                tokio::spawn(relay_to_node(from_database, to_node, relayed));
            }
        });

        Ok(Self { port: proxy_port, shared, accepting })
    }

    /// The port nodes connect to the database through.
    pub fn port(&self) -> u16 { self.port }

    /// Withhold the database's answers in each of the round trips to come
    /// until the nodes have sent all its statements: `statements` says how
    /// many each holds, in turn. Past the last, answers pass at once. The
    /// statements of every connection count together, so the round trips are
    /// those of one connection at a time.
    pub fn expect_round_trips(&self, statements: &[u64]) {
        let mut counts = self.shared.counts.lock().unwrap();
        counts.sent = 0;
        counts.ends = statements
            .iter()
            .scan(0, |end, statements| {
                *end += statements;
                Some(*end)
            })
            .collect();
    }

    /// The round trips nodes have made through the proxy so far, which fails
    /// the test if the gate gave up holding answers.
    pub fn round_trips(&self) -> u64 {
        let counts = self.shared.counts.lock().unwrap();
        assert!(
            counts.gave_up.is_none(),
            "the proxy held the database's answers for {HANG:?}: {}",
            counts.gave_up.as_deref().unwrap_or("")
        );

        counts.round_trips
    }
}

impl Drop for Proxy {
    fn drop(&mut self) { self.accepting.abort(); }
}

impl Shared {
    /// Wait until the database's answers may pass the gate, or give up on it
    /// past [`HANG`].
    async fn answers_pass(&self) {
        let deadline = Instant::now() + HANG;
        loop {
            let arrived = self.arrived.notified();
            {
                let mut counts = self.counts.lock().unwrap();
                if counts.answers_pass() {
                    return;
                }

                if Instant::now() >= deadline {
                    let end = counts.ends.iter().find(|&&end| end > counts.sent).copied().unwrap_or_default();
                    counts.gave_up =
                        Some(format!("the nodes sent {} statements, short of a round trip expected to end at {end}", counts.sent));
                    return;
                }
            }

            let _ = tokio::time::timeout_at(deadline, arrived).await;
        }
    }
}

impl Counts {
    /// Whether the database's answers may pass: no round trip the test
    /// expects is part way sent.
    fn answers_pass(&self) -> bool {
        let past_the_last = self.ends.last().is_none_or(|&last| self.sent > last);
        self.gave_up.is_some() || self.sent == 0 || self.ends.contains(&self.sent) || past_the_last
    }
}

impl Relayed {
    /// The node begins its connection with its startup message: a round
    /// trip, which the database ends once the node has authenticated, with
    /// its first `ReadyForQuery`.
    fn node_starts(&self) {
        let mut awaiting = self.awaiting.lock().unwrap();
        awaiting.under_way = true;
        awaiting.statements = 1;
        self.shared.counts.lock().unwrap().round_trips += 1;
    }

    /// The node begins a message tagged `tag`: a round trip begins with it,
    /// if none is under way.
    fn node_sends(&self, tag: u8) {
        let mut awaiting = self.awaiting.lock().unwrap();
        let mut counts = self.shared.counts.lock().unwrap();
        if tag != TERMINATE && !awaiting.under_way {
            awaiting.under_way = true;
            counts.round_trips += 1;
        }

        if matches!(tag, SYNC | QUERY) {
            awaiting.statements += 1;
            counts.sent += 1;
            self.shared.arrived.notify_waiters();
        }
    }

    /// The database begins a message tagged `tag`: the round trip under way
    /// ends with its answer to the last statement awaited.
    fn database_sends(&self, tag: u8) {
        let mut awaiting = self.awaiting.lock().unwrap();
        if tag == READY_FOR_QUERY {
            awaiting.statements -= 1;
            awaiting.under_way = awaiting.statements > 0;
        }
    }
}

/// Relay what a node sends its database, noting each message as it begins
/// before relaying it, so that no answer to it can arrive first.
async fn relay_to_database(mut from_node: OwnedReadHalf, mut to_database: OwnedWriteHalf, relayed: Arc<Relayed>) {
    // The startup message has no tag, only its length.
    let mut length = [0u8; 4];
    if from_node.read_exact(&mut length).await.is_err() {
        return;
    }

    let mut startup = vec![0u8; (u32::from_be_bytes(length) as usize).saturating_sub(4)];
    if from_node.read_exact(&mut startup).await.is_err() {
        return;
    }

    relayed.node_starts();
    if to_database.write_all(&length).await.is_err() || to_database.write_all(&startup).await.is_err() {
        return;
    }

    let mut framing = Framing::default();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let read = match from_node.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };

        for tag in framing.tags(&buffer[..read]) {
            relayed.node_sends(tag);
        }

        if to_database.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}

/// Relay what the database sends a node, noting each message as it begins.
/// Once the node's startup is answered, the gate decides when the answers
/// pass.
async fn relay_to_node(mut from_database: OwnedReadHalf, mut to_node: OwnedWriteHalf, relayed: Arc<Relayed>) {
    let mut framing = Framing::default();
    let mut started = false;
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let read = match from_database.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };

        if started {
            relayed.shared.answers_pass().await;
        }

        for tag in framing.tags(&buffer[..read]) {
            started |= tag == READY_FOR_QUERY;
            relayed.database_sends(tag);
        }

        if to_node.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}

/// Where a stream of tagged messages stands: of the message under way, the
/// bytes of its header seen so far, a tag and a length, or the bytes of its
/// body still to come.
#[derive(Default)]
struct Framing {
    header: Vec<u8>,
    body_left: usize,
}

impl Framing {
    /// The tags of the messages that begin in `bytes`, the stream's next.
    fn tags(&mut self, mut bytes: &[u8]) -> Vec<u8> {
        let mut tags = Vec::new();
        while !bytes.is_empty() {
            if self.body_left > 0 {
                let skipped = self.body_left.min(bytes.len());
                self.body_left -= skipped;
                bytes = &bytes[skipped..];
                continue;
            }

            if self.header.is_empty() {
                tags.push(bytes[0]);
            }

            let taken = (5 - self.header.len()).min(bytes.len());
            self.header.extend_from_slice(&bytes[..taken]);
            bytes = &bytes[taken..];

            if let [_, a, b, c, d] = self.header[..] {
                // The length counts itself, but not the tag.
                self.body_left = (u32::from_be_bytes([a, b, c, d]) as usize).saturating_sub(4);
                self.header.clear();
            }
        }

        tags
    }
}
