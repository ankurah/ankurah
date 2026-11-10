//! The peers registered with one node, and what the node asks of the durable ones.

pub(crate) mod durable_peer;
pub(super) mod peer_state;

use crate::connector::PeerSender;
use crate::error::RequestError;
use crate::livequery::WeakEntityLiveQuery;
use crate::peer_subscription::{SubscriptionHandler, SubscriptionRelay};
use crate::policy::PeerTrust;
use crate::session::ContextData;
use crate::util::safeset::SafeSet;
use ankurah_proto as proto;
use peer_state::PeerState;
use rand::prelude::*;
use rand::rngs::SmallRng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// What a request needs from the trust its recipient holds when it is sent.
#[derive(Clone, Copy)]
pub(super) enum RequiredTrust {
    /// Any registered peer.
    Any,
    /// A peer this node trusts as durable.
    Durable,
}

/// One connection's registration with a node: the peer it registered, and which of that peer's
/// registrations it is. The connector holds it while the connection is open and gives it back to
/// deregister, which removes this registration only, never another of the same peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionRegistration {
    peer: proto::NodeId,
    serial: u64,
}

impl ConnectionRegistration {
    /// The peer this registration registered.
    pub fn peer(&self) -> proto::NodeId { self.peer }

    /// A registration of `peer` with `serial` that no node issued, for a test double of a node's
    /// connector interface.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn for_test(peer: proto::NodeId, serial: u64) -> Self { Self { peer, serial } }
}

/// The peers registered with one node: every live registration of each, the peers trusted as
/// durable, and the choice of a durable peer to ask.
pub(crate) struct PeerRegistry<CD: ContextData> {
    /// Each registered peer's live registrations, oldest first. A peer leaves with its last.
    connections: RwLock<HashMap<proto::NodeId, Vec<Arc<PeerState<CD>>>>>,
    /// The peers at least one of whose registrations is trusted as durable.
    durable: SafeSet<proto::NodeId>,
    /// Held while a registration or deregistration updates `connections`, `durable` and the
    /// subscription relay, so all three describe the same registrations.
    registration: Mutex<()>,
    /// Source of randomness for choosing a durable peer. Seeded from entropy in production; an
    /// explicit seed can be injected at construction so the simulation harness and tests can
    /// reproduce an identical selection sequence. The lock is only ever held for a single draw.
    rng: Mutex<SmallRng>,
    /// The serial the next registration takes.
    next_serial: AtomicU64,
}

impl<CD: ContextData> PeerRegistry<CD> {
    pub(super) fn new(rng: SmallRng) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            durable: SafeSet::new(),
            registration: Mutex::new(()),
            rng: Mutex::new(rng),
            next_serial: AtomicU64::new(0),
        }
    }

    // A panic never leaves the map half-changed: each change is one push or one remove.
    fn read(&self) -> RwLockReadGuard<'_, HashMap<proto::NodeId, Vec<Arc<PeerState<CD>>>>> {
        self.connections.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<proto::NodeId, Vec<Arc<PeerState<CD>>>>> {
        self.connections.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Register a connection to `peer` that sends through `sender`, beside every other
    /// registration of `peer`. `subscription_handler` makes the handler of the subscriptions the
    /// peer installs on this registration. The peer becomes durable with its first registration
    /// trusted as durable.
    pub(super) fn register(
        &self,
        peer: proto::NodeId,
        sender: Box<dyn PeerSender>,
        trust: PeerTrust,
        subscription_handler: impl FnOnce(ConnectionRegistration) -> SubscriptionHandler<CD>,
        relay: Option<&SubscriptionRelay<CD, WeakEntityLiveQuery>>,
    ) -> ConnectionRegistration {
        let registration = ConnectionRegistration { peer, serial: self.next_serial.fetch_add(1, Ordering::Relaxed) };
        let connection = Arc::new(PeerState::new(registration, sender, trust, subscription_handler(registration)));
        let _registration = self.registration.lock().unwrap_or_else(PoisonError::into_inner);
        self.write().entry(peer).or_default().push(connection);
        if trust.durable && self.durable.insert(peer) {
            if let Some(relay) = relay {
                relay.notify_peer_connected(peer);
            }
        }
        registration
    }

    /// Remove `registration`, returning its connection; nothing, if it is not registered. The peer
    /// stops being durable with its last registration trusted as durable; until then, the queries
    /// this node subscribed through `registration` move to another.
    pub(super) fn deregister(
        &self,
        registration: ConnectionRegistration,
        relay: Option<&SubscriptionRelay<CD, WeakEntityLiveQuery>>,
    ) -> Option<Arc<PeerState<CD>>> {
        let peer = registration.peer;
        let _registration = self.registration.lock().unwrap_or_else(PoisonError::into_inner);
        let (connection, durable_remains) = {
            let mut connections = self.write();
            let registrations = connections.get_mut(&peer)?;
            let connection = registrations.remove(registrations.iter().position(|connection| connection.registration == registration)?);
            let durable_remains = registrations.iter().any(|connection| connection.trust.durable);
            if registrations.is_empty() {
                connections.remove(&peer);
            }
            (connection, durable_remains)
        };
        if connection.trust.durable {
            if durable_remains {
                if let Some(relay) = relay {
                    relay.notify_registration_closed(registration);
                }
            } else {
                self.durable.remove(&peer);
                if let Some(relay) = relay {
                    relay.notify_peer_disconnected(peer);
                }
            }
        }
        Some(connection)
    }

    /// The connection `registration` registered, while it is registered.
    pub(super) fn registered(&self, registration: &ConnectionRegistration) -> Option<Arc<PeerState<CD>>> {
        self.read().get(&registration.peer)?.iter().find(|connection| connection.registration == *registration).cloned()
    }

    /// The newest connection to `peer` whose trust meets `required`.
    pub(super) fn trusted_connection(&self, peer: &proto::NodeId, required: RequiredTrust) -> Result<Arc<PeerState<CD>>, RequestError> {
        let connections = self.read();
        let registrations = connections.get(peer).ok_or(RequestError::PeerNotConnected)?;
        match required {
            RequiredTrust::Any => registrations.last().cloned().ok_or(RequestError::PeerNotConnected),
            RequiredTrust::Durable => registrations
                .iter()
                .rev()
                .find(|connection| connection.trust.durable)
                .cloned()
                .ok_or(RequestError::NotTrustedAsDurable(*peer)),
        }
    }

    /// The connection `registration` registered, while it is registered and its trust meets `required`.
    pub(super) fn trusted_registration(
        &self,
        registration: &ConnectionRegistration,
        required: RequiredTrust,
    ) -> Result<Arc<PeerState<CD>>, RequestError> {
        let connection = self.registered(registration).ok_or(RequestError::PeerNotConnected)?;
        match required {
            RequiredTrust::Durable if !connection.trust.durable => Err(RequestError::NotTrustedAsDurable(registration.peer)),
            RequiredTrust::Any | RequiredTrust::Durable => Ok(connection),
        }
    }

    /// The trust this node grants `peer`: that of its newest registration trusted as durable, or
    /// else of its newest registration.
    pub(super) fn granted_trust(&self, peer: &proto::NodeId) -> Option<PeerTrust> {
        let connections = self.read();
        let registrations = connections.get(peer)?;
        registrations.iter().rev().find(|connection| connection.trust.durable).or(registrations.last()).map(|connection| connection.trust)
    }

    /// The durable peers, sorted by id for a stable fan-out order. The underlying set is unordered;
    /// callers that emit per peer (relay, random selection) depend on this stable order for the C1
    /// determinism audit.
    pub(super) fn durable_peers(&self) -> Vec<proto::NodeId> {
        let mut peers = self.durable.to_vec();
        peers.sort();
        peers
    }

    /// A durable peer drawn from this registry's seeded source of randomness, not `thread_rng`, so
    /// the simulation harness can reproduce selections. `choose` indexes by position, so the
    /// candidates' stable order is what makes a given seed yield a given peer.
    pub(super) fn random_durable(&self) -> Option<proto::NodeId> {
        let peers = self.durable_peers();
        let mut rng = self.rng.lock().expect("node rng mutex poisoned");
        peers.choose(&mut *rng).copied()
    }

    /// Mark `peer` durable with no connection, for tests of fan-out order and seeded selection.
    #[cfg(feature = "test-helpers")]
    pub(super) fn insert_durable_for_test(&self, peer: proto::NodeId) { self.durable.insert(peer); }
}
