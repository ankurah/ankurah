//! The peers registered with one node.

use super::PeerState;
use crate::error::RequestError;
use crate::livequery::WeakEntityLiveQuery;
use crate::peer_subscription::SubscriptionRelay;
use crate::policy::PeerTrust;
use crate::session::ContextData;
use crate::util::safemap::SafeMap;
use crate::util::safeset::SafeSet;
use ankurah_proto as proto;
use rand::prelude::*;
use rand::rngs::SmallRng;
use std::sync::{Arc, Mutex};

/// What a request needs from the trust its recipient holds when it is sent.
#[derive(Clone, Copy)]
pub(super) enum RequiredTrust {
    /// Any registered peer.
    Any,
    /// A peer this node trusts as durable.
    Durable,
}

/// The peers registered with one node: each one's connection, the ones trusted as durable, and
/// the choice of a durable peer to ask.
pub(crate) struct PeerRegistry<CD: ContextData> {
    connections: SafeMap<proto::NodeId, Arc<PeerState<CD>>>,
    durable: SafeSet<proto::NodeId>,
    /// Held while a registration or deregistration updates `durable`, the subscription relay and
    /// `connections`, so all three describe the same registration.
    registration: Mutex<()>,
    /// Source of randomness for choosing a durable peer. Seeded from entropy in production; an
    /// explicit seed can be injected at construction so the simulation harness and tests can
    /// reproduce an identical selection sequence. The lock is only ever held for a single draw.
    rng: Mutex<SmallRng>,
}

impl<CD: ContextData> PeerRegistry<CD> {
    pub(super) fn new(rng: SmallRng) -> Self {
        Self { connections: SafeMap::new(), durable: SafeSet::new(), registration: Mutex::new(()), rng: Mutex::new(rng) }
    }

    /// Register `connection` for `peer`, replacing any earlier registration of it: the latest
    /// registration decides trust.
    pub(super) fn register(
        &self,
        peer: proto::NodeId,
        connection: PeerState<CD>,
        relay: Option<&SubscriptionRelay<CD, WeakEntityLiveQuery>>,
    ) {
        let durable = connection.trust.durable;
        let _registration = self.registration.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // An id registering again without durable trust leaves durable routing before its new
        // sender is reachable.
        if !durable && self.durable.remove(&peer) {
            if let Some(relay) = relay {
                relay.notify_peer_disconnected(peer);
            }
        }
        self.connections.insert(peer, Arc::new(connection));
        if durable {
            self.durable.insert(peer);
            if let Some(relay) = relay {
                relay.notify_peer_connected(peer);
            }
        }
    }

    /// Remove `peer`'s registration, returning its connection if it had one.
    pub(super) fn deregister(
        &self,
        peer: proto::NodeId,
        relay: Option<&SubscriptionRelay<CD, WeakEntityLiveQuery>>,
    ) -> Option<Arc<PeerState<CD>>> {
        let _registration = self.registration.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        self.durable.remove(&peer);
        let connection = self.connections.remove(&peer);
        // Unconditional: the relay handles filtering.
        if let Some(relay) = relay {
            relay.notify_peer_disconnected(peer);
        }
        connection
    }

    /// The connection registered for `peer`.
    pub(super) fn connection(&self, peer: &proto::NodeId) -> Option<Arc<PeerState<CD>>> { self.connections.get(peer) }

    /// The connections registered for `peers`, in the order given.
    pub(super) fn connections(&self, peers: Vec<proto::NodeId>) -> Vec<(proto::NodeId, Option<Arc<PeerState<CD>>>)> {
        self.connections.get_list(peers)
    }

    /// The connection to `peer`, if the trust this node grants that connection now meets `required`.
    pub(super) fn trusted_connection(&self, peer: &proto::NodeId, required: RequiredTrust) -> Result<Arc<PeerState<CD>>, RequestError> {
        let connection = self.connections.get(peer).ok_or(RequestError::PeerNotConnected)?;
        match required {
            RequiredTrust::Durable if !connection.trust.durable => Err(RequestError::NotTrustedAsDurable(*peer)),
            RequiredTrust::Any | RequiredTrust::Durable => Ok(connection),
        }
    }

    /// The trust this node granted the connection registered for `peer`.
    pub(super) fn granted_trust(&self, peer: &proto::NodeId) -> Option<PeerTrust> { self.connections.get(peer).map(|state| state.trust) }

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
