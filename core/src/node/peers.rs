pub(crate) mod durable_peer;
pub(super) mod peer_state;

use crate::session::ContextData;
use crate::util::safemap::SafeMap;
use crate::util::safeset::SafeSet;
use ankurah_proto as proto;
use peer_state::PeerState;
use rand::prelude::*;
use rand::rngs::SmallRng;
use std::sync::{Arc, Mutex};

pub(crate) struct PeerRegistry<CD: ContextData> {
    pub(super) connections: SafeMap<proto::NodeId, Arc<PeerState<CD>>>,
    pub(super) durable: SafeSet<proto::NodeId>,

    /// Per-node source of randomness for peer selection. Seeded from entropy in production;
    /// an explicit seed can be injected at construction so the simulation harness and tests
    /// can reproduce an identical selection sequence. Held behind a Mutex because draws mutate
    /// RNG state and Node is shared across tasks; the lock is only ever held for a single draw.
    rng: Mutex<SmallRng>,
}

impl<CD: ContextData> PeerRegistry<CD> {
    pub(super) fn new(rng: SmallRng) -> Self { Self { connections: SafeMap::new(), durable: SafeSet::new(), rng: Mutex::new(rng) } }

    pub(super) fn durable_peers(&self) -> Vec<proto::NodeId> {
        let mut peers = self.durable.to_vec();
        peers.sort();
        peers
    }

    pub(super) fn random_durable(&self) -> Option<proto::NodeId> {
        let peers = self.durable_peers();
        let mut rng = self.rng.lock().expect("node rng mutex poisoned");
        peers.choose(&mut *rng).copied()
    }

    #[cfg(feature = "test-helpers")]
    pub(super) fn insert_durable_for_test(&self, peer: proto::NodeId) { self.durable.insert(peer); }
}
