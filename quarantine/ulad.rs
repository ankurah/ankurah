use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    ops::Deref,
};

/// Unique Lexicographic Agentic ID
/// An id which can be cryptographically verified to be uniquely generated by a given agent

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pubkey(pub [u8; 32]);
impl Deref for Pubkey {
    type Target = [u8; 32];
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Hash for Pubkey {
    fn hash<H: Hasher>(&self, state: &mut H) { self.0.hash(state); }
}

pub struct ULAD {
    timestamp: u64,
    agent: Pubkey,
    sequence: u64,
}

pub struct ULADCompact {
    timestamp: u64,
    agent: u64,
    sequence: u64,
}

use once_cell::sync::Lazy;

// construct a thread-local agent registry using lazy static
pub static AGENT_REGISTRY: Lazy<AgentRegistry> = Lazy::new(|| AgentRegistry::new());

impl From<ULAD> for ULADCompact {
    fn from(lad: ULAD) -> Self { Self { timestamp: lad.timestamp, agent: AGENT_REGISTRY.pubkey_to_id(&lad.agent), sequence: lad.sequence } }
}
impl From<ULADCompact> for ULAD {
    fn from(lad: ULADCompact) -> Self {
        Self { timestamp: lad.timestamp, agent: AGENT_REGISTRY.id_to_pubkey(lad.agent).unwrap(), sequence: lad.sequence }
    }
}

pub struct AgentRegistry {
    inner: Mutex<AgentRegistryInner>,
}

struct AgentRegistryInner {
    agent_counter: u64,
    forward_map: HashMap<Pubkey, u64>,
    reverse_map: HashMap<u64, Pubkey>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self { inner: Mutex::new(AgentRegistryInner { agent_counter: 0, forward_map: HashMap::new(), reverse_map: HashMap::new() }) }
    }

    pub fn pubkey_to_id(&self, agent: &Pubkey) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        if let Some(&id) = inner.forward_map.get(agent) {
            id
        } else {
            let agent_id = inner.agent_counter;
            inner.agent_counter += 1;
            inner.forward_map.insert(agent.clone(), agent_id);
            inner.reverse_map.insert(agent_id, agent.clone());
            agent_id
        }
    }

    pub fn id_to_pubkey(&self, agent_id: u64) -> Option<Pubkey> {
        let inner = self.inner.lock().unwrap();
        inner.reverse_map.get(&agent_id).cloned()
    }
}
