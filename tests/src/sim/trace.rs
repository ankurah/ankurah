//! The event trace and its hash: the determinism-audit primitive.
//!
//! Per the testing lit review (S2, FoundationDB, WarpStream), the load-bearing
//! check that a simulation is actually deterministic is distinct from any
//! convergence check: run one seed twice and diff the full event trace to the
//! byte. A non-identical replay is a harness bug that voids every result. The
//! `Trace` records every scheduler-visible action in order; `TraceHash` folds
//! it to a digest two runs can compare.
//!
//! The trace records logical node indices and content-derived ids, never the
//! nodes' randomly generated Ed25519 public-key identities or wall-clock
//! timestamps, so it is a pure function of (seed, scenario). That is the
//! point: if anything nondeterministic leaks
//! into the schedule, the two hashes diverge and the audit fails loudly.

use sha2::{Digest, Sha256};

/// One recorded step in the simulation, in schedule order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceEvent {
    /// A node committed/originated a change for an entity at a head clock.
    Origin { node: usize, entity: String, head: String },
    /// The scheduler delivered a message from `src` to `dst` (identified by
    /// its stable content digest), optionally a duplicate.
    Deliver { src: usize, dst: usize, digest: String, duplicate: bool },
    /// The scheduler dropped a message.
    Drop { src: usize, dst: usize, digest: String },
    /// A fault-model state transition (partition raised or healed).
    Partition { a: usize, b: usize, up: bool },
    /// A quiescence barrier was reached after N drain rounds.
    Quiesced { rounds: usize },
}

impl TraceEvent {
    /// Canonical one-line rendering; the trace hash is over these lines.
    fn canonical(&self) -> String {
        match self {
            TraceEvent::Origin { node, entity, head } => format!("ORIGIN n{node} {entity} {head}"),
            TraceEvent::Deliver { src, dst, digest, duplicate } => {
                format!("DELIVER n{src}->n{dst} {digest}{}", if *duplicate { " dup" } else { "" })
            }
            TraceEvent::Drop { src, dst, digest } => format!("DROP n{src}->n{dst} {digest}"),
            TraceEvent::Partition { a, b, up } => format!("PARTITION n{a}<->n{b} {}", if *up { "heal" } else { "cut" }),
            TraceEvent::Quiesced { rounds } => format!("QUIESCE rounds={rounds}"),
        }
    }
}

/// Ordered record of everything the scheduler did. Cheap to build; the hash is
/// what runs compare.
#[derive(Debug, Default, Clone)]
pub struct Trace {
    events: Vec<TraceEvent>,
}

impl Trace {
    pub fn new() -> Self { Self::default() }

    pub fn record(&mut self, event: TraceEvent) { self.events.push(event); }

    pub fn len(&self) -> usize { self.events.len() }

    pub fn is_empty(&self) -> bool { self.events.is_empty() }

    pub fn events(&self) -> &[TraceEvent] { &self.events }

    /// Full canonical text of the trace, newline separated. Used both for the
    /// hash and for human diffing when the audit fails.
    pub fn canonical(&self) -> String { self.events.iter().map(TraceEvent::canonical).collect::<Vec<_>>().join("\n") }

    /// SHA-256 over the canonical text. Equal hashes across two runs of one
    /// seed is the determinism audit's pass condition.
    pub fn hash(&self) -> TraceHash {
        let mut hasher = Sha256::new();
        hasher.update(self.canonical().as_bytes());
        TraceHash(hasher.finalize().into())
    }
}

/// Digest of a `Trace`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceHash([u8; 32]);

impl std::fmt::Display for TraceHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::{engine::general_purpose, Engine as _};
        write!(f, "{}", general_purpose::URL_SAFE_NO_PAD.encode(self.0))
    }
}
