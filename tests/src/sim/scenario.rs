//! The scenario driver: builds real Nodes over the virtual transport, runs a
//! seeded workload, enforces a quiescence barrier, and checks invariants.
//!
//! A scenario is a closure that, given a `Workload` handle, issues a sequence
//! of origin commits (each of which the harness propagates to every other node
//! through the scheduler under the seed's fault config). All entity ids and
//! event contents are seed-derived, so a run is a pure function of (seed,
//! scenario). The driver runs everything on a single-threaded tokio runtime so
//! intra-node task poll order is deterministic.

use ankurah::proto::{self, Attested};

use super::faults::FaultConfig;
use super::invariants::{self, ExpectedUniverse, Violation};
use super::model::{self, Field};
use super::node::{build_nodes, SimNode};
use super::rng::SimRng;
use super::scheduler::Scheduler;
use super::trace::{Trace, TraceEvent, TraceHash};
use super::transport::Captured;

/// Handle passed to a scenario body for issuing workload operations. Owns the
/// entity-id counter (so ids are deterministic and collision-free) and the
/// running set of created entities (the invariant universe).
pub struct Workload<'a> {
    nodes: &'a [SimNode],
    scheduler: &'a mut Scheduler,
    rng: &'a mut SimRng,
    trace: &'a mut Trace,
    next_entity: u64,
    created: Vec<proto::EntityId>,
    /// Cached current head per entity, so an edit can parent correctly without
    /// reading a node (reads would be nondeterministic mid-flight; the harness
    /// tracks the origin head it just produced).
    heads: std::collections::HashMap<proto::EntityId, proto::Clock>,
}

impl<'a> Workload<'a> {
    /// Number of nodes in the simulation.
    pub fn node_count(&self) -> usize { self.nodes.len() }

    /// Pick a node index uniformly from the seed (e.g. to choose an origin).
    pub fn any_node(&mut self) -> usize { self.rng.below(self.nodes.len()) }

    /// Create a new entity at `origin` with an initial field write, propagate
    /// it to all other nodes through the scheduler, and return its id. The id
    /// is deterministic (seed-derived counter).
    pub async fn create_at(&mut self, origin: usize, field: Field, value: &str) -> proto::EntityId {
        let id = model::entity_id(self.next_entity);
        self.next_entity += 1;

        let event = model::genesis_event(id, field, value);
        let head = proto::Clock::from(vec![event.id()]);
        let attested = model::attest(event);

        self.commit_and_propagate(origin, id, vec![attested], &head).await;
        self.created.push(id);
        self.heads.insert(id, head);
        id
    }

    /// Edit an existing entity at `origin`, parented on the head the harness
    /// last produced for it, propagate to all nodes, and update the tracked
    /// head. Concurrent edits (same parent, different origins) are produced by
    /// calling this against a stale head captured before an interleaving edit.
    pub async fn edit_at(&mut self, origin: usize, entity: proto::EntityId, field: Field, value: &str) {
        let parent = self.heads.get(&entity).cloned().unwrap_or_default();
        self.edit_from(origin, entity, parent, field, value).await;
    }

    /// Edit an existing entity parented on an explicit clock. This is how a
    /// scenario constructs genuinely concurrent updates and the stale-client
    /// multi-head shape: capture a head, let another edit advance the tracked
    /// head, then edit again from the captured (now stale) head.
    pub async fn edit_from(&mut self, origin: usize, entity: proto::EntityId, parent: proto::Clock, field: Field, value: &str) {
        let event = model::edit_event(entity, parent, field, value);
        let new_head = proto::Clock::from(vec![event.id()]);
        let attested = model::attest(event);
        self.commit_and_propagate(origin, entity, vec![attested], &new_head).await;
        self.heads.insert(entity, new_head);
    }

    /// The head clock the harness last produced for an entity (for building
    /// concurrent edits deliberately).
    pub fn head_of(&self, entity: proto::EntityId) -> Option<proto::Clock> { self.heads.get(&entity).cloned() }

    /// Drive the scheduler to quiescence mid-workload, so every node has
    /// received everything committed so far. Use this before issuing concurrent
    /// edits from multiple origins: a node can only edit an entity it holds, so
    /// the base state must have reached those origins first. The concurrency is
    /// still genuine (the concurrent edits do not observe one another until the
    /// next drain), and faults still perturb their propagation. Faults during a
    /// mid-workload settle are healed at its barrier, exactly as at the end.
    pub async fn settle(&mut self) { self.scheduler.run_to_quiescence(self.nodes, self.rng, self.trace).await; }

    /// Deliver a raw, harness-constructed subscription-update batch from
    /// `origin` to `dst` through the scheduler. This is the seam scenarios use
    /// to reproduce the V4 (adversarial bridge order) and V6 (unknown-entity
    /// item in a batch) wire shapes precisely, since those live in the
    /// SubscriptionUpdate applier rather than the CommitTransaction path.
    pub fn deliver_raw_update(&mut self, origin: usize, dst: usize, message: proto::NodeMessage) {
        self.scheduler.enqueue(origin, dst, message);
    }

    /// Register an entity id as expected in the invariant universe without a
    /// commit (for scenarios that assert an entity must NOT appear, the phantom
    /// case). Returns a deterministic fresh id not used by `create_at`.
    pub fn reserve_unknown_entity(&mut self) -> proto::EntityId {
        // Use a high, disjoint id range so it can never collide with a
        // counter-derived created id.
        let id = model::entity_id(u64::MAX - self.next_entity);
        self.next_entity += 1;
        id
    }

    /// Commit `events` at `origin` (real applier), then enqueue propagation to
    /// every other node.
    ///
    /// Each event is propagated as its own single-event `CommitTransaction`,
    /// which the scheduler redelivers until the receiver holds it. Single-event
    /// messages have no intra-batch ordering to get wrong, and the acceptance
    /// retry converges any causal order: an edit that reaches a node before its
    /// create is rejected by the empty-head guard and retried until the create
    /// lands. Concurrent edits from different origins still merge via
    /// DivergedSince at the receiver. This models a transport that keeps trying
    /// until delivery, which is the honest floor for "no lost write".
    async fn commit_and_propagate(
        &mut self,
        origin: usize,
        entity: proto::EntityId,
        events: Vec<Attested<proto::Event>>,
        head: &proto::Clock,
    ) {
        // Seed the change at the origin through the production remote-commit
        // path (no transport; this is the origin's own durable write).
        self.nodes[origin].origin_commit(events.clone()).await.expect("origin commit applies");
        self.trace.record(TraceEvent::Origin { node: origin, entity: entity.to_base64_short(), head: head.to_base64_short() });

        let origin_id = self.nodes[origin].id();
        let node_indices: Vec<usize> = self.nodes.iter().map(|n| n.index).filter(|&i| i != origin).collect();
        let node_ids: Vec<proto::EntityId> = self.nodes.iter().map(|n| n.id()).collect();

        for event in &events {
            let event_id = event.payload.id();
            for &dst in &node_indices {
                let request = proto::NodeRequest {
                    id: proto::RequestId::new(),
                    to: node_ids[dst],
                    from: origin_id,
                    body: proto::NodeRequestBody::CommitTransaction { id: proto::TransactionId::new(), events: vec![event.clone()] },
                };
                let message = proto::NodeMessage::Request { auth: vec![proto::AuthData(vec![])], request };
                self.scheduler.enqueue_event(origin, dst, entity, event_id.clone(), message);
            }
        }
    }
}

/// Outcome of one simulation run.
pub struct SimOutcome {
    pub seed: u64,
    pub faults: FaultConfig,
    pub scenario: &'static str,
    pub trace_hash: TraceHash,
    pub trace_len: usize,
    pub violations: Vec<Violation>,
    /// Full canonical trace text, retained for post-mortem inspection when an
    /// invariant fails.
    pub trace_text: String,
    /// Largest head-clock length seen at quiescence. `>= 2` proves the run
    /// produced a genuine multi-head, so the antichain invariant was exercised
    /// non-trivially.
    pub max_head_len: usize,
}

impl SimOutcome {
    pub fn ok(&self) -> bool { self.violations.is_empty() }

    /// The self-contained, one-line seeded-failure artifact. This exact format
    /// is what C2's nightly scale-out consumes: seed, scenario, fault config,
    /// and the failing invariants, sufficient to reproduce from the log line
    /// alone.
    pub fn artifact_line(&self) -> String {
        format!(
            "SIMFAIL seed={} scenario={} faults={} trace_hash={} violations=[{}]",
            self.seed,
            self.scenario,
            self.faults.summary(),
            self.trace_hash,
            self.violations.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("; ")
        )
    }

    /// Panic with the artifact line if any invariant failed. The standard way a
    /// scenario test asserts success.
    pub fn assert_converged(&self) {
        if !self.ok() {
            panic!("{}", self.artifact_line());
        }
    }
}

/// A scenario is an async closure over a `Workload`. It is invoked once per run
/// with a fresh, seeded world. The lifetime is the *borrow* of the `Workload`,
/// which is deliberately shorter than the lifetime of the data the `Workload`
/// borrows, so the driver can reclaim state (the created-entity set) once the
/// body's future has completed.
pub type ScenarioFut<'b> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'b>>;

/// Identity helper that pins a closure to the scenario-body type so the
/// higher-ranked lifetime is inferred at the call site. Without it, Rust cannot
/// infer the `Workload` argument type of an inline `|w| Box::pin(async ...)`
/// closure passed to a HRTB-bounded parameter. Write scenario bodies as
/// `body(|w: &mut Workload| Box::pin(async move { ... }))`.
pub fn body<F>(f: F) -> F
where F: for<'w, 'b> FnOnce(&'b mut Workload<'w>) -> ScenarioFut<'b> {
    f
}

/// Run one scenario at one seed with one fault config, returning the outcome
/// (trace hash + invariant violations). Drives everything on a single-threaded
/// runtime for deterministic intra-node scheduling.
pub fn run_once<F>(scenario_name: &'static str, seed: u64, faults: FaultConfig, node_count: usize, body: F) -> SimOutcome
where F: for<'w, 'b> FnOnce(&'b mut Workload<'w>) -> ScenarioFut<'b> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("current-thread runtime builds");

    runtime.block_on(async move {
        let mut rng = SimRng::new(seed);
        let captured = Captured::new();
        let nodes = build_nodes(node_count, captured.clone()).await.expect("nodes build");

        // Full mesh: every node knows every other. Ephemeral nodes join node 0's
        // system as a side effect of registering it as a durable peer.
        for a in &nodes {
            for b in &nodes {
                if a.index != b.index {
                    a.connect_to(b);
                }
            }
        }

        let node_ids: Vec<proto::EntityId> = nodes.iter().map(|n| n.id()).collect();
        let mut scheduler = Scheduler::new(captured.clone(), faults, node_ids);
        let mut trace = Trace::new();

        // Let the system settle (ephemeral nodes join node 0). This runs under
        // the *actual* fault config; even join traffic is subject to the
        // schedule, matching a real cold start.
        scheduler.run_to_quiescence(&nodes, &mut rng, &mut trace).await;

        // Run the workload in an inner scope so its mutable borrows of the
        // scheduler/rng/trace end before the quiescence barrier reuses them.
        let created = {
            let mut workload = Workload {
                nodes: &nodes,
                scheduler: &mut scheduler,
                rng: &mut rng,
                trace: &mut trace,
                next_entity: 0,
                created: Vec::new(),
                heads: std::collections::HashMap::new(),
            };
            body(&mut workload).await;
            std::mem::take(&mut workload.created)
        };

        // Quiescence barrier: drain all in-flight messages, healing partitions,
        // so the convergence check is well posed.
        scheduler.run_to_quiescence(&nodes, &mut rng, &mut trace).await;

        let universe = ExpectedUniverse { created };
        let violations = invariants::check_all(&nodes, &universe).await;
        let max_head_len = invariants::max_head_len(&nodes, &universe).await;

        SimOutcome {
            seed,
            faults: scheduler.faults(),
            scenario: scenario_name,
            trace_hash: trace.hash(),
            trace_len: trace.len(),
            violations,
            trace_text: trace.canonical(),
            max_head_len,
        }
    })
}

/// The determinism audit: run the same (scenario, seed, faults) twice and
/// require byte-identical traces. A mismatch is a harness determinism bug that
/// voids every result, so this is asserted, not merely logged. Returns the
/// (first-run) outcome so callers can also assert invariants.
pub fn run_with_determinism_audit<F, B>(
    scenario_name: &'static str,
    seed: u64,
    faults: FaultConfig,
    node_count: usize,
    body: F,
) -> SimOutcome
where
    F: Fn() -> B,
    B: for<'w, 'b> FnOnce(&'b mut Workload<'w>) -> ScenarioFut<'b>,
{
    let first = run_once(scenario_name, seed, faults, node_count, body());
    let second = run_once(scenario_name, seed, faults, node_count, body());
    assert_eq!(
        first.trace_hash, second.trace_hash,
        "DETERMINISM AUDIT FAILED for scenario={scenario_name} seed={seed}: identical seed produced different traces \
         (hashes {} vs {}). A nondeterminism leak voids all results.",
        first.trace_hash, second.trace_hash
    );
    first
}
