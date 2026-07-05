//! Deterministic multi-node simulation harness (concurrency phase 2, C1).
//!
//! Seeded virtual transport between real ankurah Nodes with reorder, delay,
//! duplication, drop, and partition faults; post-quiescence invariant checks
//! for all-node convergence, no lost write, no phantom entity, and head
//! antichain validity. The design and its tensions are documented in the
//! module docs below and in the workstream PR.
//!
//! Architecture (the C1-D fork resolved toward executor-swap over real Nodes,
//! not a state-machine rewrite, because the spec mandates real Nodes):
//!
//! - [`transport`]: `SimSender` implements the production `PeerSender` and
//!   captures every outbound message; the scheduler is the sole deliverer.
//! - [`scheduler`]: deterministic delivery with faults, quiescence by drain.
//! - [`rng`]: one seeded ChaCha8 stream, threaded explicitly; the only entropy.
//! - [`faults`]: swarm-style random fault subsets per seed.
//! - [`model`]: seed-derived entity ids and hand-forged content-hashed events,
//!   so a run is a pure function of (seed, scenario).
//! - [`trace`]: the determinism-audit primitive (same seed twice, hash equal).
//! - [`invariants`]: the four convergence-family checks.
//! - [`scenario`]: the driver, the `Workload` API, and the seeded-failure
//!   artifact line C2 will consume.
//!
//! Determinism boundaries (production entropy the harness must route around,
//! since it cannot change production code):
//!
//! - `EntityId::new()` mints a random ULID and `EventId` is a content hash over
//!   it, so the harness forges events with seed-derived entity ids rather than
//!   committing through `trx.create`. This is the one entropy source in the
//!   write path and it is fully neutralized.
//! - Correlation ids (`RequestId`, `TransactionId`, `UpdateId`, `QueryId`) stay
//!   random, but they never affect scheduling (the scheduler keys on queue
//!   position and the seeded RNG) and are excluded from the semantic trace
//!   digest, so they do not perturb the audit.
//! - `Node::get_durable_peer_random()` uses `rand::thread_rng()` and is reached
//!   by the event-gap-fill path in the SubscriptionUpdate applier
//!   (`CachedEventGetter`) when an event's parents are missing locally. The
//!   harness avoids triggering it: every event a scenario delivers arrives with
//!   its parents already present (single-event acceptance-retry on the
//!   CommitTransaction path; pre-placed lineages on the SubscriptionUpdate
//!   path). A future scenario that deliberately induces a cross-peer gap would
//!   reach this `thread_rng` and must expect a determinism-audit failure until
//!   that production path is made seedable. This boundary is flagged in the PR.
//! - Container iteration order: the scheduler holds cut links in a `BTreeSet`
//!   (not `HashSet`) so heal order is sorted, and all schedule-affecting
//!   collections are `Vec`/`BTreeSet`/`BTreeMap`. HashMaps in the harness are
//!   membership-only and never feed the trace. The scaled determinism audit is
//!   what guards this invariant against regression.
//! - Node-side emission order (latent, not reached by the audited scenarios).
//!   When a single `handle_message` emits more than one outbound message, their
//!   relative order in the capture queue is the order the production code
//!   emitted them, and two production paths order emission by hash iteration:
//!   the reactor buffers per-subscription candidates in a `HashMap`
//!   (`reactor.rs`, `candidates_by_sub`) so a node with two peer subscriptions
//!   emits its two `Update`s in randomized order, and `get_durable_peers`
//!   iterates a `HashSet`-backed `SafeSet` so a multi-durable relay emits its
//!   per-peer requests in randomized order. The relay also sends on
//!   `task::spawn` tasks whose polling interleaving the seeded RNG does not
//!   control. None of this is reachable by the current scenarios: they use one
//!   durable node and no live subscriptions, so every `handle_message` emits at
//!   most one captured message, and the audit is clean across 1500+ seeds. But
//!   the first scenario that establishes multiple subscriptions or a
//!   multi-durable topology and runs under the determinism audit can see a
//!   non-identical trace for one seed until those production emission orders are
//!   made deterministic. Flagged for the workstream-D scenarios.

pub mod faults;
pub mod invariants;
pub mod model;
pub mod node;
pub mod rng;
pub mod scenario;
pub mod scheduler;
pub mod trace;
pub mod transport;

pub use faults::FaultConfig;
pub use model::{Field, SimRecord, SimRecordView};
pub use scenario::{body, run_once, run_with_determinism_audit, sweep, ScenarioFut, SimOutcome, Workload};
