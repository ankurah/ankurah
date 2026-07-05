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
