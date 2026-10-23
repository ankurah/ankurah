//! The `STARTUP` line each sample prints: the case, its landmarks, the traffic and storage reads
//! behind them, and the process CPU each phase used.

use serde::Serialize;

use crate::{
    cpu::ProcessCpu,
    link::Traffic,
    scenario::{Cache, Mode},
    storage::StorageReads,
};

/// One sample's measurements. Landmarks are microseconds since the measured operation began.
#[derive(Serialize)]
pub struct Record<'a> {
    pub label: &'a str,
    pub entities: usize,
    pub result_limit: Option<usize>,
    /// How many members the measured query must hold.
    pub expected_results: usize,
    pub selection: &'a str,
    pub cache: Cache,
    /// How many rows the client fetched before the scenario began.
    pub cached_entities: usize,
    pub mode: Mode,
    pub bandwidth_bytes_per_second: u64,
    pub rtt_ms: u64,
    pub repeat: usize,
    /// The scenario's endpoint: `ready_us` or `local_result_installed_us`.
    pub elapsed_us: u128,
    /// When the durable-answer wait returned, for the scenarios that end there.
    pub ready_us: Option<u128>,
    /// When a reconnected query installed a fresh result.
    pub local_result_installed_us: Option<u128>,
    /// How long capturing that result took; `elapsed_us` leaves it out.
    pub local_result_capture_us: Option<u128>,
    /// When verification finished.
    pub verified_us: u128,
    pub verification_us: u128,
    /// The last link activity before the link went quiet. It can precede the endpoint and
    /// `verified_us`.
    pub transport_settled_us: u128,
    /// Always true: a sample that fails verification fails the run instead of printing.
    pub verified: bool,
    /// The traffic from the start of the measured operation until the link settled.
    pub traffic: Traffic,
    /// Each node's storage reads over the same interval, without the verifier's own point reads.
    pub server_reads: StorageReads,
    pub client_reads: StorageReads,
    pub phase_process_cpu: PhaseCpu,
}

/// The process CPU used in each phase of a sample. The `through_*` intervals all start with the
/// measured operation, so they overlap. The intervals that end when the link settled include its
/// 2 ms quiet confirmation, which `transport_settled_us` leaves out.
#[derive(Serialize)]
pub struct PhaseCpu {
    /// Everything before the measured operation: population, prefetch and the scenario's preparation.
    pub setup: Option<ProcessCpu>,
    pub through_ready: Option<ProcessCpu>,
    pub through_local_result_installed: Option<ProcessCpu>,
    pub local_result_capture: Option<ProcessCpu>,
    pub verification: Option<ProcessCpu>,
    pub through_verified: Option<ProcessCpu>,
    pub through_transport_settled: Option<ProcessCpu>,
    pub verified_to_transport_settled: Option<ProcessCpu>,
}

impl Record<'_> {
    pub fn print(&self) -> anyhow::Result<()> {
        println!("STARTUP {}", serde_json::to_string(self)?);
        Ok(())
    }
}
