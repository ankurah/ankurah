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
    pub expected_results: usize,
    pub selection: &'a str,
    pub cache: Cache,
    pub cached_entities: usize,
    pub mode: Mode,
    pub bandwidth_bytes_per_second: u64,
    pub rtt_ms: u64,
    pub repeat: usize,
    pub elapsed_us: u128,
    pub ready_us: Option<u128>,
    pub local_result_installed_us: Option<u128>,
    pub local_result_capture_us: Option<u128>,
    pub initial_drained_us: u128,
    pub settled_us: u128,
    pub transport_settled_us: u128,
    pub verified_us: u128,
    pub verification_us: u128,
    pub verification_attempts: usize,
    pub verified: bool,
    pub traffic: Traffic,
    pub settled_traffic: Traffic,
    pub server_reads: StorageReads,
    pub client_reads: StorageReads,
    pub phase_process_cpu: PhaseCpu,
}

/// The process CPU used in each phase of the sample.
#[derive(Serialize)]
pub struct PhaseCpu {
    pub scope: &'static str,
    pub setup: Option<ProcessCpu>,
    pub through_ready: Option<ProcessCpu>,
    pub through_initial_drain: Option<ProcessCpu>,
    pub through_local_result_installed: Option<ProcessCpu>,
    pub local_result_capture: Option<ProcessCpu>,
    pub verification: Option<ProcessCpu>,
    pub through_verified: Option<ProcessCpu>,
    pub after_verification_drain: Option<ProcessCpu>,
    pub through_final_drain: Option<ProcessCpu>,
}

impl Record<'_> {
    pub fn print(&self) -> anyhow::Result<()> {
        println!("STARTUP {}", serde_json::to_string(self)?);
        Ok(())
    }
}
