//! Instrumentation and reporting for benchmark runs.

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Captures timing and metadata for a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    /// Phase-level timings
    pub phases: Vec<PhaseReport>,
    /// Overall benchmark duration
    pub total_duration: Duration,
    /// Configuration metadata
    pub metadata: ReportMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseReport {
    pub name: String,
    pub duration: Duration,
    pub operations: usize,
    pub ops_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportMetadata {
    pub topology: String,
    pub storage: String,
    pub connector: String,
    pub timestamp: String,
}

impl Report {
    pub fn new(metadata: ReportMetadata) -> Self { Self { phases: Vec::new(), total_duration: Duration::ZERO, metadata } }

    pub fn add_phase(&mut self, name: String, duration: Duration, operations: usize) {
        let ops_per_sec = if duration.as_secs_f64() > 0.0 { operations as f64 / duration.as_secs_f64() } else { 0.0 };
        self.phases.push(PhaseReport { name, duration, operations, ops_per_sec });
    }

    pub fn finalize(&mut self, total_duration: Duration) { self.total_duration = total_duration; }

    /// Prints a concise table summary to stdout.
    pub fn print_summary(&self) {
        println!("\n=== Benchmark Report ===");
        println!("Topology: {}", self.metadata.topology);
        println!("Storage: {}", self.metadata.storage);
        println!("Connector: {}", self.metadata.connector);
        println!("Timestamp: {}", self.metadata.timestamp);
        println!("\nPhase Results:");
        println!("{:<30} {:>12} {:>12} {:>15}", "Phase", "Duration", "Operations", "Ops/sec");
        println!("{}", "-".repeat(70));
        for phase in &self.phases {
            println!("{:<30} {:>12.3}s {:>12} {:>15.2}", phase.name, phase.duration.as_secs_f64(), phase.operations, phase.ops_per_sec);
        }
        println!("{}", "-".repeat(70));
        println!("{:<30} {:>12.3}s", "Total", self.total_duration.as_secs_f64());
        println!();
    }
}

/// Timer for measuring phase durations.
pub struct PhaseTimer {
    start: Instant,
}

impl PhaseTimer {
    pub fn start() -> Self { Self { start: Instant::now() } }

    pub fn elapsed(&self) -> Duration { self.start.elapsed() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_phase_tracking() {
        let metadata = ReportMetadata {
            topology: "1-durable-2-ephemeral".to_string(),
            storage: "postgres+sled".to_string(),
            connector: "local-process".to_string(),
            timestamp: "2024-12-01T00:00:00Z".to_string(),
        };

        let mut report = Report::new(metadata);
        report.add_phase("seed".to_string(), Duration::from_secs(2), 100);
        report.add_phase("fetch".to_string(), Duration::from_secs(1), 50);
        report.finalize(Duration::from_secs(3));

        assert_eq!(report.phases.len(), 2);
        assert_eq!(report.phases[0].name, "seed");
        assert_eq!(report.phases[0].operations, 100);
        assert_eq!(report.phases[0].ops_per_sec, 50.0);
        assert_eq!(report.total_duration, Duration::from_secs(3));
    }

    #[test]
    fn test_phase_timer() {
        let timer = PhaseTimer::start();
        std::thread::sleep(Duration::from_millis(10));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(10));
    }
}
