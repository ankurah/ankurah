//! Measures how long a live query takes to reach a durable, applied result between a durable
//! server node and an ephemeral client node, and how long a retained live query takes to install a
//! fresh result after the client reconnects. `README.md` beside this directory describes the
//! scenarios, the settings and the output.

mod cpu;
mod fixture;
mod installation;
mod link;
mod record;
mod sample;
mod scenario;
mod storage;
mod verify;

use std::{str::FromStr, time::Duration};

use ankurah::{Node, PermissiveAgent};

use scenario::{Cache, Case, Mode};
use storage::CountingStorage;

/// A benchmark node: Sled storage that counts its reads, and a policy that allows everything.
type BenchNode = Node<CountingStorage, PermissiveAgent>;

/// How long one case may run before the benchmark fails instead of recording it.
const CASE_TIMEOUT: Duration = Duration::from_secs(180);

fn main() -> anyhow::Result<()> {
    let matrix = Matrix::from_env()?;
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    runtime.block_on(async {
        for case in matrix.cases() {
            tokio::time::timeout(CASE_TIMEOUT, sample::run(&case)).await??;
        }
        Ok(())
    })
}

/// Reports progress on stderr, so a case that hangs or times out shows the step it stopped in.
fn phase(name: &str) {
    eprintln!("PHASE {name}");
}

/// The cases to run, read from the `STARTUP_*` environment variables.
struct Matrix {
    sizes: Vec<usize>,
    limit: Option<usize>,
    rtts_ms: Vec<u64>,
    bandwidths: Vec<u64>,
    modes: Vec<Mode>,
    caches: Vec<Cache>,
    repeats: usize,
    label: String,
}

impl Matrix {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            sizes: list("STARTUP_SIZES", "0,8,32,256,4096")?,
            // Absence means unlimited; an explicit zero is LIMIT 0.
            limit: std::env::var("STARTUP_LIMIT").ok().map(|limit| limit.parse()).transpose()?,
            rtts_ms: list("STARTUP_RTT_MS", "0,50")?,
            bandwidths: list("STARTUP_BPS", "0")?,
            modes: list("STARTUP_MODES", "cold")?,
            caches: list("STARTUP_CACHE", "empty,half,all")?,
            repeats: setting("STARTUP_REPEATS", "3").parse()?,
            label: setting("STARTUP_LABEL", "candidate"),
        })
    }

    /// Every runnable combination, one repeat of the whole matrix after another.
    fn cases(&self) -> Vec<Case<'_>> {
        let mut cases = Vec::new();
        for repeat in 0..self.repeats {
            for &entities in &self.sizes {
                for &rtt_ms in &self.rtts_ms {
                    for &bytes_per_second in &self.bandwidths {
                        for &mode in &self.modes {
                            for &cache in &self.caches {
                                let case =
                                    Case { entities, limit: self.limit, cache, mode, rtt_ms, bytes_per_second, repeat, label: &self.label };
                                if case.is_runnable() {
                                    cases.push(case);
                                }
                            }
                        }
                    }
                }
            }
        }
        cases
    }
}

/// Reads a comma-separated setting, or `default` when the variable is unset.
fn list<T>(name: &str, default: &str) -> anyhow::Result<Vec<T>>
where
    T: FromStr,
    anyhow::Error: From<T::Err>,
{
    setting(name, default).split(',').map(|value| Ok(value.parse()?)).collect()
}

fn setting(name: &str, default: &str) -> String { std::env::var(name).unwrap_or_else(|_| default.into()) }
