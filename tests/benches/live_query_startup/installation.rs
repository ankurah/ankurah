//! The reconnect endpoint: the moment a retained live query installs a fresh result after the
//! client reconnects.
//!
//! The benchmark must mark a reconnect the same way on every revision it compares, and not every
//! revision resets a retained query's durable answer when the connection drops. So before the
//! client reconnects, the probe clears the retained result set's loaded flag, and the endpoint is
//! the first notification that finds the flag set again. Notifications about intermediate
//! membership changes still find it cleared.

use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use ankurah::LiveQuery;
use ankurah_signals::{ListenerGuard, Signal};

use crate::{cpu::ProcessCpu, fixture::StartupRowView, link::Link, verify::InstalledResult};

/// The fresh installation a probe observed.
pub struct Installation {
    /// When a notification found the result set loaded.
    pub at: Instant,
    /// The process CPU reading at that moment.
    pub cpu: Option<ProcessCpu>,
    /// The result as installed.
    pub result: InstalledResult,
    /// How long capturing the result took.
    pub capture_us: u128,
    /// The process CPU the capture used.
    pub capture_cpu: Option<ProcessCpu>,
}

/// Watches a retained query's result set for its first fresh installation.
pub struct InstallationProbe {
    installation: Arc<OnceLock<anyhow::Result<Installation>>>,
    _listener: ListenerGuard,
}

impl InstallationProbe {
    /// Clears the query's loaded flag and listens for it to be set again. The result is captured
    /// inside that notification with `link` paused, so no message delivered after the installation
    /// can repair the result before verification reads it.
    pub fn watch(query: &LiveQuery<StartupRowView>, link: &Link, ordered: bool) -> Self {
        let resultset = query.resultset();
        resultset.set_loaded(false);
        let installation = Arc::new(OnceLock::new());
        let listener = {
            let installation = installation.clone();
            let link = link.clone();
            let observed = query.resultset();
            resultset.listen(Arc::new(move |()| {
                if observed.is_loaded() {
                    link.paused(|| {
                        if installation.get().is_none() {
                            let _ = installation.set(Installation::capture(&observed, ordered));
                        }
                    });
                }
            }))
        };
        Self { installation, _listener: listener }
    }

    /// Waits for the installation. Fails if the query or a node reports an error first.
    pub async fn wait(&self, query: &LiveQuery<StartupRowView>, link: &Link) -> anyhow::Result<&Installation> {
        loop {
            if let Some(error) = query.error().value() {
                anyhow::bail!("reconnect initialization failed: {error}");
            }
            link.check_errors()?;
            if let Some(installation) = self.installation.get() {
                return installation.as_ref().map_err(|error| anyhow::anyhow!("installation capture: {error:#}"));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

impl Installation {
    fn capture(resultset: &ankurah::ResultSet<StartupRowView>, ordered: bool) -> anyhow::Result<Self> {
        let at = Instant::now();
        let cpu = ProcessCpu::now()?;
        let capture_start = Instant::now();
        let result = InstalledResult::capture(resultset, ordered);
        let capture_us = capture_start.elapsed().as_micros();
        let capture_cpu = ProcessCpu::between(cpu, ProcessCpu::now()?);
        Ok(Self { at, cpu, result: result?, capture_us, capture_cpu })
    }
}
