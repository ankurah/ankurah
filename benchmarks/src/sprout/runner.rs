//! Topology orchestration and lifecycle management.

use crate::sprout::{WorkloadConfig, workloads};
use ankurah::Node;
use ankurah_core::policy::PolicyAgent;
use ankurah_core::storage::StorageEngine;
use anyhow::Result;
use std::future::Future;

/// Orchestrates benchmark workload execution.
pub struct Runner<SE, PA, F, Fut>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    PA::ContextData: Clone + Send + Sync + 'static,
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Node<SE, PA>>> + Send + 'static,
{
    config: WorkloadConfig<SE, PA, F, Fut>,
}

impl<SE, PA, F, Fut> Runner<SE, PA, F, Fut>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    PA::ContextData: Clone + Send + Sync + 'static,
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Node<SE, PA>>> + Send + 'static,
{
    /// Creates a new runner with the given workload configuration.
    pub fn new(config: WorkloadConfig<SE, PA, F, Fut>) -> Self { Self { config } }

    /// Executes the complete benchmark workload.
    pub async fn run(&self) -> Result<()> {
        // Create the durable node
        let node = (self.config.durable_node_factory)(0).await?;

        // Initialize the system
        node.system.create().await?;

        // Get a context for the workload
        let ctx = node.context(self.config.context_data.clone())?;

        // Phase 1: Seed dataset
        workloads::seed_albums(&ctx, self.config.seed_entity_count).await?;

        // Phase 2: Fetch rounds
        workloads::fetch_rounds(&ctx, self.config.fetch_rounds).await?;

        // Phase 3: Limit/gap queries
        workloads::limit_gap_queries(&ctx, self.config.fetch_rounds).await?;

        // Phase 4: Concurrent mutations
        workloads::concurrent_mutations(&ctx, self.config.concurrency).await?;

        // Phase 5: Subscription churn
        workloads::subscription_churn(&ctx, self.config.subscription_churn_cycles).await?;

        Ok(())
    }
}
