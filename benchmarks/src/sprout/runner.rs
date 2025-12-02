//! Topology orchestration and lifecycle management.

use crate::sprout::{WorkloadConfig, workloads};
use ankurah::{Context, Node};
use ankurah_core::policy::PolicyAgent;
use ankurah_core::storage::StorageEngine;
use anyhow::{Result, anyhow};
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
    prepared_contexts: Vec<Context>,
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
    pub fn new(config: WorkloadConfig<SE, PA, F, Fut>) -> Self { Self { config, prepared_contexts: Vec::new() } }

    /// Prepares the requested number of durable nodes and stores their contexts internally.
    pub async fn setup(&mut self, node_count: usize) -> Result<()> {
        self.prepared_contexts.clear();

        for index in 0..node_count {
            let node = (self.config.durable_node_factory)(index).await?;
            node.system.create().await?;
            let ctx = node.context(self.config.context_data.clone())?;
            self.prepared_contexts.push(ctx);
        }

        Ok(())
    }

    /// Runs the workload on all prepared contexts sequentially.
    pub async fn execute(&self) -> Result<()> {
        if self.prepared_contexts.is_empty() {
            return Err(anyhow!("Runner::execute called before setup"));
        }

        for ctx in &self.prepared_contexts {
            self.execute_for_context(ctx).await?;
        }

        Ok(())
    }

    async fn execute_for_context(&self, ctx: &Context) -> Result<()> {
        workloads::seed_albums(ctx, self.config.seed_entity_count).await?;
        workloads::seed_artists(ctx, self.config.seed_entity_count).await?;

        workloads::workload_readonly_fetch(ctx, self.config.fetch_rounds).await?;

        workloads::limit_gap_queries(ctx, self.config.fetch_rounds).await?;

        // workloads::concurrent_mutations(ctx, self.config.concurrency).await?;

        workloads::subscription_churn(ctx, self.config.subscription_churn_cycles).await?;

        workloads::subscribe_rounds(ctx, self.config.subscription_churn_cycles).await?;

        Ok(())
    }
}
