//! Configuration types for benchmark workload parameters.

use ankurah::Node;
use ankurah_core::policy::PolicyAgent;
use ankurah_core::storage::StorageEngine;
use anyhow::Result;
use std::future::Future;

/// Complete workload specification including node factories and workload parameters.
pub struct WorkloadConfig<SE, PA, F, Fut>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    PA::ContextData: Clone + Send + Sync + 'static,
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Node<SE, PA>>> + Send + 'static,
{
    /// Factory closure for creating durable nodes. Takes index and returns a Node.
    pub durable_node_factory: F,

    /// Optional factory closure for creating ephemeral nodes. Takes index and returns a Node.
    pub ephemeral_node_factory: Option<F>,

    /// Context data to use for all operations
    pub context_data: PA::ContextData,

    /// Number of entities to seed in the database
    pub seed_entity_count: usize,

    /// Concurrency level for concurrent mutation operations
    pub concurrency: usize,

    /// Number of sequential fetch rounds to execute
    pub fetch_rounds: usize,

    /// Limit applied to read-heavy fetch workloads to bound result volumes
    pub fetch_limit: usize,

    /// Number of subscription setup/teardown cycles to measure notification latency
    pub subscription_churn_cycles: usize,
}

impl<SE, PA, F, Fut> WorkloadConfig<SE, PA, F, Fut>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    PA::ContextData: Clone + Send + Sync + 'static,
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Node<SE, PA>>> + Send + 'static,
{
    /// Creates a builder for a single durable node workload.
    pub fn builder(durable_node_factory: F, context_data: PA::ContextData) -> WorkloadConfigBuilder<SE, PA, F, Fut> {
        WorkloadConfigBuilder {
            durable_node_factory,
            ephemeral_node_factory: None,
            context_data,
            seed_entity_count: 1000,
            concurrency: 4,
            fetch_rounds: 10,
            fetch_limit: 100,
            subscription_churn_cycles: 5,
        }
    }
}

/// Builder for WorkloadConfig with sensible defaults.
pub struct WorkloadConfigBuilder<SE, PA, F, Fut>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    PA::ContextData: Clone + Send + Sync + 'static,
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Node<SE, PA>>> + Send + 'static,
{
    durable_node_factory: F,
    ephemeral_node_factory: Option<F>,
    context_data: PA::ContextData,
    seed_entity_count: usize,
    concurrency: usize,
    fetch_rounds: usize,
    fetch_limit: usize,
    subscription_churn_cycles: usize,
}

impl<SE, PA, F, Fut> WorkloadConfigBuilder<SE, PA, F, Fut>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
    PA::ContextData: Clone + Send + Sync + 'static,
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Node<SE, PA>>> + Send + 'static,
{
    pub fn ephemeral_node_factory(mut self, factory: F) -> Self {
        self.ephemeral_node_factory = Some(factory);
        self
    }

    pub fn seed_entity_count(mut self, count: usize) -> Self {
        self.seed_entity_count = count;
        self
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn fetch_rounds(mut self, rounds: usize) -> Self {
        self.fetch_rounds = rounds;
        self
    }

    pub fn fetch_limit(mut self, limit: usize) -> Self {
        self.fetch_limit = limit.max(1);
        self
    }

    pub fn subscription_churn_cycles(mut self, cycles: usize) -> Self {
        self.subscription_churn_cycles = cycles;
        self
    }

    pub fn build(self) -> WorkloadConfig<SE, PA, F, Fut> {
        WorkloadConfig {
            durable_node_factory: self.durable_node_factory,
            ephemeral_node_factory: self.ephemeral_node_factory,
            context_data: self.context_data,
            seed_entity_count: self.seed_entity_count,
            concurrency: self.concurrency,
            fetch_rounds: self.fetch_rounds,
            fetch_limit: self.fetch_limit,
            subscription_churn_cycles: self.subscription_churn_cycles,
        }
    }
}
