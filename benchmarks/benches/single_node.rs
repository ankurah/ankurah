//! Sprout benchmarks: single durable node for sled and Postgres.

mod common;

use ankurah::{Node, PermissiveAgent, policy::DEFAULT_CONTEXT};
use ankurah_benchmarks::sprout::{Runner, WorkloadConfig};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::postgres::{clear_database, create_container, create_storage};
use criterion::{Criterion, criterion_group, criterion_main};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;

fn postgres_single_node(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (_container, connection_string) = create_container().unwrap();
    let connection_string: &'static str = Box::leak(connection_string.into_boxed_str());

    c.bench_function("postgres_single_node", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                clear_database(connection_string).await.unwrap();

                let config = WorkloadConfig::builder(
                    async move |_index| {
                        let storage = create_storage(connection_string).await?;
                        Ok(Node::new_durable(storage, PermissiveAgent::new()))
                    },
                    DEFAULT_CONTEXT,
                )
                .seed_entity_count(100)
                .concurrency(4)
                .fetch_rounds(10)
                .subscription_churn_cycles(5)
                .build();

                let mut runner = Runner::new(config);
                runner.setup(1).await.unwrap();
                let start = Instant::now();
                runner.execute().await.unwrap();
                total += start.elapsed();
            }

            total
        });
    });
}

fn sled_single_node(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("sled_single_node", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                let config = WorkloadConfig::builder(
                    async move |_index| -> Result<Node<SledStorageEngine, PermissiveAgent>> {
                        let storage = Arc::new(SledStorageEngine::new_test()?);
                        Ok(Node::new_durable(storage, PermissiveAgent::new()))
                    },
                    DEFAULT_CONTEXT,
                )
                .seed_entity_count(100)
                .concurrency(4)
                .fetch_rounds(10)
                .subscription_churn_cycles(5)
                .build();

                let mut runner = Runner::new(config);
                runner.setup(1).await.unwrap();
                let start = Instant::now();
                runner.execute().await.unwrap();
                total += start.elapsed();
            }

            total
        });
    });
}

criterion_group!(benches, postgres_single_node, sled_single_node);
criterion_main!(benches);
