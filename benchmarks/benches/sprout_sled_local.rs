//! Sprout benchmark: Sled single durable node.

use ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent};
use ankurah_benchmarks::sprout::{Runner, WorkloadConfig};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn sled_single_node(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("sled_single_node", |b| {
        b.to_async(&rt).iter(|| async {
            let config = WorkloadConfig::builder(
                Arc::new(|_index| -> Result<Node<SledStorageEngine, PermissiveAgent>> {
                    let storage = SledStorageEngine::new_test()?;
                    Ok(Node::new_durable(Arc::new(storage), PermissiveAgent::new()))
                }),
                DEFAULT_CONTEXT,
            )
            .seed_entity_count(100)
            .concurrency(4)
            .fetch_rounds(10)
            .subscription_churn_cycles(5)
            .build();

            let runner = Runner::new(config);
            runner.run().await.unwrap();
        });
    });
}

criterion_group!(benches, sled_single_node);
criterion_main!(benches);

