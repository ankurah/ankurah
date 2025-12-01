//! Sprout benchmark: Postgres single durable node.

mod common;

use ankurah::{policy::DEFAULT_CONTEXT, Node, PermissiveAgent};
use ankurah_benchmarks::sprout::{Runner, WorkloadConfig};
use common::postgres::{clear_database, create_container, create_storage};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use tokio::runtime::Runtime;

fn postgres_single_node(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Create container once outside the benchmark loop
    let (_container, connection_string) = create_container().unwrap();
    let connection_string: &'static str = Box::leak(connection_string.into_boxed_str());

    c.bench_function("postgres_single_node", |b| {
        b.to_async(&rt).iter_batched(
            move || {
                // Setup: clear database before each iteration (not measured)
                clear_database(connection_string).unwrap();
            },
            async |_| {
                let config = WorkloadConfig::builder(
                    async |_index| {
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

                let runner = Runner::new(config);
                runner.run().await.unwrap();
            },
            BatchSize::LargeInput,
        );
    });
}

criterion_group!(benches, postgres_single_node);
criterion_main!(benches);
