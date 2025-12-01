//! Sprout benchmark: Postgres single durable node (websocket connector variant).
//!
//! NOTE: Currently disabled until single-node case is fully working.

use criterion::{criterion_group, criterion_main, Criterion};

fn postgres_websocket_placeholder(c: &mut Criterion) {
    c.bench_function("postgres_websocket_placeholder", |b| {
        b.iter(|| {
            // Disabled until single-node baseline is complete
            std::hint::black_box(1);
        });
    });
}

criterion_group!(benches, postgres_websocket_placeholder);
criterion_main!(benches);
