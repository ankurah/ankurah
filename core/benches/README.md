# Event DAG benchmarks

Criterion micro-benchmarks for the event DAG engine, landed at the start of
concurrency phase 2 (workstream E1) so a baseline predates the workstream D
refactors. The companion baseline numbers and methodology live in
[`BASELINE.md`](./BASELINE.md).

## What is measured

One bench target, `event_dag` (`core/benches/event_dag.rs`), with four groups:

- **`compare`**: the causal comparison `compare()` across DAG shapes:
  - `linear_deep` (depths 10, 100, 1000): head vs root, a `StrictDescends`
    that walks the whole chain.
  - `diamond_chain` (4, 16, 64 diamonds): two concurrent tips, `DivergedSince`.
  - `wide_antichain` (widths 4, 16, 64): a join event vs the root, stressing
    multi-parent frontier expansion.
  - `disjoint` (depths 10, 100): two independent roots, `Disjoint`.
- **`layers`**: draining a `DivergedSince` comparison into topological
  `EventLayer`s (`diamond_chain_drain`).
- **`clock`**: `Clock` operations at sizes 8, 64, 512:
  `contains` (membership), `normalize_from` (sort + dedup construction),
  `with_event` (clone + insert).
- **`toposort`**: `topo_sort_events` on a shuffled linear batch (16, 128, 1024).

DAG generation is deterministic. Event ids are content addressed, so a given
shape at a given size always produces the same DAG, and no RNG state is carried
between cases. Events are served from an in-memory `GetEvents` map
(`MemRetriever`), so no storage engine is in the hot path.

## Running

The engine lives in a `pub(crate)` module; the benches reach it through the
feature-gated `ankurah_core::bench_support` wrappers, so the feature must be on:

```sh
# Full run (publication-grade medians; several minutes)
cargo bench -p ankurah-core --features bench-internals --bench event_dag

# Quick smoke run (what CI does)
cargo bench -p ankurah-core --features bench-internals --bench event_dag -- \
  --warm-up-time 0.5 --measurement-time 1 --sample-size 10

# A single group or case
cargo bench -p ankurah-core --features bench-internals --bench event_dag -- compare/diamond_chain
```

criterion writes HTML reports and raw estimates under `target/criterion/`.

### The `bench-internals` feature

`event_dag` (and the specific compare / relation / accumulator / ordering items
the benches touch) is `pub(crate)` in the default build. Rather than restructure
the engine or scatter `pub` across it, the `bench-internals` feature raises the
module to `pub` and exposes a narrow `bench_support` module of wrappers over
exactly the primitives the benches exercise. The default build and the public
API are unchanged: the feature is never enabled except to run these benches.
`criterion` is a dev-dependency and the `[[bench]]` target sets
`harness = false`, so bench machinery stays out of the default build and test
paths.

## CI

`.github/workflows/benches.yml` runs the suite in **smoke** mode (tiny warm-up
and measurement windows) on pull requests that touch `core/` or `proto/`, and on
manual dispatch. It uploads `target/criterion` as an artifact.

Regression tracking is **advisory**: shared CI runners are too noisy to gate a
build on timing. The workflow includes a best-effort `critcmp` comparison step
that is wrapped so it can never fail the job, and it archives criterion output
so trends can be inspected by hand. Publication-grade numbers are captured
locally on a quiet machine, not on CI runners; see `BASELINE.md`.

## Old-engine baseline

The pre-#201 engine (`core/src/lineage.rs` on `origin/main`) is benchmarked from
a separate checkout with an equivalent, uncommitted bench. That bench is
preserved here as [`baseline-main-benches.patch`](./baseline-main-benches.patch)
so the old-engine numbers are reproducible without committing to the pinned
baseline tree. `BASELINE.md` documents the porting and the old-vs-new results.
