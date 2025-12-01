## Benchmark Initiative — Specification

We are building a benchmark program whose purpose is to provide a durable, comparable view of Ankurah’s baseline behaviors across storage engines, policy agents, and network topologies. Each suite (starting with sprout) describes a topology and workload mix, while its benchmark cases supply concrete node/context factories, connectors, and configuration knobs. Results must capture enough metadata (environment, parameters, commit) that runs taken months apart can still be compared honestly.

The initial benchmark, sprout, will focus on everyday operations (creating, fetching, getting, live querying) under representative concurrency and replica topologies. Subsequent suites can extend the same harness to deeper scenarios, but the governing intention remains: a flexible framework that the core team and the community can point at new integrations to understand how they behave relative to known baselines, without re-authoring the benchmark logic itself.

### Glossary

- **Benchmark initiative**: the overall effort to provide long-lived, comparable performance data for Ankurah.
- **Benchmark suite (Eg: sprout)**: a self-contained harness (types, orchestration, workloads) that defines the scenarios under test; its types live alongside the suite code rather than in a shared framework.
- **Benchmark case**: a concrete instantiation of sprout with specific factories, topology parameters, and workload knobs; each case is what we execute and report on.
- **Invocation script**: the shell/CI command (initially `cargo bench`) plus any helper scripts that choose which benchmark cases to run.
