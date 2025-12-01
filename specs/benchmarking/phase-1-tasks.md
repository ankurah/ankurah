## Phase 1 Tasks — sprout Benchmark Suite

### 1. Harness & Interfaces

- Define trait/struct contracts for durable node factories, ephemeral node factories (with configurable counts), connector factories, and context factories (aware of role/index).
- Implement a shared `BenchmarkConfig` that records topology (node counts, connectors), storage parameters, policy agent identifiers, and workload knobs.
- Add error/reporting types that every benchmark step can use for uniform logging.

### 2. Topology Orchestration

- Implement a runner that instantiates factories, wires connectors, and surfaces hooks for multi-node start/stop lifecycles.
- Support single-durable and durable+N-ephemeral layouts; ensure `N` is a runtime parameter.
- Provide utilities for seeding datasets on durable nodes and synchronizing ephemerals before workloads begin.

### 3. sprout Workloads

- Create dataset builders (albums, pets, etc.) with scalable counts; expose knobs for entity volume.
- Encode workload phases:
  1. Batched creates.
  2. Fetch/get rounds with representative predicates.
  3. ORDER BY + LIMIT live queries to exercise gap filling.
  4. Concurrent writers mutating existing entities.
  5. Subscription churn measuring notification latency.
- Ensure workloads can target either durable or ephemeral nodes depending on the scenario definition.

### 4. Instrumentation & Output

- Add timing/latency capture around each workload phase plus per-operation counters.
- Record subscription lag (commit → notification) and connector-level delays when available.
- Emit structured JSON compliant with `github-action-benchmark` plus concise console summaries; include topology + configuration metadata.

### 5. Reference Configurations & Docs

- Wire sprout benchmarks into `cargo bench`, ensuring each benchmark case registers as a named bench function.
- Provide example invocation scripts/configs for sled-only and Postgres (with bb8 pool size parameter) baselines.
- Document how to plug in alternative storage engines, policy agents, and connectors without editing the suite.
- Add README snippets describing how to run sprout locally and interpret the results.
