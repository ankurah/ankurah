## Benchmark Initiative — Plan

### Goals

- Establish Cotyledon as the first reusable benchmark suite that characterizes create/fetch/get/live-query workloads across tunable topologies.
- Keep the harness agnostic to storage engines, policy agents, connectors, and node factories so community contributors can plug in their own stacks.
- Produce comparable, metadata-rich reports that make regressions obvious and allow long-term tracking of configuration-level tradeoffs.

### Acceptance Criteria

1. **Factory-driven configuration**: benchmark entry points accept durable-node and ephemeral-node factories (or context factories) plus connector builders; nothing in the suite references concrete storage or policy types.
2. **Topology/composition parameters**: callers specify durable node count (>=1), ephemeral node count (>=0), connector choice per link, and workload concurrency; the suite validates and records the chosen topology.
3. **Workload catalog**: Cotyledon exposes a documented set of scenarios (seed dataset, mixed fetches, limit/gap queries, subscription churn, concurrent writers) that can be executed against any supplied configuration.
4. **Deterministic orchestration**: benchmark runs capture start/end timestamps, seed values, and dataset sizes so results are reproducible when parameters match.
5. **Structured output**: every run emits machine-readable metrics (JSON conforming to `github-action-benchmark`) along with human-friendly console output; results include commit SHA, storage configuration, policy agent name, connector info, and node counts.
6. **Extensibility hooks**: the plan documents how additional suites plug into the same harness (naming, directory structure, parameter surfaces) without re-implementing scaffolding.

### Approach

1. **Define benchmark interfaces**
   - Within Cotyledon (not a shared crate), create the traits/structs for `DurableNodeFactory`, `EphemeralNodeFactory`, `ConnectorFactory`, and the topology descriptor the suite requires.
   - Allow factories to return ready-made contexts or lazily construct nodes when invoked by the runner, including awareness of the context index/role the suite requests.
2. **Implement Cotyledon scenarios**
   - Build dataset builders (e.g., albums, pets) with scalable counts.
   - Encode workload steps (batch creates, sequential fetches, concurrent mutations, live-query subscriptions) with instrumentation hooks.
   - Parameterize ephemeral node count, request concurrency, and predicate mixes.
3. **Reporting + metadata**
   - Standardize metric collection (per-step timers, latencies, subscription lags).
   - Serialize results to JSON + print concise tables.
   - Capture environment metadata (topology spec, storage parameters, policy agent ID).
4. **Execution surface**
   - Integrate Cotyledon with `cargo bench`, exposing each benchmark case as a named bench function that orchestrates its scenario and prints metrics.
   - Ship reference configs (e.g., sled local baseline, postgres pooled baseline) that `cargo bench` can call into, plus helper scripts for CI (eventually via `github-action-benchmark`).

### Implementation Decisions

- **Crate layout**: Cotyledon ships as a dedicated library crate under `lib/cotyledon`; benchmark entry points live in `benches/` so `cargo bench -p cotyledon` can execute discrete cases.
- **Shared fixtures**: The suite owns its own `common` module (entities, watchers, dataset builders) so it does not depend on `tests/tests`.
- **Reference cases**: Phase 1 includes two topologies per workload—durable node on Postgres plus ephemeral node on Sled—duplicated across LocalProcess and WebSocket connectors.
- **Output contract**: Benchmarks emit the default `cargo bench` output for now; `github-action-benchmark` JSON lands in a follow-up phase.
- **Configuration knobs**: Workload knobs (node counts, concurrency, dataset sizes, predicate mixes) are exposed by the suite but fixed per benchmark case so each bench run documents its chosen parameters.

### Phase Outlook

- **Phase 1 (Cotyledon implementation)**: build the harness, parameter plumbing, scenarios, and reporting for local/manual usage.
- **Phase 2 (CI integration)**: automate benchmark execution (likely via `github-action-benchmark`), publish comparison artifacts, and gate regressions.
- **Phase 3+ (Additional suites)**: author new benchmark suites for policy-heavy scenarios, multi-master replication, browser/IndexedDB peers, and connector stress tests using the same harness.
