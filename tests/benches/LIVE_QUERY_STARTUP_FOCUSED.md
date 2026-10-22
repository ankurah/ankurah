# Focused CPU, memory and timing investigation — 2026-10-21

This pass explains more of the resource cost and reproduces a native reconnect tail. It does **not** establish universal cold/warm parity or resolve the variable 16,384-record browser cold cases. No production code or protocol changed. Design/code review can proceed; performance acceptance remains open.

The comparison remains production `ec767596` / source fingerprint `db233818433b` against main `b5fcfd3e`. The original [420-sample comparison and subsequent evidence](LIVE_QUERY_STARTUP_RESULTS.md) are unchanged. This is a whole-branch comparison, not an experiment isolating the LRU.

## Fixed pass and correctness

- 36 uninstrumented observations: six alternating main/branch pairs for each of three uncertain cases, using the original frozen binaries.
- 12 separate timeline/profile observations: two alternating pairs per case.
- Eight separate native attribution runs: two CPU-profile pairs and two allocation-profile pairs, using private symbolized release builds of the same source.

All 56 observations passed the unchanged first-readiness membership, ordering, state and head assertions. No samples were excluded or replaced. Instrumented timings are not pooled with ordinary measurements. An independent audit checked run counts/order, raw native records, workload parameters, artifact hashes, source stability and arithmetic.

The controlled timing window has 797 process observations, with a maximum 1.011-second sampling gap and no detected cargo/rustc process. Other workloads remained active: the slow native probe overlaps a snapshot reporting three unrelated processes at approximately 90%, 77% and 38% CPU. They also appear elsewhere in the pass. This documents contention, not causation of a particular delay; this was not an isolated host. Browser and host observations are aligned by recorded UTC; Node's `hrtime` and Python's monotonic clock have different origins on this machine. Durations use their own clock domains.

## Ordinary timing repeats

These measure initialized authoritative query state, not the first record. Positive paired differences mean the branch was slower. All rows use LIMIT10; cold means no local matching records. Reconnect begins with all prior matches locally and changes membership while disconnected.

| Case | Main median ms | Branch median ms | Paired median difference ms | Paired range ms | Slower pairs |
|---|---:|---:|---:|---:|---:|
| Native membership reconnect, population 4,096, RTT50 | 54.011 | 54.882 | +1.940 | −1.154 to +6.488 | 5/6 |
| Browser cold, population 16,384, RTT0 | 1,123.400 | 1,045.450 | −91.000 | −208.700 to +475.700 | 2/6 |
| Browser cold, population 16,384, RTT50 | 1,138.300 | 1,167.900 | +45.500 | −195.200 to +710.500 | 3/6 |

Native reconnect endpoint process CPU increased in all six pairs: paired median +1.498 ms, range +0.373 to +4.610 ms. The browser distributions remain very wide, and both arms' absolute times differ substantially from the original run. The sign of a small set's median is not a parity certificate. These results neither erase the earlier unfavorable samples nor demonstrate a stable branch-specific browser slowdown. No numerical regression allowance has been assumed.

## Where native CPU and memory go

The attribution workload is an unlimited cold livequery with 4,096 remote records and RTT50, with both nodes in one process. It is distinct from the LIMIT10 timing cases above.

The profiles identify actual work beyond cache lookup:

- **Digest construction:** about 43 ms of sampled endpoint CPU, mainly hashing leaves into ristretto255 elements and encoding them. Large standing queries deliberately build their digest before the first Complete response. This behavior predates the current LRU integration. Deferring it would move the cost to later demand, not remove it.
- **Repeated captured-state decoding:** `TemporaryEntity` construction accounts for approximately 114–117 ms sampled inclusive on the branch versus 7–8 ms on main. `QueryProjection::candidate` reconstructs property backends for predicate/key evaluation; Yrs decoding is a substantial inner cost. The branch also removes much of main's `ResultSetWrite::add` work, so these figures cannot be read as the net CPU difference.
- **Small allocations and retained copies:** the branch makes about 45% more Rust allocation/nonshrinking-reallocation calls during initialization, despite about 12% fewer cumulative requested growth bytes. Sampling also shows more allocator work.

These are overlapping sampled call stacks. Do not add them together or substitute them for the original uninstrumented +112 ms endpoint process-CPU measurement. The profiles do not exactly decompose every CPU millisecond.

| Rust requested-live memory, entire two-node fixture | Main MiB | Branch MiB |
|---|---:|---:|
| At readiness | 90.87–91.26 | 118.48–118.58 |
| After verification and exchange drain | 96.00–96.14 | 122.44–122.50 |

The difference averages **27.46 MiB at readiness** and **26.40 MiB after drain**. Allocation stacks identify 8.878 MiB of retained payload copies through initial attestation and subsequent source publication, plus 0.974 MiB from copying authorized-row metadata. These are specific optimization targets, not an explanation of the entire difference. Other retained allocations include tree indexes, captured states and synchronization bookkeeping.

The candidate and authorized row maps already share their raw payload through `Arc`; attestation/tree admission then deep-copies the state. Tree rows, state maps and full snapshots subsequently share the attested payload. No material live copy-on-write allocation was found at these checkpoints, which does not qualify other concurrent update/snapshot workloads.

Requested-live bytes are not RSS, allocator capacity, or reserved thread-stack space. Both instrumented processes reach their largest requested-live total, about 170 MiB, during fixture setup. Heap high-water stacks therefore describe that earlier setup peak, largely through Sled, rather than the query endpoint. Unequal setup live totals must not be subtracted to claim a cache-only increase. The original +34.5 MiB process peak-RSS difference remains a separate measurement.

## What the slow-run diagnostics establish

The native probe reproduced a 73.928 ms candidate reconnect versus 53.817 ms on main. The candidate's transport timers fired approximately 3.958 and 5.652 ms late. One blocking commit finished at 63.516 ms, but its async continuation resumed at 73.132 ms. The resident refill function entered shortly before readiness and awaited its publication fence; its ten-row storage read began after readiness. Endpoint process CPU was 7.061 ms versus 3.256 ms; the extra wall time is not all CPU work.

This localizes much of that sample's delay to timer/continuation scheduling; the later refill read cannot account for the earlier 9.616 ms continuation gap. It does not distinguish queue handoff, runtime scheduling, or OS scheduling. Open async spans do not prove CPU occupancy, and the process monitor does not prove the host caused the delay. The other diagnostic pair was near 53 ms on both arms. These instrumented observations do not replace the ordinary timing distribution.

All eight browser event traces exhausted their buffers during fixture setup and ended before the measured window. They provide **no endpoint IndexedDB/timer attribution**. Separate renderer CPU profiles cover that window and classify roughly 84–93% of in-window samples as idle, consistent with much of the interval occurring outside renderer JS/WASM execution. Some profiler time deltas are nonmonotonic, so this uses sample counts, not exact CPU-time weights. One surviving early mark anchors console phases to the profile clock; multiple-anchor drift checking is unavailable. The profiles cannot distinguish IndexedDB work, queued callbacks, timers, or OS scheduling. The raw traces, loss indicators, phase timestamps and profiles are retained; there were no extra runs after seeing the results.

## Recommendation before performance sign-off

Prioritize sharing immutable captured state and avoiding redundant property-backend decoding in resident preparation. Preserve policy checks, error behavior, captured-version coherence, active leases and wire types. Memory sharing alone will not necessarily eliminate the CPU difference. This scoped follow-up is proposed in fwd as `M1zVEHlR`; no storage or durable-replication refactor is proposed.

Keep performance acceptance open for the variable browser cold case. A further causal probe should start event tracing after population setup, verify that it covers the endpoint, and run on a quieter host. The existing evidence is insufficient to justify a speculative synchronization fix or a blanket parity claim. Human review and the landing gate remain pending.

## Reproducibility and limits

Native CPU profiles use release optimization/LTO/codegen settings with debug symbols retained, the normal allocator, 100-microsecond running-thread sampling, and a gate before fixture setup. Allocation runs separately use a benchmark-only System allocator wrapper and full macOS malloc-stack logging, pausing the owned process for live/high-water heap and VM inspection. These tools perturb scheduling and memory; their timings are diagnostic only. Counters track requested Rust bytes and net positive reallocation growth, not all allocator traffic, and concurrent checkpoint reads are approximate.

The local evidence root is `~/ak/research_notes/ankurah-replication/reviews/2026-10-21-focused-performance/`. It contains both fixed plans, raw records/traces, private instrumentation patches, build/source manifests, corrected CPU/allocation analyses, host observations and `root-independent-audit.json`. Earlier analysis versions and tooling failures are preserved. Raw profiler/process metadata stays local. This pass does not qualify million-row scale, sustained churn, numeric ORDER BY, or p95/p99 latency.
