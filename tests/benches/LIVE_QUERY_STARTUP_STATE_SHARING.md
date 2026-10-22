# Captured-state sharing — 2026-10-22

> Historical measurements: this report describes the captured-state cache at
> `034735dd`, which the subsequent maintained-tree correction removes. The
> current implementation retains only keys, IDs, cloned Clocks and digests,
> maintains every resident tree through the reactor, and reads payloads from
> entity/storage ownership when sending. These timings do **not** qualify that
> revised implementation; fresh paired performance measurements remain required.

The captured-state change reduces decoding and allocation work, with a mixed
end-to-end result. In the fixed two-pair comparison with the previous candidate,
endpoint CPU falls **56.352 ms**, but readiness rises **6.173 ms** and
whole-process peak RSS rises **10.047 MiB** (paired medians). Separate profiles
find only about **2 MiB less requested-live memory** at readiness. This is a CPU
and ownership improvement, not a demonstrated overall latency or RSS improvement.

All **420 main/candidate observations**, **four previous/new observations**,
**eight browser diagnostics** and **eight native profiles** pass their result
oracles. Browser traces cover the actual measurement window. The
[current main comparison](LIVE_QUERY_STARTUP_RESULTS.md) retains large cold/warm
gains, small consistent slowdowns and variable larger browser cases. The
[previous comparison](LIVE_QUERY_STARTUP_RESULTS_PRE_STATE_SHARING.md) and
[earlier attribution](LIVE_QUERY_STARTUP_FOCUSED.md) keep their older source pins.
Human design/code review and performance acceptance remain pending; no PR.

## Changes

One immutable entity capture now supplies the publication, resident candidates,
authorized rows and tree snapshots. It caches validated property values without
retaining mutable property backends. Committed publications capture serialized
state and values under the same state lock; storage-origin captures validate all
backends on their first read, including malformed unused backends. Policy and
event authorization remain fresh. Wire types, compatible tree reuse and active
RAII leases are unchanged.

The wider correctness gate exposed an existing branch race: catalog invalidation
could escape the resident capture retry path as an untyped error. `034735dd`
preserves that invalidation as `CatalogChanged` and permits one fresh acquisition
for Fetch and SubscribeQuery. Admission and layout are revalidated; peer-provided
TreeSpecs are not changed. Continued churn exhausts the finite retry and uses the
existing remote busy response.

## Correctness on the final source

Final source: `034735dd0d8f9885b8a00e585c571a210892d21f`.

| Check | Result |
| --- | --- |
| Core | 525 passed; one existing ignored test |
| JWT policy unit/integration suites | 125 passed |
| Affected two-node integrations | 62 passed |
| Digest-tree and protocol tests | 194 passed |
| Digest-tree documentation tests | 2 passed |
| Browser functional cases | 51 passed |
| Core, integration and JWT test-target compilation | Passed |

Captured-state tests cover immutable versions, property-backend precedence,
synthetic IDs, memberships, JSON, malformed input, sharing, authorization and
unchanged attested wire encoding. Deterministic Fetch and SubscribeQuery catalog
races fail before the retry fix and pass afterward. Retry-bound, cleanup and
later-recovery tests also pass. The original failing JWT test passes in isolation
and in the full suite after the repair.

One earlier overlapping-selection integration timed out without identifying its
wait. Twelve fixed runs of the complete six-test file then passed all 72 tests;
the final full integration gate also passed. The original timeout is retained
and remains unexplained. It is not claimed fixed by the catalog repair. Initial
compile-only mistakes and both pre-repair JWT failures are preserved separately.

## Direct previous/new comparison

Four uninstrumented native observations, two balanced pairs, at 4,096 records,
empty local cache and 50 ms RTT. Previous source: `ec767596` /
`db233818433b`; new source: `034735dd` / `f7647dbce62c`. This isolates the branch
checkpoints from the separate seven-pair main comparison, although two pairs
cannot establish a general latency or memory outcome. Positive means more time
or memory in the new version.

| Metric | Previous median | New median | Paired delta [range] |
|---|---:|---:|---|
| Readiness, ms | 394.389 | 400.562 | +6.173 [+4.384, +7.962] |
| Endpoint process CPU, ms | 1000.903 | 944.551 | -56.352 [-65.797, -46.907] |
| Whole-fixture process CPU, ms | 1331.267 | 1271.270 | -59.997 [-69.130, -50.863] |
| Whole-process peak RSS, MiB | 235.117 | 245.164 | +10.047 [+7.188, +12.906] |

Both versions send 25 requester and 25 responder messages, with 3,269 requester
bytes. Responder bytes differ by -5 and +18 bytes across the two pairs; this does
not establish a traffic change. Endpoint and whole-fixture CPU intervals overlap
and must not be added. RSS includes setup and teardown; it is not requested-live
allocation at readiness. No causal explanation for the observed readiness/RSS
increase is established by these four observations.

## Native CPU and allocation profiles

Four CPU profiles and four allocation profiles ran in old/new/new/old order on
the same unlimited cold 4,096-record / 50 ms RTT fixture. All passed the strict
endpoint oracle. These are separate instrumented observations, not replacements
for ordinary latency or RSS measurements.

Time Profiler sampled running threads at 100 microseconds. Actual samples span
the endpoint, with about ±1 ms timestamp-alignment uncertainty. Context rows
are inclusive and overlapping; they cannot be added or treated as exact
exclusive CPU savings.

| Sampled endpoint running time, ms | Old 1 | New 2 | New 3 | Old 4 |
|---|---:|---:|---:|---:|
| All running stacks | 753.9 | 702.4 | 706.9 | 761.2 |
| Backend decoding context | 75.9 | 34.0 | 35.8 | 84.2 |
| TemporaryEntity context | 104.4 | 32.1 | 34.6 | 115.3 |
| QuerySource publication context | 90.1 | 16.8 | 16.6 | 99.8 |
| Allocator context | 91.2 | 73.1 | 75.5 | 94.3 |
| Digest-tree context | 41.7 | 43.6 | 42.0 | 44.8 |
| Sled context | 329.4 | 334.0 | 331.9 | 328.9 |

The mean running-sample weight drops 52.9 ms across two runs per role. Repeated
backend reconstruction declines; digest work remains. This supports the CPU
reduction measured by the ordinary process counters, without decomposing every
millisecond or proving a readiness improvement.

The private allocator wrapper counts requested bytes. Growth calls are successful
allocations and nonshrinking reallocations (including equal-sized reallocations),
not all allocator operations. Requested growth counts allocation sizes and net
nonnegative reallocation growth.

| Allocation measurement | Old 1 | New 2 | New 3 | Old 4 |
|---|---:|---:|---:|---:|
| Endpoint growth calls | 3,756,678 | 3,192,594 | 3,191,949 | 3,758,081 |
| Endpoint requested growth, MiB | 735.882 | 650.362 | 650.074 | 736.629 |
| Requested live at readiness, MiB | 118.443 | 116.126 | 116.868 | 118.464 |
| Requested live after verification/drain, MiB | 122.498 | 120.282 | 120.312 | 122.428 |
| Setup requested peak, MiB | 170.042 | 170.051 | 169.952 | 169.694 |

Descriptive means show **15.0% fewer growth calls** and **11.7% fewer requested
growth bytes** during initialization. Requested-live memory falls **1.956 MiB at
readiness / 2.166 MiB after verification and drain**. The larger setup peak remains
about 170 MiB. These are small diagnostic samples, not confidence bounds.
Requested-live bytes exclude allocator overhead, fragmentation and freed pages
that remain resident; concurrent counter snapshots are approximate.

Retained allocation stacks explain the modest net saving:

- Serialized payload-clone contexts fall from about 8.878 MiB to 0.001 MiB.
- New compact lookup map nodes retain **6.383 MiB**, before strings/value heap
  and inline capture fields. Both lazy storage decoding and publication-side
  extraction contribute; looking only for the non-inlined helper undercounts it.
- Authorized-row context falls from about 0.974 MiB to 0.399 MiB, while Sled
  context remains about 50–51 MiB. Ownership contexts can overlap.

Sharing payloads removes duplication, while keeping decoded values is a real
memory cost. The accounting includes those values. The modest requested-live
reduction does not explain or cancel the increased uninstrumented RSS and
readiness measurements. Full allocation logging changes allocator behavior;
its physical-footprint measurements cannot substitute for ordinary RSS.

The same three-file instrumentation overlay and compiler/profile settings were
used for both sources. All four artifacts, private source exports and live
runtime/Cargo files were unchanged before and after collection. Instrumented
artifact hashes:

| Artifact | SHA256 |
|---|---|
| Previous CPU | `3852b7bdbf37ac118dcc98156504f2fa890d1cc6a273a21564a8dd42440036ef` |
| New CPU | `f9462b0c5334e92cc473b08f69f630ff7bd13e3754509d385f56de51c096962b` |
| Previous allocation | `70b55496ada28314a1528ce7aedf765bfd7881f426c2f07834af4f0592898301` |
| New allocation | `5f5d0964f740532df109bc876abb47b7be5e20eba2ee1531ad8397ca9369c5df` |

## Browser traces cover the initialization window

Eight separate browser diagnostics cover cold LIMIT10 over 16,384 records: two
balanced main/candidate pairs at each of 0 and 50 ms RTT. Capture begins after
population setup and ends after the measured initialization endpoint. Every
export is complete and loss-free, with exactly one start/end marker and actual
CPU samples bracketing both. The largest CPU-sample gaps are 3.96–5.36 ms.

The first IndexedDB transaction/cursor interval varies from roughly **394 to
2,038 ms**, while renderer task unions remain roughly **105–111 ms**. Both arms
perform essentially the same population-scale index setup: 16,406 SetIndexKeys
operations on main and 16,405 on the candidate. At 50 ms RTT, the two simulated
25 ms transport timers fire only about 0.3–1.1 ms late in these diagnostic runs.

This locates the interval in which the large elapsed-time variation accumulates.
It does not distinguish backend CPU, queues, blocking or operating-system
scheduling as its cause. Transaction/cursor lifetimes include waiting; task
unions describe elapsed occupancy, not exact CPU. Overlapping spans and separate
threads must not be added as independent costs. The ordinary browser CPU metric
covers the whole renderer fixture and excludes IndexedDB workers.

The population-scale index work exists on both versions even though the query
returns only ten rows. Equal returned-row counts alone therefore do not establish
equal work. These traces improve attribution without making the variable
16,384-record timing cells a parity result or justifying an unrelated engine
change. Their instrumented timings remain separate from the ordinary matrix.

## Measurement plan and execution

Main remains `b5fcfd3eac3ea4247c4bd7613c439393beb42aad`. The candidate source
fingerprint is `f7647dbce62c21646cb965afb3293d7206b5f43191d145d352f314b8c876e6a9`.
Both optimized artifacts have passed source, compiler and instrumentation checks.

- 420 ordinary observations: the existing 15 cases on native and browser, with
  seven alternating main/candidate pairs per case.
- Four separate native observations comparing the previous and new candidate.
- Eight separate browser diagnostic observations covering the actual measurement
  window for the variable 16,384-row LIMIT10 cases.
- Eight separate native CPU/allocation profiles comparing the previous and new
  candidate.

The original first-readiness membership, ordering, state and head oracles remain
enabled. The endpoint is initialized authoritative results, not first record.
Instrumented observations remain separate from ordinary latency results.
No numerical slowdown allowance or tail-latency certification is assumed.

Browser tracing starts after population setup and ends after the measured
endpoint. All eight final diagnostic captures pass complete endpoint-marker,
loss-free export and actual CPU-sample bracketing checks. Tooling preflights
remain separate; their timings are not performance evidence for this candidate.

Two earlier guarded launches stopped before taking any observations. Before the
final run, host monitoring was changed to aggregate machine CPU/load only. A
pre-sample guard waits at most ten seconds if total host CPU remains at least
80%, then proceeds and records the busy start. Unavailable monitoring is flagged;
no observation is discarded or replaced based on host load. The original source,
artifacts, workloads, pairing and correctness oracles are unchanged. Aggregate
CPU includes the benchmark itself and cannot identify the cause of contention.

The separate four-observation launcher initially failed on path construction
before starting any observation. Its one-line repair, before/after hashes and
zero-observation failure are retained. The ordinary 420-observation runner was
not changed. No user process was altered.

Local evidence is under
`~/ak/research_notes/ankurah-replication/reviews/2026-10-22-state-sharing/`:
correctness logs, catalog red/green tests, artifact and source manifests, frozen
plans, method amendments, tracing preflights and preserved failed launches. Raw
profiler metadata remain local. Human design/code review and landing are pending;
no PR has been opened.
