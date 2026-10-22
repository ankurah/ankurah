# Captured-state sharing — 2026-10-22

Implementation checkpoints `db6fcc28` and `034735dd` are pushed for review. The
new performance comparison is **pending**: the first launch stopped before its
first sample because the host remained busy throughout the predeclared
180-second quiet-window wait. No new parity, CPU-saving or memory-saving claim
can be made yet. The [previous comparison](LIVE_QUERY_STARTUP_RESULTS.md) and
[focused attribution](LIVE_QUERY_STARTUP_FOCUSED.md) describe their original,
older source pins.

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

## Frozen measurement plan

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
Instrumented observations will remain separate from ordinary latency results.
No numerical slowdown allowance or tail-latency certification is assumed.

Browser tracing now starts after population setup and ends after the measured
endpoint. Separate tooling preflights verified complete endpoint markers,
loss-free traces and monotonic CPU samples bracketing the window. Those
preflight timings are not performance evidence for the new candidate.

The zero-sample guarded launch is retained. Continuation uses byte-identical
frozen inputs and begins with the same first planned role. No user processes
were changed. Final timing and profile execution await a quiet window.

Local evidence is under
`~/ak/research_notes/ankurah-replication/reviews/2026-10-22-state-sharing/`:
correctness logs, catalog red/green tests, artifact and source manifests, frozen
plans, tracing preflights, and the preserved host-blocked launch. Raw process and
profiler metadata remain local. Human design/code review and landing are pending;
no PR has been opened.
