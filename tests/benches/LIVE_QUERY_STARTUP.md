# Cold and retained-query performance

`live_query_startup` exercises two real nodes, Sled, two Tokio workers and
serialized protocol messages. Identical source runs on the pre-digest public API
at main `ebdb1727254c675a4d37236ecf23c16c06c4df12` and the candidate.

```sh
STARTUP_LABEL=candidate cargo bench -p ankurah-tests --bench live_query_startup
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `STARTUP_SIZES` | `0,8,32,256,4096` | Server result sizes |
| `STARTUP_RTT_MS` | `0,50` | Propagation round trip in milliseconds |
| `STARTUP_BPS` | `0` | Full-duplex bytes/second per direction; zero is unlimited |
| `STARTUP_CACHE` | `empty,half,all` | Entity coverage before initial subscription; optional `half_blocks,half_interleaved` diagnostics |
| `STARTUP_MODES` | `cold` | `cold,warm_refresh,warm_equal,warm_edit,warm_membership` |
| `STARTUP_REPEATS` | `3` | Fresh-fixture repetitions |
| `STARTUP_LABEL` | `candidate` | Revision label in every JSON record |

Cold cases use a new client without an active or retained query tree. Partial
and full entity cache come from Fetch outside the timed interval. Node creation,
catalog negotiation, server population and prefetch are excluded from latency.
`ready_us` starts immediately before `query_wait` and ends after
`wait_durable_answered`: the durable response and local application must finish.
`settled_us` also waits for the transport tail after exact verification, including
acknowledgments. `initial_drained_us` and `settled_traffic` preserve the provisional
pre-verification drain; asynchronous application can send further control traffic.
Cold `elapsed_us` is `ready_us`.

Warm cases retain the same live query. Reconnect cases use `cache=all`.
`warm_refresh` reports each initial cache case separately: `empty` first transfers
the result during the initial subscription, `half` prefetches half, and `all`
prefetches every entity. Optional `half_blocks` prefetches alternating blocks of
256 positions; `half_interleaved` prefetches even positions. Both use bounded OR
batches outside timing (at most 16 intervals or 32 scalar comparisons per request),
identically on baseline/candidate. Their actual `cached_entities` coverage is
reported; small results under 256 rows fit entirely in the first block, so use
4096-row block diagnostics for partial coverage. This preserves first digest construction after lazy cold
transfer inside the measured refresh rather than warming it up with an earlier
refresh. `warm_refresh` keeps
the connection and calls `update_selection_wait` with the same selection, then
`wait_durable_answered`; the new version gives a portable applied `ready_us`.
Query identity is retained; actual internal tree reuse must also be checked
in implementation/work counters. The other warm cases disconnect/reconnect.
`warm_equal` has no offline change. `warm_edit` edits one body while offline.
`warm_membership` additionally moves one entity out of the predicate and adds
another. Reconnection, including registration and catalog work it triggers, is
timed. The baseline's durable-answer flag is sticky across reconnect. Before
measurement, both fixtures call the public resultset `set_loaded(false)` metadata
setter and arm an O(1) `Signal.listen` callback on that retained resultset. This
changes no selection, version, membership, cache or tree and schedules no work.
Intermediate dirty/membership broadcasts still see false; the first fresh
loaded=true callback observes local result installation after the reactor's
membership and gap work. `local_result_installed_us` and reconnect `elapsed_us`
record that callback. This precedes initialization/durable bookkeeping, so
reconnect `ready_us` remains null. The handled subscription counter also requires
a new response for the retained query ID; catalog responses cannot satisfy it.
Every installed result must immediately match the exact expected IDs and complete
applied State/head. Verification fails on mismatch rather than waiting for later
repair. `verification_us` reports the separate O(N) check; `verified_us` and the
final drained endpoint retain end-to-end work/traffic evidence.
A two-millisecond quiet confirmation is excluded from the drained-link timestamp
but included in the verification-inclusive endpoint. Protocol acknowledgment
tails can follow UI readiness. Every record requires successful exact verification. Zero-row cases use cold/empty only. `warm_refresh` also reports the true versioned readiness endpoint.

The sender schedules serialization on a shared lane per direction, then adds
RTT/2 propagation. Serialization overlaps propagation and both directions are
independent. Scheduled delivery runs on real Tokio timers, whose granularity adds
scheduling variation. There is no real socket, transport framing, packet loss,
congestion control or physical network. Message counts are not round-trip counts
when messages are pushed concurrently. A connection generation prevents delayed
messages from an old link from crossing a reconnect.

`STARTUP` JSON contains both landmarks, directional messages/bytes, largest
message, returned selection rows/bytes, point reads and requested events. Work and traffic include the transfer and application through exact verification;
`settled_traffic` preserves the provisional pre-verification snapshot. A final
post-verification drain includes control traffic emitted by asynchronous activation. Selection rows count returned
materialized states, not internal index visits. Storage counters describe public
StorageEngine calls and requested IDs, not physical disk I/O or cache misses.
The verifier's known direct point reads are subtracted from native work counts,
while asynchronous application work during verification remains included. Random entity IDs produce tree
shape variation. `PHASE` diagnostics identify setup or measurement hangs.
A 180-second fixture timeout fails rather than producing an incomplete success.

## Comparing revisions

Build both with the same compiler and optimized profile and verify shared package
versions match. Keep the original lockfiles and document additional dependencies.
Copy only this benchmark source and its target declaration into the baseline.
Use separate Cargo target directories to avoid incompatible workspace artifact
reuse; preserve each executable. Finish builds before timed sampling and keep the
machine otherwise quiet.

```sh
python3 tests/benches/compare_live_query_startup.py \
  --baseline /absolute/path/to/baseline-binary \
  --candidate /absolute/path/to/candidate-binary \
  --baseline-revision BASE_SHA --candidate-revision CANDIDATE_SHA \
  --bandwidths 0,1250000 --repeats 7 \
  --output /absolute/path/to/new-results-directory
```

The runner uses a fresh process per fixture, alternates AB/BA order, refuses to
overwrite evidence, and saves every failure with its last phase and raw output.
The default external deadline is 240 seconds. Whole-process child CPU, peak RSS
and page faults cover setup, prefetch, warmup, transfer, verification and teardown;
they are resource context, **not transfer-only CPU or memory**. On Unix,
`phase_process_cpu` separately samples process user/system CPU with `getrusage`:
setup ends at measurement start; `through_ready` ends at the public durable-ready
landmark (null for reconnect); initial drain, local result installation, verification, verified completion,
and final drain have their own intervals. These counters combine client/server
and every background thread active during the interval; they are not attributed
CPU for one query task. Cumulative intervals overlap and must not be summed. Drain CPU snapshots are
taken after the 2 ms quiet confirmation, while the recorded wire-drain wall
landmark excludes that confirmation; `through_ready` is captured immediately
after the durable-ready wall landmark.
Setup includes population, catalog, prefetch and initial subscription for warm
cases; final-drain CPU excludes teardown. Source/binary/runner
hashes, compiler, machine, settings and execution order are retained.

Compare each mode/size/cache/RTT/bandwidth case independently. Report medians,
full ranges, standard deviation, median absolute deviation and paired differences,
alongside work and traffic. Seven samples do not establish p95/p99 latency. Failed
samples remain failures even if other repetitions succeed. The original two baseline 30-second deadlines have no phase diagnostics.
Instrumented replay reproduced both workload types hanging in catalog startup:
the original fixture allowed zero-latency requests to arrive before server-side
peer registration, where baseline request handling silently ignored them. The
identical harness now completes server-side registration first. Historical failed
samples remain failures; replay does not retroactively give them timing values.

A warm reconnect win does not compensate for a cold startup regression. Small
query latency and bulk throughput both matter. Daniel has not accepted a numerical
slowdown allowance. The baseline can send an entire result in one message; report
its largest-message cost alongside latency. Its performance does not justify
removing bounds from the new protocol. These native fixtures do not qualify
million-row scaling, concurrent writes, browser storage or retained allocation
bounds; those need separate evidence.

## Browser fixtures

`tests-wasm/bindings/src/query_parity.rs` and `src/query-parity.test.ts` use
the same public-API operations against two IndexedDB nodes with serialized
messages, directional bandwidth lanes and `performance.now`. Copy the module,
its declaration, serde_json dependency and test identically into the baseline.
Build both wasm artifacts with the same release profile and wasm-pack version.
The default Vitest matrix is functional smoke coverage, not performance evidence.
Configure `VITE_QUERY_PARITY_SIZES`, `RTTS`, `BPS`, `MODES`, `CACHE`, `REPEATS`
and `LABEL` using the `VITE_QUERY_PARITY_` prefix; records start `BROWSER_STARTUP`.
Use fresh browser contexts per measured fixture, alternating baseline/candidate,
and retain browser version, module/wasm hashes and raw failures. Browser timer
granularity and IndexedDB task scheduling are part of these measurements.
