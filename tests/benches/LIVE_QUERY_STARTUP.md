# Cold and retained-query performance

`live_query_startup` exercises two real nodes, Sled, two Tokio workers and
serialized protocol messages. The current qualification compares the shared
resident-tree implementation with main `b5fcfd3eac3ea4247c4bd7613c439393beb42aad`,
using identical public-API fixtures. See [the current report](LIVE_QUERY_STARTUP_RESULTS.md)
for measurements and immutable artifact pins. The [pre-state-sharing report](LIVE_QUERY_STARTUP_RESULTS_PRE_STATE_SHARING.md) preserves the previous candidate's measurements. The [intermediate unlimited-only reuse report](LIVE_QUERY_STARTUP_RESULTS_UNLIMITED_REUSE.md) is pinned before the bounded-coverage fix. The [pre-reuse](LIVE_QUERY_STARTUP_RESULTS_PRE_REUSE.md),
[post-#526](LIVE_QUERY_STARTUP_RESULTS_PRE_ONGOING.md) and
[pre-#526](LIVE_QUERY_STARTUP_RESULTS_PRE_526.md) reports remain historical;
their source pins and measurements have not been relabelled.

The [captured-state sharing follow-up](LIVE_QUERY_STARTUP_STATE_SHARING.md)
records the newer implementation, correctness checks, direct optimization
comparison and separate CPU, allocation and browser tracing evidence. Historical
reports retain their original source pins and measurements.

```sh
STARTUP_LABEL=candidate cargo bench -p ankurah-tests --bench live_query_startup
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `STARTUP_SIZES` | `0,8,32,256,4096` | Server population sizes |
| `STARTUP_LIMIT` | absent | Optional result limit; absence is unlimited, explicit `0` is `LIMIT 0` |
| `STARTUP_RTT_MS` | `0,50` | Propagation round trip in milliseconds |
| `STARTUP_BPS` | `0` | Full-duplex bytes/second per direction; zero is unlimited |
| `STARTUP_CACHE` | `empty,half,all` | Entity coverage before initial subscription; optional `half_blocks,half_interleaved,full` diagnostics |
| `STARTUP_MODES` | `cold` | `cold,warm_refresh,warm_equal,warm_edit,warm_membership` |
| `STARTUP_REPEATS` | `3` | Fresh-fixture repetitions |
| `STARTUP_LABEL` | `candidate` | Revision label in every JSON record |

The `cold` mode measures the initial subscription. With `cache=empty`, neither
peer has a resident tree for this application query before timing. Partial and
full entity caches come from Fetch outside the timed interval. With `cache=all`,
that Fetch uses the subscription's same Selection and now warms compatible
resident trees too: report it as **cached startup**, not a tree-free cold start.
With `cache=half`, the narrower prefetch tree cannot serve the wider subscription.
With `STARTUP_LIMIT=10`, the Selection is `body >= 'row-' ORDER BY body ASC LIMIT 10`, using the existing zero-padded `row-00000000` strings; population and expected result count are recorded separately. The normal `all` prefetch uses this same bounded Selection. The separate `full` diagnostic prefetches `body >= 'row-'` without ordering or a limit, allowing the subsequent limited query to reuse a complete resident. This is a cache-hit cost experiment, not cold startup. Limited cases verify ordered IDs as well as membership, full state and heads. Bounded warm edits change a selected row's nonsort `position` payload, leaving its body key unchanged. Membership changes set the first row's body to `excluded` and add a correctly padded row at position N; the result must include the unchanged row just beyond its previous limit. Zero-result cases do not run warm mutation fixtures.
The attempted numeric-order fixture failed its strict oracle on both arms and is preserved separately. The existing Sled numeric ORDER BY defect (fwd `wJfo-978`, GitHub #210) is outside this branch; string-order qualification must not be presented as numeric-order correctness. An exploratory body-suffix edit passed main but failed the candidate, exposing the existing Small/Large sort-key comparator defect (fwd `JTAKZLw2`) through fresh result installation. Checkpoint `fcd05988` fixes that blocker by comparing actual bytes and preserving inline lengths. Separate ASC/DESC reconnect tests failed before the fix and pass afterward, checking the first installed result. Timed payload-edit cases still edit the nonsort field, as intended by that workload. No result-order, membership, state or head assertion is disabled for the replacement fixture.
The complete-resident-to-LIMIT experiment exposed eager full-digest construction in `fcd05988`. Checkpoint `ec767596` resolves spans from the maintained address index and keeps narrow digests lazy; unchanged captures share an immutable state map, and a narrowed view retains only its selected rows. The original slow measurements remain separate from the new artifact’s qualification.

Node creation, catalog negotiation, server population and prefetch are excluded
from latency.
`ready_us` starts immediately before `query_wait` and ends after
`wait_durable_answered`: the durable response and local application must finish.
After state verification, one transport drain checks subscription delivery and
the acknowledgment tail. `initial_drained_us` and `transport_settled_us` are
legacy aliases for the same wire-activity landmark observed by that check;
`settled_traffic` and `traffic` cover the same interval. The wire landmark can
precede verification completion; `verified_us` reports that completion separately.
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
This does not measure the new same-stream `refresh()` API. Query identity is
retained; actual internal tree reuse must also be checked
in implementation/work counters. The other warm cases disconnect/reconnect.
`warm_equal` has no offline change. `warm_edit` edits one selected entity while offline: its body for unlimited selections, or its nonsort position for bounded selections.
`warm_membership` additionally moves one entity out of the predicate and adds
another. Reconnection, including registration and catalog work it triggers, is
timed. The baseline's durable-answer flag is sticky across reconnect. Before
measurement, both fixtures call the public resultset `set_loaded(false)` metadata
setter and arm a `Signal.listen` callback on that retained resultset. This
changes no selection, version, membership, cache or tree and schedules no work.
Intermediate dirty/membership broadcasts still see false; the first fresh
loaded=true callback observes local result installation after the reactor's
membership and gap work. `local_result_installed_us` and reconnect `elapsed_us`
record the callback's initial timestamp, before its O(N) state capture. This
precedes initialization/durable bookkeeping, so
reconnect `ready_us` remains null. The handled subscription counter also requires
a new response for the retained query ID; catalog responses cannot satisfy it.
The reconnect callback captures immutable membership and complete resident
State/head synchronously after recording readiness. Primary cases capture
immediately after the versioned wait, before any asynchronous verification or
transport drain. Each entity's state is atomic; the collection is not one globally
atomic snapshot. This fixture performs no writes after measurement begins.
Captured IDs and states must exactly match the static sender. A later repair
cannot change that captured evidence. Asynchronous storage reads separately check
persistence after the landmark. `local_result_capture_us` and native capture CPU
report reconnect capture work, excluded from readiness; primary capture belongs
to `verification_us`. `verified_us` reports capture comparison and persistence
verification completion, followed by the own-query response gate and one drain.
A two-millisecond quiet confirmation is excluded from the drained-link timestamp
but included in drain CPU intervals. Protocol acknowledgment
tails can follow UI readiness. Every record requires successful exact verification. Zero-row cases use cold/empty only. `warm_refresh` also reports the true versioned readiness endpoint.

The sender schedules serialization on a shared lane per direction, then adds
RTT/2 propagation. Serialization overlaps propagation and both directions are
independent. Scheduled delivery runs on real Tokio timers, whose granularity adds
scheduling variation. There is no real socket, transport framing, packet loss,
congestion control or physical network. Message counts are not round-trip counts
when messages are pushed concurrently. A connection generation prevents delayed
messages from an old link from crossing a reconnect.

`STARTUP` JSON contains both landmarks, directional messages/bytes, largest
message, returned selection rows/bytes, point reads and requested events. Work
and traffic include transfer, verification and the single post-verification drain,
including control traffic emitted by asynchronous activation. Selection rows count returned
materialized states, not internal index visits. Storage counters describe public
StorageEngine calls and requested IDs, not physical disk I/O or cache misses.
The verifier's known direct point reads are subtracted from native work counts,
while asynchronous application work during verification remains included. Random entity IDs produce tree
shape variation. `PHASE` diagnostics identify setup or measurement hangs.
A 180-second fixture timeout fails rather than producing an incomplete success.

## Comparing revisions

Build both with the same compiler and optimized profile and verify shared package
versions match. Keep the original lockfiles and document additional dependencies.
For the native baseline overlay, copy `tests/benches/live_query_startup.rs`
and the identical `compare_live_query_startup.py` runner. Add the
`[[bench]]` target `name = "live_query_startup"`, `harness = false`, and
`libc = "0.2"` under `tests/Cargo.toml` dev-dependencies. The validated
baseline already has the other fixture dependencies; no production source
is changed. The runner reads the sibling Rust source for its metadata hash,
so keep the two files together.
Use separate production-only Cargo targets for baseline, candidate and wasm;
never share them with private instrumentation. Preserve each executable/package,
check source hashes before and after builds, and scan artifacts for private
production diagnostics; source stability alone cannot detect stale dependency
reuse. Finish builds before timed sampling. Record aggregate host CPU/load and
the benchmark's own resource use; aggregate totals include the benchmark and
cannot identify the cause of contention. Preserve every planned observation and
record any pre-sample wait or unavailable monitoring. The current report records
the exact monitoring policy used for its run.

```sh
python3 tests/benches/compare_live_query_startup.py \
  --baseline /absolute/path/to/baseline-binary \
  --candidate /absolute/path/to/candidate-binary \
  --baseline-revision BASE_SHA --candidate-revision CANDIDATE_SHA \
  --bandwidths 0,1250000 --repeats 7 \
  --output /absolute/path/to/new-results-directory
```

The Python runner uses Unix `wait4` and process groups; its process-resource
collection supports macOS and Linux. The Rust fixture can report null phase
CPU on other hosts, but the Python resource runner needs a separate adapter there.
It uses a fresh process per fixture, alternates AB/BA order, refuses to
overwrite evidence, and saves every failure with its last phase and raw output.
The default external deadline is 240 seconds. Whole-process child CPU, peak RSS
and page faults cover setup, prefetch, warmup, transfer, verification and teardown;
they are resource context, **not transfer-only CPU or memory**. On Unix,
`phase_process_cpu` separately samples process user/system CPU with `getrusage`:
setup ends at measurement start; `through_ready` ends at the public durable-ready
landmark (null for reconnect); local installation, reconnect capture, verification,
verified completion and post-verification drain have their own intervals. Initial
and final drain CPU fields are aliases for the same cumulative interval. These counters combine client/server
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
slowdown allowance. The main reference can send an entire result in one message;
the previous digest reference already uses bounded frames. Report the largest
message alongside latency. Main's performance does not justify
removing bounds from the new protocol. These native fixtures do not qualify
million-row scaling, concurrent writes, browser storage or retained allocation
bounds; those need separate evidence.

## Browser fixtures

`tests-wasm/bindings/src/query_parity.rs` and `src/query-parity.test.ts` use
the same public-API operations against two IndexedDB nodes with serialized
messages, directional bandwidth lanes and `performance.now`. The fixture model
uses the default numeric LWW position and YrsString body; it is not a pure-LWW
application benchmark. For the browser baseline overlay, copy
`tests-wasm/bindings/src/query_parity.rs` and `tests-wasm/src/query-parity.test.ts`,
and add `mod query_parity;` to the bindings `src/lib.rs`. Add these bindings
dependencies exactly as in the validated overlay: `serde_json = "1"`,
`async-trait = "0.1"`, `bincode = "1.3"`, `anyhow = "1.0"`, and
`gloo-timers = { version = "0.3", features = ["futures"] }`. Keep existing
wasm dependencies and `web-sys` console support. These are test instrument
changes, not a backport of digest production code.

The combined validated overlay changes eight files: the two manifests,
`Cargo.lock`, native fixture and runner, bindings module and declaration, and
the TypeScript fixture. Original dependency versions remain unchanged; only
the test packages' dependency edges are added to the effective lockfile. The
pristine source hashes, exact changed-file hashes and isolated target identities
are recorded in the current evidence ledger's `baseline-overlay-manifest.json`.
Keep that original/effective lockfile proof with any future historical adapter.

Build both wasm artifacts with the same release profile and wasm-pack version.
The default Vitest matrix is functional smoke coverage, not performance evidence.
Configure `VITE_QUERY_PARITY_SIZES`, `RTTS`, `BPS`, `MODES`, `CACHE`, `REPEATS`
and `LABEL` using the `VITE_QUERY_PARITY_` prefix; records start `BROWSER_STARTUP`.
Use fresh browser contexts per measured fixture, alternating baseline/candidate,
and retain browser version, module/wasm hashes and raw failures. Browser timer
granularity and IndexedDB task scheduling are part of these measurements.
