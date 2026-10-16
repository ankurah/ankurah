# Ongoing query synchronization qualification — 2026-10-16

Candidate `81299f2185fa89918c79decbe02106e4ed66a82f` passes all **532 planned timing samples** across 38 cases, with exact-state verification, zero runner failures and stable source/artifact pins. The same optimized artifacts pass **40 native and 41 browser functional checks**. References are main after #526, `ae8962798085dbf6d717429e4b6566940f0051e1`, and the previous digest implementation, `6ef6f35e08064d2ae0d1c299d02a6acf1bee5eb1`.

The engineering recommendation is to proceed to Daniel's in-depth review. The large cold-start blocker is resolved: 4,096 uncached rows improve by paired medians of 110 ms natively and 2,284 ms in the browser versus main. There are measured exceptions: native 256-row half-cache startup costs 7.4 ms, browser eight-row first refresh costs 2.6 ms, and native edited reconnect costs 4.0 ms versus main. Compared with the previous digest implementation, large browser first refresh costs 47.2/13.0/10.6 ms for empty/half/all initial caches. Native setup CPU and process memory also remain higher. These are disclosed tradeoffs, not a claim of universal zero regression or a numerical allowance.

Root and independent Codex review found no further concrete production correction warranted by the completed results. No PR or human acceptance is implied. Claude/Fable was not called during this pass, as requested. The [post-#526 report](LIVE_QUERY_STARTUP_RESULTS_PRE_ONGOING.md) and [pre-#526 report](LIVE_QUERY_STARTUP_RESULTS_PRE_526.md) retain their original measurements and source pins.

## What changed

Fetch uses a finite captured comparison. A live query keeps one standing stream that can carry finite comparison rounds and live publications concurrently. Acknowledging an applied round releases its captured state; it does not unlock live delivery. Frame receipts release transport capacity separately from acknowledgments of applied causal targets. A publication's source membership and applied events become visible together to its owning query, and finishing an older baseline preserves newer committed membership.

The receiver bounds concurrent application to 128 finite-round jobs plus 16 Live/Repair jobs per stream. It acquires those slots after chunk assembly, preserves per-entity order and retains the existing commit/publication gate. This correction removed a large browser scheduling regression without combining transactions or weakening causal checks. Optional round residency avoids reloading just-applied entities before finite installation. Pins are admitted against full encoded-state bytes; canonical entities can grow after admission, so that charge is not a hard heap bound. These policies do not bound total node memory.

## Readiness versus main

Each case has seven alternating reference/candidate pairs at simulated 50 ms RTT. Times are milliseconds; differences are candidate minus reference. The paired median need not equal the difference of the two arm medians. Ranges include every pair, including unfavorable observations. References and platforms are not pooled.

| Platform | Rows | Operation | Initial cache | Bandwidth | Reference median | Candidate median | Paired change [range] | Slower pairs |
|---|---:|---|---|---|---:|---:|---:|---:|
| Native | 8 | Cold | empty | Unlimited | 57.2 | 57.6 | -0.1 [-2.6, +2.4] | 3/7 |
| Native | 256 | Cold | half | Unlimited | 71.7 | 79.8 | +7.4 [-4.3, +9.7] | 6/7 |
| Native | 4,096 | Cold | empty | Unlimited | 442.2 | 327.3 | -110.4 [-121.6, -105.0] | 0/7 |
| Native | 4,096 | First refresh | empty | Unlimited | 140.3 | 131.8 | -9.1 [-20.3, -5.4] | 0/7 |
| Native | 4,096 | Reconnect, edit | all | Unlimited | 170.3 | 173.9 | +4.0 [-0.1, +5.8] | 6/7 |
| Native | 2,048 | Reconnect, equal | all | 125 kB/s | 1369.5 | 1368.1 | -1.4 [-4.4, +1.5] | 1/7 |
| Browser | 256 | Cold | empty | Unlimited | 225.9 | 199.6 | -25.3 [-32.2, -23.6] | 0/7 |
| Browser | 4,096 | Cold | empty | Unlimited | 4378.1 | 2095.0 | -2284.1 [-2357.4, -2245.4] | 0/7 |
| Browser | 8 | First refresh | empty | Unlimited | 58.0 | 60.4 | +2.6 [+0.3, +3.1] | 7/7 |
| Browser | 4,096 | First refresh | empty | Unlimited | 840.6 | 689.9 | -150.7 [-189.0, -128.8] | 0/7 |
| Browser | 4,096 | Reconnect, edit | all | Unlimited | 755.4 | 644.7 | -111.2 [-114.8, -98.5] | 0/7 |
| Browser | 256 | Reconnect, membership | all | 125 kB/s | 291.5 | 275.7 | -10.1 [-23.7, +15.3] | 3/7 |

Large cold/refresh browser cases and large native cold startup improve in every pair against main. Small-case costs remain visible above; faster large cases do not erase them. The slow-link membership result has mixed signs.

## Readiness versus the previous digest implementation

This compares the ongoing implementation with the previous digest candidate under the same fixtures; it does not attribute costs to individual changes. It is a separate comparison, not a substitute for main.

| Platform | Rows | Operation | Initial cache | Bandwidth | Reference median | Candidate median | Paired change [range] | Slower pairs |
|---|---:|---|---|---|---:|---:|---:|---:|
| Browser | 4,096 | Cold | empty | Unlimited | 2245.9 | 2128.1 | -74.3 [-246.3, -8.4] | 0/7 |
| Browser | 4,096 | Cold | all | Unlimited | 1029.1 | 1041.2 | +17.9 [-8.0, +32.8] | 5/7 |
| Native | 2,048 | Reconnect, equal | all | 125 kB/s | 1361.5 | 1358.8 | -1.0 [-13.0, +17.3] | 3/7 |
| Native | 256 | Cold | empty | Unlimited | 80.5 | 84.1 | +2.7 [-0.6, +12.7] | 6/7 |
| Native | 256 | Cold | half | Unlimited | 74.2 | 75.4 | +1.1 [-7.2, +4.7] | 4/7 |
| Native | 256 | Cold | all | Unlimited | 62.0 | 64.4 | +3.1 [-1.8, +4.1] | 5/7 |
| Native | 4,096 | Cold | empty | Unlimited | 363.3 | 328.7 | -34.7 [-95.8, -20.8] | 0/7 |
| Native | 256 | Reconnect, edit | all | 1.25 MB/s | 75.9 | 75.1 | +1.0 [-7.4, +7.6] | 4/7 |
| Native | 256 | Reconnect, membership | all | 1.25 MB/s | 77.3 | 80.5 | +1.7 [-9.2, +6.2] | 5/7 |
| Native | 8 | Cold | empty | Unlimited | 57.7 | 57.6 | -0.0 [-1.7, +2.1] | 3/7 |
| Native | 4,096 | Cold | all | Unlimited | 189.5 | 190.9 | +1.8 [-4.9, +3.7] | 5/7 |
| Native | 4,096 | First refresh | empty | Unlimited | 123.8 | 130.9 | +8.2 [+6.6, +10.1] | 7/7 |
| Native | 4,096 | First refresh | all | Unlimited | 105.9 | 108.2 | +2.4 [-4.9, +5.6] | 6/7 |
| Native | 4,096 | Reconnect, edit | all | Unlimited | 171.5 | 172.9 | +2.1 [-7.3, +7.0] | 5/7 |
| Browser | 256 | Cold | empty | Unlimited | 207.5 | 201.9 | -3.5 [-11.3, -0.5] | 0/7 |
| Browser | 256 | Cold | half | Unlimited | 139.5 | 140.4 | +0.4 [-5.7, +3.5] | 5/7 |
| Browser | 4,096 | First refresh | empty | Unlimited | 669.7 | 718.8 | +47.2 [-16.2, +127.1] | 6/7 |
| Browser | 4,096 | First refresh | half | Unlimited | 516.8 | 526.8 | +13.0 [-0.3, +17.0] | 6/7 |
| Browser | 4,096 | First refresh | all | Unlimited | 466.6 | 478.8 | +10.6 [-0.8, +29.4] | 6/7 |
| Browser | 8 | First refresh | empty | Unlimited | 59.2 | 60.9 | +1.6 [-1.7, +5.3] | 6/7 |
| Browser | 8 | Reconnect, edit | all | Unlimited | 61.4 | 63.0 | +1.7 [-0.1, +4.0] | 6/7 |
| Browser | 8 | Reconnect, membership | all | Unlimited | 62.4 | 64.0 | +1.6 [-1.4, +3.6] | 6/7 |
| Browser | 4,096 | Reconnect, edit | all | Unlimited | 639.7 | 642.2 | +1.3 [-7.2, +13.9] | 4/7 |
| Browser | 4,096 | Reconnect, membership | all | Unlimited | 643.1 | 645.8 | +2.7 [-1.8, +29.9] | 6/7 |
| Browser | 256 | Reconnect, edit | all | 125 kB/s | 267.5 | 277.9 | +9.2 [-8.2, +28.0] | 5/7 |
| Browser | 256 | Reconnect, membership | all | 125 kB/s | 281.0 | 280.4 | +0.9 [-15.0, +18.6] | 4/7 |

The first-refresh residual is systematic: all three large browser cache cases are slower in six of seven pairs, and native first refresh after an empty initial cache is slower in all seven. Large browser edited/membership reconnect is close in readiness, while its whole-fixture CPU is higher. No case was repeated or excluded for its sign.

## Resource costs and wire work

- Native slow-link 2,048-row equal reconnect has setup CPU medians of 533.1/795.0 ms (main/candidate), paired change **+259.189 ms**, range +247.644 to +292.879 ms, all seven positive. Whole-process CPU rises by 256.938 ms. Versus the previous digest branch, setup CPU rises by 16.230 ms with mixed signs. Setup includes population, prefetch and initial query; counters from the later timed operation do not quantify that work.
- Native 4,096-row edited reconnect adds **102.122 ms setup CPU** versus main, range +78.753 to +118.820 ms, all seven positive; whole-process CPU adds 90.922 ms. Endpoint CPU is lower. These overlapping intervals must not be summed. The browser admission diagnostic does not quantitatively attribute these native setup costs.
- Native large-case peak process memory rises by paired medians of about **21–28 MiB versus main** and **28–35 MiB versus the previous digest branch**. For context, main/candidate peak medians are 211.3/234.4 MiB for cold 4,096 and 213.6/243.1 MiB for edited reconnect. These cover both peers and the entire fixture, not retained per-tree allocation or evidence of a leak.
- Browser whole-fixture main-thread CPU versus main falls by paired medians of 486.072/586.384/378.362 ms for large cold/first-refresh/edited-reconnect cases, every pair lower. The eight-row first-refresh fixture instead adds **18.314 ms**, range +14.991 to +18.832 ms, every pair positive. Slow membership adds 11.702 ms in every pair; 256-row cold adds 5.432 ms with mixed signs.
- Versus the previous digest branch, browser eight-row fixtures add about 6–7 ms whole-fixture CPU. Large cached cold/refresh/edit/membership fixtures add about 52–83 ms in paired medians. Empty/half first-refresh fixtures use less whole-fixture CPU despite slower readiness; this does not identify the cause of that latency difference.
- Browser final wasm capacity rises about 3.1–5.3 MiB in the large main-relative cases and 2.4–4.1 MiB versus the previous digest branch. JS heap varies. These final capacities are not peak RSS, and they must not be summed with one another or described as query-phase memory.
- Native 4,096-row first-refresh requester bytes fall from 311,598 to 6,860 versus main. Cold empty's largest serialized envelope falls from about 1.44 MB to 65,834 bytes. Versus the previous digest browser reference, large cold uses 19 fewer messages in each direction and 3,256 fewer requester bytes. Other cells have small control-byte increases. Serialized bytes exclude transport framing; pushed message counts are not dependent round-trip counts.

## Correctness and review

On the final production source, 449 core tests pass with one existing ignore; the final 95 state-sync checks also pass after strengthening the incomplete-chunk admission test. These cover separate bulk/live admission, actual repair progress while bulk reads are stalled, per-entity ordering, cancellation, publication/event ordering, finite readiness, refresh, policy checks and stale-generation rejection. Source and receiver reviews led to removal of the obsolete unsequenced update path and unused helpers.

The broad native integration run at production-identical `e56efea5` passed 372 tests and failed one repair-path assertion, with 24 existing ignores; JWT passed 125 tests. The failing fixture had already passed its final membership, merged-value, both-head and persisted-coverage assertions. A concurrent live echo could legitimately satisfy the target without the separate EventBridge that its final assertion demanded.

Test-only `81299f21` creates divergence offline before reconnecting, preserves all original assertions, checks the captured key explicitly, and withholds the actual required repair to prove readiness waits. That focused test passes on a Git tree identical to the measured candidate. The original broad failure is retained; it is not relabelled as a new 373-pass suite. Exact optimized native40/browser41 checks and all timing-state checks are additional evidence.

The browser functional set includes ordinary tests, two real IndexedDB-backed nodes, standard query cases, gapped caches and an interleaved-cache summary-frontier case. Earlier unchanged standalone/protocol checks retain their own pins. Existing ignores are not silently converted into coverage.

## Measurement boundaries and preserved attempts

Cold and first refresh measure the current version's public durable readiness. Reconnect measures the first fresh loaded result after resetting loading metadata, with the query's own response required afterward as a validity check. Immutable full states, causal heads and membership are captured at the endpoint before asynchronous persistence verification or transport drain. Capture is atomic per entity; these fixtures perform no concurrent writes during timing.

`warm_refresh` calls the portable same-selection `update_selection_wait`; it does **not** measure the new same-stream `refresh()` API. Same-stream rounds and ongoing interleaving have directed correctness coverage. These reconnect timings also do not establish sustained live-update throughput, million-row behavior or tail latency.

Native process CPU covers both nodes and all threads; setup/through-ready/verification intervals overlap. Browser CDP CPU covers the entire main-thread fixture, including initialization, setup, prefetch, verification and cleanup; it excludes off-thread IndexedDB and later context teardown. It is not endpoint CPU.

Every final sample uses unchanged public v8 fixtures, fresh production-only artifacts and alternating pair order. Builds, tests, profilers and timing stages were sequential for this task. Compiler checks, before/after source and artifact checks, raw records and failures are retained. The shared host had unrelated work; seven pairs do not establish p95/p99 or eliminate variability.

Earlier candidates remain historical. The `92bf6ac2` pilot exposed extra cold reloads, corrected by bounded optional round residency. The `f1735cbe` browser run exposed a 1.5–1.8-second cold regression and stopped after 63 verified samples plus one recorded shutdown interruption. A separate six-sample private active-application diagnostic demonstrated the scheduling improvement; it is not pooled with final qualification. Its initial zero-sample launch failure is also preserved. The production two-lane correction then received fresh correctness/build/functional checks and this complete 532-sample matrix, without extra pilots or sign-based reruns.

## Scope and follow-up

Both peers still materialize query results. Native transaction/storage adapters, durable replication, idle-tree retention, million-row reads and broader hardening remain later work. Source queues and receiver work are bounded and may require stream restart; sustained churn can exhaust limited-query refill attempts. An uncertain interrupted commit makes source publication unavailable until node reconstruction.

The existing CPU-attribution task (`AcnKJDI9`) now leads with these current costs. Bounded transactional application batching (`xL8b6_sJ`) and retained-tree lifetime (`YC_KEwJL`) remain proposals; limiting active jobs does not implement batching. Prebranch ORDER BY, IndexedDB teardown and reader-adoption event-notification issues remain separate. There is no promise of exactly-once notification for every committed event.

The [benchmark history proposal](DIGEST_BENCHMARK_PLAN.md) remains suitable for the separate website/dashboard work; no publication pipeline is enabled here. Fwd implementation/validation tasks are submitted for review with this evidence, while landing still awaits Daniel's review.

## Reproduction and evidence

[LIVE_QUERY_STARTUP.md](LIVE_QUERY_STARTUP.md) defines fixtures, runner settings, reference overlays and endpoints. Native uses pinned Rust 1.97 nightly; browser uses pinned Rust 1.101 nightly, identical within each platform/reference comparison.

Candidate native binary SHA-256: `4836670e2ff5e3b16740865b75709c8c0682a762e0046523a1532be507c58b45`. Candidate wasm SHA-256: `8b8a8f9ebb837acd2215c85f282a94f7d3693466846bff8b6957708c19aca657`.

Local evidence root: `~/ak/research_notes/ankurah-replication/reviews/2026-10-15-ongoing-sync-performance/`. The frozen plan is `qualification-plan-81299f2185fa-frozen.json`, SHA-256 `60f103d9327e701e73b679111374f0c83dfdb67ce98324b95179684da0dbca6c`. `final-81299f2185fa-recomputed.{json,md}` contains all 38 cases, every arm/pair value, resource/work counters and evidence hashes. The five stage reports retain separate references. `root-final-81299f2185fa-audit.json` independently checks all 532 raw samples, 38 endpoint vectors and 629 evidence-file hashes, including paired standard deviation and median absolute deviation.

Build/functional manifests and `correctness-applicability-81299f2185fa.json` retain actual source pins and the original fixture failure. The private design review and correction evidence are under the sibling `2026-10-15-ongoing-sync-stress/` directory. Historical samples and failed/interrupted attempts remain intact.
