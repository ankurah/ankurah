# Compatible resident-tree performance — 2026-10-21

The shared node-owned cache now reuses compatible complete trees, keeps bounded coverage for cold limited queries, and pins active trees with RAII leases. Production checkpoints are `19816db5`, `1b4c989d`, `fcd05988` and `ec767596`. No storage or durable-replication integration is included.

All **420 primary measurements**, **56 separately planned native replication measurements**, and **51 browser functional checks** passed their correctness assertions. Core has 515 passing tests (one existing ignore), digest-tree has 149 plus two documentation tests, and 53 affected two-node integrations pass. The lead independently checked the 476 timing records, pairing, raw native output, arithmetic, source inputs and frozen artifact hashes.

Large unlimited cold and warm results consistently improve initialization latency. Small and bounded results have modest central latency differences, but the original native membership tail and highly variable 16,384-row browser cold cases prevent an unqualified parity claim. The separately planned native replication did not reproduce the original median slowdown; every original slow observation remains below. A fixed twelve-run scratch-only timeline diagnostic also passed; it did not reproduce the original tail and does not replace these measurements. The implementation is ready for design/code review, with resource costs and tail-latency uncertainty explicit. Human review and landing remain pending.

Resident maintenance is not free: native unlimited cold startup at 4,096 rows/50 ms RTT adds 112 ms endpoint CPU and 34.5 MiB whole-process peak RSS despite improving readiness by 101 ms. Browser readiness improves by 2.26 seconds with 13.9 MiB additional WASM capacity. Full-prefetch costs include making the initial complete resident; they are excluded from the later LIMIT10 cache-hit latency but included in whole-fixture CPU/memory.

Measurements are local shared-workstation evidence, not isolated-host tail-latency certification. Numeric ORDER BY, million-row scaling and sustained concurrent churn remain outside this qualification. See [the method](LIVE_QUERY_STARTUP.md), [the historical unlimited-only reuse report](LIVE_QUERY_STARTUP_RESULTS_UNLIMITED_REUSE.md), and [the pre-reuse report](LIVE_QUERY_STARTUP_RESULTS_PRE_REUSE.md).

## Primary comparison

Six unlimited sentinel cases and nine padded-string LIMIT10 cases per platform, including complete-resident reuse for LIMIT10. Numeric ORDER BY remains outside this performance qualification; long-sort-key reconnect correctness is covered by separate red/green integration tests. Exploratory samples are retained separately and excluded from this final matrix.

420 verified of 420 recorded / 420 planned samples; 0 failures.

Baseline: main b5fcfd3e. Candidate pin and artifact hashes are recorded in candidate-native.json / candidate-wasm.json.

cold and first same-selection refresh measure versioned durable readiness; reconnect measures fresh loaded result installation. Immutable ordered membership (when ordered), full state and heads captured at that endpoint must match the external oracle; persistence is checked separately. This is not time to first record.

Seven alternating pairs per case. No excluded samples or outliers. Positive deltas mean slower candidate. Cold/all prefetches the measured selection before timing. Cold/full prefetches the same predicate without ORDER BY or LIMIT before timing, then measures a new LIMIT10 livequery over the complete resident.

| Platform | Population | Limit | Operation/cache | RTT ms | Main median ms | Candidate median ms | Paired delta ms [range] | Slower pairs |
|---|---:|---:|---|---:|---:|---:|---|---:|
| native | 8 | none | cold/empty | 0 | 0.735 | 0.828 | +0.102 [-0.033, +0.139] | 6/7 |
| native | 8 | none | cold/empty | 50 | 69.490 | 69.773 | +0.283 [-15.802, +23.316] | 4/7 |
| native | 256 | none | cold/half | 50 | 77.624 | 77.869 | -0.338 [-2.315, +9.162] | 3/7 |
| native | 4096 | none | cold/empty | 50 | 449.929 | 349.737 | -101.141 [-116.012, -77.638] | 0/7 |
| native | 4096 | none | warm_refresh/empty | 50 | 143.623 | 96.798 | -47.417 [-49.508, -42.967] | 0/7 |
| native | 4096 | none | warm_edit/all | 50 | 173.543 | 139.439 | -32.897 [-40.218, -30.192] | 0/7 |
| native | 4096 | 10 | cold/empty | 0 | 10.353 | 10.620 | +0.177 [-0.527, +0.681] | 5/7 |
| native | 4096 | 10 | cold/empty | 50 | 69.267 | 70.467 | +1.714 [-2.630, +3.754] | 5/7 |
| native | 4096 | 10 | warm_refresh/empty | 0 | 0.362 | 0.252 | -0.125 [-0.153, -0.089] | 0/7 |
| native | 4096 | 10 | warm_refresh/empty | 50 | 56.732 | 56.167 | -0.565 [-2.981, +0.515] | 1/7 |
| native | 4096 | 10 | warm_edit/all | 50 | 53.953 | 54.223 | +0.531 [-1.353, +2.997] | 4/7 |
| native | 4096 | 10 | warm_membership/all | 50 | 54.294 | 64.882 | +10.190 [-1.175, +20.345] | 5/7 |
| native | 16384 | 10 | cold/empty | 0 | 66.854 | 65.377 | -3.355 [-8.250, +6.870] | 3/7 |
| native | 16384 | 10 | cold/empty | 50 | 129.473 | 140.837 | +13.747 [-4.315, +28.087] | 4/7 |
| native | 4096 | 10 | cold/full | 0 | 0.547 | 0.864 | +0.353 [+0.288, +0.408] | 7/7 |
| wasm | 8 | none | cold/empty | 0 | 11.400 | 11.100 | -0.300 [-0.800, +0.000] | 0/7 |
| wasm | 8 | none | cold/empty | 50 | 68.100 | 68.200 | +0.100 [-2.000, +2.200] | 4/7 |
| wasm | 256 | none | cold/half | 50 | 150.300 | 139.700 | -11.300 [-12.800, -8.700] | 0/7 |
| wasm | 4096 | none | cold/empty | 50 | 4439.100 | 2239.100 | -2260.900 [-2366.500, -2120.800] | 0/7 |
| wasm | 4096 | none | warm_refresh/empty | 50 | 839.200 | 145.000 | -694.200 [-702.600, -680.800] | 0/7 |
| wasm | 4096 | none | warm_edit/all | 50 | 775.700 | 147.300 | -634.000 [-729.000, -606.200] | 0/7 |
| wasm | 4096 | 10 | cold/empty | 0 | 217.700 | 221.100 | -1.900 [-61.300, +17.400] | 3/7 |
| wasm | 4096 | 10 | cold/empty | 50 | 267.000 | 141.700 | -88.700 [-145.600, +146.300] | 3/7 |
| wasm | 4096 | 10 | warm_refresh/empty | 0 | 5.000 | 1.300 | -3.800 [-8.100, -3.000] | 0/7 |
| wasm | 4096 | 10 | warm_refresh/empty | 50 | 58.000 | 53.800 | -4.300 [-5.600, -2.400] | 0/7 |
| wasm | 4096 | 10 | warm_edit/all | 50 | 61.000 | 56.000 | -5.200 [-10.200, -2.900] | 0/7 |
| wasm | 4096 | 10 | warm_membership/all | 50 | 61.900 | 58.400 | -3.800 [-9.800, -1.600] | 0/7 |
| wasm | 16384 | 10 | cold/empty | 0 | 824.900 | 823.900 | +28.700 [-351.500, +1365.800] | 4/7 |
| wasm | 16384 | 10 | cold/empty | 50 | 462.500 | 472.000 | +12.000 [-7.200, +793.900] | 6/7 |
| wasm | 4096 | 10 | cold/full | 0 | 5.100 | 5.400 | +0.300 [-7.200, +0.700] | 5/7 |

## Resource costs

Representative RTT0 cells; medians are paired candidate-minus-main. CPU phases overlap and must not be added together. Browser CPU covers the whole main-thread fixture and excludes IndexedDB worker CPU. WASM capacity is allocated linear-memory capacity, not RSS or live allocations.

| Platform / operation | Endpoint CPU delta ms | Whole-fixture CPU delta ms | Memory delta MiB |
|---|---:|---:|---:|
| Native / N4,096 / cold/empty / LIMIT10 / RTT0 | +0.390 | +4.491 | +2.234 peak RSS |
| Native / N4,096 / warm_refresh/empty / LIMIT10 / RTT0 | -0.127 | +3.839 | +1.797 peak RSS |
| Native / N4,096 / cold/full / LIMIT10 / RTT0 | +0.373 | +349.071 | +25.719 peak RSS |
| Browser / N4,096 / cold/empty / LIMIT10 / RTT0 | — | +45.385 | +0.062 WASM capacity |
| Browser / N4,096 / warm_refresh/empty / LIMIT10 / RTT0 | — | +16.412 | +0.062 WASM capacity |
| Browser / N4,096 / cold/full / LIMIT10 / RTT0 | — | -131.336 | +5.250 WASM capacity |

## Host observations

This was a shared interactive workstation, not an isolated performance host. The native primary window and the eventual native replication window had no cargo/rustc process in their before/after snapshots; sampled intermediate browser inventories also had no compiler. Those snapshots are not continuous profiling or proof of zero contention.

The browser end snapshot at 02:17:58 UTC contains cargo at 0% reported CPU. Twenty seconds later, the replication launch guard found cargo/rustc compiling under an unrelated Python task whose working directory was /Users/daniel/vt/greyorange. Possible overlap with the final browser full-prefetch case is retained as a caveat. Both blocked replication launches produced zero samples and are archived separately. Once the compiler guard cleared, the unchanged, predeclared 56-sample replication ran once. No user process was altered or killed.

The browser N16,384 / zero-RTT cell includes a +1,365.8 ms paired outlier and substantial variability. A nearby process snapshot shows rust-analyzer, VSCode renderer, WindowServer, and unrelated resident test binaries consuming CPU; it does not prove causation or identify the outlier's database/worker cost. Whole-fixture browser CPU includes setup and excludes IndexedDB workers, so it cannot isolate endpoint CPU. All seven pairs and these limitations are recorded in browser-16384-zero-rtt-observation.md. The N16,384 / 50-ms cell also retains two large unfavorable deltas (+730.6 and +793.9 ms).

All 420 primary samples and all 56 separately planned replication samples are retained. The replication changes the appraisal of the native variation without replacing the original rows: combined 14-pair median differences are −0.381 ms for N16,384 cold/50-ms and +0.291 ms for bounded warm membership, whose original +20.345 ms tail remains. Bounded warm-membership endpoint CPU is consistently higher across both sets (combined median +0.760 ms). The separate scratch-only diagnostic below used different, instrumented artifacts and does not change this performance evidence.

Native endpoint process CPU includes both peers/background threads during the measured interval; whole-process CPU/RSS also include setup/prefetch/verification/teardown. Browser CDP task/script CPU covers the whole main-thread fixture and excludes IndexedDB worker CPU; WASM capacity is allocated linear memory, not peak RSS or live allocation. CPU phase intervals overlap and must not be summed.

No numerical slowdown allowance is assumed. Report each case and resource tradeoffs without averaging unlike cases into a parity claim; disclose unrelated host activity.

Qualifies the sampled unlimited and padded-string LIMIT10 selections only. Numeric ORDER BY has a separately recorded pre-existing baseline defect and is not covered here. Does not qualify million-row scale, sustained concurrent churn, or p95/p99 latency.

Full arm values, paired distributions, CPU/memory/work/traffic metrics and raw-result hashes are in qualification-summary.json.

## Exact artifact pins

| Role / platform | Revision | Artifact SHA256 |
|---|---|---|
| baseline / native | `b5fcfd3eac3ea4247c4bd7613c439393beb42aad` | `d8477e71dd6d75bafc5c385e40ce6f4b6e3d31eb5ded2370d6a7b736542d73d4` |
| baseline / wasm | `b5fcfd3eac3ea4247c4bd7613c439393beb42aad` | `2e8011fc3cda7d94cd00187c0db308a98c8372e8353a228e93c5c0b44d444574` |
| candidate / native | `ec767596ff6c5105d97939cc6e0879da3d515bd2+working-source-db233818433b` | `d7697141a7800239fa259c9aa409530dab4800f58851870b3a87868bd0e6e5c8` |
| candidate / wasm | `ec767596ff6c5105d97939cc6e0879da3d515bd2+working-source-db233818433b` | `9d6aac281f87fe14fcb07043997ededb912f4a722827962f2340623f2220a820` |

Source exports, manifests, complete working patches, runner hashes and before/after stability audits are retained beside this report. The candidate source archive and runtime manifest bind the exact measured implementation; benchmark harness files are identical on both arms.

## Earlier preflights and historical evidence

These are separate artifacts and are not pooled with the final matrix. Every failure and exploratory sample is retained. The current final matrix follows a predeclared schedule without sign-based repeats or exclusions.

- first artifact: held before timing after DESC gap-fill ordering integration failure.
- attempt-2: historical unlimited-only qualification; immutable and separate from current artifact.
- attempt-3-bounded: numeric ORDER BY oracle failed on both main and candidate; six failed trial samples retained, no valid parity samples. Exact suffix-order probe then failed candidate while main passed; source ordering fix has independent red/green coverage.
- attempt-4-bounded-string: built unchanged current main overlay artifacts; candidate superseded before builds/timings for the ordering fix.
- attempt-5-bounded-order-fix: 24 verified exploratory samples exposed material Complete4096→LIMIT10 cost; no final matrix samples on that artifact.
- attempt-6-lazy-bounded-view exploration: 24 verified exploratory samples after lazy-view fix, excluded from final 420-sample matrix.

## Large unlimited startup and reconnect resource costs

Population 4,096, simulated RTT 50 ms. The empty cache label describes fixture startup; first refresh is measured after that initial query has populated matches. Warm edit uses prefetched matches. These resource costs accompany endpoint latency improvements; faster initialization does not imply lower CPU or memory.

| Platform / population / operation / cache / RTT | Paired endpoint CPU ms | Paired whole-fixture CPU ms | Paired memory MiB |
|---|---:|---:|---:|
| Native / N4,096 / cold/empty / 50 ms | +112.338 | +126.887 | +34.547 peak RSS |
| Native / N4,096 / warm_refresh/empty / 50 ms | -27.741 | +169.985 | +30.375 peak RSS |
| Native / N4,096 / warm_edit/all / 50 ms | -83.727 | +206.437 | +28.469 peak RSS |
| Browser / N4,096 / cold/empty / 50 ms | unavailable | -384.224 main-thread fixture CPU | +13.938 WASM capacity |
| Browser / N4,096 / warm_refresh/empty / 50 ms | unavailable | -639.839 main-thread fixture CPU | +13.250 WASM capacity |
| Browser / N4,096 / warm_edit/all / 50 ms | unavailable | -832.277 main-thread fixture CPU | +12.812 WASM capacity |

Returned-row counts measure states returned/hydrated, not every row the storage engine visits. Native cold LIMIT10 returns ten server rows on both arms, but the unchanged Sled engine may build an on-demand index by visiting the full population first. The N16,384 cold cases therefore include common index-construction work; equal returned-row counts alone do not establish equal CPU cost. Browser CPU excludes IndexedDB workers and cannot localize database waiting or index work.

The original native bounded warm-membership tail and larger-population browser cold measurements have unresolved variability. The separately predeclared 56-sample replication and scratch-only timeline diagnostic remain distinct from this 420-sample qualification; no precise broad parity or tail-latency claim follows from these rows alone.

## Separate native replication

One additional predeclared set of 56 samples on the same frozen main/candidate artifacts. All original 420 samples remain unchanged and separate. No exclusions, sign-based retries, or repeat-until-pass. Combined columns concatenate the two independently paired sets; they do not pair unrelated runs.

Replication verified: 56/56.

| Case | Dataset | Main median ms | Candidate median ms | Paired latency delta ms [range] | Slower pairs | Paired endpoint CPU delta ms [range] |
|---|---|---:|---:|---|---:|---|
| limit10-cold-16384-empty-0ms | original | 66.854 | 65.377 | -3.355 [-8.250, +6.870] | 3/7 | -2.769 [-8.791, +6.481] |
| limit10-cold-16384-empty-0ms | replication | 81.011 | 78.311 | -0.919 [-3.926, +2.049] | 3/7 | -0.333 [-4.504, +1.574] |
| limit10-cold-16384-empty-0ms | combined | 74.262 | 72.044 | -1.902 [-8.250, +6.870] | 6/14 | -1.551 [-8.791, +6.481] |
| limit10-cold-16384-empty-50ms | original | 129.473 | 140.837 | +13.747 [-4.315, +28.087] | 4/7 | +10.432 [-4.723, +31.335] |
| limit10-cold-16384-empty-50ms | replication | 134.774 | 132.510 | -1.685 [-5.522, +13.795] | 2/7 | -3.202 [-6.345, +14.480] |
| limit10-cold-16384-empty-50ms | combined | 131.107 | 135.511 | -0.381 [-5.522, +28.087] | 6/14 | +1.303 [-6.345, +31.335] |
| limit10-warm_membership-4096-all-50ms | original | 54.294 | 64.882 | +10.190 [-1.175, +20.345] | 5/7 | +2.383 [+0.744, +4.268] |
| limit10-warm_membership-4096-all-50ms | replication | 52.810 | 52.880 | +0.195 [-0.677, +1.888] | 4/7 | +0.387 [+0.312, +0.775] |
| limit10-warm_membership-4096-all-50ms | combined | 53.745 | 53.161 | +0.291 [-1.175, +20.345] | 9/14 | +0.760 [+0.312, +4.268] |
| unlimited-cold-8-empty-50ms | original | 69.490 | 69.773 | +0.283 [-15.802, +23.316] | 4/7 | +1.273 [-0.687, +3.363] |
| unlimited-cold-8-empty-50ms | replication | 54.606 | 54.878 | +0.318 [-0.457, +1.602] | 5/7 | +0.673 [+0.203, +0.861] |
| unlimited-cold-8-empty-50ms | combined | 56.344 | 56.701 | +0.300 [-15.802, +23.316] | 9/14 | +0.807 [-0.687, +3.363] |

Full arm/pair distributions, process CPU, peak RSS, work and message counters, and raw-result hashes are in native-replication-summary.json. Timer/host noise is not a reason to discard an unfavorable sample; CPU/work differences must be considered alongside the endpoint measurements.


## Narrow native timeline diagnostic

A separately prepared, instrumented copy ran exactly six alternating main/candidate pairs of the native 4,096-row population, LIMIT10, warm-membership reconnect at 50 ms RTT. Every run used a fresh process and the unchanged first-installation membership, ordering, state and head oracle. Trace capture was disabled during setup, buffered during measurement, and flushed afterward. These twelve runs are diagnostic evidence, not replacement timing samples.

All twelve passed. Candidate-minus-main activation differences were +0.261, +0.402, +0.241, +0.024, −0.159 and +1.541 ms. In every candidate run, the resident refill began 25–37 microseconds **after** the first loaded=true callback, returned ten rows in 41–57 microseconds, and required no validation retry. Response wake to readiness took 521–676 microseconds; commit-gate waits were 0–175 microseconds and observed index-lock waits approximately 0–1 microsecond.

This confirms that resident refill is not a required initialization step in those runs. It does **not** establish the cause of the original slow observations, where bounded reads returned nine rows and then ten as entity commits progressed. The original counters cannot distinguish an accepted intermediate read from a read discarded by revision validation. Either case is valid maintenance, and no redundant work or safe corrective production change was established. The implementation remains unchanged; the original tail, shared-host limitations, higher native CPU and larger browser variability remain review considerations.

## Local evidence

The full retained dataset is under `~/ak/research_notes/ankurah-replication/reviews/2026-10-21-compatible-reuse/attempt-6-lazy-bounded-view/`: `qualification-plan.json`, raw records/logs, `qualification-summary.json`, `replication-plan.json`, `native-replication-summary.json`, `root-independent-audit.json`, and `measurement-delivery.json`. The evidence includes source exports, complete patches, build manifests, runner hashes and process snapshots. Production source is frozen at `ec767596`; the candidate source-export fingerprint is `db233818433b`. The four public-API harness files are identical on both arms and are checkpointed with this report. Documentation changes do not relabel the measured artifacts.

The diagnostic report is `warm-membership-diagnostic/report.txt` under that evidence directory (SHA256 `118cfc61eab4c788243cd5140a079823eda523d95c17e9b900cfb54b2e9657e6`); its `results/samples.json`, raw traces, instrumentation patches and `source-audit.json` preserve all twelve runs and verify that the measured artifacts and live runtime source remained unchanged.
