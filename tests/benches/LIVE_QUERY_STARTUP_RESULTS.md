# Memory-first query qualification — 2026-10-13

Frozen candidate `4c9b0482ef749f9a77b09ee8df4fdae945f59df1` (production change `2e98ff93`) was compared with main `ebdb1727254c675a4d37236ecf23c16c06c4df12`. All **350 final timing samples passed exact-state checks**, and **80 functional fixtures passed**. Ordinary browser unit tests cover their own assertions rather than full replicated-state capture. These are the final representative cases; earlier wider measurements retain their original checkpoint labels.

Large cold and first-refresh cases improve. Repeatable small-query costs remain, as do a separate wide-key first-refresh cost and higher native setup CPU. These results support a review of the tradeoffs; they do not establish zero-regression parity or an accepted numerical slowdown allowance.

## Final readiness measurements

Each row has seven alternating baseline/candidate pairs at simulated 50 ms RTT. Values are milliseconds. “Change” is the median of paired differences, so it need not equal the difference of the two medians. Negative favors the candidate. Initial cache describes entity coverage before the first query; reconnects retain that query. Bandwidth is per direction; “unlimited” still includes propagation delay.

| Platform | Rows | Operation | Initial cache | Bandwidth | Main median | Candidate median | Paired change |
|---|---:|---|---|---|---:|---:|---:|
| Native | 256 | Cold | empty | Unlimited | 82.7 | 88.7 | +6.7 |
| Native | 256 | Cold | half | Unlimited | 74.4 | 79.2 | +4.9 |
| Native | 256 | Cold | all | Unlimited | 65.5 | 66.5 | +1.0 |
| Native | 4,096 | Cold | empty | Unlimited | 441.7 | 364.3 | -77.7 |
| Native | 256 | Reconnect, edit | all | 1.25 MB/s | 80.0 | 82.1 | +2.6 |
| Native | 256 | Reconnect, membership | all | 1.25 MB/s | 76.2 | 84.0 | +6.0 |
| Native | 2,048 | Reconnect, equal | all | 125 kB/s | 1372.4 | 1366.1 | -6.1 |
| Browser | 8 | Reconnect, edit | all | Unlimited | 60.4 | 62.4 | +2.1 |
| Browser | 8 | Reconnect, equal | all | Unlimited | 56.7 | 57.3 | +0.3 |
| Browser | 8 | Reconnect, membership | all | Unlimited | 62.5 | 65.1 | +2.6 |
| Browser | 8 | First refresh | empty | Unlimited | 58.0 | 59.4 | +1.3 |
| Browser | 256 | Cold | empty | Unlimited | 225.2 | 210.2 | -18.3 |
| Browser | 256 | Reconnect, edit | all | Unlimited | 96.7 | 94.2 | -1.8 |
| Browser | 256 | Reconnect, edit | all | 125 kB/s | 280.3 | 277.0 | -3.3 |
| Browser | 256 | Reconnect, equal | all | 125 kB/s | 276.1 | 273.8 | -2.1 |
| Browser | 256 | Reconnect, membership | all | 125 kB/s | 294.9 | 287.5 | -6.9 |
| Browser | 256 | First refresh | empty | Unlimited | 106.3 | 90.4 | -15.8 |
| Browser | 2,048 | Cold | all | Unlimited | 564.9 | 460.8 | -103.8 |
| Browser | 2,048 | Cold | empty | Unlimited | 1696.6 | 1076.5 | -624.9 |
| Browser | 2,048 | First refresh | all | Unlimited | 438.9 | 262.6 | -174.2 |
| Browser | 2,048 | First refresh | empty | Unlimited | 439.7 | 303.2 | -144.3 |
| Browser | 4,096 | Cold | all | Unlimited | 1228.3 | 1038.4 | -188.6 |
| Browser | 4,096 | Cold | empty | Unlimited | 4369.2 | 2251.3 | -2123.7 |
| Browser | 4,096 | First refresh | all | Unlimited | 817.2 | 469.8 | -347.7 |
| Browser | 4,096 | First refresh | empty | Unlimited | 835.4 | 649.6 | -186.7 |

Native cold 256/empty is slower in all seven pairs, from +4.8 to +16.0 ms (paired median +6.7 ms, about 8% of the baseline median). Native 256-row membership reconnect is also slower in every pair, +0.6 to +9.3 ms. Browser eight-row first refresh and membership reconnect have seven positive differences, with paired medians +1.3 and +2.6 ms. These are costs, not equality. Slow-link browser medians improve, but individual edit and membership samples retain +20 and +21 ms tails. No outlier was removed.

## Resource costs and limits

- In the native 2,048-row slow-link warm fixture, setup CPU medians are 546.4/804.4 ms (main/candidate), with paired change **+272.3 ms**. Whole-process paired CPU increases by 278.7 ms; local-installation CPU improves by 17.3 ms. Setup includes prefetch and the initial query. Earlier private diagnostics located most excess in public Fetch; the focused attribution below supports a substantial cost from spreading application across streamed arrivals. Overlapping CPU intervals must not be added.
- Browser eight-row whole-fixture main-thread CPU increases by about 16–21 ms. This includes module initialization, setup, prefetch/initial query, verification and cleanup; it cannot be attributed to the timed query phase. It excludes off-thread IndexedDB work. The 2,048- and 4,096-row browser cases improve this whole-fixture measure; several 256-row cases use more CPU.
- A separate native diagnostic used 64 real 8 KiB String membership keys and forced Summary fallback above the inventory byte cap. On the original **d07 private artifacts**, first refresh after an initially empty query costs +10.683 ms paired median (all seven positive; baseline about 65 ms); fully cached is +1.716 ms with mixed signs. This is not a final-checkpoint/browser measurement, and its cost has not been quantitatively apportioned.
- Focused private attribution found substantial running CPU in storage/runtime worker synchronization. A same-binary experiment then compared ordinary streaming with holding all 2,048 fresh snapshots and applying them at Complete. All five pairs used less setup CPU with the single burst: paired median **-194.458 ms**, range -253.171 to -40.597 ms; whole-process CPU changed by -210.941 ms, while whole-process elapsed time increased by 81.891 ms. All ten samples and both preliminary fixtures verified exact state. The high final coalesced sample is retained. This supports the scheduling/write-pattern mechanism in this fixture, without fully apportioning the official +272.301 ms or proving a lower bound on streaming cost. The unbounded diagnostic was never applied to production and will not ship.
- Final observed-head reuse removes exactly one branch-added point read per received Fetch row: 8,192 to 6,144 for 2,048 rows in every private A/B sample. CPU and elapsed effects were mixed. The read reduction is established; a speedup is not. The speculative offer-sizing optimization was discarded after no demonstrated benefit.
- Wire counters include serialized envelopes, not TCP framing. For browser 4,096-row retained/full-cache cases, request bytes fall from 311,598 to 6,678. At 2,048 rows, the complete inventory is still sent and grows from 155,950 to 157,823 bytes. More pushed messages do not mean more dependent round trips. Native cold 4,096 maximum messages are about 65.8 kB versus 1.42 MB on main.

## What was verified

Cold/first refresh measure public versioned durable readiness. Reconnect measures the first fresh local installation for the retained query, with its own response gate. Immutable full states, heads and membership are captured at that boundary before asynchronous persistence verification or transport drain. State capture is atomic per entity; the static fixture does not claim a global concurrent snapshot. First refresh has no extra warming refresh.

The final source passes 356 core tests (one existing ignored), including 26 applier and 59 state-transfer tests. The full real-node digest-state target initially passed 20 tests and exposed a stale reconnect assertion that waited for equality items deliberately omitted by the optimized protocol. Its isolated red backtrace and focused green correction are preserved: the test now requires its own Complete response with no items and still verifies subsequent live delivery. The other 20 were not unnecessarily rerun. Native and browser 4,098-row/2,049-alternating-cache tests exercise a genuine Summary frontier and exact state at readiness.

Native and browser measurements ran sequentially, without owned builds/tests/profilers overlapping. Source, dependencies, harness, runner and immutable artifact hashes match before/after; separate production-only targets and marker scans prevent private diagnostic contamination. The 16-logical-CPU host had unrelated applications and Spotlight activity, which were recorded and left running. Seven pairs do not establish p95/p99 or eliminate host variability. Earlier contaminated comparative runs were withdrawn; this table uses only the final pinned artifacts.

## Review and follow-up

Storage/native transaction integration, durable replication and million-row readiness are outside this pass. Both peers still materialize results; retained trees refresh before comparison, while direct change delivery and lifetime history bounds remain later work. Fwd records completed Fetch CPU attribution (`AcnKJDI9`) for review, plus proposed bounded application batching (`xL8b6_sJ`), wide-key construction policy (`PMPS6R1y`) and retained-tree lifetime (`YC_KEwJL`) work. A proposal does not waive a current material blocker. Prebranch ORDER BY and IndexedDB teardown issues remain separate; concurrent-snapshot defect #273 was required for truthful target coverage.

Root and the continuing independent Fable session found no concrete correctness blocker and consider the branch suitable for in-depth review with the small readiness costs disclosed. The focused CPU attribution is complete; both recommend ending this pass and reviewing the documented tradeoffs before selecting another policy change. No PR, blanket parity claim or human acceptance is implied.

## Reproduction and evidence

See [LIVE_QUERY_STARTUP.md](LIVE_QUERY_STARTUP.md) for the fixture, runner, endpoints and resource semantics. The exact final native binary SHA-256 is `4f313fda34d8a2f10f250345069ae4559a353d2430eb82ce2fa120af061a364f`; final browser wasm is `1e8f919075f32f22efe3f1b8157e10542e2a020fc1400b3ef60a26bd0e35c904`.

The local research ledger is `~/ak/research_notes/ankurah-replication/reviews/2026-10-12-proto-parity-plan/`: `native-final-4c9b0482/{REPORT.md,assessment.json}` and `browser-4c9b0482-final-{report.md,summary.json}` retain per-sample values, medians, spreads, CPU, work, traffic and links to raw logs/configurations/pins. Root independently recomputed every final latency from all 350 raw records. The private CPU evidence is in `private-prefetch-symbols-4c9b0482/{REPORT.md,named-assessment.json}` and its `coalesced-fetch/` subdirectory, with root independent recomputation and the continuing Fable disposition. Failed test/build experiments and superseded reports are also retained in that ledger.
