# Memory-first query qualification after #526 — 2026-10-13

Candidate `7c2e64bca5ededea92466f6e12c20607893f3d71` was rebased onto merged main `ae8962798085dbf6d717429e4b6566940f0051e1` and compared again with that main baseline. All **364 final timing samples passed exact-state checks**, and **80 functional fixtures passed their own assertions**. The [pre-#526 report](LIVE_QUERY_STARTUP_RESULTS_PRE_526.md) remains historical; its measurements have not been relabelled.

The measured cold and warm readiness results support proceeding to Daniel's in-depth review. Large cases improve, and the small-case paired median readiness costs are at most about 1.2 ms in this matrix. CPU and memory costs remain. These results do not establish universal zero-regression parity, a numerical slowdown allowance, or human acceptance.

## Readiness measurements

Each row has seven alternating baseline/candidate pairs at simulated 50 ms RTT. Values are milliseconds. Paired change is the median of the seven candidate-minus-main differences, which need not equal the difference of the two medians. Initial cache describes entity coverage before the first query; reconnects retain that query. Bandwidth is per direction, and unlimited still includes propagation delay.

| Platform | Rows | Operation | Initial cache | Bandwidth | Main median | Candidate median | Paired change |
|---|---:|---|---|---|---:|---:|---:|
| Native | 256 | Cold | all | Unlimited | 60.5 | 61.0 | +0.6 |
| Native | 256 | Cold | empty | Unlimited | 75.2 | 74.7 | -0.9 |
| Native | 256 | Cold | half | Unlimited | 67.3 | 68.0 | +1.1 |
| Native | 4,096 | Cold | all | Unlimited | 246.1 | 196.6 | -51.2 |
| Native | 4,096 | Cold | empty | Unlimited | 480.7 | 385.9 | -92.3 |
| Native | 8 | Cold | empty | Unlimited | 54.6 | 55.1 | +0.3 |
| Native | 256 | Reconnect, edit | all | 1.25 MB/s | 72.3 | 72.4 | +0.1 |
| Native | 4,096 | Reconnect, edit | all | Unlimited | 180.7 | 166.7 | -12.4 |
| Native | 4,096 | First refresh | all | Unlimited | 145.6 | 98.3 | -46.5 |
| Native | 4,096 | First refresh | empty | Unlimited | 147.7 | 122.8 | -22.9 |
| Native | 256 | Reconnect, membership | all | 1.25 MB/s | 73.1 | 72.8 | -0.4 |
| Native | 2,048 | Reconnect, equal | all | 125 kB/s | 1348.2 | 1340.6 | -7.6 |
| Browser | 256 | Cold | empty | Unlimited | 231.4 | 206.9 | -22.8 |
| Browser | 256 | Cold | half | Unlimited | 149.8 | 140.3 | -9.5 |
| Browser | 4,096 | Cold | empty | Unlimited | 4572.8 | 2302.1 | -2260.2 |
| Browser | 4,096 | Cold | all | Unlimited | 1266.5 | 1062.9 | -209.0 |
| Browser | 256 | Reconnect, edit | all | 125 kB/s | 260.4 | 254.7 | -6.2 |
| Browser | 256 | Reconnect, membership | all | 125 kB/s | 269.4 | 265.0 | -4.4 |
| Browser | 8 | Reconnect, edit | all | Unlimited | 59.3 | 59.4 | +0.1 |
| Browser | 8 | Reconnect, membership | all | Unlimited | 60.1 | 60.6 | +0.6 |
| Browser | 4,096 | Reconnect, edit | all | Unlimited | 799.1 | 657.5 | -142.7 |
| Browser | 4,096 | Reconnect, membership | all | Unlimited | 792.4 | 666.2 | -125.0 |
| Browser | 4,096 | First refresh | empty | Unlimited | 890.5 | 692.0 | -212.0 |
| Browser | 4,096 | First refresh | half | Unlimited | 854.8 | 548.3 | -303.4 |
| Browser | 4,096 | First refresh | all | Unlimited | 869.8 | 487.4 | -373.8 |
| Browser | 8 | First refresh | empty | Unlimited | 55.8 | 56.4 | +0.5 |

All large browser pairs improve. The eight-row browser refresh/edit/membership paired changes are +0.5/+0.1/+0.6 ms, with mixed signs and individual differences ranging from -1.3 to +1.5 ms across those cells. Native 256-row cold empty/half/all paired changes are -0.876/+1.143/+0.565 ms; the empty-cache cell retains a +8.903 ms individual difference. No final outlier was removed or rerun for its sign.

## Resource costs and limits

- Native 2,048-row slow-link equal reconnect has setup CPU medians of 543.809/566.210 ms (main/candidate), paired change **+30.154 ms**, range -53.414 to +117.218 ms, six positive pairs. Whole-process paired CPU increases by 15.784 ms, while local-installation CPU improves by 19.398 ms. This is much smaller than the historical +272.301 ms setup gap, but separate runs on a busy shared host do not isolate the cause. Overlapping CPU intervals must not be added.
- Native 256-row fully cached cold setup/whole-process CPU rises by paired medians of 4.138/5.856 ms; the whole-process increase appears in all seven pairs. Eight-row cold whole-process CPU rises by 0.728 ms, also in all seven pairs. The 256-row half-cached cold and edit-reconnect whole-process medians rise by 5.744/4.147 ms. The 4,096-row fully cached cold case is faster to readiness while whole-process CPU rises by 47.420 ms, with a wide -42.006 to +486.422 ms paired range. These are resource costs even when readiness improves.
- Browser eight-row whole-fixture main-thread CPU rises by paired medians of 11.410–13.287 ms, with all seven pairs positive in each of the three cells. This includes module initialization, setup, prefetch/initial query, verification and cleanup; it is not query-phase attribution and excludes off-thread IndexedDB work. Large browser cases improve this whole-fixture measure. The 256-row cold and slow-link edit cases retain increases of about 3–12 ms; slow-link membership improves by about 1 ms.
- Native eight/256-row peak process memory increases by roughly 1–3 MiB in paired medians, and the 2,048-row slow-link fixture by 3.5 MiB. The eight-row cold and 256-row fully cached cold memory increases appear in all seven pairs. Large native memory results are mixed. Process peaks are not per-tree retained memory.
- Native 4,096-row fully cached request bytes fall from 311,598 to 6,678. Small native cells add 373–1,046 requester bytes, and the 2,048-row cell adds 2,446 bytes. For cold empty transfer, maximum serialized envelope size falls from 1,437,713 to 65,834 bytes. Counters exclude transport framing; pushed message counts do not establish dependent round trips.
- The original wide-key first-refresh diagnostic and private coalescing experiment remain in the historical report with their original pins. Neither was rerun here. No unbounded coalescing variant entered production, and the former CPU magnitude is not the current-baseline result.

## Rebase correctness and verification

The rebase preserves #526's atomic event-batch application and fresh forks on storage-conflict retries, while retaining truthful digest target coverage. Fable found one conflict-resolution error: the adopted-snapshot path suppressed its reactor change when a reader hydrated the committed state before publication. A deterministic regression for both Initial and Live delivery failed before the correction and passed after restoring upstream's unconditional notification following successful adoption. Existing competing-commit tests still prevent duplicate publication. The same persistent Fable session reviewed and approved that correction.

On the corrected source, **390 core tests pass** (one existing ignored), plus **21 digest-state, nine inter-node and three stream-credential tests**. Immediately before the two-line notification correction, 54 targeted integration and 125 auth tests passed, with existing ignores unchanged. The initial removed-backend-API fixture compile failures and the notification regression's red run are preserved. The browser functional set includes 17 ordinary unit tests, 18 standard query fixtures, four gapped-cache fixtures, and a 4,098-row/2,049-interleaved-cache Summary-frontier case. The native functional set has 36 standard and four gapped-cache fixtures.

Cold/first refresh measure public versioned durable readiness. Reconnect measures the first fresh local installation after reset, with its own query response as a later validity gate. Immutable full states, heads and membership are captured at that boundary before asynchronous persistence verification or transport drain. Capture is atomic per entity; these static fixtures do not claim a global concurrent snapshot.

Native and browser final timing ran sequentially without our builds, tests or profilers overlapping. Source, dependency, public v8 harness and immutable artifact hashes match before/after; production-only targets and marker scans exclude private diagnostics. The shared 16-logical-CPU host had high load and unrelated applications running. Seven pairs do not establish p95/p99 or eliminate host variability.

One browser attempt was mistakenly started before a failed compiler inventory check was examined. Its 42 completed samples and one sample interrupted by our cleanup are preserved separately and excluded for that known overlap, independently of their signs. The qualification controller (`orchestrate.py` in the local evidence ledger) stops before launch when the compiler check fails; a fresh complete 196-sample browser attempt passed. The 40 browser functional checks remained valid. Unrelated user processes were left untouched.

## Review and follow-up

Storage/native transaction integration, durable replication, idle LRU and million-row readiness remain later work. Both peers still materialize results, and retained trees refresh before comparison. Bounded application batching (`xL8b6_sJ`), wide-key construction policy (`PMPS6R1y`) and retained-tree lifetime (`YC_KEwJL`) remain proposals; batching now explicitly requires reassessment against the smaller post-#526 CPU gap. Prebranch ORDER BY and IndexedDB teardown issues remain separate. The upstream events-path reader/publication race is recorded on the existing task `djIq9rF8` and is not repaired by the adopted-snapshot correction.

Root and the same persistent Fable session find no concrete correctness or material cold/warm readiness blocker in this requalification and recommend returning to Daniel's in-depth review. Fable's remaining requests were report-level disclosure of the consistent small-cell CPU/memory costs and updating attribution to lead with the new baseline. The requalification task `OPI6g2Zr` gates first landing. No PR or human acceptance is implied.

## Reproduction and evidence

See [LIVE_QUERY_STARTUP.md](LIVE_QUERY_STARTUP.md) for fixture, runner and endpoint definitions. The candidate native binary SHA-256 is `ac9685224e51e3bde833c77d01e8ea6755cf3c500724c5e75f5f92d36f8a7eb0`; candidate browser wasm is `bb6c5995ffab65397d95690f103b523372a6189e85b9ee83556f389303d215b4`.

The local evidence root is `~/ak/research_notes/ankurah-replication/reviews/2026-10-13-rebase-526/`. `native-7c2e64bca5ed-report.md` and `browser-7c2e64bca5ed-report.md`, their JSON assessments, and `root-{native,browser}-7c2e64bca5ed-recomputed.json` retain every final pair, range, CPU/resource result and raw-log reference. Accepted raw files are under `{native,browser}/7c2e64bca5ed/`. Build manifests, before/after pins, the original pristine-main overlay manifest and `correctness-summary.json` bind them to the sources. The compiler-overlap attempt remains under `excluded/browser-7c2e64bca5ed-compiler-overlap-attempt1/`. Persistent Fable session `8545fb99-2d53-4961-982c-802ff54c57f5` has its rebase finding, correction review and final assessment preserved alongside them.
