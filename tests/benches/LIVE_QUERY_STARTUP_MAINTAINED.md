# Maintained-tree live-query qualification

All 448 ordinary observations at `10c98fb5` passed their strict result checks.
The browser traces and a separate native operation diagnostic are complete.
A follow-up optimization was stopped mid-implementation for the requested handoff.
Its WIP checkpoint includes a deliberately red operation-count regression test:
insertion and removal each encode five times, versus the intended two. The
bounded-count adapter is written but not yet used by topology maintenance; its
focused test has not been run. There are no post-optimization measurements, and
this WIP checkpoint must not be described as green or performance-qualified.
These measurements replace the
captured-entity-state implementation's performance claims; the historical reports
retain their original revisions and results. Performance acceptance remains open.

## Implementation and scope

The candidate keeps one node-owned resident-tree LRU. Every resident has reactor
maintenance, and active leases prevent eviction. Trees retain keys, entity IDs,
Clocks and digests; they do not retain entity state or strong entity handles.
Payloads are read from the current canonical resident entity or storage, with
current authorization and attestation checks. Captured membership and current
payload heads remain separate.

This pass corrected four costs exposed by measurements:

- Read an already resident payload directly instead of decoding its storage copy.
- Let current resident payloads fill the bounded frame without consuming the
  smaller storage/history-work allowance; retain handles only while preparing
  that payload.
- Traverse child prefixes and count bounded leaf rows without decoding aggregate
  digests that the operation discards. Actual reads, audits and rebuilds still
  reject corrupt digest data.
- Coalesce unconsumed reactor metadata publications within the existing 8 MiB
  bound. A burst of small commits no longer invalidates a tree merely because
  it crosses 1,024 publications. All projected changes and event IDs remain in
  order, and consuming a window detaches it from later publications.

A fresh optional policy proof also avoids reading every candidate's entity state
when every state is readable. The default is conservative; state-dependent
policies still perform their ordinary current-state checks. A positive proof is
not cached, requires an unrestricted retrieval predicate, and is accepted only
when the maintained membership contains the complete candidate-ID set. It does
not bypass event or attestation checks. Both benchmark arms use PermissiveAgent;
these timings do not qualify JWT or custom policy costs. The reactor projector
uses the same fresh proof to avoid serializing every backend solely for a
permission check. Grant → revoke → grant regression coverage verifies that this
short circuit is reevaluated for every publication.

Integration validation additionally caught a live-notification history defect:
the receiver unnecessarily tried to load an initial snapshot's baseline event
payload. Comparing the incoming event toward the delivered frontier permits
that omitted baseline payload while still requiring all connecting history.

## Measurement contract

Main is pinned at `b5fcfd3eac3ea4247c4bd7613c439393beb42aad`; candidate production
is pinned at `10c98fb5bf5a76ae0ce1d93415fb900e166bb173`. The candidate archive's
source fingerprint is
`b5eaabbf34bbd7d6c509ffde8c4b18c3043ef49269df844acf75010072069d86`.
Both arms use the same compiler, optimized profiles and public-API fixtures.

The fixed plan has 16 cases, seven alternating pairs per case and platform:
448 ordinary observations across native Sled and browser IndexedDB. A separate
24-observation diagnostic plan captures browser CPU profiles and traces over the
actual measurement interval. Instrumented observations are never pooled with
ordinary timings. No timing-based exclusions or sign-based repeats are allowed.

Latency ends at fully applied, versioned durable readiness for cold startup and
same-selection refresh, or the first freshly installed result for reconnect.
It is **not time to first record**. Every observation must verify exact membership,
ordered IDs where applicable, full state and heads at that endpoint, with
persistence checked separately.

`empty` means no local matching entities. `half` prefetches a narrower query that
cannot supply the wider query's tree. `all` prefetches the measured selection.
The `full` LIMIT10 case prefetches the complete unbounded selection, then measures
reuse for the narrower query; it is cached startup, not a tree-free cold start.
Population/setup/prefetch are outside endpoint latency. Ordinary fixtures retain
their application-created server views. A separate earlier diagnostic releases
those views without forcing eviction; it is not pooled with this qualification.

Native endpoint CPU includes both peers and background threads; peak RSS covers
the whole process, including setup. Browser fixture task CPU covers setup, prefetch, transfer, verification and
cleanup; it excludes IndexedDB worker CPU. WASM capacity is allocated linear memory, not peak RSS or live bytes.
Overlapping phase CPU values must not be summed. Host monitoring collects only
aggregate CPU/load, and flags affected samples without discarding them.

Seven pairs do not establish p95/p99 performance. These fixtures do not qualify
million-row scale, sustained concurrent churn, or numeric ORDER BY (whose
preexisting storage defect is separately tracked). Bounded cases use padded
string ORDER BY with LIMIT10 over 4,096 and 16,384 rows.

## Ordinary results at `10c98fb5`

Positive paired deltas mean a slower candidate. Each row contains seven pairs;
the paired median need not equal the difference between the two arm medians.
No slow observation was excluded.

### Native Sled

| Case | Main median ms | Candidate median ms | Paired delta ms [range] | Slower pairs |
|---|---:|---:|---|---:|
| `limit10-cold-16384-empty-0ms` | 60.862 | 64.148 | +4.197 [-0.754, +11.349] | 6/7 |
| `limit10-cold-16384-empty-50ms` | 126.342 | 131.355 | +10.304 [-1.881, +32.117] | 6/7 |
| `limit10-cold-4096-empty-0ms` | 10.318 | 10.523 | +0.236 [-0.227, +1.158] | 5/7 |
| `limit10-cold-4096-empty-50ms` | 68.654 | 71.363 | +2.681 [+1.141, +5.315] | 7/7 |
| `limit10-cold-4096-full-0ms` | 0.539 | 16.084 | +15.521 [-0.094, +42.434] | 6/7 |
| `limit10-warm_edit-4096-all-50ms` | 53.343 | 54.481 | +1.010 [-0.415, +3.541] | 5/7 |
| `limit10-warm_membership-4096-all-50ms` | 53.552 | 54.368 | +1.275 [-0.289, +2.155] | 4/7 |
| `limit10-warm_refresh-4096-empty-0ms` | 0.380 | 0.232 | -0.145 [-0.193, -0.094] | 0/7 |
| `limit10-warm_refresh-4096-empty-50ms` | 54.603 | 54.769 | +0.384 [-0.154, +2.353] | 6/7 |
| `unlimited-cold-256-half-50ms` | 75.028 | 93.094 | +17.694 [+6.667, +23.347] | 7/7 |
| `unlimited-cold-4096-empty-50ms` | 446.063 | 370.925 | -72.164 [-82.409, -66.857] | 0/7 |
| `unlimited-cold-8-empty-0ms` | 0.870 | 1.161 | +0.261 [+0.035, +0.350] | 7/7 |
| `unlimited-cold-8-empty-50ms` | 58.598 | 59.096 | +1.231 [-1.231, +4.460] | 4/7 |
| `unlimited-warm_edit-4096-all-50ms` | 171.933 | 136.428 | -37.585 [-40.650, -32.590] | 0/7 |
| `unlimited-warm_equal-4096-all-50ms` | 171.964 | 79.316 | -92.585 [-95.827, -90.846] | 0/7 |
| `unlimited-warm_refresh-4096-empty-50ms` | 140.685 | 83.415 | -54.928 [-61.349, -52.858] | 0/7 |

### Browser IndexedDB

| Case | Main median ms | Candidate median ms | Paired delta ms [range] | Slower pairs |
|---|---:|---:|---|---:|
| `limit10-cold-16384-empty-0ms` | 834.300 | 936.900 | +129.800 [-335.000, +418.700] | 4/7 |
| `limit10-cold-16384-empty-50ms` | 487.000 | 473.400 | +4.800 [-791.500, +762.200] | 4/7 |
| `limit10-cold-4096-empty-0ms` | 218.500 | 218.500 | -0.900 [-15.200, +15.000] | 3/7 |
| `limit10-cold-4096-empty-50ms` | 269.800 | 276.800 | +7.700 [+0.400, +152.600] | 7/7 |
| `limit10-cold-4096-full-0ms` | 5.700 | 3.300 | -2.400 [-29.300, -2.200] | 0/7 |
| `limit10-warm_edit-4096-all-50ms` | 60.100 | 58.400 | -1.300 [-3.400, -0.300] | 0/7 |
| `limit10-warm_membership-4096-all-50ms` | 63.000 | 60.000 | -2.700 [-5.800, -0.100] | 0/7 |
| `limit10-warm_refresh-4096-empty-0ms` | 4.300 | 1.100 | -3.200 [-4.000, -2.500] | 0/7 |
| `limit10-warm_refresh-4096-empty-50ms` | 57.100 | 54.100 | -3.400 [-4.400, -1.500] | 0/7 |
| `unlimited-cold-256-half-50ms` | 147.900 | 138.000 | -8.100 [-12.400, -4.700] | 0/7 |
| `unlimited-cold-4096-empty-50ms` | 4704.200 | 2584.400 | -2123.300 [-2311.300, -1834.800] | 0/7 |
| `unlimited-cold-8-empty-0ms` | 11.900 | 11.500 | +0.100 [-1.000, +0.400] | 5/7 |
| `unlimited-cold-8-empty-50ms` | 65.800 | 67.100 | +1.000 [+0.500, +2.900] | 7/7 |
| `unlimited-warm_edit-4096-all-50ms` | 826.600 | 147.800 | -677.400 [-688.400, -661.500] | 0/7 |
| `unlimited-warm_equal-4096-all-50ms` | 855.600 | 88.100 | -766.700 [-798.900, -729.100] | 0/7 |
| `unlimited-warm_refresh-4096-empty-50ms` | 935.900 | 91.900 | -845.900 [-863.000, -831.800] | 0/7 |

Broad cold and warm queries improve on both platforms. This does **not**
establish blanket parity: native half-cached startup, native bounded reuse after
a broad prefetch, and browser bounded cold startup retain unfavorable cases.
The 16,384-row browser cases have wide mixed-sign variation. Seven pairs do not
resolve tail latency, and their favorable observations do not cancel the slow ones.

### Resource costs

Native CPU is process CPU within the endpoint interval; peak RSS is the maximum
over the whole fixture, including setup. The following are paired median changes
against main, not isolated costs of the LRU.

| Native case | Endpoint CPU delta ms | Whole-fixture peak RSS delta MiB |
|---|---:|---:|
| `limit10-cold-16384-empty-0ms` | +4.867 | +2.438 |
| `limit10-cold-16384-empty-50ms` | +12.403 | +2.938 |
| `limit10-cold-4096-empty-0ms` | +0.415 | +1.250 |
| `limit10-cold-4096-empty-50ms` | +3.534 | +1.797 |
| `limit10-cold-4096-full-0ms` | +16.172 | +14.844 |
| `limit10-warm_edit-4096-all-50ms` | +1.168 | +2.219 |
| `limit10-warm_membership-4096-all-50ms` | +1.361 | +2.109 |
| `limit10-warm_refresh-4096-empty-0ms` | -0.142 | +2.531 |
| `limit10-warm_refresh-4096-empty-50ms` | -0.122 | +2.594 |
| `unlimited-cold-256-half-50ms` | +18.955 | +2.734 |
| `unlimited-cold-4096-empty-50ms` | +223.725 | +14.891 |
| `unlimited-cold-8-empty-0ms` | +0.409 | +1.875 |
| `unlimited-cold-8-empty-50ms` | +1.994 | +1.844 |
| `unlimited-warm_edit-4096-all-50ms` | -87.793 | +12.438 |
| `unlimited-warm_equal-4096-all-50ms` | -92.601 | +8.422 |
| `unlimited-warm_refresh-4096-empty-50ms` | -62.423 | +14.266 |

Native cold 4,096-row startup finishes sooner while consuming more CPU.
Warm reuse saves CPU. The retained metadata and maintained digest work therefore
remain a resource tradeoff, even where initialization latency improves.

Browser CPU below covers the **entire fixture** on the main thread, including
setup, prefetch and verification. It excludes IndexedDB workers and is not an
endpoint CPU measurement. WASM capacity is allocated linear memory at fixture end,
not live allocations or peak RSS. Stored CDP metric deltas are retained; separate
original before/after CDP packets were not saved.

| Browser case | Whole-fixture task CPU delta ms | End WASM capacity delta MiB |
|---|---:|---:|
| `limit10-cold-16384-empty-0ms` | +11.613 | +0.062 |
| `limit10-cold-16384-empty-50ms` | -15.555 | +0.062 |
| `limit10-cold-4096-empty-0ms` | +30.289 | +0.062 |
| `limit10-cold-4096-empty-50ms` | +33.152 | +0.062 |
| `limit10-cold-4096-full-0ms` | +284.863 | +0.062 |
| `limit10-warm_edit-4096-all-50ms` | +39.878 | +0.062 |
| `limit10-warm_membership-4096-all-50ms` | +26.865 | +0.062 |
| `limit10-warm_refresh-4096-empty-0ms` | +31.351 | +0.062 |
| `limit10-warm_refresh-4096-empty-50ms` | +33.054 | +0.062 |
| `unlimited-cold-256-half-50ms` | +41.149 | +0.625 |
| `unlimited-cold-4096-empty-50ms` | -201.554 | +3.000 |
| `unlimited-cold-8-empty-0ms` | +21.560 | +0.250 |
| `unlimited-cold-8-empty-50ms` | +26.543 | +0.250 |
| `unlimited-warm_edit-4096-all-50ms` | -711.969 | +0.125 |
| `unlimited-warm_equal-4096-all-50ms` | -755.891 | +0.375 |
| `unlimited-warm_refresh-4096-empty-50ms` | -533.023 | +2.375 |

Host guards recorded two busy native starts and no unavailable starts. One native
aggregate observation exceeded 80% utilization; all observations remain included.
Browser guards recorded no busy or unavailable starts, and aggregate utilization
remained below 80%. Aggregate host data cannot identify the cause of any sample.

## Diagnostic findings at `10c98fb5`

The browser plan covers 24 observations. Two initial validator stops left an
extra warm pair, so 26 actual captures are retained. All query outcomes passed.
The first validator rejected signed CPU timestamp deltas; Chromium DevTools
instead reconstructs absolute timestamps and sorts timestamp/sample pairs.
The reviewed second validator follows that ordering without adding, removing
or changing samples. Raw profiles and the earlier rejection records remain
unchanged; revalidation is saved separately. See
[Chromium's profile parser](https://github.com/ChromeDevTools/devtools-frontend/blob/main/front_end/models/cpu_profile/CPUProfileDataModel.ts).

All 26 captures have one start/end marker, observed CPU samples bracketing both
markers within original profile bounds, a completely parsed JSON export and
metadata, and no reported trace-data loss. This establishes observed window
coverage, not gapless sampling. Short cached captures contain few samples. The
profiles and traces are diagnostics, never pooled with ordinary timings.

The browser traces support these conclusions:

- Large cold transfer performs 16,647 IndexedDB get operations in each candidate
  capture versus 28,680 in each baseline capture. Warm-edit reconnect also avoids
  the baseline's broad reread. These are trace-event counts within the endpoint,
  not a count of logical application requests.
- In the 16,384-row bounded cold captures, renderer task activity remains around
  106–112 ms on both arms while observed endpoint spans vary from roughly
  417–1,227 ms. IndexedDB cursor opening and index-key work occupy much of those
  spans. Both main and candidate show this variability; these observations do not
  establish a branch-specific fix or tail-latency parity.
- Browser broad-prefetch-to-LIMIT10 captures remain short, about 5–8 ms. They do
  not reproduce the native maintenance carryover described below.

Thread task intervals are elapsed activity, not measured CPU. IndexedDB async
lifetimes include waiting. Their intervals overlap and must not be added together.
Release WASM profiles contain numbered functions, so no function-level source
attribution is inferred from those names.

A separate private native build added operation scopes and hash/decode/encode
counters. It ran each of two predetermined cases once with the unchanged strict
oracle. These instrumented observations are not parity measurements:

- Half-cached 256: two initial tree builds (128 and 256 leaves) took 8.14 ms
  combined. A 121-insert sparse update took 7.99 ms. The 505 leaf hashes exactly
  match those builds plus inserts; this did not reveal repeated leaf hashing.
  All point encodings together took 3.80 ms, only part of which is redundant.
- Broad-prefetch-to-LIMIT10: a 358-insert maintenance batch began before the
  measurement and overlapped 12.46 ms of its 13.99 ms window. The snapshot request
  waited 12.93 ms; subsequent authorization took 0.027/0.074 ms and selecting ten
  rows took 0.021/0.015 ms on requester/responder. Bypassing that wait would permit
  stale tree metadata. The fixture does not add a maintenance-settle step to hide it.

Those scopes overlap, include waits and perturb scheduling; they are not additive
CPU attribution. Lowering the bulk-rebuild threshold to 64 was tested separately
and rejected: both half-cached observations became slower. The production
threshold remains 128.

## Correctness checks

The final projector fix passes 542 core tests (one existing ignore), eight
bridge-policy tests, four stream-credential tests and nine targeted JWT tests.
The raw-count change passes 151 digest tests and two documentation examples.
Formatting and diff checks pass.

Regression evidence includes old-code failures for the arbitrary publication
limit, unnecessary candidate-state reads and missing-baseline event retrieval.
The 4,096-row original-tree stress test verifies continued maintenance, compatible
reuse without a storage rescan and idle updates; it is coverage, not claimed red
evidence. Retry tests verify wire Busy/retry counts and the final version/Clock,
without depending on observing a fleeting pending UI state.

## Evidence

Local raw evidence is under
`research_notes/ankurah-replication/reviews/2026-10-22-maintained-parity/` in the
shared workspace. `qualification-projector/` holds exact source/artifact manifests and
build logs; `runner/final-qualification-plan.json` and `runner/projector-trace-plan.json`
fix the observation plans before timing. `maintenance-lifecycle/validation-summary.json`,
`validation-credential-history/summary.json` and
`validation-projector-proof/summary.json` index correctness logs.

Earlier exploratory runs, failed attempts, private attribution builds and the
rejected lower-rebuild-threshold experiments remain separately recorded. Their
numbers must not be relabelled as measurements of the final candidate.
