# Proposed digest benchmark history adapter

Proposal for the separate benchmark producer and website work. This enables no
pipeline, publication, timing gate or new fixture. Existing qualification retains
its original source pins; review the adapter and budget before running or backfilling.

## Collector compatibility

The website producer already discovers Cargo targets before sampling and discovers
Criterion workload IDs through `--list`. Keep registered, unsupported, failed and
removed workloads visible even without measurements; execution policy can select
a subset without hiding the rest of the catalog.

The current `tools/benchmarks/catalog.py` identifies Criterion by a package-level
dependency and `harness=false`. That misclassifies the custom
`ankurah-tests/live_query_startup` target: its executable ignores `--list` and runs
fixtures. Add an explicit custom-runner adapter, with a versioned declarative case
catalog, before automatic execution. The website should consume that catalog,
not a hand-maintained UI list or only successful sample records.

`ankurah-digest-tree/digest` is Criterion and can use normal workload discovery.
Its primitive cost series starts when the crate exists; an absent historical target
is not zero cost or an infinite speedup. Keep it separate from protocol latency.

## Initial public-query before/after set

Reuse the unchanged public fixture: numeric LWW position and default YrsString
body, not a pure-LWW application benchmark. All six proposed cells use RTT 50 ms,
seven alternating AB/BA pairs and a fresh process/context per arm:

| Case / mode | Rows | Initial cache | Directional bandwidth | Endpoint |
|---|---:|---|---|---|
| Small cold (`cold`) | 8 | empty | Unlimited | Versioned durable readiness |
| Partial cold (`cold`) | 256 | half | Unlimited | Versioned durable readiness |
| Bulk cold (`cold`) | 4,096 | empty | Unlimited | Versioned durable readiness |
| First refresh (`warm_refresh`) | 4,096 | initially empty | Unlimited | New version's durable readiness |
| Sparse reconnect (`warm_edit`) | 4,096 | all | Unlimited | Fresh local result installation |
| Membership reconnect (`warm_membership`) | 256 | all | 125,000 B/s | Fresh local result installation |

This is 84 samples per platform. Start on native; add browser as a separate series
after reviewing its adapter. Keep equal reconnect, fully cached cold results,
inventory-boundary sizes and gapped caches in the discovered catalog and existing
functional coverage for later bounded profiles.

First refresh has no extra warming refresh. Reconnect retains the query and resets
only loading metadata before timing; installation precedes durable bookkeeping.
Capture immutable full States/heads/IDs before asynchronous verification or drain;
verify exact membership/state and persistence, then require the own-query response.
Capture is atomic per entity, not globally atomic; these fixtures stop writes before
timing. A failed/incomplete case produces no timing point. Preserve every attempt
and do not repeat for unfavorable signs.

## Metrics and uncertainty

Retain primary wall time, every arm/pair value, median, range, sample standard
deviation and MAD. Seven pairs do not establish p95/p99. Capture/verification and
transport quiet landmarks are diagnostics; legacy drain aliases are one observation.

Report directional serialized bytes/messages, largest envelope and control tail.
Bytes exclude transport framing; message counts do not establish dependent RTTs.
Native public storage calls/materialized rows/requested events exclude known
verifier reads and are not physical I/O or internal index visits. Do not add the
obsolete head-question metric.

Native setup/through-ready/installation CPU covers both peers and background work;
overlapping intervals must not be added. Whole-child CPU and peak RSS include
setup/teardown and are not retained-tree allocation. Browser CDP CPU covers the full
main-thread fixture, including setup/prefetch/initial query/verification; it excludes
off-thread IndexedDB and later browser teardown. It cannot identify phase CPU.
End heap/wasm capacities are not peaks and must not be summed; no browser setup
CPU, storage counter or process RSS is claimed.

The current producer's `dataset.py` requires Criterion median/bootstrap intervals
and validates them against raw Criterion files; it accepts ns/reads, not generic
bytes/memory. A custom adapter/schema must distinguish empirical ranges from
confidence intervals, allow uncertainty to be absent, and label units explicitly.
Until reviewed, link custom raw/resource evidence instead of fabricating Criterion
files or using min/max as confidence limits. Preserve genuine Criterion intervals
for primitive/DAG series.

## Provenance and historical adapters

Compare main `ae8962798085dbf6d717429e4b6566940f0051e1` and the candidate with
identical public fixtures. [LIVE_QUERY_STARTUP.md](LIVE_QUERY_STARTUP.md) specifies
the validated eight-file test-only overlay, including native libc and browser
dependencies. Preserve original/effective lockfiles and existing dependency versions;
backport no production algorithm or wire field. Historical API adapters need their
own semantic audit; unsupported meanings stay gaps or separate series.

Use production-only targets per revision/platform, never private profiling targets.
Preserve immutable binaries/packages, before/after source/package hashes, marker
checks, compiler/profile/lockfile and harness/adapter hashes. A runner source hash
alone does not prove a binary's origin. Record host load and abort before launch if
the sequential compiler guard fails; leave unrelated user processes alone.

Series keys include fixture/endpoint, backend/storage/runtime, compiler/profile,
machine class, sampling and adapter semantics. Native, browser and hosted smoke
never share a line. Preserve exact source SHA, release/PR markers, source/measurement
dates, run URL, raw evidence and catalog status; never rewrite old measurements.

## Proposed CI budget

- PR smoke: compile/list digest Criterion, run a short leaf/update/decode subset,
  and one untimed 8-row candidate fixture per public mode at RTT 0/unlimited link.
  Proposed execution cap: two minutes after compilation. Preserve failures; shared
  CI smoke is not a publication-quality point or a timing regression gate.
- Stable main/release: six native cells above, seven pairs each, sequentially after
  builds. Proposed execution cap: 15 minutes after compilation; retain partial
  attempts if exceeded. These caps are operator budgets, not predicted runtimes.
  Add browser only under a separately approved stable-browser budget.
- Criterion primitives: the current 15 workloads at one-second warmup/three-second
  target measurement imply about 60 seconds of nominal measurement/warmup per
  revision before setup/extensions. Auto-discover them in the stable producer.
- Historical expansion: start with immediate predecessor/latest compatible release;
  audit adapters before all-merge backfill. Avoid the Cartesian fixture default on
  every PR. The existing DAG smoke workflow alone does not discover these targets.

Keep the existing lane API repair separate (Hu1h8cihStg-RZYNCzAJQfI7dwj5Qj_QEXnZCYuMSHo).
Bridge gaps 32/100/200 preserve that lane under the new work cap; historical 1000-gap
bridge points must not become snapshots or 200-gap substitutes. Generation/DAG
clone-in-timer, borrowed getter, layer preparation, real LWW batch application and
bridge-stage proposals retain their own contracts/tasks. This plan does not complete
them or the wider volume-performance program.
