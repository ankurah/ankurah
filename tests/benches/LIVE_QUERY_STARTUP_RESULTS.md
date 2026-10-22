# Livequery startup after captured-state sharing — 2026-10-22

> Historical measurements: this report describes the captured-state cache at
> `034735dd`, which the subsequent maintained-tree correction removes. The
> current implementation retains only keys, IDs, cloned Clocks and digests,
> maintains every resident tree through the reactor, and reads payloads from
> entity/storage ownership when sending. These timings do **not** qualify that
> revised implementation; fresh paired performance measurements remain required.

The final production source is `034735dd` (captured-state sharing `db6fcc28`
plus the catalog retry repair). All **420 ordinary main/candidate observations**,
**four separate previous/new observations** and **16 diagnostic captures** passed
their strict result oracles. Source, artifact and trace-coverage checks pass.
The [optimization report](LIVE_QUERY_STARTUP_STATE_SHARING.md) records the
correctness gates, direct comparison and CPU/allocation/browser attribution.

Large unlimited results retain their initialization gains over main: at 4,096
records and 50 ms RTT, paired medians improve by **93 ms native / 2.395 seconds
browser** for cold startup, **48 ms / 722 ms** for the first refresh, and
**45 ms / 662 ms** for an offline-edit reconnect. Every pair improves in these
six cells. The endpoint is fully initialized authoritative results, not the first
record or the arrival of a subscription acknowledgement.

There are costs. Native eight-record cold startup at zero RTT adds 0.181 ms;
LIMIT10 over a complete cached tree adds 0.972 ms native / 0.400 ms browser.
All seven pairs are slower in each of those cells. Large native cold startup
still adds 90.441 ms endpoint CPU and 30.125 MiB whole-process peak RSS versus
main; browser WASM capacity adds 13.812 MiB. These compare the whole branch
with main, not LRU bookkeeping in isolation.

The 16,384-record browser cold cells still have wide mixed-sign differences,
including +1,281 ms and +759 ms unfavorable pairs. Neither their medians nor the
new traces establish universal or tail-latency parity. The direct optimization
comparison also has mixed outcomes: lower CPU and slightly fewer live allocated
bytes, but higher measured readiness time and peak RSS. These tradeoffs remain
explicit for review; no numerical slowdown allowance or human acceptance is
assumed. No further implementation change follows from this measurement pass.

See [the method](LIVE_QUERY_STARTUP.md), the unchanged numerical evidence in
[the previous candidate report](LIVE_QUERY_STARTUP_RESULTS_PRE_STATE_SHARING.md),
and its [focused attribution](LIVE_QUERY_STARTUP_FOCUSED.md). Historical source
pins and measurements have not been relabelled or pooled with this dataset.

## Primary comparison

Six unlimited sentinel cases and nine padded-string LIMIT10 cases per platform, including complete-resident reuse for LIMIT10. Numeric ORDER BY remains outside this performance qualification; long-sort-key reconnect correctness is covered by separate red/green integration tests. The four previous/new native observations and eight browser diagnostic observations are reported separately; no recorded observation is discarded.

420 verified of 420 recorded / 420 planned samples; 0 failures.

Baseline: main `b5fcfd3e`. Candidate: `034735dd`, runtime source fingerprint `f7647dbce62c`. Exact artifact pins are below.

Cold startup and the first same-selection refresh measure versioned durable readiness; reconnect measures fresh loaded result installation. Immutable ordered membership (when ordered), full state and heads captured at that endpoint must match the external oracle; persistence is checked separately. This is not time to first record.

Seven alternating pairs per case. No excluded samples or outliers. Positive deltas mean slower candidate. `empty`, `half` and `all` describe the local entity cache at fixture startup. `warm_refresh/empty` measures a refresh after the initial query has populated that cache. `warm_edit` and `warm_membership` change data while disconnected, then measure reconnect; they are not sustained connected-update throughput. `cold/full` prefetches the same predicate without ORDER BY or LIMIT before timing, then measures a new LIMIT10 livequery over the complete resident. Prefetch is excluded from readiness latency but included in whole-fixture CPU and memory.

| Platform | Population | Limit | Operation/cache | RTT ms | Main median ms | Candidate median ms | Paired delta ms [range] | Slower pairs |
|---|---:|---:|---|---:|---:|---:|---|---:|
| native | 8 | none | cold/empty | 0 | 0.671 | 0.882 | +0.181 [+0.068, +0.287] | 7/7 |
| native | 8 | none | cold/empty | 50 | 55.512 | 55.162 | -0.300 [-0.973, +0.461] | 3/7 |
| native | 256 | none | cold/half | 50 | 67.083 | 67.162 | -0.108 [-1.319, +2.803] | 3/7 |
| native | 4096 | none | cold/empty | 50 | 456.217 | 368.053 | -93.058 [-112.135, -79.881] | 0/7 |
| native | 4096 | none | warm_refresh/empty | 50 | 139.802 | 91.099 | -48.492 [-52.835, -44.618] | 0/7 |
| native | 4096 | none | warm_edit/all | 50 | 173.722 | 129.657 | -45.317 [-46.860, -41.064] | 0/7 |
| native | 4096 | 10 | cold/empty | 0 | 11.062 | 11.120 | +0.366 [-1.029, +1.659] | 4/7 |
| native | 4096 | 10 | cold/empty | 50 | 65.547 | 65.447 | +0.039 [-1.805, +0.788] | 4/7 |
| native | 4096 | 10 | warm_refresh/empty | 0 | 0.381 | 0.236 | -0.141 [-0.222, -0.062] | 0/7 |
| native | 4096 | 10 | warm_refresh/empty | 50 | 54.587 | 53.822 | -0.497 [-1.722, +0.786] | 1/7 |
| native | 4096 | 10 | warm_edit/all | 50 | 52.772 | 52.662 | -0.068 [-1.130, +0.883] | 3/7 |
| native | 4096 | 10 | warm_membership/all | 50 | 52.762 | 52.859 | +0.168 [-0.168, +0.988] | 5/7 |
| native | 16384 | 10 | cold/empty | 0 | 72.005 | 71.447 | -1.428 [-4.258, +3.219] | 3/7 |
| native | 16384 | 10 | cold/empty | 50 | 138.165 | 128.611 | +0.791 [-20.424, +7.779] | 4/7 |
| native | 4096 | 10 | cold/full | 0 | 0.586 | 1.572 | +0.972 [+0.816, +1.202] | 7/7 |
| wasm | 8 | none | cold/empty | 0 | 11.700 | 11.100 | -0.200 [-0.700, +0.500] | 1/7 |
| wasm | 8 | none | cold/empty | 50 | 64.500 | 64.000 | -0.600 [-1.500, +1.100] | 1/7 |
| wasm | 256 | none | cold/half | 50 | 147.100 | 133.600 | -14.000 [-26.300, -12.200] | 0/7 |
| wasm | 4096 | none | cold/empty | 50 | 4558.800 | 2226.300 | -2395.100 [-2471.900, -2126.900] | 0/7 |
| wasm | 4096 | none | warm_refresh/empty | 50 | 865.800 | 144.600 | -722.200 [-734.900, -694.000] | 0/7 |
| wasm | 4096 | none | warm_edit/all | 50 | 802.800 | 147.200 | -661.500 [-725.900, -612.400] | 0/7 |
| wasm | 4096 | 10 | cold/empty | 0 | 225.300 | 211.900 | -12.200 [-19.200, -1.600] | 0/7 |
| wasm | 4096 | 10 | cold/empty | 50 | 264.900 | 261.400 | -6.400 [-126.500, +125.900] | 2/7 |
| wasm | 4096 | 10 | warm_refresh/empty | 0 | 3.800 | 1.100 | -2.700 [-3.000, -2.600] | 0/7 |
| wasm | 4096 | 10 | warm_refresh/empty | 50 | 58.500 | 55.900 | -3.000 [-3.500, -0.800] | 0/7 |
| wasm | 4096 | 10 | warm_edit/all | 50 | 58.800 | 56.400 | -2.600 [-3.400, -2.300] | 0/7 |
| wasm | 4096 | 10 | warm_membership/all | 50 | 61.000 | 58.400 | -2.400 [-7.200, -0.500] | 0/7 |
| wasm | 16384 | 10 | cold/empty | 0 | 1039.200 | 979.700 | +1.100 [-383.200, +1281.000] | 4/7 |
| wasm | 16384 | 10 | cold/empty | 50 | 1024.200 | 1038.800 | -207.500 [-548.000, +759.300] | 3/7 |
| wasm | 4096 | 10 | cold/full | 0 | 4.900 | 5.200 | +0.400 [+0.100, +1.100] | 7/7 |

## Resource costs

All qualified cells; medians are paired candidate-minus-main. CPU phases overlap and must not be added together. Browser CPU covers the whole main-thread fixture and excludes IndexedDB worker CPU. WASM capacity is allocated linear-memory capacity, not RSS or live allocations.

| Platform / operation | Endpoint CPU delta ms | Whole-fixture CPU delta ms | Memory delta MiB |
|---|---:|---:|---:|
| Native N=8 / cold/empty / unlimited / RTT0 | +0.420 | +1.103 | +1.703 peak RSS |
| Native N=8 / cold/empty / unlimited / RTT50 | +0.267 | +1.596 | +1.812 peak RSS |
| Native N=256 / cold/half / unlimited / RTT50 | +3.200 | +11.913 | +5.234 peak RSS |
| Native N=4096 / cold/empty / unlimited / RTT50 | +90.441 | +104.322 | +30.125 peak RSS |
| Native N=4096 / warm_refresh/empty / unlimited / RTT50 | -29.483 | +57.940 | +32.953 peak RSS |
| Native N=4096 / warm_edit/all / unlimited / RTT50 | -94.957 | +72.474 | +30.422 peak RSS |
| Native N=4096 / cold/empty / LIMIT10 / RTT0 | +0.350 | +3.601 | +1.297 peak RSS |
| Native N=4096 / cold/empty / LIMIT10 / RTT50 | +0.394 | +5.385 | +1.656 peak RSS |
| Native N=4096 / warm_refresh/empty / LIMIT10 / RTT0 | -0.142 | +6.132 | +1.359 peak RSS |
| Native N=4096 / warm_refresh/empty / LIMIT10 / RTT50 | -0.130 | +5.498 | +2.281 peak RSS |
| Native N=4096 / warm_edit/all / LIMIT10 / RTT50 | +0.216 | +7.012 | +2.203 peak RSS |
| Native N=4096 / warm_membership/all / LIMIT10 / RTT50 | +0.121 | +7.427 | +1.781 peak RSS |
| Native N=16384 / cold/empty / LIMIT10 / RTT0 | -1.192 | +7.553 | +2.312 peak RSS |
| Native N=16384 / cold/empty / LIMIT10 / RTT50 | -0.812 | +13.989 | +3.047 peak RSS |
| Native N=4096 / cold/full / LIMIT10 / RTT0 | +0.959 | +300.149 | +30.297 peak RSS |
| Browser N=8 / cold/empty / unlimited / RTT0 | — | +16.566 | +0.250 WASM capacity |
| Browser N=8 / cold/empty / unlimited / RTT50 | — | +18.176 | +0.250 WASM capacity |
| Browser N=256 / cold/half / unlimited / RTT50 | — | +20.853 | +1.688 WASM capacity |
| Browser N=4096 / cold/empty / unlimited / RTT50 | — | -439.451 | +13.812 WASM capacity |
| Browser N=4096 / warm_refresh/empty / unlimited / RTT50 | — | -717.837 | +13.125 WASM capacity |
| Browser N=4096 / warm_edit/all / unlimited / RTT50 | — | -892.776 | +12.750 WASM capacity |
| Browser N=4096 / cold/empty / LIMIT10 / RTT0 | — | +16.225 | +0.062 WASM capacity |
| Browser N=4096 / cold/empty / LIMIT10 / RTT50 | — | +18.982 | +0.062 WASM capacity |
| Browser N=4096 / warm_refresh/empty / LIMIT10 / RTT0 | — | +17.154 | +0.062 WASM capacity |
| Browser N=4096 / warm_refresh/empty / LIMIT10 / RTT50 | — | +35.857 | +0.062 WASM capacity |
| Browser N=4096 / warm_edit/all / LIMIT10 / RTT50 | — | +22.602 | +0.062 WASM capacity |
| Browser N=4096 / warm_membership/all / LIMIT10 / RTT50 | — | +28.407 | +0.062 WASM capacity |
| Browser N=16384 / cold/empty / LIMIT10 / RTT0 | — | -175.805 | +0.062 WASM capacity |
| Browser N=16384 / cold/empty / LIMIT10 / RTT50 | — | +40.978 | +0.062 WASM capacity |
| Browser N=4096 / cold/full / LIMIT10 / RTT0 | — | -130.853 | +5.000 WASM capacity |

## Host observations

The study records total host CPU utilization and load once per second. These totals include the benchmark itself; they do not identify other processes or establish the cause of a slow sample. A pre-sample high-load wait lasts at most ten seconds; every planned sample is retained, including busy or unavailable starts.

- native: 210 samples; 1 busy start, 0 unavailable starts; 129 aggregate observations; utilization 18.6–87.5%; largest observation gap 1.005159708 seconds.
- wasm: 210 samples; 3 busy starts, 0 unavailable starts; 1130 aggregate observations; utilization 12.0–100.0%; largest observation gap 1.013169375 seconds.
- previous-vs-new: 4 samples; 0 busy starts, 0 unavailable starts; 6 aggregate observations; utilization 32.2–41.3%; largest observation gap 1.01003025 seconds.
- browser-trace: 8 samples; 0 busy starts, 0 unavailable starts; 162 aggregate observations; utilization 10.8–81.3%; largest observation gap 1.010160125 seconds.

Native endpoint process CPU includes both peers/background threads during the measured interval; whole-process CPU/RSS also include setup/prefetch/verification/teardown. Browser CDP task/script CPU covers the whole main-thread fixture and excludes IndexedDB worker CPU; WASM capacity is allocated linear memory, not peak RSS or live allocation. CPU phase intervals overlap and must not be summed.

No numerical slowdown allowance was set. These per-case results and resource tradeoffs do not become a parity claim by averaging unlike workloads; the shared-host limitations remain.

Qualifies the sampled unlimited and padded-string LIMIT10 selections only. Numeric ORDER BY has a separately recorded pre-existing baseline defect and is not covered here. Does not qualify million-row scale, sustained concurrent churn, or p95/p99 latency.

Full arm values, paired distributions, CPU/memory/work/traffic metrics and raw-result hashes are in qualification-summary.json.

## Exact artifact pins

| Role / platform | Production revision | Artifact SHA256 |
|---|---|---|
| main / native | `b5fcfd3eac3ea4247c4bd7613c439393beb42aad` | `d8477e71dd6d75bafc5c385e40ce6f4b6e3d31eb5ded2370d6a7b736542d73d4` |
| main / browser | `b5fcfd3eac3ea4247c4bd7613c439393beb42aad` | `2e8011fc3cda7d94cd00187c0db308a98c8372e8353a228e93c5c0b44d444574` |
| candidate / native | `034735dd0d8f9885b8a00e585c571a210892d21f` | `0aa6fb733394aeb57aeef98a9ba62f4eac765707d1d5e4bae4e6058f2b484158` |
| candidate / browser | `034735dd0d8f9885b8a00e585c571a210892d21f` | `67165ad8caf41bdae982b4eb1ab9953f0bbee8f5a7686aa2955f2805693b1221` |

The candidate runtime fingerprint is
`f7647dbce62c21646cb965afb3293d7206b5f43191d145d352f314b8c876e6a9`.
Both arms use rustc 1.101.0-nightly (`4ebd25825`, 2026-10-15) and identical
public-API fixture/oracle sources. Documentation-only checkpoints do not change
these measured inputs.

## Retained evidence

Local evidence is under
`~/ak/research_notes/ankurah-replication/reviews/2026-10-22-state-sharing/`.
`benchmarks/aggregate-1` contains all raw records, the fixed plan, source/artifact
manifests, paired distributions, aggregate monitoring and the endpoint-covered
browser traces. `native-attribution` contains the separate old/new CPU and
allocation profiles. Independent audits verify counts, order, raw oracles,
arithmetic, provenance and coverage. Earlier zero-observation launch failures,
method amendments and every earlier correctness failure are retained separately.
No observation was discarded, replaced or repeated based on its timing.

The reports cover memory-first query synchronization. Storage/durable replication,
million-record scale, numeric ORDER BY and sustained concurrent churn remain
outside this qualification. Human design/code review and performance acceptance
still gate landing; no PR has been opened.
