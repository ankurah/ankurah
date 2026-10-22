# Compatible tree reuse: unlimited-selection qualification

This is the intermediate compatible-reuse / RAII qualification, before the bounded-coverage fix. It does not qualify LIMIT queries or later source changes.

- Baseline: main `b5fcfd3eac3ea4247c4bd7613c439393beb42aad`.
- Candidate: `02a951d66492e9ab879c9dcee5e077797c94f4c4+working-source-751cce426d2e`, subsequently checkpointed as `19816db5` with a test-only durable-answer fixture correction.
- Native candidate SHA-256: `d94d374e7eb4fc29dcb7eb99d3dc7af5b2eef1acd8e9e91c2a3dee755a178328`.
- Browser candidate WASM SHA-256: `b1fc773767f359bbd6b11a08a97df94bb33c5245f2264038243e04d85cc34a7c`.
- Preserved raw evidence, source archive, diff and manifests: `research_notes/ankurah-replication/reviews/2026-10-21-compatible-reuse/attempt-2` in the shared local workspace.
- See [method and endpoint definitions](LIVE_QUERY_STARTUP.md).


448 verified of 448 recorded / 448 planned samples; 0 failures.


Cold startup and the first same-selection refresh measure durable readiness. Reconnect measures the first newly installed result. Immutable state, heads and membership are checked at that endpoint, followed by a separate persistence check.

Seven alternating pairs per case. No excluded samples or outliers. Positive deltas mean slower candidate. Cold/all includes prefetch and may use a warm resident cache.

| Platform | Rows | Operation/cache | RTT ms | Main median ms | Candidate median ms | Paired delta ms [range] | Slower pairs |
|---|---:|---|---:|---:|---:|---|---:|
| native | 8 | cold/empty | 0 | 0.734 | 0.910 | +0.187 [-0.120, +0.218] | 6/7 |
| native | 8 | cold/empty | 50 | 54.858 | 54.975 | -0.345 [-1.239, +0.576] | 2/7 |
| native | 8 | warm_refresh/empty | 50 | 54.255 | 54.234 | -0.053 [-0.212, +1.046] | 3/7 |
| native | 256 | cold/half | 0 | 14.297 | 13.726 | -0.841 [-1.947, +2.126] | 2/7 |
| native | 256 | cold/half | 50 | 67.435 | 67.339 | -0.191 [-2.109, +0.829] | 3/7 |
| native | 4096 | cold/empty | 0 | 437.196 | 349.645 | -89.725 [-95.221, -80.605] | 0/7 |
| native | 4096 | cold/empty | 50 | 487.296 | 395.313 | -95.062 [-100.608, -79.520] | 0/7 |
| native | 4096 | warm_refresh/empty | 0 | 92.420 | 39.869 | -52.473 [-55.346, -51.410] | 0/7 |
| native | 4096 | warm_refresh/empty | 50 | 150.129 | 93.308 | -55.460 [-63.207, -51.782] | 0/7 |
| native | 4096 | warm_equal/all | 0 | 126.328 | 23.436 | -102.408 [-105.170, -99.923] | 0/7 |
| native | 4096 | warm_equal/all | 50 | 179.509 | 76.624 | -100.984 [-105.088, -99.147] | 0/7 |
| native | 4096 | warm_edit/all | 0 | 126.921 | 25.235 | -102.002 [-120.529, -100.475] | 0/7 |
| native | 4096 | warm_edit/all | 50 | 180.201 | 131.121 | -48.548 [-54.285, -45.793] | 0/7 |
| native | 4096 | warm_membership/all | 0 | 127.383 | 25.986 | -102.819 [-105.388, -97.392] | 0/7 |
| native | 4096 | warm_membership/all | 50 | 181.110 | 130.748 | -48.179 [-54.071, -45.925] | 0/7 |
| native | 4096 | cold/all | 50 | 245.430 | 167.178 | -78.982 [-84.615, -71.657] | 0/7 |
| wasm | 8 | cold/empty | 0 | 11.200 | 11.000 | -0.200 [-0.500, +1.600] | 2/7 |
| wasm | 8 | cold/empty | 50 | 65.300 | 64.500 | -0.500 [-2.000, +1.200] | 2/7 |
| wasm | 8 | warm_refresh/empty | 50 | 56.200 | 53.700 | -2.400 [-3.200, -2.000] | 0/7 |
| wasm | 256 | cold/half | 0 | 104.900 | 88.300 | -17.600 [-24.500, -10.800] | 0/7 |
| wasm | 256 | cold/half | 50 | 148.100 | 135.300 | -12.100 [-42.000, -9.300] | 0/7 |
| wasm | 4096 | cold/empty | 0 | 4593.700 | 2340.100 | -2306.000 [-2523.300, -2200.800] | 0/7 |
| wasm | 4096 | cold/empty | 50 | 4570.600 | 2298.400 | -2287.400 [-2565.200, -2115.500] | 0/7 |
| wasm | 4096 | warm_refresh/empty | 0 | 817.400 | 87.100 | -732.600 [-796.500, -718.300] | 0/7 |
| wasm | 4096 | warm_refresh/empty | 50 | 898.100 | 144.600 | -756.200 [-767.900, -668.500] | 0/7 |
| wasm | 4096 | warm_equal/all | 0 | 797.300 | 31.300 | -764.300 [-848.000, -734.400] | 0/7 |
| wasm | 4096 | warm_equal/all | 50 | 834.400 | 83.400 | -749.800 [-771.500, -702.600] | 0/7 |
| wasm | 4096 | warm_edit/all | 0 | 783.600 | 41.700 | -738.800 [-887.500, -692.300] | 0/7 |
| wasm | 4096 | warm_edit/all | 50 | 821.300 | 145.400 | -680.500 [-773.900, -616.300] | 0/7 |
| wasm | 4096 | warm_membership/all | 0 | 766.400 | 47.700 | -716.700 [-829.800, -673.100] | 0/7 |
| wasm | 4096 | warm_membership/all | 50 | 841.900 | 151.400 | -688.600 [-718.800, -673.400] | 0/7 |
| wasm | 4096 | cold/all | 50 | 1371.100 | 525.600 | -857.800 [-959.300, -753.200] | 0/7 |

## Resource costs

Representative RTT0 cells; medians are paired candidate-minus-main. CPU phases overlap and must not be added together. Browser CPU covers the whole main-thread fixture and excludes IndexedDB worker CPU. WASM capacity is allocated linear-memory capacity, not RSS or live allocations.

| Platform / operation | Endpoint CPU delta ms | Whole-fixture CPU delta ms | Memory delta MiB |
|---|---:|---:|---:|
| Native / cold | +163.417 | +175.105 | +21.328 peak RSS |
| Native / warm_refresh | -38.243 | +121.487 | +22.328 peak RSS |
| Native / warm_equal | -103.007 | +157.444 | +19.641 peak RSS |
| Native / warm_edit | -102.109 | +134.497 | +18.109 peak RSS |
| Native / warm_membership | -100.538 | +136.591 | +21.328 peak RSS |
| Browser / cold | — | -447.174 | +12.250 WASM capacity |
| Browser / warm_refresh | — | -773.020 | +12.875 WASM capacity |
| Browser / warm_equal | — | -925.891 | +12.312 WASM capacity |
| Browser / warm_edit | — | -1010.631 | +12.500 WASM capacity |
| Browser / warm_membership | — | -864.808 | +13.125 WASM capacity |

## Host observations

Three browser launches stopped before samples because another task was compiling; all three guard records are preserved. During the later browser membership case, an unrelated cargo test process was observed. A follow-up snapshot showed cargo at 0.0% CPU and its test child at 0.1%; snapshots cannot prove zero contention throughout the interval. All samples are retained without reruns or exclusions based on timing. Native sampling finished before the observed external build activity. See browser-launch-attempts.json, wasm-host-observations.jsonl, and wasm-observed-external-cargo-59803*.json.

No numerical slowdown allowance was agreed. Each case is reported separately; improvements in larger cases do not erase costs in smaller ones. These measurements do not qualify million-row scale, sustained concurrent writes, or p95/p99 latency.

Full arm values, paired distributions, CPU/memory/work/traffic metrics and raw-result hashes are in qualification-summary.json.
