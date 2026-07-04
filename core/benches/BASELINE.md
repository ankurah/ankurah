# Event DAG baseline (phase 2 start)

Baseline captured at the start of concurrency phase 2, before any workstream D
refactor, so later optimization work (workstream E2) has a fixed reference. Two
engines are measured: the new event_dag engine on this branch (main plus the
#201 feature commit), and the pre-#201 `lineage` engine on `origin/main`.

Numbers are criterion medians (the middle value of criterion's
`[lower median upper]` estimate). They are wall-clock micro-benchmark figures on
one machine and are meant for relative comparison and regression tracking, not
as absolute throughput guarantees.

## Hardware and toolchain

- Machine: Apple M4 Max, 16 cores, 128 GB RAM
- OS: macOS 26.5.1 (build 25F80)
- Toolchain: rustc 1.97.0-nightly (ca9a134e0 2026-04-26), cargo 1.97.0-nightly
- criterion 0.5, default sampling (100 samples, 3 s measurement) unless noted
- Quiet machine, on AC power. Absolute values will differ on other hardware and
  on shared CI runners; capture your own baseline on the target machine before
  reading regressions into small deltas.

## New engine (this branch)

Commit: the E1 branch head (main plus #201 feature commit 90f9a67d).

Command:

```sh
cargo bench -p ankurah-core --features bench-internals --bench event_dag
```

| Group / case                    | Size | Median   |
| ------------------------------- | ---- | -------- |
| compare / linear_deep           | 10   | 14.52 us |
| compare / linear_deep           | 100  | 131.8 us |
| compare / linear_deep           | 1000 | 2.613 ms |
| compare / diamond_chain         | 4    | 18.14 us |
| compare / diamond_chain         | 16   | 71.92 us |
| compare / diamond_chain         | 64   | 292.0 us |
| compare / wide_antichain        | 4    | 10.05 us |
| compare / wide_antichain        | 16   | 30.43 us |
| compare / wide_antichain        | 64   | 109.2 us |
| compare / disjoint              | 10   | 26.05 us |
| compare / disjoint              | 100  | 263.7 us |
| layers / diamond_chain_drain    | 4    | 20.94 us |
| layers / diamond_chain_drain    | 16   | 84.29 us |
| layers / diamond_chain_drain    | 64   | 361.6 us |
| clock / contains                | 8    | 2.68 ns  |
| clock / contains                | 64   | 5.01 ns  |
| clock / contains                | 512  | 9.11 ns  |
| clock / normalize_from          | 8    | 27.19 ns |
| clock / normalize_from          | 64   | 510.9 ns |
| clock / normalize_from          | 512  | 4.99 us  |
| clock / with_event              | 8    | 54.95 ns |
| clock / with_event              | 64   | 105.3 ns |
| clock / with_event              | 512  | 502.4 ns |
| toposort / shuffled_chain       | 16   | 7.20 us  |
| toposort / shuffled_chain       | 128  | 66.04 us |
| toposort / shuffled_chain       | 1024 | 704.3 us |

## Old engine (origin/main, pre-#201)

Commit: `81082afd` (detached HEAD in a separate `baseline-main` checkout).

The pre-#201 comparison machinery is `core/src/lineage.rs`. Its entry point is
`lineage::compare<G, C>(getter, subject, other, budget) -> Ordering<Id>`, which
walks the generic `TClock` / `TEvent` / `GetEvents` traits with integer ids (no
content addressing). Its verdict enum differs from the new engine:

| Old `Ordering`     | New `AbstractCausalRelation` | Scenario                    |
| ------------------ | ---------------------------- | --------------------------- |
| `Descends`         | `StrictDescends`             | linear_deep, wide_antichain |
| `NotDescends{meet}`| `DivergedSince`              | diamond_chain               |
| `Incomparable`     | `Disjoint`                   | disjoint                    |
| `PartiallyDescends`| (folded into DivergedSince)  | not exercised here          |

The old-engine bench (`core/benches/lineage_baseline.rs` in `baseline-main`,
preserved as `baseline-main-benches.patch`) ports the same DAG shapes to this
API and asserts the mapped verdict on every case.

Command (from the `baseline-main` checkout):

```sh
cargo bench -p ankurah-core --bench lineage_baseline
```

| Group / case             | Size | Median   |
| ------------------------ | ---- | -------- |
| compare / linear_deep    | 10   | 2.33 us  |
| compare / linear_deep    | 100  | 17.16 us |
| compare / linear_deep    | 1000 | 182.3 us |
| compare / diamond_chain  | 4    | 699 ns   |
| compare / diamond_chain  | 16   | 706 ns   |
| compare / diamond_chain  | 64   | 709 ns   |
| compare / wide_antichain | 4    | 1.01 us  |
| compare / wide_antichain | 16   | 2.44 us  |
| compare / wide_antichain | 64   | 7.94 us  |
| compare / disjoint       | 10   | 2.85 us  |
| compare / disjoint       | 100  | 26.89 us |

Budget note: the old engine charges budget per BFS step (the mock returns cost 1
per `retrieve_event` batch) and does NOT self-escalate, whereas the new engine
escalates internally from `DEFAULT_BUDGET = 1000` up to 4x. The old-engine bench
uses a high fixed budget (1_000_000) so every shape reaches its terminal verdict
and the two engines measure comparable logical work. With the new engine's
default budget the old engine would hit `BudgetExceeded` on `linear_deep/1000`.

## Old vs new, comparable cases

Same DAG, same clocks, same logical verdict. Ratio is new / old.

| Scenario              | Size | Old      | New      | New / Old |
| --------------------- | ---- | -------- | -------- | --------- |
| compare / linear_deep | 10   | 2.33 us  | 14.52 us | ~6.2x     |
| compare / linear_deep | 100  | 17.16 us | 131.8 us | ~7.7x     |
| compare / linear_deep | 1000 | 182.3 us | 2.613 ms | ~14x      |
| compare / wide_antichain | 4 | 1.01 us  | 10.05 us | ~10x      |
| compare / wide_antichain | 16| 2.44 us  | 30.43 us | ~12x      |
| compare / wide_antichain | 64| 7.94 us  | 109.2 us | ~14x      |
| compare / disjoint    | 10   | 2.85 us  | 26.05 us | ~9x       |
| compare / disjoint    | 100  | 26.89 us | 263.7 us | ~10x      |

The new engine is consistently slower per comparison. That is the expected cost
of the #201 correctness work: the new engine accumulates the full DAG structure
during traversal, computes grounding ancestry (the V3 root-containment guard),
and builds forward chains for layer application, none of which the old engine
did. The old engine is faster but is the one the phase 1 verification found to
be wrong on the concurrency shapes #201 fixed. The baseline exists precisely so
E2 can recover some of this gap (the accumulator-memory and streaming-
application targets in workstream E3) with the correctness gates held.

### diamond_chain is measured but NOT directly comparable

The old engine's `diamond_chain` medians are flat (~700 ns regardless of chain
length) because it terminates as soon as the relationship is clear: comparing
the two pre-join tips of the final diamond, it reaches their immediate meet (the
previous join, one step down) and stops. The new engine does NOT early-terminate
on `DivergedSince`: `check_result` emits the verdict only once both frontiers
have fully drained, so it walks the entire chain to the root even though the
meet is one step down. Measured `get_event` calls scale as 3 * n (n=4 -> 12,
n=16 -> 48, n=64 -> 192), which is the linear scaling seen in the new-engine
table.

Both benches drive the same DAG, clocks, and verdict (meet = previous join), so
they are logically equivalent comparisons, but the wall-clock numbers reflect
different termination strategies and should not be divided against each other.
The new engine's full-walk-on-divergence is a candidate E2 optimization
(bounded-by-divergence-window rather than by history depth); it is filed as a
follow-up. The `layers/diamond_chain_drain` group has no old-engine equivalent
(the old engine had no `EventLayers` iterator; layering was introduced by #201),
so it is a new-engine-only baseline.

### clock and toposort

`Clock` operations and `topo_sort_events` are new-engine-only baselines. The old
tree has a `Clock` type but the phase 1 work changed it materially (normalizing
construction and binary-search membership are #201-era invariants), and
`topo_sort_events` did not exist pre-#201 (it is the V4 ordered-application fix).
They are recorded here as forward baselines rather than old-vs-new comparisons.
