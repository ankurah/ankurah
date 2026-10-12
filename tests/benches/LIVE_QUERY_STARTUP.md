# Cold live-query performance

`live_query_startup` measures a new subscription on a new ephemeral node, using
two real nodes, Sled, a two-worker Tokio runtime and serialized protocol messages.
The identical source can run against the pre-digest public API. Its baseline is
the branch point on main, `ebdb1727254c675a4d37236ecf23c16c06c4df12`.

```sh
STARTUP_LABEL=candidate cargo bench -p ankurah-tests --bench live_query_startup
```

The environment controls the matrix:

| Variable | Default | Meaning |
| --- | --- | --- |
| `STARTUP_SIZES` | `0,8,32,256,4096` | Server result sizes |
| `STARTUP_RTT_MS` | `0,50` | Injected round-trip latency in milliseconds |
| `STARTUP_CACHE` | `empty,half,all` | Local entity coverage before subscription |
| `STARTUP_REPEATS` | `3` | Fresh-fixture repetitions of each case |
| `STARTUP_LABEL` | `candidate` | Revision/run label in every JSON record |

The cache cases all start without an active query or retained query tree.
`half` and `all` prefetch entities outside the timed interval. Node creation,
catalog negotiation and server population are also excluded. The timed interval
starts immediately before `query_wait` and ends after `wait_durable_answered`.
The harness then verifies the exact membership and every applied entity state.
Errors and a 180-second sample timeout fail the run rather than produce a fast
but incomplete sample.

`STARTUP` JSON lines record elapsed microseconds, directional message counts and
serialized bytes, largest message, returned selection rows/bytes, point reads,
and requested events. Selection rows count returned materialized states, not all
internal index scan visits. Traffic includes all node messages during the timed
interval. Point/event counters describe calls or IDs requested through the
StorageEngine API, not physical disk I/O or cache misses. Traffic
excludes transport framing. Delay is per message; no bandwidth cap,
packet loss or real socket cost is simulated. Entity IDs are minted normally, so
repeat the workload to account for resulting tree-shape variation.

## Comparing revisions

Build both revisions with the same compiler, lockfile and optimized profile,
copying only this benchmark source and target declaration into the baseline.
Use separate Cargo target directories for the two source trees: sharing a target
can reuse incompatible workspace artifacts even when the package versions match.
Preserve each compiled executable separately; run them sequentially on the same
quiet machine in alternating baseline/candidate order. Do not run timed samples
while another build or benchmark is active. Retain raw samples and record source
revisions, harness hash, compiler, machine, run order and settings.

Use the comparison runner to isolate every fixture in a fresh process, alternate
revision order, enforce an external deadline and preserve failed samples:

```sh
python3 tests/benches/compare_live_query_startup.py \
  --baseline /absolute/path/to/baseline-binary \
  --candidate /absolute/path/to/candidate-binary \
  --baseline-revision BASE_SHA --candidate-revision CANDIDATE_SHA \
  --output /absolute/path/to/new-results-directory
```

Builds must finish before this runner starts. If lockfiles differ because the
candidate adds dependencies, verify that shared dependency versions match and
record the difference. Process isolation avoids carrying fixture teardown and
background work from one sample into the next. A failed sample remains a failure,
including when other repetitions succeed; raw output and sample counts are kept.

Compare each size/cache/RTT case separately: medians and the complete sample range,
plus work and traffic. A few repetitions do not establish p95/p99 latency. Measure
small-query startup as well as large transfer throughput; a warm reconnect win
does not compensate for a material cold-start regression. Investigate repeatable
increases before landing. Daniel has not accepted a numerical slowdown allowance.

For baseline runs the old implementation can send a whole result in one message;
report that largest-message cost alongside latency. It is a performance reference,
not a reason to remove bounds from the new protocol. Similarly, cached data may
still require full tree construction in the candidate: do not call it a warm-tree
measurement.

Full release qualification separately adds bandwidth limits, concurrent writes,
browser runs, peak allocations/retained memory and million-row cases after the
bounded data-source work. These small native measurements alone do not qualify
those paths. Steady-state live updates remain a separate regression concern.
