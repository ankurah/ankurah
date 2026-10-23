# Benchmarks

Wall-clock benchmarks for multi-node scenarios in `ankurah-tests`. They are advisory, not CI
gates: compare only runs from the same machine, toolchain and build profile.

## `lane_perf`

Criterion benchmarks of commits on a single node, and, over the in-process connector, of
commit-to-subscriber latency, a fresh client's first fetch, a stale client's catch-up and live query
establishment. The top of `lane_perf.rs` describes each lane, and
`specs/concurrency/LANE-BASELINE.md` records a baseline.

```sh
cargo bench -p ankurah-tests --bench lane_perf
```

## `live_query_startup`

Measures how long a live query takes to become ready: from issuing it, or reconnecting a client that
retains it, until the client has applied a durable answer for the query's current version. Each
sample then verifies the result the client held at that moment against the server: membership,
order, state and clocks. Each sample builds a durable Sled server node and an ephemeral Sled client
node in one process, on two Tokio worker threads, joined by a simulated link that serializes every
message.

```sh
cargo bench -p ankurah-tests --bench live_query_startup
```

### Scenarios

`STARTUP_MODES` chooses the scenarios. Every scenario except `cold` opens its live query before
timing starts and keeps it: the retained query.

| Mode | Measured operation | Endpoint |
| --- | --- | --- |
| `cold` | A new live query | `wait_durable_answered` returns |
| `warm_refresh` | The retained query reissues its selection over the open connection | `wait_durable_answered` returns for the new version |
| `warm_equal` | The client reconnects to an unchanged server | The retained query installs a fresh result |
| `warm_edit` | As `warm_equal`, after the server edited one selected row while the client was offline | As above |
| `warm_membership` | As `warm_edit`, after one row also left the result and a new row joined it | As above |

The reconnect endpoint has to mean the same thing on every revision the benchmark compares, and not
every revision resets a retained query's durable answer when its connection drops. So before
reconnecting, the benchmark clears the retained result set's loaded flag; the endpoint is the first
notification that finds the flag set again.

`STARTUP_CACHE` chooses what the client fetched before the scenario began:

- `empty`: nothing.
- `half`: the first half of the result.
- `all`: the whole result, fetched with the measured selection itself, so `cold` with `all` measures
  a cached start rather than a cold one.
- `full`: every row, fetched with the unlimited predicate, so that with `STARTUP_LIMIT` a limited
  query starts from a broader cached selection.

Reconnect scenarios run only with `all`, and an empty server runs only `cold` with `empty`.

The server holds `STARTUP_SIZES` rows. Row `i` has `position` `i` and `body` `row-` followed by `i`
zero-padded to eight digits. Without a limit the selection is `position >= 0`. With
`STARTUP_LIMIT=k` it is `body >= 'row-' ORDER BY body ASC LIMIT k`, ordered by the padded string
because ORDER BY sorts `i64` values as strings (GitHub #210). The offline edit changes a field the
selection neither filters nor orders by: `body` without a limit, `position` with one.
`warm_membership` also moves the first row out of the selection and adds a new row after the last
one.

### What is timed

Node creation, catalog negotiation, population, the prefetch and the scenario's preparation are not
timed. The clock starts immediately before the measured operation and stops at the endpoint. A
reconnect's measured operation includes both nodes registering each other again and the catalog work
that triggers.

### Verification

Each sample captures the query's result at its endpoint: for a reconnect, inside the notification
that marks the endpoint, while the link holds back further deliveries; otherwise immediately after
the durable-answer wait. It then checks the capture against the server's storage:

- exact membership;
- the order, for a limited selection;
- each entity's full state, head clock included;

and checks that the client persisted the same states. The benchmark writes nothing once timing
starts, so the server's storage is the reference. A failed check fails the run, and so does a case
that takes longer than 180 seconds. After verification the sample waits until the query's own
subscription response has been handled and the link has been quiet for 2 ms.

### Link model

Each message is serialized with bincode. Each direction serializes one message at a time at
`STARTUP_BPS` bytes per second, then the message propagates for half of `STARTUP_RTT_MS`;
propagation overlaps the serialization of the messages behind it, and the two directions are
independent. Delivery waits on Tokio timers. There is no socket, framing, loss or congestion
control. A reconnect opens a new connection, and a message still in flight from the old one is
dropped on arrival.

### Settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `STARTUP_SIZES` | `0,8,32,256,4096` | Rows on the server |
| `STARTUP_LIMIT` | unset | The selection's LIMIT; unset is unlimited, and `0` is `LIMIT 0` |
| `STARTUP_RTT_MS` | `0,50` | Round-trip propagation delay, in milliseconds |
| `STARTUP_BPS` | `0` | Bytes per second in each direction; `0` is unlimited |
| `STARTUP_MODES` | `cold` | Scenarios, from the table above |
| `STARTUP_CACHE` | `empty,half,all` | Client caches, from the list above |
| `STARTUP_REPEATS` | `3` | Repetitions of the whole matrix |
| `STARTUP_LABEL` | `candidate` | A name for the revision under test, copied into every record |

Lists are comma-separated, and the benchmark runs every combination that the rules above allow.

### Output

Each sample prints one line to stdout: `STARTUP` followed by a JSON record. `PHASE` lines on stderr
show progress, so a case that hangs shows the step it stopped in. A record names its case
(`label`, `entities`, `result_limit`, `expected_results`, `selection`, `cache`, `cached_entities`,
`mode`, `bandwidth_bytes_per_second`, `rtt_ms`, `repeat`) and holds:

- Landmarks, in microseconds since the measured operation began:
  - `elapsed_us`: the endpoint, equal to `ready_us` or `local_result_installed_us`.
  - `ready_us`: the durable-answer wait returned (`cold` and `warm_refresh`).
  - `local_result_installed_us`: the retained query installed a fresh result (reconnects).
  - `verified_us`: verification finished.
  - `transport_settled_us`: the last link activity before the link went quiet. It can precede the
    endpoint and `verified_us`.
- Durations: `local_result_capture_us`, the reconnect's capture of its result, which `elapsed_us`
  leaves out; and `verification_us`.
- `verified`: always `true`, since a sample that fails verification prints nothing.
- Work from the start of the measured operation until the link settled:
  - `traffic`: the messages and serialized bytes each node sent, and the largest message.
  - `server_reads` and `client_reads`: storage engine calls (selection reads with the states and
    bytes they returned, point reads, event reads with the events they requested), without the
    verifier's own point reads. They count calls, not disk reads.
  - `phase_process_cpu`: user and system CPU time of the whole process, both nodes and every thread
    included, for `setup` (everything before timing), `through_ready`,
    `through_local_result_installed`, `local_result_capture`, `verification`, `through_verified`,
    `through_transport_settled` and `verified_to_transport_settled`. The `through_` intervals all
    start with the measured operation, so they overlap and must not be added together. The
    intervals that end when the link settled include the 2 ms quiet confirmation. Hosts without
    `getrusage` report `null`.

### Comparing a branch with main

The benchmark uses only public node, connection and query APIs, so the same source can run on
another revision:

1. Copy `tests/benches/live_query_startup/` into a checkout of the other revision, and add this
   crate's `live_query_startup` `[[bench]]` entry and its `libc` dev-dependency to that checkout's
   `tests/Cargo.toml`.
2. Build each revision with the same toolchain and `cargo bench -p ankurah-tests --bench
   live_query_startup --no-run`, each in its own target directory.
3. Run both executables with identical settings and different `STARTUP_LABEL`s, alternating between
   them. Running one case per process keeps cases from affecting each other; every setting accepts
   a single value.
4. Compare each case separately: the median, the spread and the paired differences of `elapsed_us`,
   beside `traffic`, the largest message and the storage reads. A handful of samples does not
   establish tail latency.

The benchmark does not cover servers with millions of rows, concurrent writers, browser storage or
numeric ORDER BY.
