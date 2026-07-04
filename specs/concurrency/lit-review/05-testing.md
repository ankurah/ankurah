# Topic 5: Testing (deterministic simulation and convergence checking)

Scope: what makes a multi-node simulation harness actually deterministic, and what
vocabulary lets us state convergence claims precisely enough to check them. This
feeds C1 (seeded virtual transport with reorder/delay/dup/drop/partition and
post-quiescence invariant checks) and C8 (the optional stateright or TLA+ model of
the #268 scheduler). The literature answers two questions: (a) which sources of
nondeterminism a Rust async harness must capture to get bit-for-bit replay from a
seed, and (b) how Jepsen, the CRDT literature, and the model checkers phrase eventual
and causal convergence so an invariant check is well posed. Running theme: phase 2
assumes a pure-Rust harness suffices, and the strongest challenges come from teams
who found pure-language determinism leaks.

## Sources

### FoundationDB simulation testing (official docs + Zemb walkthrough)

https://apple.github.io/foundationdb/testing.html , https://pierrezemb.fr/posts/diving-into-foundationdb-simulation/

FoundationDB runs the real database (not mocks) as many logical servers inside a
single-threaded discrete-event simulator, built on the Flow actor language so every
await returns control to one event loop. All randomness flows through one seeded
PRNG (`deterministicRandom()`): "Every network latency value, every backoff delay
... every process crash timing goes through the same deterministic stream. Same
seed, same execution path, every single time." Time is virtual: when all actors
block, the loop "finds the next scheduled event (the earliest timestamp) and jumps
the simulated clock forward." Faults come from `BUGGIFY` macros firing
probabilistically in simulation only ("fires 25% of the time, deterministically"),
which also randomize tuning knobs (a 60s timeout becomes 0.1s) and swap or wipe
disks to model recovery.

Verdict: AGREES with C1's core shape (single-threaded, seeded, virtual time, fault
injection, real code under test); it is the archetype for "V4 and V6 class bugs
findable by search rather than by construction." Caution: FDB got determinism by
writing the whole system in Flow, which we cannot retrofit onto tokio for free.

### madsim (repo + docs.rs)

https://github.com/madsim-rs/madsim and https://docs.rs/madsim

madsim is the closest existing thing to what C1 needs: a drop-in tokio replacement
(`madsim-tokio`) that owns task scheduling, so poll order is a function of the seed.
It controls time via a patched `quanta`, randomness via a patched `getrandom`, and
reproduces any run from the `MADSIM_TEST_SEED` env var. It intercepts std entropy by
overloading libc (`gettimeofday`, `clock_gettime`, `getrandom`, `sysconf`) so time,
RNG, and sysconf reads cannot escape through std or third-party crates. It exposes
kill-process, disconnect, latency, and clog APIs, and is the harness RisingWave runs
in CI.

Verdict: AGREES with C1, and is a candidate to adopt outright rather than rebuild.
Tension: madsim only wins if our Nodes and every dependency they touch route through
its patched crates; anything using real threads, a non-patched RNG, or `std::time`
directly punches a hole in determinism (see S2 and the turmoil case study below).

### turmoil (repo + docs.rs + retry-race case study)

https://github.com/tokio-rs/turmoil , https://docs.rs/turmoil/latest/turmoil/ ,
https://www.tiarebalbi.com/en/blog/catching-retry-race-deterministic-simulation-rust-turmoil

turmoil, from the tokio team, runs multiple hosts on one thread with drop-in
replacements for `tokio::net`. Its fault API is exactly C1's list: `partition` /
`partition_oneway`, `repair` / `repair_oneway`, `hold` (holds in-flight messages),
`release`, plus `Sim::crash` (discards pending writes) and `Sim::bounce` (synced
data survives); recent releases add an unstable simulated filesystem. The Builder
takes a seed (`build_with_rng(...seed_from_u64(seed))`). The retry-race case study
is a near-exact V6 idempotency bug: a client applies "42", the ack is dropped by
`turmoil::partition` at `attempt == 0`, the client retries, the server
double-applies. The author loops seeds 0..32, pins the failing one, and it "remains
reproducible across all runs."

The virtual time both turmoil and madsim rely on is tokio's own
(https://docs.rs/tokio/latest/tokio/time/fn.pause.html): `start_paused` requires the
current-thread runtime and, when idle, "the clock is auto-advanced to the next
pending timer," because tokio splits the executor (polls until blocked) from the
reactor (holds timers). Gotcha: a task running forever via `spawn_blocking` makes
tokio think work remains and refuses to auto-advance; use `std::thread::spawn`.

Verdict: AGREES with C1's fault vocabulary almost verbatim, and validates the "loop
seeds, pin the failure" workflow C2 depends on. CHALLENGES the assumption that
turmoil alone is deterministic: the seed only "pins the simulator's choices," while
"direct calls to `std::time::Instant::now`, HashMap iteration order (seeded by
`OsRng`), `getrandom`, OS threads, and direct `tokio::net` bypass the simulator's
determinism entirely." turmoil controls the network, not intra-host poll order the
way madsim does. Because C1 checks invariants "after quiescence," the `spawn_blocking`
gotcha is load-bearing: quiescence is only well defined if the harness can tell all
tasks are blocked, and detached threads or `spawn_blocking` defeat that signal.

### S2.dev, "Deterministic simulation testing for async Rust"

https://s2.dev/blog/dst

The most concrete public account of making async Rust deterministic. It names the
enemies: tokio's work-stealing scheduler, "Rust's HashMaps being randomized for DOS
prevention," `std::time::Instant`, uncontrolled RNG, external I/O, and every
dependency as "a possible source of multi-threading, reliance on an RNG, current
system time, or external IO." Their fix is `mad-turmoil` (turmoil scheduling plus
madsim-style libc overrides of `getrandom`, `getentropy`, `CCRandomGenerateBytes`,
and `clock_gettime` via a `SimClocksGuard`; real I/O becomes in-memory emulators over
the sim network). They verify determinism by re-running the same seed and diffing
TRACE logs "down to the last bytes on the wire," and found "timestamps in HTTP
packets" broke determinism across runs.

Verdict: EXTENDS C1 with two things phase 2 omits. (1) The harness needs a
first-class determinism check (same seed twice, diff the full event trace), distinct
from the invariant check; a non-bit-identical replay is a harness bug that voids
every result. (2) Neither turmoil nor madsim alone suffices; production users combine
them and still hand-patch entropy. A direct challenge to any "just use turmoil" or
"just use madsim" plan.

### RisingWave deterministic simulation, parts 1 and 2

Part 1: https://risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/
Part 2: https://risingwave.com/blog/applying-deterministic-simulation-the-risingwave-story-part-2-of-2/

The largest published production case of madsim. Part 1 confirms the libc-override
mechanism (`gettimeofday`, `clock_gettime`, `getrandom`, `sysconf`) and names the
HashMap problem: you can seed your own `RandomState`, but "controlling this in
dependencies proved problematic." Part 2 is the honest experience report: recovery
testing meant "a rather painful debugging period of two months, as new bugs were
always discovered"; bug classes were panics, deadlocks, calculation errors,
cache-invalidation and operator-state-recovery correctness bugs, and data loss or
duplication during migration. They verify scaling by comparing simulated output
against the no-reschedule baseline, and state hard limits: "limited to Rust,"
external connectors in other languages cannot be simulated, and they "have not yet
injected failures during data modification because these operations are not atomic
and idempotent."

Verdict: EXTENDS C1 and C6. Evidence the approach finds exactly the V4/V6 class we
care about (data loss, duplication, recovery), so the bet is sound. CHALLENGES the
"land C1 early" schedule: RisingWave needed months to stabilize the harness against
its own nondeterminism, and its "no fault injection during non-idempotent writes"
gap maps straight onto our #272 commit_event/set_state ordering that C6 wants to
fault-inject. If our writes are non-idempotent the same gap bites us.

### Polar Signals, "DST in Rust: A Theater of State Machines"

https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust

The counter-architecture. Rather than swap the executor, they write each component
as a synchronous state machine behind one message bus:
`fn receive(&mut self, m) -> Option<Vec<(Message, Destination)>>` and
`fn tick(&mut self, curtime: Instant) -> ...`. The bus alone ticks machines, applies
messages, chooses "current time," and injects faults ("failure injection by sending
back an error ... before or after applying the original message ... or even drop
messages at any point"), so it "needs to be implemented only once." Machines have
"no direct access to system time" and the interface "does not include the `async`
keyword, which ensures at compile time that there are no runaway futures spawned."
Payoff: they "discovered two critical bugs ... one causing data loss and another
causing data duplication." Cost: "considerable cognitive overhead," greenfield only.

Verdict: CHALLENGES C1's implied architecture. Ankurah wants to simulate "real
Nodes," pointing at the executor-swap (madsim/turmoil) path; Polar Signals argues
the synchronous state-machine model is more robust because it makes nondeterminism a
compile error rather than a discipline sustained forever. This is the sharpest C1
fork and phase 2 leaves it unresolved. It also informs C8: their `StateMachine`
trait is already a hand-rolled model of the kind stateright would check.

### Swarm testing (Groce, Zhang, Eide, Chen, Regehr, ISSTA 2012)

https://agroce.github.io/issta12.pdf (ACM 2012; abstract and stack-ADT example verified from the PDF)

The primary source for how to inject faults well. Conventional random testing
enables all features every test; swarm testing runs "a large swarm of randomly
generated configurations, each of which omits some features." Omission helps two
ways: some features suppress bugs (a "pop" stops a stack ever overflowing, hiding an
overflow bug at capacity 32), and absent suppression, features "compete for space in
each test, limiting the depth to which logic driven by features can be explored."
Result: "in a week of testing it found 42% more distinct ways to crash a collection
of C compilers than did the heavily hand-tuned default configuration."

Verdict: EXTENDS C1's fault-injection design. C1 lists reorder/delay/dup/drop/
partition as if each run exercises their union; swarm testing says the opposite
maximizes yield: each seed enables a random subset (some partition-only, some
drop-only), because a run that drops everything masks the reordering bug. FDB and
Antithesis both do this; cheap change, outsized return for C2's high-seed runs.

### Antithesis DST explainer + Will Wilson lineage

https://antithesis.com/docs/resources/deterministic_simulation_testing/ , https://cerebralvalley.beehiiv.com/p/antithesis-the-last-word-in-autonomous-software-testing

Antithesis, built by the FDB team, lays out three ways to get DST: (1) rewrite the
system in a deterministic framework (Flow); (2) use a deterministic runtime or
executor library (madsim, turmoil), "generally impractical for systems already in
production" and requiring every external dependency to be "mocked or otherwise
plugged to ensure determinism"; (3) run unmodified software in a deterministic
hypervisor (their bhyve fork), which "ensures that non-deterministic code can be
made deterministic" without touching the app. They also stress intelligent
state-space search over pure random seeds. WarpStream reports Antithesis caught, day
one, a data race present since the project's first month, missed by their own tests
across "10s of thousands of hours," in 233 seconds.

Verdict: CHALLENGES C1 at the root. The people who invented the technique argue
language-level determinism (our plan) is the leaky option and only whole-system
(hypervisor) determinism closes the holes S2 and RisingWave hit. We will not build a
hypervisor, but the honest reading is that a pure-Rust harness will have determinism
gaps, so C1 must budget a determinism-audit loop, or consider Antithesis for the
nightly C2 tier.

### Jepsen consistency models reference + causal page

https://jepsen.io/consistency , https://jepsen.io/consistency/models ,
https://jepsen.io/consistency/models/causal

Jepsen's model taxonomy is the vocabulary C1's checks should speak. Verified from
the pages: causal consistency means "causally-related operations should appear in
the same order on all processes, though processes may disagree about the order of
causally independent operations," it is "sticky available" (progress under partition
only if a client sticks to one server), and "Real-Time Causal is proven to be the
strongest consistency model in an always-available, one-way convergent system."
Models at or stronger than sequential "cannot be totally available in asynchronous
networks." Session guarantees (monotonic reads, monotonic writes, read-your-writes,
writes-follow-reads) compose into causal.

Verdict: AGREES with and sharpens the #267 contract and the per-entity causal-DAG
design; our target is exactly "causal+ / real-time causal, sticky available," which
Jepsen names. EXTENDS C1: the C5 reactor/LiveQuery coherence work should be phrased
as monotonic-reads and read-your-writes checks in Jepsen's terms, not ad hoc, so the
claims are legible and comparable.

### Elle and the jepsen causal-consistency suite

https://github.com/jepsen-io/elle , https://github.com/nurturenature/jepsen-causal-consistency

Elle is Jepsen's transactional checker: it builds a dependency graph from a history
and finds cycles mapping to named anomalies (G0 dirty write, G1a/G1b/G1c read
anomalies, G2 anti-dependency cycle, lost update). For scale, "Knossos runtimes
diverge exponentially with concurrency; Elle is effectively constant," since cycle
detection beats brute-force linearizability search. The causal suite operationalizes
convergence with a final read:
after quiescence "each client does a final read of all keys," and Strong Convergence
asserts "all nodes have ok final reads, final reads contain all ok writes, no
unexpected read values, final reads are equal for all nodes."

Verdict: AGREES with C1's "all-node convergence after quiescence" invariant and
gives a phrasing to copy: every node's final read is defined, contains every
acknowledged write, and is byte-equal across nodes. EXTENDS C1: prefer an Elle-style
graph-cycle checker over a Knossos-style linearizability search, since our DAG is
already a dependency graph and the exponential checker will not scale to C2.

### Kleppmann and Howard, "Byzantine Eventual Consistency and the Fundamental Limits of Peer-to-Peer Databases" (2020)

https://arxiv.org/abs/2012.00472 (PDF read directly; definitions quoted verbatim)

Gives the formal convergence conditions a check must encode. Strong Eventual
Consistency (Shapiro et al.) has two parts: Eventual update, "If an update is applied
by a correct replica, then all correct replicas will eventually apply that update,"
and Convergence, "Any two correct replicas that have applied the same set of updates
are in the same state (even if the updates were applied in a different order)." The
commutativity condition `forall S. apply(apply(S, u1), u2) = apply(apply(S, u2), u1)`
gives "apply the same commutative sets of updates in a different order, and still
converge to the same state." Byzantine Eventual Consistency strengthens SEC with
Atomicity, Authenticity, Causal consistency, Invariant preservation, and the central
theorem: a fault-tolerant BEC algorithm preserving an app's invariants exists "if
and only if the set of all transactions executed by correct replicas is I-confluent
with respect to each of the invariants."

Verdict: AGREES with and formalizes #267's "resolution may depend only on graph facts
and event contents, never wall clocks or arrival order," the order-independence half
of Convergence. EXTENDS C3 and C4: the I-confluence theorem is the sharp line between
merges made convergent without coordination and those that cannot be, the right frame
for which LWW or Yrs laws are achievable. CHALLENGES any convergence test checking
only equal final state without order-independence: SEC requires convergence for the
same set applied in different orders, so C3's "layer permutation invariance" and
"cross-order determinism" laws are the definition of the property, not extras.

### stateright (repo + TLA+ comparison)

https://github.com/stateright/stateright , https://www.stateright.rs/comparison-with-tlaplus.html

The concrete tool for C8. stateright is a Rust model checker with an actor model, an
explorer UI, and a network model for "lossy/lossless duplicating/non-duplicating"
delivery. It checks "always" (safety), "sometimes" (nontriviality), and "eventually"
(liveness) properties, and ships linearizability and sequential-consistency testers
plus Paxos and two-phase-commit examples. Headline advantage over TLC/TLA+: "systems
implemented using Stateright can also be run on a real network without being
reimplemented in a different language."

Verdict: AGREES with C8's "stateright (or TLA+) model of the scheduler"; the
run-the-real-code property argues for stateright over TLA+ so the model and #268
cannot drift into two artifacts. EXTENDS the safety/liveness distinction below:
convergence is stateright's "eventually" (liveness), head-antichain and no-lost-write
are its "always" (safety), which is how C1's invariants should be classified.

### Strong-consistency-models framing (aphyr) and the safety/liveness distinction

https://aphyr.com/posts/313-strong-consistency-models

Kingsbury's hierarchy (linearizable > sequential > causal > PRAM > read-your-writes)
grounds why our target is causal, not linearizable (not totally available). The
deeper testing point, standard in the literature this post sits in: eventual
consistency and convergence are liveness properties ("something good eventually
happens"), which no finite execution can falsify, so a test cannot prove convergence,
only detect its violation within a bounded window.

Verdict: AGREES with the causal target. CHALLENGES how C1 phrases convergence:
because it is liveness, C1 cannot check "the nodes converged" in general; it must
inject quiescence (stop faults, flush in-flight via a turmoil-style `release`, drain
the sim) then assert byte-equal final reads. Without an enforced quiescence point the
check is vacuous or flaky. The single most important methodological constraint here.

## Findings

1. [C1 determinism] A deterministic async-Rust harness must capture five entropy
   sources, and public reports show each one bites in practice: executor poll
   order (work-stealing scheduler), time (`Instant`/`clock_gettime`), RNG
   (`getrandom`/`getentropy`), HashMap iteration order (randomized for DoS), and
   any escape into real threads or real I/O. madsim captures all five via libc
   overrides plus executor replacement; turmoil captures the network but not
   intra-host poll order or std entropy on its own.

2. [C1 architecture fork] There are two proven architectures. Executor-swap
   (madsim/turmoil) simulates real async Nodes with least code change but leaves
   determinism as an ongoing discipline. State-machine-behind-a-bus (Polar
   Signals) makes nondeterminism a compile error but forces a synchronous rewrite.
   Phase 2 assumes the former ("real Nodes") without acknowledging the latter
   exists or is more robust.

3. [C1 determinism audit] Every serious practitioner (S2, WarpStream, FDB)
   verifies determinism itself by running a seed twice and diffing the full event
   trace to the byte. This is a distinct check from the convergence invariant and
   phase 2 does not list it. A non-reproducible run is a harness bug that voids all
   results.

4. [C1 quiescence] Convergence and eventual consistency are liveness properties:
   unprovable by a finite run, only violated within a window. The convergence
   invariant is only well posed if C1 enforces an explicit quiescence point (faults
   healed, in-flight released, sim drained) before the all-node final-read check.

5. [C1 convergence assertion] Jepsen's causal suite gives the assertion to copy:
   after quiescence, every node's final read exists, contains every acknowledged
   write, has no unexpected value, and is byte-equal across nodes. Kleppmann's SEC
   adds that it must hold for the same update set applied in different orders, so
   order-independence is part of the definition, not a bonus.

6. [C1 fault strategy] Swarm testing shows enabling all fault kinds every run
   suppresses bugs (a run that drops everything hides a reordering bug). C1 and C2
   should randomize which faults are active per seed, matching FDB and Antithesis;
   ISSTA-2012 measured 42% more distinct crashes from this alone.

7. [C1 checker choice] Prefer an Elle-style dependency-graph cycle checker over a
   Knossos-style linearizability search: Knossos is exponential in concurrency,
   Elle effectively constant, and our per-entity DAG is already the dependency
   graph such a checker consumes. This matters for C2's 100k-seed tier.

8. [C8 tool] stateright is the right C8 tool over TLA+ because the same Rust code
   can run in the model checker and on a real network, preventing model/impl
   drift; it natively expresses safety ("always": head-antichain validity, no lost
   write) versus liveness ("eventually": convergence).

9. [#267 formalization] Kleppmann's I-confluence theorem is the precise line
   between merges achievable without coordination and merges that need it, and it
   validates #267's "graph facts and event contents only, never wall clock or
   arrival order." C3's layer-permutation and cross-order laws are the operational
   form of SEC's Convergence condition, not optional extras.

10. [C1 vs #272 gap] RisingWave declined to fault-inject during non-idempotent
    writes. Our commit_event/set_state ordering (#272, C6) is exactly such a
    non-idempotent boundary; the same gap will bite unless our ingest is genuinely
    idempotent under replay, which the turmoil retry-race case study shows is the
    first thing a seeded harness will attack.

## Candidate design deltas

- C1 should add an explicit determinism self-check (same seed twice, hash the full
  event trace, fail on any diff) as a first-class harness invariant, separate from
  the convergence invariant. Practitioner consensus; phase 2 omits it.

- C1 should define its convergence invariant only at an enforced quiescence point
  (faults healed, in-flight released, sim drained), phrased in Jepsen terms: every
  node's final read contains all acknowledged writes and is byte-equal across nodes.
  Convergence is liveness and otherwise not checkable. The equality must hold across
  permuted apply orders, not one order: SEC defines convergence as
  order-independence, so a single-order check under-tests the property.

- C1 fault injection should be swarm-style: each seed enables a random subset of
  {reorder, delay, dup, drop, partition}, not the union every run. Cheap change,
  large measured bug-yield gain.

- Phase 2 should make an explicit, recorded decision on the C1 architecture fork:
  adopt madsim (executor-swap, least change, run real Nodes) versus a
  state-machine-bus model (nondeterminism becomes a compile error, but a rewrite).
  Today it silently assumes the former.

- If C1 goes the madsim/turmoil route, it must budget for a recurring
  determinism-audit against dependency and std entropy leaks (HashMap ordering,
  `Instant::now`, detached threads, `spawn_blocking` defeating time auto-advance),
  and consider a hypervisor tool (Antithesis) for the nightly C2 tier where
  language-level determinism is known to leak.

- C3's #267 conformance laws (layer permutation invariance, cross-order
  determinism) should be framed as the operational encoding of SEC's Convergence
  condition, and the backend law set should be checked against Kleppmann's
  I-confluence to decide which LWW/Yrs guarantees are even achievable without
  coordination.

- Reuse Jepsen and Elle vocabulary and, where feasible, Elle itself for C5 reactor
  or LiveQuery coherence (monotonic reads, read-your-writes) so ankurah's
  convergence claims are legible and comparable rather than bespoke.
