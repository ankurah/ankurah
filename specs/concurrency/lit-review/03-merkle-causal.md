# Topic 3: Merkle-causal systems

Scope: Ankurah's per-entity event DAG is, structurally, a Merkle-CRDT. EventId is a
SHA-256 content hash of (entity id, operations, parent clock); events reference
predecessors by hash; the head is the tip antichain; cycles are structurally
impossible; identity is anchored at a creation event. This is exactly the
construction that the Merkle-CRDT, Byzantine-causal-broadcast, and hash-graph-gossip
literature study. This file asks the question the phase 2 plan poses for workstream C4
(threat model) and RFC #274 (validated ingress): what do these systems pay for
adversarial tolerance, and which of those costs do we already pay for free by content
addressing? The short answer that emerges: content addressing buys us tamper-evidence,
de-duplication, cycle-freedom, and unforgeable happened-before, which is exactly the
regime in which arbitrary-Byzantine causal ordering is solvable at all. It does NOT
buy us equivocation attribution, a DoS bound, strong-safety causal order, or the
ability to prune, and every one of those gaps lands on a named phase 2 RFC.

## Sources

### 1. Sanjuan, Poyhtari, Teixeira, Psaras, "Merkle-CRDTs: Merkle-DAGs meet CRDTs" (Protocol Labs, arXiv:2004.00107, 2020)

https://arxiv.org/abs/2004.00107

The foundational paper. It defines a Merkle-Clock as a Merkle-DAG where each node is an
event identified by the CID (content hash) of its payload plus the CID-set of its
direct children, proves the Merkle-Clock is a Grow-Only-Set state-based CRDT whose join
is set union, and defines Merkle-CRDTs as Merkle-Clocks carrying CRDT payloads. Their
implementation rule ("every new event must be a new root whose child-set contains the
CIDs of the previous roots") is our head/parent-clock construction verbatim; their
partial order (a < b iff a is a descendant of b) is our causal comparison; their
general anti-entropy algorithm (walk down from a broadcast root, collect the CID-set D
of unknown nodes, sort by the clock, apply lowest-to-highest, then compare roots to
classify included / includes / concurrent) is our symmetric backward BFS plus Kahn
topo-sorted ingest. The paper is candid about limitations: the DAG is permanent and
ever-growing, cold-sync of a deep thin DAG can be slower than shipping a snapshot, and
merge/inclusion checks are costly "if DAGs have diverged significantly (or long ago)."
Verdict: AGREES with the whole per-entity-DAG architecture; CHALLENGES the "budget is
the only backstop" stance for #271 by naming DAG-divergence merge cost as the core
limitation and by stating GC requires knowing the replica set (see Finding 5).

### 2. Kleppmann, "Making CRDTs Byzantine Fault Tolerant" (PaPoC 2022)

https://martin.kleppmann.com/papers/bft-crdt-papoc22.pdf

The most directly load-bearing source for C4 and #274. It shows that unlike Byzantine
consensus (which needs 3f+1 and is Sybil-vulnerable), operation-based CRDTs can tolerate
ANY number of Byzantine nodes with only modest changes: (a) identify each update by
H(update) where the update carries the set of predecessor hashes, forming a hash DAG
"resembling a Git commit history" with heads = updates that are nobody's predecessor;
(b) derive unique operation IDs from the update hash rather than a per-node counter,
which defeats equivocation-by-duplicate-ID; (c) crucially, the validity of an update u
must be decided only over before(u), the transitive closure reachable through u's
predecessor hashes, "not taking into account any updates that are not in before(u),"
because before(u) is identical on every correct node even when their delivered sets
differ. Signatures are needed only for authenticity ("which node generated which
update"), "not necessary for achieving Strong Eventual Consistency." A Byzantine node
generating an update with predecessor hashes that resolve to nothing "simply results in
the update ... never being delivered by correct nodes." A Byzantine node may generate
many concurrent updates or many predecessors to degrade performance, "but these updates
will not affect the correctness of the algorithm."
Verdict: AGREES strongly with the phase 2 resolution invariant (#267): "may depend only
on graph facts and event contents, never wall clocks or arrival order" is exactly
Kleppmann's before(u) rule and is the sufficient condition he identifies for Byzantine
convergence. CHALLENGES #274/#266: our retrieval budget can make before(u) only
partially known (BudgetExceeded), which breaks the "before(u) identical on every node"
premise the convergence proof rests on (see Findings 1 and 2).

### 3. Kleppmann, Howard, "Byzantine Eventual Consistency and the Fundamental Limits of Peer-to-Peer Databases" (arXiv:2012.00472, 2020)

https://arxiv.org/abs/2012.00472

The formal backbone. It defines Byzantine Eventual Consistency (BEC) as seven
properties: self-update, eventual update, convergence, atomicity, authenticity, causal
consistency, and invariant preservation. Its central impossibility result, Theorem 3.1,
is: "There exists a fault-tolerant algorithm that ensures BEC if and only if the set of
all transactions executed by correct replicas is I-confluent with respect to each of the
invariants." Non-negative-balance and uniqueness constraints are shown NOT to be
I-confluent ("if T1 and T2 both create data items with the same value in that attribute,
then {T1,T2} is not I-confluent"), so no BEC algorithm can maintain them. Their
Byzantine causal broadcast (Algorithm 1) sends (value, predecessor-hash-set, signature)
triples, exchanges heads, requests missing messages by hash via needs/msgs rounds,
discards incorrectly-signed messages, buffers any message whose predecessors do not yet
resolve, and delivers in topological order once dependencies are present. Validity is
required to be "a function of only the updates and the invariants, without depending on
the current replica state." On storage: M grows monotonically and unboundedly; pruning
of a message's predecessors is safe only "once every replica has delivered a message m
(i.e. m is stable)," explicitly likening this to Byzantine-agreement append-only logs
that never consider truncation.
Verdict: AGREES with the deterministic-validity invariant. CHALLENGES #274 by drawing a
hard line (I-confluence) under what validated ingress can convergently enforce; a
cross-entity uniqueness or referential-integrity check cannot be a BEC invariant.
CHALLENGES #271: "prunable only when stable across the full replica set" is the same
wall the Merkle-CRDT paper hits, and it is fatal to a naive sealed-prefix horizon in an
open membership model (see Findings 3, 4, 5).

### 4. Jacob, Bayreuther, Hartenstein, "On CRDTs and Equivocation in Byzantine Setups" (arXiv:2109.10554, 2021)

https://arxiv.org/abs/2109.10554

A brief announcement that isolates precisely why content addressing is load-bearing. It
defines an algorithm as equivocation-tolerant if it "neither needs to detect, prevent,
nor remedy equivocation" beyond coping with omission. Theorem 2: operation-based CRDTs
that provide "inherent identity" (identity derived from content, "the identifier of an
operation is the hash of its content") and "inherent ordering" (the happened-before
relation recorded in the payload, so that "Byzantine attackers cannot tamper with the
happened-before relation, as hashes verifiably prove that an operation referenced by
another operation via its hash has happened before") are equivocation-tolerant, and any
equivocation "can be reduced to a hash collision." Their conjecture: "A hash-chained
directed acyclic graph ... is the only operation-based CRDT with non-commutative
operations that provides SEC for any number of Byzantine faults." They note state-based
CRDTs are unconditionally equivocation-tolerant but lose update identity, so access
control needs operation-based signed updates.
Verdict: AGREES emphatically. This is the strongest theoretical endorsement of the
architecture: our content-hash EventId (inherent identity) plus hash-referenced parent
clock (inherent ordering) is, by their conjecture, essentially the unique design that
works for our fault class, and we get it for free. CHALLENGES nothing directly, but
frames the honest boundary: the guarantee is SEC / convergence, not authorization
(see Finding 6).

### 5. Almeida, Shapiro, "The Blocklace: A Byzantine-repelling and Universal CRDT" (arXiv:2402.08068, 2024)

https://arxiv.org/abs/2402.08068

The sharpest CHALLENGES source. A blocklace generalizes a blockchain to a partial order
where each block carries "any finite number of signed hash pointers to preceding
blocks," and block identity is "a signed hash of the block content" (so the creator is
cryptographically identifiable, unlike a plain block-DAG's unsigned hashes). Its
motivating critique of prior hash-DAG CRDTs (ours included) is exact: they are merely
"equivocation-tolerant (they do not detect or prevent equivocations), allowing a
Byzantine node to cause an arbitrary amount of harm by polluting the CRDT state with an
unbounded number of equivocations." The blocklace's signed hashes make equivocation
(two same-author blocks incomparable under the precede relation) provable, and the
Byzantine-repelling protocol buffers blocks from known-equivocators so that "a
Byzantine node may harm only a finite prefix of the computation." Cost: one signature
per block; the DAG still grows unboundedly and pruning is not addressed.
Verdict: CHALLENGES C4 and #274 head-on. It names the precise cost we are NOT paying:
because our EventIds are unsigned content hashes, we get equivocation TOLERANCE (correct
nodes still converge) but no equivocation ATTRIBUTION and, critically, no finite-harm
bound. A malicious peer can flood unbounded distinct-but-valid concurrent events (each a
real hash), forcing unbounded staging, deep merges, and wide antichains, with no
structural cap. This is the adversarial version of the #271 thrash problem and the #246
oversized-batch problem combined (see Findings 6, 7, 8).

### 6. Kshemkalyani, Misra, "Deterministic Causal Order Under Byzantine Sybil Tolerance: Techniques and Limitations" (SSS 2025)

https://www.cs.uic.edu/~ajayk/ext/SSS2025b.pdf  (also Springer 10.1007/978-3-032-11127-2_29)

Directly names Matrix as its motivating real-world Byzantine-Sybil-tolerance use case
and maps the solvability frontier for deterministic causal ordering of broadcast
messages at the highest fault tolerance f<n (arbitrarily many Byzantine, matching our
open-membership assumption). Key results, quoting the summary tables: for broadcasts,
weak safety plus liveness is achievable for f<n WITHOUT cryptography ("Weak safety and
liveness even without cryptography can be guaranteed for f < n"); but "Strong safety
cannot be guaranteed without the use of cryptography," and the standout negative result,
"For broadcasts without cryptography, liveness cannot be guaranteed when f < n, whereas
it could be guaranteed when f < n/3." Strong safety here means: if m1 causally precedes
m2, every correct process delivers m1 before m2; weak safety is the relaxation that only
holds among messages a correct process actually sends/relates.
Verdict: AGREES that our unsigned content-addressed DAG lands in the one solvable
regime (weak-safety causal order at f<n). CHALLENGES the implicit assumption that
content hashing alone gives full causal-order guarantees under adversaries: it delivers
weak safety, not strong safety, and the liveness result warns that broadcast liveness at
f<n is a knife-edge that our unsigned scheme does not automatically clear (it depends on
the correct-node connectivity assumption, not on the hashes) (see Findings 9, 11).

### 7. Auvolat, Frey, Raynal, Taiani, "Byzantine-Tolerant Causal Broadcast" (Theoretical Computer Science 885, 2021)

https://doi.org/10.1016/j.tcs.2021.06.021

Provides a formal Byzantine causal broadcast abstraction and a simple algorithm
implementing it, encoding dependencies by message digests so a receiver delays delivery
until causal predecessors are present. It establishes that a "No-Duplication /
No-Creation / Validity / causal-order" safety set is implementable against Byzantine
processes without consensus, given reliable pairwise communication among correct
processes. It is the precursor that Jacob et al. (source 4) build on to argue hash-DAG
equivocation tolerance.
Verdict: AGREES with the buffer-until-dependencies-resolve ingest discipline in #268
(events are staged and become discoverable before any head references them). Its
delivery model (delay, never reject, on a missing predecessor) CHALLENGES our Disjoint
= reject verdict, which is a rejection where the causal-broadcast literature would
buffer (see Finding 10).

### 8. Jacob, Beer, Henze, Hartenstein, "Analysis of the Matrix Event Graph Replicated Data Type" (IEEE Access 9, 2021)

https://doi.org/10.1109/access.2021.3058576

Formal analysis showing the Matrix Event Graph (the room DAG: an add-only DAG of
hash-referenced events) is a CRDT that tolerates an arbitrary number of Byzantine nodes,
i.e. a production-scale instance of the same design as ours. Kleppmann (source 2) cites
it as a real system in this class. It is the empirical bridge to the Matrix operational
record below.
Verdict: AGREES that a hash-referenced add-only event DAG is a sound Byzantine-tolerant
CRDT at scale, corroborating the architecture. See sources 9 and 10 for where Matrix
found the sharp edges in practice.

### 9. Matrix MSC1442, "State Resolution" proposal

https://github.com/matrix-org/matrix-doc/blob/erikj/state_res_rejections/proposals/1442-state-resolution.md

Matrix's canonical statement of why event ordering must be derived from DAG structure,
not from claimed metadata. Direct quote: "Since depth of an event cannot be reliably
calculated without possessing the full DAG, and cannot be trusted when provided by other
servers, it can not be used in future versions of state resolution." It names the two
adversarial motivations: "Moderation evasion - where an attacker can avoid e.g. bans by
forking and joining the room DAG in particular ways" and "State resets - where a server
... sends an event that points to disparate parts of the graph, causing state resolution
to pick old state rather than later versions." Ordering is instead computed by "reverse
topological power ordering" and "mainline ordering" over the auth DAG plus power levels,
with event-ID lexicographic tiebreak.
Verdict: AGREES emphatically with the phase 2 invariant that resolution uses only graph
facts and content, with content-hash tiebreak (our LWW tournament ends on content-hash
tiebreak exactly as Matrix ends on event-ID). CHALLENGES us to audit: does any part of
our LWW provenance or applied-set logic smuggle in arrival order or a trusted claimed
field the way depth did? (see Finding 11).

### 10. Matrix.org, "Security update: Synapse 0.28.1" (2018)

https://matrix.org/blog/2018/05/01/security-update-synapse-0-28-1/

The post-mortem that turned the depth argument from theory into a CVE. The depth field
"is used primarily as a way for servers to signal the intended cosmetic order of their
events." The exploit: "malicious events injected with depth = 2^63 - 1 render rooms
unusable," and "This vulnerability has already been exploited in the wild." Mitigation
was to clamp depth to [0, 2^63 - 1]; the long-term fix "involves no longer trusting
'depth' information from servers."
Verdict: CHALLENGES / cautionary for #266 and #271. Matrix's depth is the analogue of a
generation number; this is the concrete failure of trusting a monotone
distance-from-genesis value that arrives on the wire. It validates the phase 2 rule that
generations are "local, derived, never trusted from the wire," and warns the #271
generation-distance horizon that any horizon computed from a wire-supplied depth/gen is
adversarially forgeable (see Finding 11).

## Findings

1. [C4 / #274 / #267] The before(u) rule is the sufficient condition for Byzantine
   convergence, and we already state it. Kleppmann (2) and Kleppmann-Howard (3) both
   prove that convergence in a Byzantine hash-DAG holds exactly when an event's
   accept/reject and resolution decisions are a deterministic function of before(u), its
   transitive causal-predecessor closure, and never of the local delivered-set, wall
   clock, or arrival order. This is our resolution invariant word-for-word. The
   architecture is on the correct side of the theory.

2. [C4 / #266 / D3] The retrieval budget is in tension with the before(u) premise. The
   convergence proof requires before(u) to be identical on every correct node. Our
   comparison walks with a finite retrieval budget and can return BudgetExceeded, i.e.
   compute a decision over a PARTIAL before(u). If node A (deep cache, budget spare)
   resolves a StrictDescends and node B (cold cache, budget exhausted) returns
   BudgetExceeded and takes a different branch, they diverge. The phase 2 doc's own
   convergence requirement ("a policy where one node rejects what another accepts is a
   permanent partition machine") is exactly this hazard. #266 generations are the
   principled fix (staleness distance becomes deterministic, budget demotes to an anomaly
   guard); until then, any decision gated on budget outcome is not provably convergent.

3. [#274] I-confluence bounds what validated ingress can convergently enforce. BEC
   Theorem 3.1 (3) says an invariant is BEC-maintainable iff all transactions are
   I-confluent w.r.t. it. Per-property, causal-history-local checks (a delete references
   an insert in before(u); an LWW value is well-formed) are I-confluent and safe to make
   load-bearing in #274. Cross-entity uniqueness, referential integrity, and
   non-negative-quantity invariants are NOT I-confluent and cannot be enforced by any
   convergent validated-ingress rule without consensus. #274 must state this boundary
   explicitly rather than let ValidatedEvent imply "globally valid."

4. [#271 / D3] Pruning provably requires either a known replica set or an external
   truth. Two independent sources agree: Merkle-CRDTs (1) says GC "should not be
   attempted before making sure that every replica is aware of them ... requires either
   having knowledge of the current replica-set or using an external source of truth";
   BEC (3) says a message is prunable only "once every replica has delivered [it] (i.e.
   m is stable)." Ankurah's model is open-membership with dynamic peers, so global
   stability is unobservable. #271 sealed-prefix checkpoints therefore cannot be
   justified by "everyone has it"; they need an authority-scoped seal (a durable-tier
   attestation that a prefix is canonical) plus the carried genesis attestation the plan
   already anticipates. This is the load-bearing constraint on the sealing RFC.

5. [#271 / E] Merge cost scales with divergence age, which is the thrash the horizon
   targets. Merkle-CRDTs (1) explicitly flags that inclusion and merge are "costly if
   DAGs have diverged significantly (or long ago)," and cold-sync of a deep thin DAG can
   beat shipping a snapshot. This is independent corroboration of the phase 2 streaming
   concern and of the #271 rationale that a months-stale branch forces a deep re-layering
   merge. It also supports the E-workstream target "memory bounded by divergence window,
   not history."

6. [C4 / #274] Content addressing buys tamper-evidence, de-dup, cycle-freedom, and
   unforgeable happened-before; it does NOT buy authenticity or authorization. Merkle-CRDTs
   (1): nodes are "self-verified ... immune to corruption and tampering ... fetched from
   any source, trusted or not," and identical nodes de-dup by design; hash one-wayness
   makes cycles infeasible. Jacob (4): the hash reference verifiably proves happened-before.
   But Kleppmann (2) and Jacob (4) are equally clear that WHO authored an event, and
   whether they were allowed to, needs signatures on top; OrbitDB (source 11) enforces
   write permission via a signed identity and canAppend, not via the linking. #274's
   PolicyAgent surface must treat authorship/authorization as a signature concern layered
   over the content-addressed DAG, distinct from structural validity.

7. [C4 / #246 / #271] Equivocation tolerance without a finite-harm bound is a DoS gap.
   The Blocklace (5) names the exact cost we do not pay: unsigned hash-DAG CRDTs let a
   Byzantine node "pollute the CRDT state with an unbounded number of equivocations." For
   us each forged concurrent event is a genuine distinct hash, so nothing de-dups it; an
   adversary can inflate the antichain and force unbounded staging and deep/wide merges.
   Our content-hash de-dup only stops REPLAY of identical events, not FLOODING of distinct
   ones. C4's threat model must list "equivocation/concurrency flooding" as its own arm;
   the mitigations are signatures-for-attribution (blocklace-style, heavier) or
   rate/quantity caps (lighter, ties to #246 and Phase 3 backpressure).

8. [C4] Forged-parentage attacks are correctness-safe but resource-unsafe, and our
   budget is the only current shield. Kleppmann (2): a Byzantine node may add "updates
   that seem to have occurred far in the past" or "a large number of predecessor hashes";
   these cannot break correctness but can degrade performance. Matrix MSC1442 (9) notes a
   malicious deep chain as a performance attack. For us, an event citing deep or bogus
   parents forces a deep reverse walk; the retrieval budget caps it but (per Finding 2) at
   the cost of convergence, and an unresolvable parent leaves the event permanently
   staged. C4 must assert containment (per-item failure, no head advance) for the
   deep-parent and dangling-parent arms.

9. [C4] We get weak-safety causal order at f<n, not strong safety. Kshemkalyani-Misra
   (6): weak safety + liveness for broadcast is solvable for arbitrarily many Byzantine
   nodes without cryptography, but "Strong safety cannot be guaranteed without the use of
   cryptography." Our unsigned EventIds therefore give the weak-safety guarantee (correct
   nodes converge; genuine happened-before is unforgeable) but NOT strong safety across
   all events an adversary can craft. The threat model should state the guarantee at this
   precision rather than claim full Byzantine causal order.

10. [#268 / comparison] Disjoint = reject is a rejection where the causal-broadcast
    canon buffers. BEC (3) and Auvolat et al. (7) never reject on a missing or unknown
    predecessor; they buffer and re-request (needs/msgs), delivering once dependencies
    resolve. Our Disjoint (no shared history) verdict rejects. This is defensible (an
    entity is anchored at one creation event; genuinely disjoint histories are a
    different entity or an attack) but it is a DIVERGENCE risk unless the reject is a
    deterministic function of before(u): if "disjoint" is concluded from a
    budget-limited walk that merely failed to FIND the shared root, two nodes can
    disagree. The Disjoint decision must be grounded (the plan's "grounded exploration
    boundary": every genesis root and unexplored frontier inside the comparison side)
    and must not be reachable via BudgetExceeded.

11. [#266 / #267 / C4] Do not trust any wire-supplied monotone distance; keep tiebreak
    structural. Matrix's depth CVE (10) is the canonical failure of trusting a
    claimed distance-from-genesis; the fix was "stop trusting depth from servers." This
    validates the phase 2 rule that generations are local/derived and MUST NOT be read
    from the wire, and it warns the #271 generation-distance horizon that a horizon
    computed from a peer-claimed gen is forgeable. Matrix's convergent alternative
    (DAG-topology + power-level ordering, event-ID lexicographic tiebreak) is our design
    (graph facts + content-hash tiebreak); the audit action is to confirm no LWW
    provenance field or applied-set input is a trusted wire claim.

### Source 11 (real-world implementation, referenced above)

OrbitDB / ipfs-log API (https://github.com/orbitdb/ipfs-log/blob/master/API.md): the
first production Merkle-CRDT. Each entry references predecessors via a next set forming
a hash-linked DAG; heads are unreferenced entries; entries are content-addressed AND
signed by an Identity; write authorization is enforced by an AccessController.canAppend
over the signed identity, not by the linking. A joining replica reconstructs the log by
traversing backward from heads (Log.fromEntryHash) to fetch missing entries from IPFS.
Verdict: AGREES with the ingest/traversal model and confirms Finding 6: authorization is
a signature layer above the content-addressed DAG.

## Candidate design deltas

- #274: State the I-confluence boundary in the RFC. ValidatedEvent must not imply global
  validity; only I-confluent, before(u)-local invariants are convergently enforceable.
  Cross-entity uniqueness / referential integrity are out of scope for validated ingress
  and belong to the durable/consensus tier. (Source 3, Finding 3.)
- #266 before D3: Make every accept/reject and resolution decision provably independent
  of budget outcome. A verdict reachable via BudgetExceeded is not convergent; gate
  wholesale-adoption and Disjoint on grounded ancestry, not on a walk that merely ran out
  of budget. Generations are the mechanism that lets staleness distance be deterministic.
  (Sources 2, 3, 6; Findings 2, 10.)
- #271: Justify sealing by authority scope, not by global stability. Open membership
  makes "every replica has it" unobservable; a sealed prefix must be a durable-tier
  attestation carrying a genesis attestation, and the rejection horizon must be a
  deterministic function of structural facts (below-seal parentage), never of a
  wire-supplied depth/gen. (Sources 1, 3, 10; Findings 4, 11.)
- C4 threat model: add explicit arms for equivocation/concurrency flooding (unbounded
  distinct valid events; no finite-harm bound without signatures), forged/deep parentage
  (correctness-safe, resource-unsafe, budget-capped, containment asserted), and
  dangling-parent staging (permanent stage, no head advance). Decide per arm whether the
  answer is a signature/attribution layer (blocklace-style) or a rate/quantity cap
  (#246, Phase 3 backpressure). (Sources 2, 5, 9; Findings 7, 8, 9.)
- C4 / docs: State the Byzantine guarantee at its true precision: weak-safety causal
  order and SEC among correct nodes for arbitrarily many Byzantine peers, WITHOUT
  authorship, authorization, strong-safety causal order, or a DoS bound, all of which
  require a signature layer we do not currently have. (Sources 4, 5, 6; Findings 6, 9.)
- #267 audit: Confirm the LWW tournament and applied-set index read only before(u)-derived
  graph facts and content, with content-hash tiebreak, and smuggle in no arrival-order or
  trusted-wire field (the depth trap). If clean, this is a documented Byzantine-safety
  property worth asserting in the conformance kit. (Sources 2, 9, 10; Findings 1, 11.)
