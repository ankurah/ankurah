# Topic 1: Compaction and history lifecycle

Scope: this file checks phase 2's history-lifecycle design intent (RFC #271, workstream D3) against systems that have already shipped compaction, garbage collection, forking, and shallow/grafted history. The phase 2 intent under review: sealed-prefix checkpoints that fold history below a clock C into an attested snapshot, prune beneath, clamp traversals at C, and carry a genesis attestation so Disjoint detection and entity identity survive sealing; plus the GDPR/compaction story and the concurrency rejection horizon that lives in the same RFC. Adjacent design intents touched: snapshot authority (#273, snapshot as a cached proof of a prefix), the applied-set index (#266), and the ingest/staging invariants (#268). The literature splits cleanly: CRDT log systems that refuse to drop history (Automerge, Yjs, OpSets) are a standing challenge to the very idea of pruning, while the systems that do prune (Yjs GC, Figma, git shallow, pure op-based CRDTs, event-sourcing GDPR practice) each pay a specific, documented price that tells us what our seal must carry and what it can never recover.

The single throughline across every pruning system reviewed here, from PostgreSQL's xmin horizon to Kafka's delete-retention window to git's shallow boundary to CRDT causal stability, is that the safe prune point is bounded by the slowest participant that still needs the history, and that measuring "slowest relevant peer" in ankurah's open topology (where a complete-membership view is not guaranteed) is the load-bearing unsolved problem for RFC #271, more than the seal mechanism itself.

## Sources

### Automerge binary document format specification
https://automerge.org/automerge-binary-format-spec/ (verified)

The normative spec for Automerge's on-disk and on-wire format. It mandates complete history: "A document always contains a complete history of changes: for each change in the document, all the changes that were made to the document before that change was made are also included." Changes are columnar-compressed with run-length encoding; each change's id is the SHA-256 hash of its chunk, document heads are stored as head hashes, and an implementation "MUST abort if the hashes don't match" after topological traversal. This is content addressing structurally identical to ours (EventId as SHA-256 of entity id, operations, and parent clock), which matters because it means Automerge's refusal to compact is a design choice, not a hash-chain limitation.

Verdict: CHALLENGES #271. A mature, content-addressed CRDT with the same hashing model as ours deliberately keeps full history and offers no in-format way to seal a prefix. Our sealing intent must justify departing from the format that the closest comparable system treats as inviolable.

### Automerge 2.0 and 3.0 blog posts (runtime compaction is lossless)
https://automerge.org/blog/automerge-3/ (verified)

Automerge 3.0 re-architected the runtime to use the on-disk columnar compression format in memory, cutting memory usage over 10x (pasting Moby Dick dropped from 700MB in v2 to 1.3MB in v3). Crucially the word "compaction" here means lossless re-encoding of the same changes into a denser representation, never dropping changes. This is the industry's answer to "history is too big": compress the representation, do not truncate the graph.

Verdict: CHALLENGES #271. The leading production system solved the size problem that motivates sealing by compression alone, without a horizon. Before we adopt structural pruning we should be able to state why columnar/RLE compression of the event log is insufficient for our workload.

### Automerge Repo concurrent compaction (Alex Good / patternist.xyz)
https://patternist.xyz/posts/concurrent-compaction-in-automerge-repo/ (verified)

Describes Automerge Repo's storage compaction: incremental changes accumulate at `<doc>/incremental/<hash>` keys until their size exceeds the snapshot size, then a snapshot is written at `<doc>/snapshots/<hash-of-heads>` and the incrementals are deleted. The correctness hazard: if two instances each read state, compact what they saw, and write in different orders, the later write clobbers the earlier, losing events. The fix is to write to uniquely-named `compressed/<hash>` keys (never a single mutable key) and to delete originals only after confirming the new write succeeded. Compaction is explicitly lossless: it "repack[s] the same changes."

Verdict: EXTENDS #271 and #268. Even lossless compaction has a write-ordering correctness hazard identical in shape to our ingest staging rule (stage/commit before any head or snapshot references the new state). Our seal-write must obey the same discipline: never overwrite a mutable seal key, delete pruned events only after the seal is durably committed and referenced.

### Yjs internals: garbage collection and the delete set
https://github.com/yjs/yjs/blob/main/INTERNALS.md (verified)

Yjs GC, when enabled, replaces a deleted item's content with a lightweight `GC` struct that "only stores the length of the removed content," while a separate delete set retains all deleted item ids (clientID, clock ranges). No data is kept on when or by whom an item was deleted. The fundamental constraint stated in the Yjs README: Yjs cannot garbage collect tombstones while it must still guarantee a unique total order of structs; it can only fully drop a struct's identity once "it doesn't care about the order of the structs anymore" (for example when the parent is deleted).

Verdict: AGREES with #271's genesis-attestation requirement, and CHALLENGES a naive seal. Yjs proves you can discard content while retaining just enough id-range metadata to keep sync and ordering correct: this is exactly the shape of a genesis attestation (keep identity anchors, drop payload). But it also shows the metadata floor is not zero: the (client, clock-range) skeleton must survive or peers desync. Our seal cannot be a bare snapshot; it must retain the id skeleton needed to answer causal-comparison probes from peers who are behind the seal.

### Yjs: why you cannot delete a document (Liveblocks guide)
https://liveblocks.io/docs/guides/why-you-cant-delete-yjs-documents (verified)

Product-level consequence of the above. Because Yjs stores a complete change history and deletions add tombstones, "the document data size has got larger instead of smaller" after a delete, and there is no safe way to selectively prune history while keeping CRDT guarantees. The only supported "reset" is to abandon the document/room, create a fresh one, and manually copy visible content across, which loses all history and breaks sync for any peer that was offline at reset time.

Verdict: CHALLENGES #271 and #273. The escape hatch that real deployments actually use is a hard fork (new identity, copy live state) precisely because in-place sealing is unsafe there. Two warnings for us: (1) any seal that changes what stale offline peers can reconcile against is observably a partition event for those peers, and (2) if our seal keeps entity identity stable (which #271 wants, unlike Yjs's fork), we inherit the obligation to still answer those stale peers, which Yjs sidesteps by changing identity.

### Chronofold: a data structure for versioned text (Grishchenko, Patrakeev, PaPoC 2020)
https://arxiv.org/abs/2002.09511 and HTML at https://ar5iv.labs.arxiv.org/html/2002.09511 (both verified; DOI 10.1145/3380787.3393680)

Chronofold is a subjectively ordered append-only log of tuples over the Causal Tree/RGA model. Log timestamps are pairs (n, alpha) where the lexicographic order is compatible with causal order, and each replica keeps a causally closed log: "if s subseteq t and t in LOG_i(alpha), then s in LOG_i(alpha)." Most relevant to us, the paper gives an explicit rebase: "If a history of a document is discarded entirely, then the text is represented as a single sequential insertion, with no tombstones." That is a total prefix seal that collapses history into a fresh linear state and eliminates tombstones, permitted only when the history is no longer needed for synchronization.

Verdict: EXTENDS #271, and supplies the strongest structural precedent for sealing. Chronofold's rebase is our sealed-prefix checkpoint in miniature: fold everything below a point into one collapsed state, drop tombstones. Two transferable constraints: the seal is only safe once no peer needs the discarded history (a stability precondition, see causal stability below), and the retained structure must stay causally closed, which validates our "clamp traversals at C" rule (never expose a dangling parent below the seal).

### Pure operation-based CRDTs and causal stability (Baquero, Almeida, Shoker; Sypytkowski writeup)
https://www.bartoszsypytkowski.com/pure-operation-based-crdts/ (verified; based on Baquero et al., "Making Operation-Based CRDTs Operation-Based" / "Pure Operation-Based Replicated Data Types")

Defines causal stability via a stable timestamp: fold the minimum over the most recent vector clocks received from every replica (a matrix clock), yielding the point in logical time seen by everyone. An operation is causally stable once no operation still in flight could be concurrent with it (its version is <= the stable timestamp). Once stable, the operation and its per-operation metadata can be folded into a compacted stable state and dropped from the log, "because there are no events concurrent to a stable vector version."

Verdict: EXTENDS and partially CHALLENGES #271. This is the theoretical backbone of when a prefix is safe to seal: not "old enough" but "stable," meaning provably behind every replica's frontier. It challenges any seal keyed purely on generation distance or wall clock (both proposed under #271's rejection horizon), because those can seal a prefix that a lagging replica still holds concurrent operations against, which is the exact thrash/partition failure the horizon is meant to avoid. The catch it imposes: causal stability requires knowing the full membership and each member's frontier (a matrix clock over all replicas), which ankurah's open, partially connected topology does not naturally have.

### git shallow clone: the shallow file and grafts (git-scm docs)
https://git-scm.com/docs/shallow (verified)

The `$GIT_DIR/shallow` file lists commit ids that git treats as roots (parents cauterized away). It is implemented as internal grafts with `nr_parent < 0`, distinct from user grafts; traversal stops at these commits and `git fsck` is taught not to complain about the missing parents. Deepening rewrites the shallow file (the special depth 0x7fffffff means unshallow to a full repo). The objects still exist upstream; the client simply cannot see past the boundary.

Verdict: AGREES with #271's "clamp traversals at C." git's shallow boundary is precisely a per-graph seal line: a set of ids beyond which traversal is defined to stop, with the objects hidden rather than fabricated. It validates the mechanism (a stored frontier list that the traversal engine honors) and the invariant that the seal must be internally consistent so integrity checks (our Disjoint/genesis detection, git's fsck) do not false-alarm on the cauterized parents.

### git shallow: the boundary cost and unreliable history (GitHub engineering blog)
https://github.blog/open-source/git/get-up-to-speed-with-partial-clone-and-shallow-clone/ (verified)

The canonical warning. If someone starts a topic branch below the shallow boundary and it gets merged into the default branch, "the server needs to walk the full history and serve the client what amounts to almost a full clone," and it must do so "without the advantage of performance features like reachability bitmaps." Also: "Since the commit history is truncated, commands such as `git merge-base` or `git log` show different results than they would in a full clone." GitHub concludes it does "not recommend shallow clones except for builds that delete the repository immediately afterwards."

Verdict: CHALLENGES #271's rejection-horizon assumption, hard. This is the DoS/thrash failure mode written down by the largest git host: a stale branch rooted below the seal does not fail cheaply; it forces a full re-materialization on the party that sealed. It maps directly to #271's own stated worry ("a months-stale branch can force a deep re-layering merge"). The lesson is that a seal without an explicit, deterministic policy for "an event whose parents fall below the seal" does not remove the deep-walk cost; it relocates and can amplify it. It also warns that once sealed, causal-comparison verdicts computed near the boundary (our meet, StrictDescends grounding) can differ from what a full graph would return, exactly as git's merge-base does.

### Kafka log compaction, tombstone retention, and the zombie/resurrection problem
https://docs.confluent.io/kafka/design/log_compaction.html (verified) and analysis at https://javierholguera.com/2020/02/17/kafka-quirks-tombstones-that-refuse-to-disappear/ (verified)

Kafka compaction keeps the last value per key; a null-payload record is a tombstone (a delete). Tombstones are not removed immediately: `delete.retention.ms` (default one day) keeps them so that a lagging or replaying consumer has "a grace period to observe deletion markers before they are permanently removed." The documented failure when this window is too short is the zombie/resurrection problem: a leader compacts away a value, deletes the tombstone after the window, and an offline replica that never saw the tombstone rejoins (or is elected leader) still holding the old value, so replicas "permanently disagree about what is in the log," and which version a consumer sees depends on which broker is leader at read time. Each broker compacts its own log independently, which is what makes the divergence possible.

Verdict: CHALLENGES #271's rejection horizon and #266 applied-set. This is a production, deterministic-looking system whose compaction is NOT convergent across replicas because pruning is per-node and time-bounded rather than stability-bounded: precisely the "one node rejects/prunes what another keeps is a permanent partition machine" hazard the phase 2 staleness-horizon requirement names. It argues that any ankurah seal or horizon keyed on a time or count window inherits the zombie class unless the prune point is a deterministic function all nodes compute identically, and that the safe retention floor is "until every relevant peer has observed the newer state," not a fixed duration.

### PostgreSQL MVCC VACUUM and the xmin horizon
https://www.postgresql.org/docs/current/routine-vacuuming.html (verified) and https://pganalyze.com/blog/5mins-postgres-autovacuum-dead-tuples-not-yet-removable-postgres-xmin-horizon (verified)

VACUUM reclaims dead tuples (old row versions left by MVCC UPDATE/DELETE), but "a tuple becomes dead only when PostgreSQL knows that no active snapshot needs it." The xmin horizon is the oldest transaction id any live snapshot might still read; VACUUM cannot remove anything newer than that horizon. A single long-running or idle-in-transaction session (or a lagging replication slot with `hot_standby_feedback`) holds the horizon back and blocks cleanup database-wide, causing bloat. Oracle's analogous shortfall surfaces as the "ORA-01555 snapshot too old" error when old versions needed by a still-open reader were already reclaimed.

Verdict: AGREES with #271 (via causal stability) and EXTENDS the rejection-horizon design. The xmin horizon is the database-internals name for exactly the seal precondition finding 2 argues: you may reclaim a prefix only up to the oldest reader that still needs it. It confirms two things for us: (1) the safe seal point is dynamic and dictated by the slowest relevant participant (our lagging peer is their long transaction / replication slot), and (2) the failure of reclaiming past that point is "snapshot too old," a read that can no longer be served, which is exactly the case #273 must handle by degrading a stale snapshot to a needs-events request rather than returning wrong data.

### OpSets: Sequential Specifications for Replicated Datatypes (Kleppmann, Gomes, Mulligan, Beresford, 2018)
https://arxiv.org/abs/1805.04263 (extended version; verified) and AFP entry https://www.isa-afp.org/entries/OpSets.html (verified)

An OpSet is a set of (id, operation) pairs with globally unique ids drawn from a totally ordered set, where the order is a linear extension consistent with causality; document state is the deterministic result of interpreting operations in id order. The framework is Isabelle-mechanized. State is defined over the entire operation set; the model has no notion of removing an operation, and correctness (convergence) rests on every replica interpreting the same set.

Verdict: CHALLENGES #271 at the semantics level. Our merge (generalized topo sort from the meet, layered resolution) is an OpSet-style deterministic fold over graph facts. OpSets defines state as a function of the whole set, so sealing changes the mathematical object being interpreted: after a seal, two nodes that sealed at different points hold different operation sets and the convergence proof no longer applies unmodified. This is the sharpest formal reason the seal must be a deterministic, node-agnostic function of graph facts (which #271 asserts) rather than a local policy, and it argues the sealed snapshot must be treated as itself a synthetic operation with a content-addressed id so that "interpreting the same set" still holds post-seal.

### GDPR erasure in append-only/event-sourced systems (crypto-shredding)
https://event-driven.io/en/gdpr_in_event_driven_architecture/ (verified) and AxonIQ GDPR module https://www.prnewswire.com/news-releases/axoniq-delivers-gdpr-module-to-enable-mandatory-erasure-of-data-in-immutable-event-driven-systems-657197853.html (verified)

The industry-standard answer to "delete from an immutable log": do not delete events. Encrypt each data subject's PII under a per-subject key held outside the log; to erase, destroy the key, leaving ciphertext that is "mathematically equivalent to random noise" while hashes, Merkle inclusion, and integrity proofs (computed over ciphertext) stay intact. Secondary patterns: log compaction with a PII-free tombstone event that retains the stream id; forgettable-payload references; never embed PII in stream/topic names (they survive tombstones). The recommendation is to combine crypto-shredding with retention policies.

Verdict: EXTENDS #271's GDPR/compaction story, and CHALLENGES the assumption that sealing is the GDPR mechanism. Crypto-shredding erases content without touching the causal/hash structure, which is a much cheaper path to erasure than folding-and-pruning and, critically, does not perturb causal comparison or convergence at all. For ankurah, whose EventId already hashes operations, sealing a prefix to satisfy erasure would break exactly the hash-chain that crypto-shredding is designed to preserve. This suggests separating the two goals RFC #271 currently bundles: erasure via per-field crypto-shredding, compaction/size via sealing, on independent mechanisms.

### Figma multiplayer technology (deleted-object handling, no full CRDT)
https://www.figma.com/blog/how-figmas-multiplayer-technology-works/ (verified)

Figma "doesn't store any properties of deleted objects on the server; that data is instead stored in the undo buffer of the client that performed the delete," specifically "to keep long-lived documents from continuing to grow in size as they are edited." Figma deliberately does not use true CRDTs because the server is the central authority, so last-writer-wins on the server suffices and avoids CRDT metadata overhead. Checkpoints are recorded to version history (every 30 minutes).

Verdict: EXTENDS #271 and informs #273. Figma shows a fourth model: push deleted-state retention to the party that can undo it (the originating client), so the durable server state never accumulates tombstones. This is only available to them because a single authority linearizes writes. It is a useful contrast for our snapshot-authority rule (#273): a central authority can prune aggressively because it defines truth; ankurah's snapshots are caches that must degrade to needs-events, so we cannot prune as freely as Figma without the snapshot-as-proof fallback #273 specifies.

## Findings

1. [seal mechanism] A per-graph stored frontier that the traversal engine honors, with objects hidden rather than removed, is a proven mechanism: git's shallow file and internal grafts implement exactly "clamp traversals at C." Adopt the shape; the seal is a list of ids beyond which comparison stops, plus a durable snapshot, not a rewrite of event objects.

2. [seal safety precondition] Chronofold's rebase and pure op-based causal stability agree that collapsing a prefix is safe only when no peer holds operations concurrent with the sealed frontier. "Old enough" (generation distance or wall clock) is not the same as "stable" (behind every frontier), and sealing on age can seal away history a lagging peer still needs.

3. [rejection horizon is a relocation, not an elimination, of cost] GitHub's shallow-clone warning is the empirical proof of #271's own worry: a branch rooted below the seal forces the sealing party into a near-full re-materialization, without bitmap acceleration. A seal with no explicit policy for sub-seal parents does not make deep walks cheap; it moves and can amplify them. The horizon policy must be chosen as deliberately as the seal itself.

4. [metadata floor] Yjs proves content can be dropped while a (client, clock-range) id skeleton must survive for sync/ordering correctness. Our genesis attestation is necessary but a bare snapshot is insufficient: the seal must retain enough id skeleton to answer causal-comparison probes from peers who are behind it, or those peers desync.

5. [convergence semantics] Under OpSets, state is a deterministic function of the whole operation set; sealing changes that set per node. To preserve convergence, the sealed snapshot must itself be a content-addressed synthetic event (deterministic function of graph facts, node-agnostic), so "all nodes interpret the same set" still holds after the seal. A locally-chosen seal point that leaks into identity breaks the convergence argument.

6. [identity survival is a cost, not a freebie] Yjs and Automerge's real-world "reset" is a hard fork with new identity precisely because keeping identity stable across a prune obligates you to keep serving stale peers. #271 wants identity to survive sealing (correctly, for the app model), so we must own the resulting obligation: a sealed entity must still resolve Disjoint/genesis queries and degrade gracefully for peers stranded below the seal.

7. [erasure and compaction are different problems] Event-sourcing practice erases via crypto-shredding (destroy a per-subject key, leave hashes intact), not via pruning, exactly because pruning breaks the hash chain. #271 currently bundles GDPR and compaction; the literature says split them: crypto-shred for erasure (preserves causal comparison), seal for size.

8. [compaction has a write-ordering hazard] Automerge Repo's concurrent-compaction bug (later writer clobbers earlier compaction, losing events; fixed via unique `compressed/<hash>` keys and delete-after-confirm) is the same class as our ingest staging rule. The seal write must never overwrite a mutable key and must delete pruned events only after the seal is durably committed and referenced, mirroring #268/#273.

9. [compression may beat pruning for the size goal] Automerge 3.0 solved the history-size problem with lossless columnar/RLE compression in memory and on disk, no horizon. If our motivation for sealing is primarily storage, we owe a comparison against just compressing the event log before accepting the correctness cost of structural pruning.

10. [convergence of the prune point is the whole ballgame] Kafka's zombie problem shows that per-node, time-windowed compaction is not convergent: two brokers compacting independently can permanently disagree, and the observed value depends on who is leader. This is the concrete instantiation of the phase 2 requirement that "a policy where one node rejects what another accepts is a permanent partition machine." Any ankurah horizon or seal must compute its prune/reject point as a deterministic function of shared graph facts that every node evaluates identically, never a local timer.

11. [the horizon is the oldest reader, named twice] Independently, git's shallow boundary cost, pure op-based causal stability, Kafka's delete.retention window, and PostgreSQL's xmin horizon all converge on one rule: the safe reclaim/seal point is bounded by the slowest participant that still needs the history. Four different subfields reinventing the same constraint is strong evidence it is not optional. The design question for #271 is not whether this horizon exists but how ankurah measures "slowest relevant peer" in an open topology, since unlike Postgres (one xmin) or pure op-based CRDTs (a matrix clock over known members) we may not have a complete membership view.

12. [pruning past the horizon is a read that must fail loudly] Oracle's ORA-01555 "snapshot too old" is the canonical outcome of reclaiming history a live reader still needs: the read is refused rather than answered wrong. This is the exact contract #273 assigns to a stale snapshot (degrade to needs-events, never serve incorrect state). The lifecycle RFC and the snapshot-authority RFC therefore share a single failure mode and should specify one shared "cannot serve below the seal, request events" path.

## Candidate design deltas

- Split RFC #271 into two mechanisms: crypto-shredding for GDPR erasure (per-field key destruction, hash chain untouched) and sealed-prefix checkpoints for size. The literature (event-sourcing GDPR practice vs Automerge/Yjs) shows these have opposite relationships to the hash chain and should not share one lever. Why: sealing to erase breaks content addressing; crypto-shredding to save space does not.

- Make the seal precondition causal stability (provably behind every reachable peer frontier), not generation distance or wall clock alone. Why: Chronofold and pure op-based CRDTs both gate prefix collapse on stability; age-based seals can prune history a lagging peer still needs, causing the resurrection/partition class.

- Treat the rejection-horizon choice as a first-class, separately justified decision, and default toward NOT sealing while any peer may be below the boundary. Why: GitHub's documented shallow-boundary cost shows a sub-seal branch forces near-full re-materialization; the horizon relocates the deep-walk cost rather than removing it, so "budget as the only backstop" (an allowed #271 outcome) may be safer than an aggressive structural horizon.

- Require the sealed snapshot to be a content-addressed synthetic event (deterministic, node-agnostic function of graph facts) and to retain a (source, clock-range) id skeleton below the seal. Why: OpSets convergence requires all nodes to interpret the same set, and Yjs shows the id skeleton is the irreducible metadata floor for answering behind-the-seal comparison probes.

- Before adopting structural pruning at all, benchmark lossless columnar/RLE compression of the event log against sealing for the storage target (feeds workstream E). Why: Automerge 3.0 hit 10x to 100x reductions by compression alone with zero correctness cost; sealing should only be adopted where compression demonstrably does not suffice.

- Adopt Automerge Repo's compaction write discipline for the seal write path: unique seal keys, delete pruned events only after the seal is durably committed and referenced by heads/snapshots. Why: the concurrent-compaction data-loss bug is the same ordering hazard #268/#273 already guard for ingest; the seal must inherit those invariants.

- Forbid any horizon or seal keyed on a wall-clock or fixed-count window; require the prune point to be a pure function of shared graph facts that every node computes identically. Why: Kafka's zombie problem is the documented result of per-node, time-windowed compaction, and it is exactly the non-convergent-decision failure the phase 2 staleness requirement prohibits.

- Specify a single "cannot serve below the seal" path shared by #271 and #273 that degrades to a needs-events request. Why: Oracle's ORA-01555 and Postgres's xmin horizon show that reclaiming past the oldest reader must surface as a refused read, which is precisely #273's snapshot-as-proof fallback; do not let the two RFCs invent separate handling.

- Define, as an explicit open problem for the RFC, how ankurah estimates the "slowest relevant peer" horizon without the complete-membership view that Postgres (xmin) and pure op-based CRDTs (matrix clock) assume. Why: every prior system that prunes safely relies on knowing its readers; ankurah's open, partially connected topology does not, so the horizon measurement is the load-bearing unsolved piece, not the seal mechanism.
