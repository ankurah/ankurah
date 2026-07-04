# Topic 4: Merge semantics

Scope: this file checks phase 2's backend contract law (#267) and the concrete
LWW and Yrs merge rules against the CRDT and collaborative-editing literature
that already fought these battles. The phase 2 design under test: property
backends resolve concurrency inside a topologically-emitted layer; LWW runs a
per-property tournament using stored provenance, then causal descent, then a
content-hash tiebreak; Yrs merges CRDT updates within the layer; and resolution
may depend only on graph facts and event contents, never on wall clocks or
arrival order. The #267 conformance kit intends executable laws: round-trip,
layer permutation invariance, cross-order determinism, and provenance. The
central question this literature poses back at us is whether those laws are the
right laws, that is, whether convergence-and-determinism is a sufficient
correctness bar or whether it silently admits results that converge but violate
user intent.

## Sources

### Kleppmann, Gomes, Mulligan, Beresford, "Interleaving anomalies in collaborative text editors" (PaPoC 2019)

https://martin.kleppmann.com/papers/interleaving-papoc19.pdf

Primary source, mechanically-informed. Shows that several published list CRDTs
(Logoot, LSEQ) randomly interleave two concurrently-inserted runs of text
character by character, producing an unreadable jumble, and that RGA has a
"lesser" variant that interleaves at word granularity when insertions are made
non-sequentially (Figures 2 to 5). The decisive framing for us is in Section 1.1
and Section 2: strong eventual consistency (convergence) is necessary but "not
sufficient, as it does not capture all of the consistency properties that we
require. For example, the convergence property for a document could be met by
storing all inserted characters in lexicographic order, but this does not
represent a useful document editing system." The paper also shows Attiya et
al.'s strong list specification is itself too weak because it permits
interleaving, and adds a clause (d) to forbid it.

Verdict: CHALLENGES the #267 conformance-kit law set. Round-trip, permutation
invariance, and cross-order determinism are all satisfied by an interleaving
merge, because interleaving is a convergent, arrival-order-independent,
deterministic outcome. The laws as listed cannot detect a semantically wrong
merge; they certify convergence, not usefulness. #267 needs at least one
intent-preservation law per structured backend, or an explicit written scope
statement that intent quality is out of contract.

### Weidner, Kleppmann, "The Art of the Fugue: Minimizing Interleaving in Collaborative Text Editing" (arXiv 2305.00583; IEEE TPDS 2025)

https://arxiv.org/abs/2305.00583 (PDF: https://arxiv.org/pdf/2305.00583)

Primary source. Defines a new correctness property, maximal non-interleaving,
proves FugueMax satisfies it, and tabulates (Table 1) which existing algorithms
interleave. Three facts here bear directly on our Yrs backend. First, "it is
impossible to avoid interleaving in every situation": some concurrent-insert
configurations force interleaving under any algorithm, so no backend can promise
zero anomaly. Second, Table 1 marks Yjs (the algorithm yrs implements) as having
proven forward non-interleaving but as still exhibiting backward interleaving in
the single-replica case and unproven in the multi-replica case; the shopping-list
example in Figure 2 shows backward interleaving putting "bread" under the
"Fruit" header. Third, and sharpest: "Occurrence of interleaving is often
nondeterministic ... in some algorithms it depends on the exact order in which
concurrently sent network messages are received, and in some it depends on random
numbers."

Verdict: CHALLENGES the "resolution depends only on graph facts and event
contents, never arrival order" invariant, specifically for the Yrs backend. If
yrs inherits any Yjs behavior whose outcome depends on integration order, then
feeding it a topologically-sorted layer is not by itself enough to guarantee the
cross-order determinism law; the law must be tested against yrs directly, not
assumed from the DAG discipline. It also CHALLENGES any implicit promise that the
Yrs backend produces intent-clean merges: it inherits Yjs's documented backward
interleaving.

### Kleppmann, Gomes, Mulligan, Beresford, "OpSets: Sequential Specifications for Replicated Datatypes" (arXiv 1805.04263, Isabelle-verified)

https://martin.kleppmann.com/2018/05/11/opsets-sequential-specifications.html
(PDF: https://arxiv.org/pdf/1805.04263)

Primary source, fully mechanized in Isabelle/HOL. This is the closest structural
analogue to ankurah's merge in the literature. OpSets defines an interpretation
function that takes the set of all operations, sorts them into ascending order by
a globally-unique totally-ordered operation ID (Lamport timestamps), and folds a
pure, deterministic interpretation over that sequence: "the interpretation
function is pure, i.e. deterministic, side-effect free, and its result depends
only on O. All nodes in the system employ the same interpretation function ...
this construction trivially ensures eventual consistency." The paper is explicit
that convergence is not the goal: "Convergence is a stronger property than
eventual consistency, but it also fails to define what exactly the converged
state should be." A late-arriving operation with a lower ID is handled by
re-folding: the fold definition inserts it in ID order and recomputes downstream
state.

Verdict: AGREES with the #267 purity requirement (resolution as a pure function
of operations and their order) and with the phase 2 no-wall-clock, no-arrival-
order rule; the OpSets interpretation function is almost exactly ankurah's
"depends only on graph facts and event contents." It also AGREES that the goal
is a specified converged state, not mere convergence, which is the #267 thesis.
But it EXTENDS and mildly CHALLENGES on two points. (1) OpSets orders operations
by Lamport timestamp, which is causally consistent by construction (a cause
always sorts before its effect); ankurah's LWW tiebreak is a content hash, which
is not causally consistent on its own and is only made safe by the prior causal-
descent stage of the tournament. The lesson: the content-hash tiebreak must
provably run strictly after causal descent, never as a standalone total order,
or LWW can pick a causally-earlier write over a causally-later one. (2) OpSets
re-interprets over a single total order; ankurah's layered topological sort
preserves concurrency as layers. These are different specifications, so the
per-property tournament result must be checked against an OpSets-style total-
order oracle to confirm they agree where both apply (this is a natural addition
to the C3 conformance kit).

### Litt, Lim, Kleppmann, van Hardenberg, "Peritext: A CRDT for Collaborative Rich Text Editing" (Ink & Switch; CSCW 2022, PACMHCI)

https://www.inkandswitch.com/peritext/ (paper:
https://www.inkandswitch.com/peritext/static/cscw-publication.pdf)

Primary source with a working prototype. Peritext is the reference treatment of
merging formatting (bold, italic, color, links, comments) on top of a text
sequence. Its Example 3 is a concrete intent-violation proof: Alice bolds "The
fox" while Bob concurrently bolds "fox jumped"; the intended merge bolds the whole
sentence, but a naive markdown-or-control-character merge yields a result where
"fox" ends up non-bold, "both users bolded the word 'fox', but in the merged
result it has ended up non-bold." Peritext fixes this by keeping formatting spans
separate from the character sequence, anchoring marks to stable character
boundaries, and deriving the rendered format deterministically. Crucially for us,
Peritext resolves genuinely-incompatible marks (red versus blue on the same span)
with LWW keyed on operation ID: "the winning operation is the one with the greater
counter, or the one with the greater node ID if the counters are the same," and it
scopes each operation to one aspect via a markType so orthogonal formats never
contend.

Verdict: AGREES strongly with two phase 2 choices: per-property (per-markType)
resolution of orthogonal concerns maps directly onto the LWW per-property
tournament, and Peritext's ID-based LWW tiebreak is the same shape as ankurah's
content-hash tiebreak (a deterministic, intent-agnostic winner selection for
truly conflicting values). But it CHALLENGES the sufficiency of per-property LWW
for any property whose value is itself structured. A formatted paragraph modeled
as a single "body" property under whole-value LWW would discard one user's
concurrent formatting entirely; Peritext exists precisely because that loses
intent. The #267 backend menu therefore needs a rich-text backend with Peritext-
style span semantics, or a documented rule that structured text must live in a
CRDT backend and never in an LWW register.

### Kleppmann, "Moving Elements in List CRDTs" (PaPoC 2020)

https://martin.kleppmann.com/2020/04/27/papoc-list-move.html
(PDF: https://martin.kleppmann.com/papers/list-move-papoc20.pdf)

Primary source. States plainly that the naive move (delete then re-insert)
duplicates an element under concurrency: "if two replicas concurrently move the
same element, the result is that the element is duplicated: the two deletions ...
behave the same as one deletion, and the two insertions independently re-insert
the element twice" (Section 2, Figure 2). The proposed fix is a per-element
last-writer-wins register holding the element's position: "we can use a long-
established CRDT: a last-writer wins (LWW) register. We need one such register for
each list element, containing the position of that element ... Any deterministic
method of picking the winner is suitable (e.g. based on a priority given to each
replica, or based on the timestamp of the operations)." It also names the
multi-value register alternative for when discarding a concurrent move is
unacceptable.

Verdict: AGREES with the LWW-register model and with ankurah's deterministic
tiebreak: content-hash winner selection is a legitimate instance of "any
deterministic method." But it CHALLENGES the completeness of the current backend
set in two ways. (1) There is no move primitive: any application that expresses
reordering as insert-plus-delete on a yrs sequence inherits the duplication
anomaly, which converges and so passes every #267 law while corrupting the list.
(2) It EXTENDS the LWW question: the paper explicitly offers a multi-value
register so a concurrent move is not silently dropped, whereas ankurah's LWW
always discards the loser. For fields like position or assignee, silent LWW loss
may be the wrong default and an MV option belongs on the backend menu.

### Kleppmann, Mulligan, Gomes, Beresford, "A highly-available move operation for replicated trees" (arXiv 2021; IEEE TPDS)

https://martin.kleppmann.com/2021/10/07/crdt-tree-move-operation.html
(PDF: https://martin.kleppmann.com/papers/move-op.pdf)

Primary source with real production post-mortems and an Isabelle proof. Section 2
documents that Google Drive and Dropbox both mishandle concurrent moves: Dropbox
duplicates a concurrently-moved directory, and Google Drive can land in a
permanently-inconsistent state that requires a human to manually re-move
directories (Figure 3 shows the actual "unknown error"). The winner of two
concurrent moves is chosen by a timestamp that "need not come from a physical
clock; it could also be logical, such as a Lamport timestamp," which is LWW in
spirit. The deep result is that local validity does not compose: citing Najafzadeh
et al.'s CISE analysis, "it is not sufficient for the replica that generates a
move to check whether the operation introduces a cycle," because two moves that
are each locally cycle-free can combine into a global cycle; the algorithm needs
a do-undo-redo reinterpretation to repair this.

Verdict: EXTENDS and CHALLENGES beyond the current per-property model. ankurah's
event DAG makes cycles impossible in the history graph, but nothing prevents an
application-level cycle when a tree is encoded as per-node parent-pointer LWW
registers: two concurrent re-parent writes each win their own register and the
resulting parent graph can contain a cycle. This is the same failure Google Drive
and Dropbox exhibit. The finding for #274 (validated ingress) and the local
applied-set is that local structural validity does not imply global structural
validity for application invariants; and the finding for #267 is that a movable-
tree backend, if it is ever offered, cannot be built from independent per-property
LWW registers.

### Attiya, Burckhardt, Gotsman, Morrison, Yang, Zawirski, "Specification and Complexity of Collaborative Text Editing" (PODC 2016)

https://software.imdea.org/~gotsman/papers/editing-podc16.pdf

Primary source, proof-heavy. Gives the first precise specification of a replicated
list (the strong and weak list specifications) and then proves a lower bound: "for
a large class of list protocols, implementing either the strong or the weak list
specification requires a metadata overhead that is at least linear in the number
of elements deleted from the list." The bound is information-theoretic and holds
even under causal atomic broadcast and even for push-based protocols. The worked
example: the George W. Bush Wikipedia page shows about 500 visible lines but had
accumulated roughly 1.6 million deletions, so tombstone metadata dwarfs live
content.

Verdict: CHALLENGES the phase 2 memory and lifecycle story where a sequence CRDT
is involved. This is a hard theoretical floor: yrs cannot escape metadata growth
proportional to total historical deletions, independent of current visible size.
It confirms the #271 compaction problem is not optional polish but a correctness-
adjacent necessity for any long-lived text-bearing entity, and it bounds what the
E-workstream streaming-application optimization can achieve: divergence-window
bounding helps the reverse walk, but the CRDT's own tombstone set is bounded by
deletion history, not by the divergence window. Sealing (D3) must be able to fold
tombstone-heavy CRDT state, not just event history.

### "Local-first software: You own your data, in spite of the cloud" (Kleppmann, Wiggins, van Hardenberg, McGranaghan; Ink & Switch, 2019)

https://www.inkandswitch.com/local-first/

Primary essay, the manifesto the seed list names. It presents CRDTs as merging
concurrent edits automatically toward convergence, and is careful to claim
convergence rather than semantic correctness. Its own later postscript records the
counter-narrative that "CRDTs solve convergence but not collaboration," that "the
abstraction leaks when you need undo/redo stacks, selection state, and cursor
awareness that the CRDT doesn't model," and that automatic merge may not match
user intent so an application-level protocol is still required. The essay's
ideals that touch us are long-term preservation of data (data outlives the app)
and user control.

Verdict: AGREES with the phase 2 premise that events are the sole truth and
snapshots are caches (#273), and with the local-first framing generally. It
CHALLENGES any assumption that a converging backend closes the collaboration
problem: undo/redo and intent are explicitly out of what CRDT convergence buys,
which reinforces that #267 must state, as contract, which semantic guarantees a
backend does and does not provide rather than leaving "it converges" to imply "it
is correct."

### Kleppmann, "CRDTs: The Hard Parts" (Hydra 2020 talk)

https://martin.kleppmann.com/2020/07/06/crdt-hard-parts-hydra.html

Secondary but authoritative survey by the same author, useful as a map of the
open problems: interleaving of concurrent insertions, moving list items (why
delete-plus-reinsert fails), and metadata or tombstone overhead and whether it
can be garbage collected. It frames these as unavoidable "hard parts" that simple
implementations get wrong, and points at the primary papers cited above.

Verdict: AGREES that the phase 2 hard problems (backend contract, lifecycle
compaction) are the right hard problems to be spending #267 and #271 on;
CHALLENGES nothing new on its own but corroborates that each anomaly above is
recognized, load-bearing, and not a corner case.

### Preguica, "Conflict-free Replicated Data Types: An Overview" (arXiv 1806.10254)

https://arxiv.org/pdf/1806.10254

Secondary survey, cited for the LWW-versus-MV register distinction on which
several findings above turn. It states the standard result that the LWW register
imposes a total order at the cost of losing concurrent updates (the lost-update
problem: a value is silently overwritten by a concurrent write), whereas the
multi-value register keeps all concurrently-written values and returns the set.

Verdict: EXTENDS the #267 LWW analysis. It grounds the claim that ankurah's LWW
tournament, by design, silently drops the losing concurrent write, and that this
is a known trade rather than a bug, but one the contract must surface so
applications can choose MV semantics where silent loss is unacceptable.

## Findings

1. [#267 conformance-kit laws] Convergence, determinism, permutation invariance,
   and round-trip do not detect semantically wrong merges. Interleaving,
   move-duplication, and formatting-intent loss all converge deterministically
   and would pass every currently-listed law (interleaving paper; list-move;
   Peritext). The laws certify that replicas agree, not that they agree on
   something useful.

2. [#267 scope statement] The literature's consensus correctness bar for
   collaborative data is intent preservation, not just convergence (OpSets:
   convergence "fails to define what exactly the converged state should be";
   local-first postscript: convergence is not collaboration). #267 must either
   add intent-quality laws per structured backend or explicitly declare intent
   quality out of contract and name it as the application's responsibility.

3. [Yrs backend, cross-order determinism invariant] yrs implements Yjs, which is
   documented to interleave backward insertions and whose interleaving is, for
   some algorithms in its family, sensitive to integration order (Fugue Table 1).
   The "depends only on graph facts and event contents, never arrival order"
   invariant must be verified against yrs empirically, not inherited from the
   topological-sort discipline.

4. [LWW tiebreak ordering] A content-hash tiebreak is not causally consistent on
   its own; OpSets orders by Lamport timestamp precisely to keep cause before
   effect. ankurah is only safe because causal descent runs before the hash
   tiebreak. The tournament's stage ordering (provenance, then causal descent,
   then content hash) is load-bearing and must be an asserted law, not an
   implementation detail.

5. [LWW silent loss] LWW always discards the losing concurrent write (Preguica;
   list-move). For structured or high-stakes fields (position, assignee, a
   formatted body), this is the wrong default. The backend menu needs a
   multi-value option, and #267 must document which fields may silently lose.

6. [per-property model completeness] Independent per-property LWW registers
   cannot express two things the literature shows applications need: ordered
   sequences with a real move primitive (list-move duplication) and
   application-level structural invariants such as an acyclic tree (move-op:
   locally-safe moves compose into a global cycle). The event DAG's own acyclicity
   does not extend to application structures built from registers.

7. [Peritext concordance] Per-markType (per-property) resolution of orthogonal
   concerns and ID-based LWW winner selection for truly conflicting values are
   exactly what Peritext does; this is real independent validation of the phase 2
   per-property tournament shape for the orthogonal-and-conflicting cases.

8. [#271 lifecycle, hard floor] Sequence-CRDT metadata overhead is provably at
   least linear in total historical deletions (Attiya et al.), independent of
   visible size. Compaction of tombstone-heavy CRDT state is a necessity, not a
   nicety, and sealing (D3) must fold CRDT state, not only event history. The
   E-workstream streaming target (memory bounded by divergence window) does not
   bound the CRDT's own tombstone set.

9. [structural merge cost] Richer-than-register merge (movable tree, movable
   list) costs a do-undo-redo reinterpretation on late-arriving operations
   (move-op) and extra per-element position registers (list-move). If ankurah ever
   adds these, they cannot be free per-property backends; they carry a
   reinterpretation and metadata tax that #267 and the E benchmarks must budget.

## Candidate design deltas

- Add at least one intent-preservation law to the #267 conformance kit per
  structured backend (non-interleaving for sequences, span-intent for rich text),
  or write an explicit contract clause declaring intent quality out of scope and
  the application's responsibility. Convergence laws alone are insufficient
  (findings 1, 2).

- Make the cross-order-determinism law test yrs directly with adversarial
  integration orders, rather than asserting arrival-order independence from the
  topological-sort discipline; yrs inherits Yjs behavior that may be
  order-sensitive (finding 3).

- Promote the LWW tournament's stage ordering (provenance, then causal descent,
  then content-hash tiebreak) to an asserted law with a test that a
  causally-later write always beats a causally-earlier one regardless of hash
  order (finding 4).

- Add a multi-value register option to the backend menu and require #267 to
  document, per field, whether concurrent loss is silent (LWW) or preserved (MV);
  default high-stakes fields away from silent LWW (finding 5).

- State as an explicit non-goal, or add as explicit backends, the move primitive
  for sequences and structural invariants for trees; document that these must not
  be emulated with independent per-property LWW registers because that
  reintroduces the Google Drive and Dropbox duplication and cycle anomalies
  (finding 6).

- Fold #271 sealing to include tombstone-heavy CRDT state, not only event
  history, and set E-workstream memory expectations accordingly: the divergence
  window bounds the reverse walk but not the CRDT tombstone set, whose lower
  bound is deletion history (findings 8, 9).
