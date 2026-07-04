# Topic 2: Indexed causality

Scope: this file checks phase 2's intended indexed-causality design (RFC #266) against
the systems that already built persistent, derived indexes over a content-addressed or
DAG-shaped history. RFC #266 proposes a persistent per-event `generation: u32` (1 + max
parent generation; 1 for genesis), stored in a per-engine sidecar, recomputed on ingest
and never trusted from the wire, used to (a) short-circuit StrictAscends/StrictDescends
verdicts with an O(heads) order precheck, (b) order the backward-BFS frontier by
descending generation, (c) give meet-search a sound stop condition, and (d) demote the
retrieval budget from a load-bearing bound to an anomaly guard. Workstream D2 layers a
per-entity applied-set membership index on top of generations so redelivery and gap
detection become set operations. The literature that matters most here is git's
commit-graph (which took exactly this maturation step, twice, and paid twice), Mercurial's
linkrev (a derived cross-reference that is famously non-monotone), the reachability-index
research corpus (GRAIL, tree-cover vs 2-hop, dynamic maintenance) that #266 waves off as
"overkill," and git's reachability bitmaps (the positive-membership index that is the
closest analog to the applied-set). The valuable output is the CHALLENGES cluster around
what goes wrong when a persisted derived index silently disagrees with the graph it
indexes.

## Sources

### 1. Git commit-graph: generation numbers, corrected commit dates (design notes)

Primary: <https://www.kernel.org/pub/software/scm/git/docs/technical/commit-graph.html> and
the GitHub engineering write-up
<https://github.blog/open-source/git/gits-database-internals-ii-commit-history-queries/>.
Git's commit-graph stores a per-commit generation number that is a "negative reachability
index": the formal property is "If A and B are commits with generation numbers N and M,
respectively, and N less-than-or-equal M, then A cannot reach B," and for mixed graphs git
weakens this to strict "N less-than M" so unindexed commits (sentinel INFINITY or ZERO)
stay correct at the cost of walking a few extra commits. Generation number v1 is the
topological level (1 at roots, else 1 + max parent level); the cutoff "we can stop
exploring a commit if its generation number is below the generation number of our target"
turns `git tag --contains` from 7.34s to 0.04s and `merge-base --is-ancestor` from 2.64s
to 0.02s on the Linux kernel. Crucially git leaves generation-number *equality* as an
"unknown state" that still requires a walk.

Relevance: AGREES with #266. Ankurah's `generation = 1 + max(parent generations)` is
git's topological level exactly, the O(heads) precheck is git's cutoff, and the
descending-generation frontier is git's ordering. The design's claim that generations turn
"most comparisons into cheap monotone checks" is the measured git result. Note for D2: git
proves generations alone cannot decide the equal-generation case, so ankurah's precheck
must fall through to the full walk on ties rather than guess, and the budget-as-anomaly-
guard framing is correct only because the walk still runs when the index is inconclusive.

### 2. Git generation number v1 was insufficient: the "old commit" pathology and v2

Primary: format spec <https://git-scm.com/docs/commit-graph-format>; design context in
source 1 and <https://devblogs.microsoft.com/devops/supercharging-the-git-commit-graph-iii-generations/>.
Topological level "fails when developers create commits based on ancient code": merging a
bugfix built on an old codebase gives the merge an artificially small level, far below its
true recency, so the cutoff prunes almost nothing and reachability walks degrade toward the
unindexed cost. Git's fix (generation number v2, corrected commit date) sets a root's value
to its commit date and otherwise `max(commit_date, 1 + max parent corrected date)`,
restoring a tight monotone bound while preserving the reachability property. Git deliberately
did not use raw commit dates because "not every computer has a synchronized clock," so a
skewed timestamp could violate the monotonicity the cutoff depends on; the +1 correction
launders clock skew into a sound generation.

Relevance: CHALLENGES / EXTENDS #266. #266 names "git commit-graph generation numbers
(corrected commit date v2)" as prior art but the *proposal body* defines only the v1
topological level. This is a latent gap: ankurah's `1 + max(parent generation)` is
literally v1, and it inherits v1's pathology. A months-stale offline branch that reattaches
to old parents is ankurah's exact analog of "a commit based on ancient code": its
generation will be small, the precheck's pruning bound will be loose, and the very
deep-re-layering thrash that #266 and D3 want generations to bound is where v1 helps least.
Ankurah has no wall clocks it trusts (by design), so the literal v2 corrected-date fix is
unavailable; the honest position is that ankurah's generation is v1, with v1's limits, and
the rejection-horizon work (D3) cannot lean on generation distance being tight in the
adversarial-staleness case.

### 3. Git commit-graph corruption: v1-to-v2 upgrade underflow (post-mortem)

Primary: <https://lorenz.brun.one/repairing-git-commit-graphs/>; git tracker/GitLab issue
<https://gitlab.com/gitlab-org/gitaly/-/issues/4327>. A real GitLab instance hit "fatal:
commit-graph requires overflow generation data but has none," which broke `git log` and
even `git fsck`. Root cause: a bug upgrading commit-graph data from generation v1 to v2
caused an underflow that "flipped the overflow flag, rendering the commit-graph unreadable"
(fixed in git 9550f6c16a, v2.37.2). The corrupted derived index also "interfered with its
own replacement": regenerating the graph failed until the operator deleted
`.git/objects/info/commit-graph*` by hand. The corruption spread through normal operation
because the commit-graph is generated and shipped as part of housekeeping and fetch.

Relevance: CHALLENGES #266's migration/storage plan. #266 calls migration risk "low
because the data is derived and rebuildable," and in the steady state that is true. This
post-mortem shows the failure mode that risk assessment omits: a *format* or *offset* bug in
the persisted representation can make the index un-parseable, break unrelated operations,
and block its own regeneration. #266's "lazy backfill migration: compute on first touch"
plus "sidecar column/keyspace per storage engine" is three independent encodings (sled tree,
postgres column, IndexedDB store) of a bit-packed derived value, which is precisely the
surface where git got bitten. Design delta below: a cheap self-check ("does stored
generation equal 1 + max parent generation?") and a "delete-and-rebuild" recovery path are
not optional polish, they are the thing that turned a git outage into a one-line fix.

### 4. Git generation number v2 overflow bug (primary patch)

Primary: Derrick Stolee, "commit-graph: fix generation number v2 overflow values,"
<https://patchwork.kernel.org/project/git/patch/193217c71e0aaf3f56a02d9abec6753bd19aba71.1646056423.git.gitgitgadget@gmail.com/>.
Corrected commit dates are stored as 31-bit offsets from the commit's own date with the MSB
as an overflow flag pointing into a separate 64-bit overflow chunk. The bug: the writer
"writes the offsets to the chunk (as dictated by the format) but fill_commit_graph_info()
treats the value in the chunk as if it is the full corrected commit date (not an offset)."
It stayed hidden under normal timestamps and only surfaced with a `FUTURE_DATE` above four
billion, i.e. once offsets needed more than 31 bits. The fix adds the base date back:
`generation = item->date + get_be64(overflow + 8*pos)`.

Relevance: CHALLENGES #266's `u32` sizing decision. #266 picks a bare `u32` generation with
no discussion of overflow, offsets, or the width of "1 + max." A `u32` counting generations
(not seconds) will not overflow for any realistic entity, so ankurah is likely safe where git
was not, but the lesson generalizes: the moment a derived index is *persisted* in a
bit-packed on-disk form, its representation acquires edge cases (overflow, sign, base
offsets) that pure recomputation does not have, and those edge cases fail silently until a
boundary input hits them. Recommend an explicit note in the RFC that generation is a small
unsigned count with a documented saturating behavior, plus a property-test arm that drives
pathologically deep chains.

### 5. Git commit-graph mixed-version corruption; GitLab disabled corrected dates (post-mortem)

Primary: Gitaly MR "Fix commit-graph corruption caused by corrected committer dates,"
<https://gitlab.com/gitlab-org/gitaly/-/merge_requests/4677>. At scale, GitLab saw
"corruption in commit-graphs that seemingly happens when running with a mixture of
commit-graphs generated by Git v2.35.0 or earlier and Git v2.36.0 and later." The v2.36
read-side fix for corrected committer dates surfaced pre-existing bad data. GitLab's team:
"Multiple attempts to find the root cause of this corruption have failed until now and
nobody has yet been able to find a reproducer," so they *disabled corrected committer dates
entirely* and reverted to topological generation numbers, which "still provided query
optimization without triggering the corruption bug."

Relevance: CHALLENGES #266's confidence in generations, EXTENDS the D2 story. A major
operator could not root-cause a generation-index corruption and chose to turn the fancier
generation scheme off, keeping the simpler one. Two takeaways for ankurah. First, the split
commit-graph format (per commit-graph-format spec) already had to add the rule that git uses
corrected dates only if the *topmost* layer carries them, otherwise it falls back to
topological level, because mixing versions of a derived index is a live hazard even inside
one tool; ankurah's three storage engines are that mixing surface. Second, "revert to the
simpler index" was a viable production escape hatch precisely because the negative property
holds for both v1 and v2, so a node can always fall back to recomputation. #266 should state
that the applied-set/generation index must be *disableable at runtime* with a pure-walk
fallback, so a suspected-bad index is a config flip, not an outage.

### 6. Mercurial linkrev and crossed linkrevs: a derived cross-reference that is non-monotone

Primary: <https://wiki.mercurial-scm.org/CrossedLinkrevs>; revlog background at
<https://www.mercurial-scm.org/wiki/Revlog>. Each filelog revision stores a `linkrev`, a
derived pointer to the changeset that introduced it. Because "filerevs created with the same
content and same parents have the same hash," identical content dedups to one filelog rev
whose linkrev can point to a changeset other than the one currently in view, and pull "will
fix up linkrevs" so "an old file revision can be fixed up to point to a new cset" and "the
next linkrev will probably 'cross' it by pointing to an earlier revision." The load-bearing
warning: "you cannot for instance assume that if you find a filelog with linkrev x, you have
found all filelogs with linkrev less-than x." The invariant that *is* preserved is only
topological: "the filelog graph itself will continue to be topologically sorted ... as will
the changelog graph, but the orderings may be different." Mercurial carries `adjustlinkrev`
logic to repair the stored value when it is not valid in the current traversal context.

Relevance: CHALLENGES #266, and the sharpest analogy in this file. Content addressing is the
mechanism that creates crossed linkrevs (identical content, one node, one stored index
value), and ankurah's EventId is a content hash of exactly (entity id, operations, parent
clock). Two events that are byte-identical except for provenance would collide; the design's
inclusion of the *parent clock* in the hash is what prevents this, so ankurah is protected,
but only because of that specific hashing choice. The transferable hazard is subtler and
directly threatens the *applied-set index*: a derived per-entity index value can be correct
with respect to one traversal and stale/wrong with respect to another. Mercurial's rule
"you cannot assume linkrev < x enumerates a prefix" is exactly the assumption a naive
applied-set gap detector ("everything with generation below the head's is present") would
make. #266 must specify that the applied-set is a set of ids, not a generation range, and
that gap detection is true set difference, never a `generation < g` interval test, or it
reproduces the crossed-linkrev bug class.

### 7. GRAIL: randomized-interval reachability labeling (the rejected alternative)

Primary: Yildirim, Chaoji, Zaki, "GRAIL: Scalable Reachability Index for Large Graphs,"
VLDB 2010, PDF <http://www.cs.rpi.edu/~zaki/PaperDir/VLDB10.pdf> (DOI 10.14778/1920841.1920879).
GRAIL (Graph Reachability via RAndomized Interval Labeling) assigns each node `d`
min-post intervals from `d` random DFS traversals. The property is asymmetric and negative:
"Lv not-subset-of Lu implies u cannot reach v," but interval containment is necessary, not
sufficient, so on a positive containment GRAIL falls through to a guided DFS that itself uses
the intervals to prune ("when the DFS visits vertex v, the intervals of v and t can be used
to check whether t is not reachable from v"). Construction is linear, O(d(n+m)) time and
O(dn) space, and GRAIL adds a topological-level filter as an extra negative cut, the same
device git uses.

Relevance: EXTENDS #266's "alternatives considered." #266 rejects "interval/reachability
labeling (GRAIL-style)" as "stronger queries, but labels are invalidated by concurrent
inserts and are overkill." That rejection is defensible but the stated reasoning
undersells the actual relationship. GRAIL is *the same shape* as generations: a negative
filter that must fall through to a real walk on the inconclusive case, plus a topological
level as backup. What GRAIL buys over a single generation is a *tighter* negative filter
(multiple random intervals catch more non-reachable pairs than one scalar), at the cost of
maintenance. For ankurah's access pattern (compare two clocks' antichains, find the meet)
a scalar generation plus the parent DAG is genuinely sufficient, so "overkill" is fair, but
the RFC would be stronger if it said *why* the query shape does not need the extra
filtering power (meet-finding needs the parent edges anyway, so the walk is unavoidable and
a tighter negative filter only trims constant factors) rather than resting on "invalidated
by inserts," which as source 8 shows is not uniformly true.

### 8. Dynamic reachability maintenance: DAGGER, 2-hop labeling, and the "no dominator" survey

Primary: reachability-index survey (ACM Computing Surveys 2024, DOI 10.1145/3776737),
freely readable at <https://arxiv.org/html/2311.03542v2>; DAGGER, Yildirim/Chaoji/Zaki 2013,
<https://arxiv.org/pdf/1301.0977>. The survey splits reachability indexes into tree-cover
(interval) labeling, 2-hop labeling, and approximate transitive closure, and states the
core tradeoff is "to strike a balance between offline index construction and online query
processing." Tree-cover/interval indexes are the ones that are hard to maintain: "edge
insertions or deletions might lead to merging or splitting SCCs," and "indexing techniques
designed with the DAG assumption have to deal with the problem of maintaining strongly
connected components, which can be computationally expensive," so a structural change "risks
invalidating containment relationships, potentially requiring recomputation across multiple
vertices." Critically, 2-hop methods (TOL 2014, DBL 2021) support incremental insertion
without tree relabeling because "they don't encode tree structure," and DAGGER extends GRAIL
to fully dynamic graphs with O(k(V+E)) but at the cost of k spanning trees. The survey's
bottom line is there is no universal dominator: the right index "depends on graph size,
update rate, and query pattern."

Relevance: CHALLENGES / EXTENDS #266. This directly qualifies #266's blanket "labels are
invalidated by concurrent inserts." That is true for *interval/tree-cover* labels (the
GRAIL family), which is what #266 named, but it is *not* true for 2-hop labels, which are
designed for incremental insertion. Ankurah's history is append-mostly and, critically,
SCC-free by construction (the event DAG is acyclic by content-hash design, cycles
"structurally impossible"), which removes the single most expensive part of interval
maintenance (SCC merge/split) that the survey identifies as the bottleneck. So the
literature's real message is not "labeling is too expensive for us," it is "labeling's
expense is dominated by dynamism and SCCs, and ankurah has neither the dynamism (events are
immutable once committed) nor the SCCs, so the maintenance objection is weaker than #266
implies." The honest reason to still prefer a scalar generation is the survey's "no
dominator" point: for the specific query (pairwise clock comparison + meet), a scalar plus
the parent edges is the cheapest complete solution, and a richer label would be paid-for
capability the query does not use. #266 should cite this as the reason, and should note that
if a future access pattern needs frequent `dag_contains(id)` without a meet (e.g. the
applied-set membership check at high volume), a 2-hop or bitmap positive index (source 9),
not generations, is the tool.

### 9. Git reachability bitmaps: the positive-membership index (the applied-set analog)

Primary: format <https://git-scm.com/docs/bitmap-format>; fill-in semantics
<https://git-scm.com/docs/gitpacking/2.55.0>. Generation numbers answer "can A reach B" but
not "what is the reachable set of A" cheaply, so git maintains a *second*, positive index:
a per-commit EWAH-compressed bitmap over object bit-positions, "a string of bits indicating
which objects are reachable from each commit," XOR-chained against up to 160 preceding
entries for compression. Bitmaps are tied to a packfile, so objects not in the pack are not
in any bitmap; git handles this with *fill-in traversal*: "When the full set of WANTs is not
available in the index, Git performs a partial revision walk using the commits that don't
have bitmaps as roots, and limits the revision walk as soon as it reaches a commit that has
a corresponding bitmap." Bitmaps are preferentially stored "for commits at the tips of refs."

Relevance: EXTENDS D2's applied-set index. This is the closest existing system to the
per-entity applied-set membership index that D2 wants, and it teaches three things #266 does
not yet state. First, the negative index (generations) and the positive index (applied-set)
are *different tools for different queries*, and git needs both; #266 correctly separates
them but should say why (redelivery/gap = membership = positive; StrictAscends order-check =
negative). Second, git's bitmap is a *cache with a principled fallback*: a boundary walk that
terminates the moment it hits a covered commit. That is exactly the discipline ankurah's
"budget demotes to an anomaly guard" needs: the applied-set answers the common case, and a
bounded walk (terminating at the index frontier, not at genesis) covers the newly-arrived
tail. Third, git stores bitmaps at ref tips, i.e. it indexes the parts most queried; a
per-entity applied-set is naturally head-anchored the same way. The one caution: EWAH+XOR
chaining is real complexity that only pays off at git's object counts; a per-entity ankurah
applied-set is small, so a plain hash-set (as #266 implies) is right, and the bitmap
machinery is a warning about when *not* to over-engineer the positive index.

### 10. Git commit-graph as an optional, verifiable, regenerable cache (operational model)

Primary: <https://git-scm.com/docs/commit-graph> and split-chain rules in
<https://git-scm.com/docs/commit-graph-format>. The commit-graph is strictly an
optimization: git works without it, writes it via `git commit-graph write --reachable` or
`git maintenance start`, verifies it with `git commit-graph verify`, and the split-chain
format lets new data land as a thin top layer without rewriting old layers. The generation
data lives only in layers written by capable versions, and git inspects the topmost layer to
decide whether corrected dates are usable at all.

Relevance: AGREES with #266 and D2's stance that generations are "derived data ... never
trusted from peers" and the applied-set is "local, derived, never trusted from the wire."
Git's whole posture is that the index is a *local, optional, verifiable, rebuildable cache*
that never changes semantics (verdicts stay pinned by content hashes / object ids). #266
matches this exactly: "no verdict semantics change," "generation is never transmitted as
authority." The EXTENDS nudge is operational: git ships a first-class `verify` subcommand and
a `write` command, treats the graph as maintenance-owned, and has a split format so writes
are incremental. #266's "sidecar column plus lazy backfill" is the storage half; the missing
half is the operational surface git found necessary, a verify/rebuild entry point and an
explicit incremental-write story per engine.

## Findings

1. [generation representation] Ankurah's `1 + max(parent generation)` is git's generation
   number *v1* (topological level), verbatim. It inherits v1's documented pathology: a
   commit reattaching to old parents gets an artificially low generation, loosening the very
   pruning bound the design relies on. Git needed a v2 correction to fix this; ankurah cannot
   copy v2 because it trusts no wall clock. (Sources 1, 2)

2. [rejection horizon / D3] The staleness case #266 and D3 most want to bound (a
   months-stale branch forcing deep re-layering) is exactly the case where v1 generations
   help least, because the stale branch's generations are small. `gen(head) - gen(meet)` is a
   real, deterministic distance, but it is a *loose* proxy for "how much merge work,"
   loosest precisely in the adversarial-staleness scenario. (Source 2)

3. [derived-index corruption] A persisted derived index is not free even when "rebuildable":
   git suffered real outages from a v1-to-v2 overflow-flag underflow, from mixed-version
   writes, and from an offset-vs-absolute confusion, several of which stayed silent until a
   boundary input, and one of which blocked its own regeneration. (Sources 3, 4, 5)

4. [runtime fallback] The production escape hatch that saved operators was "fall back to the
   simpler / recomputed index." Git's negative property holds for both v1 and v2 and the
   split format falls back to topological level; GitLab disabled corrected dates wholesale.
   #266 should make the generation/applied-set index disableable at runtime with a pure-walk
   fallback. (Sources 5, 10)

5. [applied-set must be a set, not a range] Mercurial's crossed linkrevs prove a derived
   per-item index value cannot be assumed to enumerate a monotone prefix. Gap detection in
   D2 must be true set difference over ids, never a `generation < g` interval test, or it
   reproduces the crossed-linkrev bug class. (Source 6)

6. [labeling rejection is right, reasoning is thin] #266 rejects GRAIL-style interval
   labeling as "overkill" and "invalidated by inserts." The stronger, literature-grounded
   reasons are: (a) the query shape (pairwise clock compare + meet) needs the parent edges
   anyway, so a walk is unavoidable and a tighter negative filter only trims constants; (b)
   the interval-maintenance cost the survey identifies is dominated by SCC merge/split and
   dynamism, neither of which ankurah has (immutable, acyclic events), so "invalidated by
   inserts" overstates the objection. (Sources 7, 8)

7. [two indexes, two jobs] Negative (generation, order) and positive (applied-set,
   membership) indexes answer different questions; git maintains both deliberately. #266
   already separates them but does not state the principle. The positive index wants a
   bounded fallback walk that terminates at the index frontier, which is git's fill-in
   traversal and is the correct shape for "budget as anomaly guard." (Sources 1, 9)

8. [do not over-engineer the positive index] Git's EWAH+XOR bitmap complexity is justified
   only by huge object counts and preferentially stored at ref tips. A per-entity applied-set
   is small; a plain hash-set is right. The bitmap literature is a boundary marker, not a
   template. (Source 9)

9. [operational surface] Git treats the index as a local, optional, verifiable, rebuildable,
   incrementally written cache with `verify`/`write` commands and a split-chain format. #266
   specifies storage and backfill but not the verify/rebuild/incremental-write surface that
   git found necessary in practice. (Sources 3, 10)

10. [equality is unknown] Git leaves equal-generation pairs as an explicit "unknown" that
    still requires a walk. Ankurah's O(heads) precheck must fall through to the full walk on
    equal generations rather than infer a verdict; generations accelerate the decision, they
    do not replace it. (Source 1)

## Candidate design deltas

- #266: state explicitly that `generation` is git's v1 topological level and inherits the
  "old-parent" looseness; drop or heavily caveat the "corrected commit date v2" reference,
  since v2's fix (wall-clock anchoring) is unavailable to a clock-free design. (Findings 1, 2)
- #266 / D3: do not let the rejection-horizon policy assume `gen(head) - gen(meet)` tightly
  bounds merge cost; it is loosest in the adversarial-staleness case the horizon targets.
  Pair generation-distance with a structural (sealed-prefix) horizon rather than trusting
  the distance alone. (Finding 2)
- #266: add a self-consistency check (stored generation equals 1 + max parent generation)
  and a first-class delete-and-rebuild recovery path; treat these as correctness
  requirements, not polish, given the git corruption post-mortems. (Findings 3, 9)
- #266: document generation as a small saturating unsigned count and add a property-test arm
  driving pathologically deep chains, to pre-empt the persisted-representation edge cases
  (overflow/offset) that bit git. (Findings 3)
- #266 / D2: make the generation and applied-set indexes disableable at runtime with a
  pure-recompute/pure-walk fallback, so a suspected-bad index is a config flip, not an
  outage; the negative property and verdict semantics must not depend on the index existing.
  (Finding 4)
- D2: specify the applied-set as a set of event ids with gap detection by true set
  difference; forbid any `generation < g` interval test as a membership or prefix proxy.
  (Finding 5)
- #266: replace "invalidated by concurrent inserts / overkill" with the accurate rejection
  rationale (query shape needs parent edges regardless; ankurah is acyclic and immutable so
  the SCC/dynamism maintenance cost that dooms interval labels does not apply), and note
  2-hop labeling as the tool to reach for only if a future high-volume `dag_contains` path
  without a meet emerges. (Finding 6)
- D2: adopt git's fill-in-traversal discipline for the positive index: the applied-set
  answers the common case and a bounded walk terminating at the index frontier (not genesis)
  covers the newly-arrived tail; this is the concrete meaning of "budget demotes to an
  anomaly guard." (Finding 7)
- D2: keep the per-entity positive index a plain hash-set; do not import bitmap/EWAH
  machinery, which only pays at git-scale object counts. (Finding 8)
- #266: add the operational surface git found necessary: a verify entry point, an explicit
  per-engine incremental-write path, and index ownership by a maintenance routine rather than
  the hot ingest path where feasible. (Finding 9)
- #266: pin that the O(heads) precheck returns "inconclusive, walk" on equal generations,
  never a verdict; the walk remains the source of truth. (Finding 10)
