# Concurrency Threat Model

Phase 2 workstream C item 4, part 1 of 2. Tracking: #269. Scope:
specs/concurrency/phase-2.md. This is the document the wire-level adversarial
suite (part 2) asserts against: every future adversarial test maps to a named
claim (C4-NN) in the registry below. The validated-ingress RFC (#274) calls this
document "the checklist this ingress implements."

Status of this file: skeleton. Sections 1, 2, and the claims-registry format are
in place with the first verified claims. The full registry, attestation map, and
gaps table land in the follow-up commits on this branch.

## How to read this document

- Section 1 fixes the trust context: node classes, what peers are trusted with
  today, and precisely what content addressing does and does not buy us.
- Section 2 catalogs adversary capability tiers and what each can attempt on each
  wire arm.
- Section 3 is the core artifact: a numbered claims registry. Each claim states
  an invariant, its trust tier, the enforcing code seam (file plus function that
  was read and verified, not assumed), the attack that would falsify it, and the
  planned adversarial test arm.
- Section 4 maps every place an attestation is consumed or assumed, and what
  breaks if it lies.
- Section 5 is the known-gaps table.

Every claim about current behavior cites the code seam verified against the tree
at commit 90f9a67d (PR #201). Where the literature challenges a claim, the
challenge is incorporated or explicitly rebutted inline. Where enforcement does
not exist today, the claim records the current status honestly (open gap, closed
by #274 or #271 when it lands) rather than aspirationally.

## Trust tiers (used throughout)

Three tiers classify every invariant:

- **Byzantine-safe**: holds against arbitrary peers, including malicious ones that
  craft wire payloads freely. The guarantee derives from structure (content
  addressing, deterministic graph facts) and needs no honest-peer assumption.
- **Trusted-peer**: holds only if peers are honest but possibly buggy. A
  malicious peer can violate it. These are the invariants that a real PolicyAgent
  or a future hardening item (#274, #246, #271) must convert to Byzantine-safe.
- **Attestation-dependent**: holds if and only if the attestations involved
  verify. The guarantee is exactly as strong as the PolicyAgent's signature
  scheme; with the default PermissiveAgent there is no guarantee at all.

The Byzantine guarantee stated at its true precision (per lit review topic 3,
sources 4/5/6): the content-addressed event DAG delivers **weak-safety causal
order and strong eventual consistency among correct nodes for arbitrarily many
Byzantine peers, without authorship attribution, authorization, strong-safety
causal order, or a DoS bound**. The last four require a signature layer the
codebase does not currently carry.

---

## 1. System and trust context

TODO (next commit): node classes (durable vs ephemeral), the PermissiveAgent
default vs real PolicyAgent surface, and the content-addressing ledger (what it
buys, what it does not).

---

## 2. Adversary catalog

TODO (next commit): capability tiers and per-wire-arm attack surface.

---

## 3. Claims registry

The core artifact. Format for each claim:

> **C4-NN. Short imperative invariant name.**
> **Invariant:** what must hold.
> **Trust tier:** Byzantine-safe | trusted-peer | attestation-dependent.
> **Enforcing seam:** `path::function` (verified) or "none today".
> **Falsifying attack:** the specific thing an adversary does to break it.
> **Planned test arm:** the C4 part-2 test that pins it.
> **Status:** enforced | open gap (owner) | partial.

### Structural integrity claims (content addressing)

**C4-01. Event identity is a verifiable content hash.**
Invariant: an event's id equals `SHA-256(bincode(entity_id) || bincode(operations)
|| bincode(parent))`; the id is recomputed from contents on every use and is never
read from the wire. A peer cannot present an event under an id whose contents do
not hash to that id.
Trust tier: Byzantine-safe.
Enforcing seam: `proto/src/data.rs` `EventId::from_parts` (the hash) and
`Event::id()` (recomputes on every call; no id field is stored or deserialized).
`core/src/event_dag/accumulator.rs` `EventAccumulator::accumulate` keys the DAG by
`event.id()`, i.e. by the recomputed hash, so a lying id cannot enter the graph.
Falsifying attack: send an event whose declared id does not match its contents
(forged id), hoping a consumer trusts the declared id.
Planned test arm: forged-id (content/id mismatch) rejection; verify the
recomputed id is what the DAG and staging key on.
Status: enforced. Note (lit review Finding 6): content addressing gives inherent
identity and tamper-evidence, not authorship. See C4-20.

**C4-02. Collection is excluded from event identity.**
Invariant: two events differing only in their `collection` field hash to the same
`EventId`. Identity is `(entity_id, operations, parent)` only.
Trust tier: Byzantine-safe (structural fact), but see the caveat below.
Enforcing seam: `proto/src/data.rs` `EventId::from_parts` hashes entity_id,
operations, and parent, and deliberately omits collection (the codebase comment
records collection "getting excised from identity"). `From<(EntityId,
CollectionId, EventFragment)>` in the same file supplies collection from the
receiving message context, not from the event body.
Falsifying attack: a peer delivers an event fragment for entity E claiming
collection X in the message envelope while the same content is legitimately in
collection Y; both produce one id.
Planned test arm: cross-collection id-collision arm (same operations/parent, two
collection envelopes) asserting no state cross-contamination.
Status: enforced as a hashing fact. Caveat: because collection rides the message
envelope and is not bound into the id, collection attribution is a trusted-peer
property, not Byzantine-safe. Flagged in the gaps table.

---

## 4. Attestation load-bearing map

TODO (next commit): every consumer of an attestation and what a lying attestation
breaks.

---

## 5. Known gaps

TODO (next commit): gap, severity rationale, owning RFC or workstream item.

---

## Appendix: source grounding

Claims were verified by reading, at commit 90f9a67d:
`core/src/event_dag/{comparison,ordering,accumulator,relation,mod}.rs`,
`core/src/{entity,node,node_applier,retrieval,policy}.rs`,
`proto/src/{data,clock,auth,id,request}.rs`; issues #242, #243, #244, #246, #247;
RFCs #271 and #274; the phase 2 lit review (topic 3, Merkle-causal, and
design-deltas.md); and the July 2026 verification review (V1 through V7) on
archive/201-concurrent-updates-specs.
