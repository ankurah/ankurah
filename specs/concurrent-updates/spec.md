## Concurrent Updates: Functional Specification

> **Implementation status:** See [implementation-status.md](./implementation-status.md) for current progress on the `trusted_causal_assertions` branch.

### Motivation

Ephemeral and durable peers observe edits at different times. When a peer receives updates for an entity, it must determine if those updates descend from its current head, are ancestors, or represent true concurrency. The system must converge deterministically without regressions or spurious multi-heads.

### Terms and Semantics

- **CausalRelation**: Semantic relation between two heads/clocks (working type in `core`, wire type in `proto`). Variants:
  - Equal
  - StrictDescends
  - StrictAscends
  - DivergedSince { meet, subject, other }
  - Disjoint { gca?, subject_root, other_root }
  - BudgetExceeded { subject, other }

### Functional Requirements

- **Determinism and Safety**

  - Do not regress state. Never “apply latest” while skipping required intermediate events.
  - Keep at most one head unless there is true concurrency. Never create multi-heads for linear histories.
  - Final state equals the result of applying the accepted event log in order.

- **Event or State Application**

  - Equal: no-op.
  - StrictDescends:
    - From the meet, apply the forward chain (oldest → newest) that connects the meet to the incoming head. Advance the frontier accordingly.
  - StrictAscends: ignore (incoming is older than current head).
  - DivergedSince { meet, subject, other } (true concurrency):
    - From the meet, apply the subject’s forward chain in causal order. In the same step, consider the known immediate descendants of the meet that we currently have (from staged → local → remote) and prune those that are subsumed by the applied chain’s tip. Any remaining (still-unsubsumed) descendants are bubbled into the resulting frontier as concurrent head members.
    - Apply a deterministic per-backend LWW policy for value resolution with the following precedence:
      1. Lineage precedence relative to the meet (later along its branch wins),
      2. Tiebreak by lexicographic ordering of event id,
      3. Optionally, Yrs as a stronger causal metric in the future.
    - The system MAY preserve multi-heads when unsubsumed concurrency remains. It MUST NOT create multi-heads for linear histories.
  - Disjoint: reject (no common genesis) per policy.
  - BudgetExceeded { subject, other }: traversal stopped early; return the frontiers to resume in a subsequent attempt.

- **Server-Assisted Bridge for Known Matches**

  - For entities present in `known_matches` whose heads are not Equal, the server SHOULD prefer sending an EventBridge (connecting chain) over a full State snapshot whenever feasible under policy budget.
  - EventBridge MUST be applicative (oldest → newest) on the client; staged events are preferred for zero-cost lineage checks.

- **Budget and Retrieval Model**

  - Staged events: zero cost; local batch retrieval: fixed low cost; remote retrieval: higher cost.
  - On exceeding budget, the system returns resumable frontiers that allow continuing the traversal later.

- **Atomicity and TOCTOU**

  - Replay/merge plans are computed without holding the entity write lock. Before applying, take the lock, verify the head is unchanged, else recompute. Apply operations and advance head under the lock.

- **Invariants**
  - Applied state equals replayed event operations (for EventBridge and Descends replay).
  - Concurrency is resolved deterministically (LWW + lexicographic tiebreak). No spurious multi-heads for linear histories; unsubsumed concurrency may be bubbled to the frontier.
  - Security: acceptance of externally attested relations is governed by `PolicyAgent` (see lineage-attestation spec).

### Ascending application semantics

- After determining the meet, the system SHALL apply the subject chain in causal order (oldest → newest), prune subsumed immediate descendants of the meet, and bubble any remaining unsubsumed descendants into the resulting frontier.
- The resulting frontier MAY be a single-member clock or multi-member when unsubsumed concurrency remains.
- The system MAY repeat this process iteratively when additional forward events become available or after resumption from BudgetExceeded; each iteration uses the previously returned frontier as the new meet/frontier.
- The set of immediate descendants of the meet used for pruning MAY be partial depending on budget and availability; conservative bubbling is permitted until more descendants are known.

### Out of Scope (for this spec)

- Attested lineage shortcuts (`DeltaContent::StateAndRelation`): specified in `specs/lineage-attestation/spec.md` and implemented after this concurrent-updates spec.
