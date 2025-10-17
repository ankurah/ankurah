## Concurrent Updates: Functional Specification

> **Implementation Status**: Partial. Server-side EventBridge generation and client application are complete. Entity-level `StrictDescends` replay is implemented. `DivergedSince` deterministic resolution via backend transactions and ForwardView/ReadySet streaming are in progress. See `plan.md` and `tasks.md` for details.

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

### Nomenclature

- **Clock**: An antichain (set of event IDs where no member is an ancestor or descendant of any other member in the set) representing a consistent frontier in the event DAG. Members may share common ancestors. Used to track entity heads and comparison points.
- **ForwardView**: Immutable forward subgraph (DAG slice) from the meet clock to the union of target heads. Offers iterators over ReadySets without requiring full materialization. (Implementation: to be added; currently a topologically-sorted `Vec<Event>` via Kahn's algorithm.)
- **ReadySet**: The causally-ready batch at a given topological layer: all events whose parents are already in the applied set within the ForwardView. Represents events that can be applied in parallel (subject to backend semantics). (Implementation: to be added.)
- **Primary**: The clock (single-member or multi-member antichain) representing the chosen reference branch - the entity's current head. This is the "subject" in causal comparisons. For StrictDescends, we advance from subject toward other; for DivergedSince, the subject branch is maintained and concurrent events from other are resolved deterministically.
- **Concurrency**: Set of Events in the same ReadySet that are concurrent with the Primary (Clock) at that topological layer.
- **Branch path**: Conceptual; represents the causal chain from meet to a given head. Used to reason about which events lie on the "main" path vs concurrent siblings. Not a materialized data structure; instead inferred from parent relationships during ReadySet iteration.
- **BackendTransaction**: An ephemeral, per-backend session used to apply a ForwardView with atomic commit/rollback semantics. Each backend implements its own deterministic resolution strategy. (Implementation: to be added.)
- **EventAccumulator**: An injected collector used during lineage traversal to accumulate subject-side events for server-side EventBridge construction. (Implementation: ✅ complete; used by `Node::collect_event_bridge`.)

### Functional Requirements

- **Determinism and Safety**

  - Do not regress state. Never "apply latest" while skipping required intermediate events.
  - Final state equals the result of applying the accepted event log in causal order.
  - For linear histories (single causal chain), result in a single-member head clock. For true concurrency, the system MAY preserve multi-member head clocks when unsubsumed branches remain.

- **Event or State Application**

  Relation is always expressed as `subject <relation> other`. The `CausalRelation` variants have fixed semantics:

  - `Equal`: subject equals other (same clock)
  - `StrictDescends`: subject descends from other (subject is newer, other is older)
  - `StrictAscends`: subject is ancestor of other (subject is older, other is newer)

  In `Entity::apply_event` and `Entity::apply_state`:

  - **Current implementation**: incoming is subject, entity head is other
    - `Equal`: heads match → no-op
    - `StrictDescends`: incoming descends from head → apply forward
    - `StrictAscends`: incoming is ancestor of head → ignore
  - **After argument inversion** (head is subject, incoming is other):
    - `Equal`: heads match → no-op (unchanged)
    - `StrictDescends`: head descends from incoming → ignore (incoming is older)
    - `StrictAscends`: head is ancestor of incoming → apply forward (incoming is newer)

  The `DivergedSince` case is symmetric and doesn't change.

  - **DivergedSince** { meet, subject, other } (true concurrency):
    - The entity's current state already reflects the subject (Primary) branch head. From the meet, iterate the ForwardView's ReadySets in causal order. At each ReadySet, identify events on the subject path (already applied to entity) and concurrent events from other. This is used by the PropertyBackend to determine what changes should be applied to the entity's state on commit. For LWW, we will compare the registers at each step and perform tiebreaking as we go. This is necessary because we do not have a snapshot of the entity's state at the meet, so we need to be a bit clever. Yrs has it easy - just slam all the ops into the entity's state without a care in the world. nevermind if they've been applied already or not.
    - For each backend, call `begin()` to start a transaction, then `apply_ready_set(rs)` for each ReadySet.
    - Each backend applies its own deterministic resolution strategy, accumulating diffs from the current state:
      - **LWW backend** (Causal LWW-Register): maintain an in-memory winner per property using: (1) lineage precedence relative to the meet (descendants overwrite ancestors), (2) lexicographic ordering of event ID (content-hash) for concurrent siblings. On `commit()`, call `to_state_buffer()` to serialize only the diffs and persist with the new head.
      - **Yrs backend**: apply unseen ops from other; skip seen; on `commit()`, serialize state buffer and persist.
    - Consider the known immediate descendants of the meet (from staged → local → remote) and prune those subsumed by the applied other tip. Any remaining (still-unsubsumed) descendants are bubbled into the resulting head clock as concurrent members.
  - **Disjoint**: reject (no common genesis) per policy.
  - **BudgetExceeded** { subject, other }: traversal stopped early; return the frontiers to resume in a subsequent attempt.

- **Backend Application Contract**

  - The Entity constructs a ForwardView and iterates its ReadySets once. The entity's current state already reflects the Primary (subject) branch up to the meet.
  - For each property backend, the Entity calls `backend.begin()` to obtain a `BackendTransaction`.
  - For each ReadySet in order, the Entity calls `tx.apply_ready_set(rs)` for every backend transaction. The transaction accumulates diffs from the current state.
  - On any error, the Entity calls `tx.rollback()` for all transactions and returns the error.
  - On success, the Entity calls `tx.commit()` for all transactions, then calls `backend.to_state_buffer()` to serialize state, and atomically persists the state buffers with the updated entity head(s) per lineage result.

- **Server-Assisted Bridge for Known Matches** (✅ implemented)

  - For entities present in `known_matches` whose heads are not Equal, the server prefers sending an EventBridge (connecting chain of events) over a full State snapshot whenever feasible under policy budget.
  - EventBridges are applied oldest → newest on the client; staged events are preferred for zero-cost lineage checks.
  - Implementation: `Node::collect_event_bridge` uses `EventAccumulator` during lineage traversal; integrated in Fetch and SubscribeQuery paths.

- **Budget and Retrieval Model**

  - Staged events: zero cost; local batch retrieval: fixed low cost; remote retrieval: higher cost.
  - On exceeding budget, the system returns resumable frontiers that allow continuing the traversal later.

- **Atomicity and TOCTOU**

  - Plan phase (no write lock): compute lineage relation, identify the meet, and prepare the ForwardView for ReadySet iteration.
  - Apply phase (with write lock):
    - Acquire the entity write lock and re-compare the current head against the expected head from the plan. If different, release the lock and recompute the plan.
    - If heads match, start transactions for all backends via `backend.begin()`.
    - Iterate ReadySets; call `tx.apply_ready_set(rs)` for each backend; on any failure, call `tx.rollback()` for all backends and return the error.
    - If all succeed, call `tx.commit()` for all backends, then update the entity head(s) per the lineage result in the same critical section.

- **Invariants**
  - Applied state equals the result of replaying event operations in causal order.
  - Concurrency is resolved deterministically per backend semantics (e.g., LWW uses lineage + lexicographic tiebreak; Yrs uses op-based convergence).
  - Linear histories converge to a single-member head clock; unsubsumed concurrent branches may result in multi-member head clocks.
  - Security: acceptance of externally attested relations is governed by `PolicyAgent` (see lineage-attestation spec).

### Ascending application semantics

- After determining the meet, the system SHALL iterate the ForwardView's ReadySets in causal order (oldest → newest along the subject path), prune subsumed immediate descendants of the meet, and bubble any remaining unsubsumed descendants into the resulting frontier.
- The resulting frontier MAY be a single-member clock or multi-member when unsubsumed concurrency remains.
- The system SHALL repeat this process iteratively when additional forward events become available or after resumption from BudgetExceeded; each iteration uses the previously returned frontier as the new meet/frontier.
- The set of immediate descendants of the meet used for pruning MAY be partial depending on budget and availability; conservative bubbling is permitted until more descendants are known.

### Out of Scope (for this spec)

- Attested lineage shortcuts (`DeltaContent::StateAndRelation`): specified in `specs/lineage-attestation/spec.md` and implemented after this concurrent-updates spec.
