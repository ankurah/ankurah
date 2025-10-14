## Event Staging for Lineage-Assisted Apply (Deferred)

### Problem

Blindly persisting received events can create orphan segments and storage churn. Staging (holding in suspense) can amortize costs and avoid promoting unverified branches.

### Direction

- Introduce a per-entity staging area. Retrieval consults staging+store.
- Promote staged events to canonical when:
  - A complete bridge proves connectivity to current head; or
  - A trusted attestation indicates Descends for the new head.

### Scope (Deferred)

- This spec is secondary to lineage attestation and small bridges. Implement only after primary objective lands.
