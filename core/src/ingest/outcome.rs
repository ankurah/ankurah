//! Per-event pipeline outcomes. Outcomes are not errors: an event that
//! cannot apply YET (missing state, missing parents) is retained in the
//! staging area and reported, never dropped (268-B). Errors are the
//! `IngestError` taxonomy in `crate::error`.

use ankurah_proto::{EntityId, EventId};

/// What happened to one event in an executed plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IngestOutcome {
    /// Applied to the entity and committed to the event log.
    Applied,
    /// Nothing to do; success for ack purposes, so a lost ack plus sender
    /// retry cannot double-apply (268-A).
    Skipped(SkipReason),
    /// Non-creation event for an entity with no local state. The event stays
    /// staged; feeders on context-bearing lanes may recover by requesting a
    /// snapshot (the existing Get request) and re-planning.
    NeedsState { entity: EntityId },
    /// One or more parents are neither applied, staged, nor fetchable. The
    /// event stays staged and integrates via descendant re-drive when a
    /// parent arrives. In the D1 world there is no seal, so buffering is the
    /// only correct response; the D3 rejection horizon later extends this
    /// surface with a PolicyAgent decision.
    NeedsEvents { missing: Vec<EventId> },
}

/// Why an event was skipped. Both variants are idempotency working as
/// intended, not failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// Already durably committed here (redelivery or retry).
    AlreadyCommitted,
    /// The comparison verdict was Equal or StrictAscends: the head already
    /// incorporates this event.
    AlreadyIntegrated,
}
