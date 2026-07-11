//! Per-comparison observability counters (D2 M5, oracle brief dispositions
//! Q4 as amended by the red-team fold).
//!
//! WRITE-ONLY DURING THE WALK (the covert-channel rule, red-team fold item
//! 5): comparison code may only INCREMENT these counters while a comparison
//! is in flight; nothing in the comparison path may READ a counter before
//! the comparison completes. Reading one mid-walk would turn the stats
//! surface into a generation-to-control-flow channel that the 5b-ii
//! immunity theorem's quantifier does not cover. The only readers are
//! post-completion consumers: tests, the immunity oracle, and bench
//! evidence.
//!
//! Registry-ban compliance (plan REV 5 section A): these are AGGREGATE
//! counters; no field holds a generation value or any per-event mapping.

/// Counters for one comparison (one `compare` / `compare_with` call,
/// budget-escalation retries included: the accumulator that carries them is
/// warm across retries, so the totals describe the logical comparison).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CompareStats {
    /// Events fetched from the UNDERLYING retriever (accumulator cache
    /// misses). LRU hits are not fetches; this is the walk-cost observable
    /// the bench evidence and the immunity arm's FREE-delta assertions read.
    pub(crate) events_fetched: usize,
    /// P1/P2 precheck rejections that suppressed the positive fast-path
    /// attempt (the StrictDescends quick check). Counted per suppressed
    /// ATTEMPT, whether or not the attempt would have succeeded (the
    /// red-team fold's counter semantics; the B1 seed's suppressed attempt
    /// was inapplicable in baseline, and the delta must still be visible).
    pub(crate) precheck_suppressions: usize,
    /// Walk-time edge-check EQUATIONS actually evaluated: child and all
    /// parents in hand, the `1 + max` comparison ran (pass or fail).
    /// Genesis completions carry no edge and are not counted. The M6
    /// kill-switch dormancy pin asserts this is ZERO with the switch
    /// thrown; the enabled world shows it positive on any corpus with a
    /// fully in-hand parent edge.
    pub(crate) edge_checks_evaluated: usize,
    /// Walk-time edge-check violations (D2-4): children whose in-hand
    /// payload generation contradicts `1 + max(parent generations)` over
    /// their in-hand parents, saturating arithmetic. One count per failed
    /// child check.
    pub(crate) edge_check_violations: usize,
    /// Children demoted to per-comparison ineligibility by a failed edge
    /// check (D2-4: transient demotion, never retroactive rejection).
    pub(crate) demotions: usize,
    /// Served payloads whose recomputed id differed from the requested id
    /// (R1 observability, dispositions Q1: a counter is permitted, a
    /// verify-and-error is not). Counted at underlying-fetch time only, so
    /// the recompute cost rides fetches, not cache hits.
    pub(crate) id_mismatches: usize,
    /// Budget decrements (processed frontier occupants). The oracle's
    /// non-binding-budget guarantee rests on the invariant that this is at
    /// most TWO per distinct event under any schedule (once per side;
    /// Side::processed makes per-side expansion idempotent). The budget
    /// invariant pin reads this; a frontier rewrite that breaks the bound
    /// turns BudgetExceeded into a schedule-dependent oracle artifact
    /// (red-team fold item 7).
    pub(crate) budget_decrements: usize,
}
