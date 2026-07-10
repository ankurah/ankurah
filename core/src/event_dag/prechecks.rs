//! Rejection prechecks P1/P2 (D2 M5; derivations section 2, plan D2-4).
//!
//! Sound REJECTION rules over eligible tip generations: under Axiom A
//! (gen(a) < gen(b) for a < b, saturation excluded) they prune the
//! StrictDescends hypothesis before any fetch. They are wired
//! SUPPRESS-ONLY: a rejection may only skip the positive fast-path attempt
//! (the quick check in `comparison::compare_with`); it never selects,
//! skips, or concludes a verdict path, so a wrong operand costs time,
//! never a verdict (the 5b-ii theorem; the gen-corruption immunity oracle
//! pins it). The mirror rules P1'/P2' (rejecting StrictAscends) have no
//! consumer: no positive StrictAscends fast path exists to suppress, so
//! they are deliberately not implemented.
//!
//! ELIGIBILITY (derivations section 2 precondition, corrected per 5b-ii:
//! "present" alone is not enough): every consulted generation must be
//! structurally paired with its clock, below the saturation sentinel, and
//! not admitted-unverified, else the precheck DISABLES for the comparison
//! and the plain walk runs. Disabled is not suppressed: a disabled
//! precheck leaves the quick-check attempt alone.

use crate::ingest::UnverifiedEvents;
use ankurah_proto::{Clock, GClock};

/// Whether P1/P2 REJECT the hypothesis StrictDescends(subject over
/// comparison), given per-tip generation operands for both clocks.
///
/// Returns false when the precheck is DISABLED (ineligible or mismatched
/// operands) or when no rule fires. Pure over its inputs; the caller wires
/// the result suppress-only.
pub(crate) fn rejects_strict_descends(
    subject: &Clock,
    subject_gens: &GClock,
    comparison: &Clock,
    comparison_gens: &GClock,
    unverified: Option<&UnverifiedEvents>,
) -> bool {
    // Structural eligibility: the operands must annotate exactly the clocks
    // they claim to (defense in depth; the resident maintenance invariant
    // and the wire ingress validation already enforce this upstream).
    if !subject_gens.matches_head(subject) || !comparison_gens.matches_head(comparison) {
        return false;
    }
    // Value eligibility, computed AT CONSUMPTION (D2-4): a saturated value
    // loses Axiom A's contrapositive (266-C.iv), and an admitted-unverified
    // id's value never feeds an acceleration. ANY ineligible operand
    // disables the whole precheck: the P1/P2 proofs quantify over the
    // entire ancestor set, so a partial operand set cannot support the
    // rejection (5b-ii correction 1).
    let eligible = |gens: &GClock| gens.iter().all(|(g, id)| *g != u32::MAX && !unverified.is_some_and(|set| set.contains(id)));
    if !eligible(subject_gens) || !eligible(comparison_gens) {
        return false;
    }

    // Nonempty by matches_head against nonempty clocks; the comparison's
    // empty-clock shapes early-return before any precheck runs.
    let (Some(max_subject), Some(max_comparison)) = (subject_gens.max_generation(), comparison_gens.max_generation()) else {
        return false;
    };

    // P1: if maxg(C) > maxg(S) then NOT StrictDescends(S over C). Every
    // element of anc*(S) has gen <= maxg(S) (tips by definition, strict
    // ancestors by Axiom A), so a comparison tip above that bound cannot
    // lie in the subject's ancestry.
    if max_comparison > max_subject {
        return true;
    }

    // P2 (per-tip refinement): a comparison tip c with gen(c) == maxg(S)
    // that is not itself a subject tip cannot be a strict ancestor of any
    // subject tip (that would force gen(c) < gen(s) <= maxg(S)), so it is
    // outside anc*(S). Values above maxg(S) are already covered by P1.
    comparison_gens.iter().any(|(g, id)| *g == max_subject && !subject.contains(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::EventId;

    fn id(b: u8) -> EventId { EventId::from_bytes([b; 32]) }

    fn clock(ids: &[EventId]) -> Clock { Clock::from(ids.to_vec()) }

    #[test]
    fn p1_rejects_when_comparison_max_exceeds_subject_max() {
        let (s, c) = (id(1), id(2));
        assert!(rejects_strict_descends(&clock(&[s.clone()]), &GClock::from((3, s)), &clock(&[c.clone()]), &GClock::from((5, c)), None));
    }

    #[test]
    fn p1_does_not_reject_when_subject_dominates() {
        let (s, c) = (id(1), id(2));
        assert!(!rejects_strict_descends(&clock(&[s.clone()]), &GClock::from((5, s)), &clock(&[c.clone()]), &GClock::from((3, c)), None));
    }

    /// P2: an equal-to-max comparison tip that is NOT a subject tip rejects;
    /// the same tip AS a subject tip does not (c in anc*(S) via c in S).
    #[test]
    fn p2_rejects_equal_max_non_member_only() {
        let (s, c) = (id(1), id(2));
        assert!(rejects_strict_descends(
            &clock(&[s.clone()]),
            &GClock::from((4, s.clone())),
            &clock(&[c.clone()]),
            &GClock::from((4, c.clone())),
            None
        ));
        // Shared tip: subject {s, x} vs comparison {s} at equal max.
        let x = id(3);
        assert!(!rejects_strict_descends(
            &clock(&[s.clone(), x.clone()]),
            &GClock::new(vec![(4, s.clone()), (2, x)]),
            &clock(&[s.clone()]),
            &GClock::from((4, s)),
            None
        ));
    }

    /// Saturation disables (266-C.iv): a u32::MAX operand anywhere makes
    /// the precheck inapplicable, even on shapes P1 would otherwise reject.
    #[test]
    fn saturated_operand_disables() {
        let (s, c) = (id(1), id(2));
        assert!(!rejects_strict_descends(
            &clock(&[s.clone()]),
            &GClock::from((3, s.clone())),
            &clock(&[c.clone()]),
            &GClock::from((u32::MAX, c.clone())),
            None
        ));
        assert!(!rejects_strict_descends(
            &clock(&[s.clone()]),
            &GClock::from((u32::MAX, s)),
            &clock(&[c.clone()]),
            &GClock::from((5, c)),
            None
        ));
    }

    /// An admitted-unverified tip disables (D2-4 eligibility), on either side.
    #[test]
    fn unverified_operand_disables() {
        let (s, c) = (id(1), id(2));
        let set = UnverifiedEvents::with_cap(8);
        set.insert(c.clone());
        assert!(!rejects_strict_descends(
            &clock(&[s.clone()]),
            &GClock::from((3, s.clone())),
            &clock(&[c.clone()]),
            &GClock::from((5, c.clone())),
            Some(&set)
        ));
        let set = UnverifiedEvents::with_cap(8);
        set.insert(s.clone());
        assert!(!rejects_strict_descends(
            &clock(&[s.clone()]),
            &GClock::from((3, s)),
            &clock(&[c.clone()]),
            &GClock::from((5, c)),
            Some(&set)
        ));
    }

    /// A structurally mismatched annotation disables rather than rejecting.
    #[test]
    fn mismatched_annotation_disables() {
        let (s, c, other) = (id(1), id(2), id(9));
        assert!(!rejects_strict_descends(
            &clock(&[s.clone()]),
            &GClock::from((3, other)), // annotates the wrong tip
            &clock(&[c.clone()]),
            &GClock::from((5, c)),
            None
        ));
        let _ = s;
    }
}
