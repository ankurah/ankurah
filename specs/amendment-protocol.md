# Amendment protocol: retraction audits

A spec is a dependency graph of decisions, not a set of independent facts.
When a ruling amends, vetoes, or supersedes a premise, every conclusion that
cited it is suspect until re-justified, and the failure mode this protocol
exists to prevent is extending orphaned machinery because nobody re-asked
why it was there.

**The rule: no amendment ships without a retraction audit.** Before writing
any code against a premise-changing ruling:

1. Grep the initiative's spec files (rfc, plan, tasks) AND the code (doc
   comments cite decisions and sections too) for every reference to the
   changed decision number, section, or named mechanism.
2. Disposition each hit, in writing, as one of:
   - RETRACT: the text or code was a corollary of the repealed premise;
     delete it (code) or mark it superseded in place with the date and the
     governing decision (spec text).
   - RE-JUSTIFY: it survives, but under a different justification; say
     which.
   - HISTORICAL: a completion record or an original block whose governing
     AMENDED block sits directly beneath it (the in-place supersession
     convention); leave it, marking it only if a reader could mistake it
     for current truth (e.g. a shipped-inventory line naming a deleted
     error variant).
3. The audit list rides the amendment: in the AMENDED block, the plan
   decision entry, the commit message, or the ratification-trail note --
   wherever the ruling itself is recorded.

Conventions this leans on: dated `AMENDED (...)` blocks appended beneath
the text they supersede; decisions cited by number ("decision 15") and
sections by number with a gloss ("rfc.md 5.6, the retype rules"); vetoes
recorded on the decision they veto, not only in the new decision.

Origin: the 2026-07-10 canonical value_type and gate-removal rulings
(#289; plan decisions 30/31), where the read-time gates outlived the
fork-on-retype premise they were corollaries of, and the retroactive audit
that introduced this protocol caught four more unmarked stragglers across
plan.md and tasks.md.
