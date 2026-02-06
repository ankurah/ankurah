# Phase 2 Review Matrix — Agent Prompts

> These are the prompts for the 8 independent review agents.
> Each agent receives ONLY its designated seed and NOTHING else.

---

## Common Preamble (included in all 8)

```
WORKTREE: /Users/daniel/ak/ankurah-201
BRANCH: concurrent-updates-event-dag (based off main)

KEY FILES TO READ:
- Core new module: core/src/event_dag/{mod.rs, relation.rs, comparison.rs, navigator.rs, frontier.rs, layers.rs, traits.rs, tests.rs}
- Integration: core/src/entity.rs, core/src/property/backend/lww.rs, core/src/property/backend/mod.rs, core/src/property/backend/yrs.rs
- Integration: core/src/retrieval.rs, core/src/node.rs, core/src/node_applier.rs, core/src/context.rs, core/src/transaction.rs
- Spec: specs/concurrent-updates/spec.md
- Tests: tests/tests/*.rs

OUTPUT FORMAT: Your review MUST end with a "Confidence-Rated Findings" section:
| # | Finding | Severity (Critical/High/Medium/Low) | Confidence (High/Medium/Low) | Details |
Each finding should be a specific, actionable observation — not a vague concern.
```

---

## Compartmentalization Rules

| Agent | Receives | MUST NOT access |
|-------|----------|-----------------|
| A1–A4 | PR Comments Digest file | Holistic code review file, `gh` PR comments directly |
| B1–B4 | Holistic Code Review file | PR Comments Digest file, `gh` PR comments directly |

---

## A-Row Agents (Seeded with PR Comments Digest)

### A1 — Algorithm + Invariants

```
ROLE: You are a distributed systems algorithm specialist reviewing a causal DAG comparison implementation.

SEED CONTEXT: Read /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-digest-pr-comments.md
This contains a digest of prior reviewer feedback on this PR. Use it to understand what has already been scrutinized, but form your OWN assessment of the algorithm.

COMPARTMENTALIZATION: Do NOT run any `gh` commands. Do NOT read any file named *holistic*. Your only external context is the digest above.

MANDATE: Deep review of the core DAG comparison algorithm.

FOCUS AREAS:
1. Trace through the backward BFS in comparison.rs step by step. Is the termination condition correct? Can it miss ancestors?
2. Verify chain construction: when DivergedSince is returned, are the forward chains from meet→tip correctly built?
3. Enumerate the key invariants this system relies on (e.g. "all replicas converge given the same event set", "compare(A,B) and compare(B,A) are symmetric/antisymmetric"). For each invariant, assess whether the code upholds it.
4. CRITICAL — Asymmetric depth: Walk through scenarios where concurrent branches have very different depths (1 vs 50, 3 vs 300). Does the meet-point computation stay correct? Does the BFS budget interact badly with deep asymmetry?
5. The BudgetExceeded fallback — what happens when the BFS budget runs out? Is the frontier-based resumption sound?
6. Multi-head entities: when an entity has multiple tips, does comparison correctly handle events extending just one tip?

Read the code thoroughly. Trace specific scenarios through the actual code paths, don't just reason abstractly.

OUTPUT: Write to /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-matrix-A1-algorithm.md
```

### A2 — LWW Causal Register

```
ROLE: You are a CRDT/replication specialist reviewing a Last-Writer-Wins register implementation with causal awareness.

SEED CONTEXT: Read /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-digest-pr-comments.md
This contains a digest of prior reviewer feedback. Use it to understand what has been discussed, but form your OWN assessment.

COMPARTMENTALIZATION: Do NOT run any `gh` commands. Do NOT read any file named *holistic*. Your only external context is the digest above.

MANDATE: Deep review of the LWW property backend and layer comparison logic.

FOCUS AREAS:
1. The committed/pending/uncommitted model in lww.rs — are state transitions correct? Can states get stuck?
2. Per-property resolution: when DivergedSince produces two chains, does the LWW resolver correctly pick the winner for EACH property independently?
3. Depth-based precedence: "further from meet wins." Trace through cases:
   - Property written at depth 3 on branch X, depth 20 on branch Y → Y should win
   - Property written on only one branch (empty other chain) → writer should win
   - Property written at equal depth on both branches → lexicographic tiebreak
   - Property written multiple times on one branch → only the latest matters
4. CRITICAL — Cross-replica convergence: If replica R1 sees events in order [A, B, C] and replica R2 sees [C, A, B], do they converge to the same state? Trace through the actual resolution code.
5. The layer comparison API in layers.rs — does it correctly extract per-property operation metadata from the chains?
6. Interaction between LWW resolution and Yrs (CRDT text) backend — are they handled consistently?

OUTPUT: Write to /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-matrix-A2-lww-register.md
```

### A3 — Spec Conformance

```
ROLE: You are a technical specification auditor. Your job is to verify that the implementation matches the specification, and that the specification is complete.

SEED CONTEXT: Read /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-digest-pr-comments.md
This contains a digest of prior reviewer feedback. Use it for context, but your job is specifically about spec↔code alignment.

COMPARTMENTALIZATION: Do NOT run any `gh` commands. Do NOT read any file named *holistic*. Your only external context is the digest above.

MANDATE: Line-by-line cross-reference of spec.md against the implementation.

FOCUS AREAS:
1. Read specs/concurrent-updates/spec.md thoroughly first
2. For EACH algorithm step described in the spec, find the corresponding code and verify they match
3. For EACH variant of AbstractCausalRelation described in the spec, verify the code handles it as documented
4. For EACH resolution rule (depth precedence, lexicographic tiebreak, empty chain handling), verify the code implements exactly what the spec says
5. Identify anything the code does that the spec DOESN'T describe (undocumented behavior)
6. Identify anything the spec promises that the code DOESN'T implement (missing behavior)
7. Check test coverage against spec claims — does the spec claim behaviors that aren't tested?
8. Are there ambiguities in the spec that the code resolves in a particular way without documentation?

OUTPUT: Write to /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-matrix-A3-spec-conformance.md
```

### A4 — Adversarial / Edge Cases

```
ROLE: You are a security-minded adversarial tester. Your job is to try to BREAK this implementation — find inputs, topologies, timing, and edge cases that produce incorrect behavior.

SEED CONTEXT: Read /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-digest-pr-comments.md
This contains a digest of prior reviewer feedback including concerns that were raised. Your job is to determine: were those concerns ACTUALLY addressed? And what did the reviewers MISS?

COMPARTMENTALIZATION: Do NOT run any `gh` commands. Do NOT read any file named *holistic*. Your only external context is the digest above.

MANDATE: Actively try to break the implementation. Construct failure scenarios.

FOCUS AREAS:
1. CRITICAL — Extreme depth asymmetry: What happens with 1 vs 1000 depth branches? Does BFS budget blow up? Does memory usage explode? Does depth precedence still produce correct results?
2. Cascading diverge-then-reconverge: A diverges into B and C, then B and C each diverge further, then reconverge at different points. Does the meet-point finder handle nested diamonds?
3. TOCTOU retry loop in entity.rs: Is there a termination guarantee? Can concurrent writers cause infinite retry? What's the worst-case retry count?
4. Race conditions: Two threads calling apply_state simultaneously with events that reference each other's events. Can you construct a deadlock or inconsistent state?
5. Malicious/malformed events: What if an event claims parents that don't exist? What if the event graph has cycles? What if event IDs collide?
6. Resource exhaustion: Can an attacker craft a DAG topology that causes O(n²) or worse comparison time? What's the worst-case memory usage of chain construction?
7. Multiple properties with mixed backends (LWW + Yrs): concurrent updates to different properties on the same entity with different backends — any interaction bugs?
8. The "empty chain" edge case in LWW: when one branch never wrote to a property but the resolution still needs to handle it.

For each potential issue, trace through the actual code to determine if it's a real bug or handled correctly.

OUTPUT: Write to /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-matrix-A4-adversarial.md
```

---

## B-Row Agents (Seeded with Holistic Code Review)

### B1 — Algorithm + Invariants

```
ROLE: You are a distributed systems algorithm specialist reviewing a causal DAG comparison implementation.

SEED CONTEXT: Read /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-holistic-code.md
This contains one engineer's independent code-level assessment (no PR discussion context). Use it as a starting point, but form your OWN deep assessment.

COMPARTMENTALIZATION: Do NOT run any `gh` commands. Do NOT read any file named *digest* or *pr-comments*. Do NOT access GitHub in any way. Your only external context is the holistic review above.

MANDATE: Deep review of the core DAG comparison algorithm.

FOCUS AREAS:
1. Trace through the backward BFS in comparison.rs step by step. Is the termination condition correct? Can it miss ancestors?
2. Verify chain construction: when DivergedSince is returned, are the forward chains from meet→tip correctly built?
3. Enumerate the key invariants this system relies on (e.g. "all replicas converge given the same event set", "compare(A,B) and compare(B,A) are symmetric/antisymmetric"). For each invariant, assess whether the code upholds it.
4. CRITICAL — Asymmetric depth: Walk through scenarios where concurrent branches have very different depths (1 vs 50, 3 vs 300). Does the meet-point computation stay correct? Does the BFS budget interact badly with deep asymmetry?
5. The BudgetExceeded fallback — what happens when the BFS budget runs out? Is the frontier-based resumption sound?
6. Multi-head entities: when an entity has multiple tips, does comparison correctly handle events extending just one tip?

Read the code thoroughly. Trace specific scenarios through the actual code paths, don't just reason abstractly.

OUTPUT: Write to /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-matrix-B1-algorithm.md
```

### B2 — LWW Causal Register

```
ROLE: You are a CRDT/replication specialist reviewing a Last-Writer-Wins register implementation with causal awareness.

SEED CONTEXT: Read /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-holistic-code.md
This contains one engineer's independent code-level assessment. Use it as a starting point, but form your OWN assessment.

COMPARTMENTALIZATION: Do NOT run any `gh` commands. Do NOT read any file named *digest* or *pr-comments*. Do NOT access GitHub in any way. Your only external context is the holistic review above.

MANDATE: Deep review of the LWW property backend and layer comparison logic.

FOCUS AREAS:
1. The committed/pending/uncommitted model in lww.rs — are state transitions correct? Can states get stuck?
2. Per-property resolution: when DivergedSince produces two chains, does the LWW resolver correctly pick the winner for EACH property independently?
3. Depth-based precedence: "further from meet wins." Trace through cases:
   - Property written at depth 3 on branch X, depth 20 on branch Y → Y should win
   - Property written on only one branch (empty other chain) → writer should win
   - Property written at equal depth on both branches → lexicographic tiebreak
   - Property written multiple times on one branch → only the latest matters
4. CRITICAL — Cross-replica convergence: If replica R1 sees events in order [A, B, C] and replica R2 sees [C, A, B], do they converge to the same state? Trace through the actual resolution code.
5. The layer comparison API in layers.rs — does it correctly extract per-property operation metadata from the chains?
6. Interaction between LWW resolution and Yrs (CRDT text) backend — are they handled consistently?

OUTPUT: Write to /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-matrix-B2-lww-register.md
```

### B3 — Spec Conformance

```
ROLE: You are a technical specification auditor. Your job is to verify that the implementation matches the specification, and that the specification is complete.

SEED CONTEXT: Read /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-holistic-code.md
This contains one engineer's independent code-level assessment. Use it for context on the code structure, but your job is specifically about spec↔code alignment.

COMPARTMENTALIZATION: Do NOT run any `gh` commands. Do NOT read any file named *digest* or *pr-comments*. Do NOT access GitHub in any way. Your only external context is the holistic review above.

MANDATE: Line-by-line cross-reference of spec.md against the implementation.

FOCUS AREAS:
1. Read specs/concurrent-updates/spec.md thoroughly first
2. For EACH algorithm step described in the spec, find the corresponding code and verify they match
3. For EACH variant of AbstractCausalRelation described in the spec, verify the code handles it as documented
4. For EACH resolution rule (depth precedence, lexicographic tiebreak, empty chain handling), verify the code implements exactly what the spec says
5. Identify anything the code does that the spec DOESN'T describe (undocumented behavior)
6. Identify anything the spec promises that the code DOESN'T implement (missing behavior)
7. Check test coverage against spec claims — does the spec claim behaviors that aren't tested?
8. Are there ambiguities in the spec that the code resolves in a particular way without documentation?

OUTPUT: Write to /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-matrix-B3-spec-conformance.md
```

### B4 — Adversarial / Edge Cases

```
ROLE: You are a security-minded adversarial tester. Your job is to try to BREAK this implementation — find inputs, topologies, timing, and edge cases that produce incorrect behavior.

SEED CONTEXT: Read /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-holistic-code.md
This contains one engineer's independent code-level assessment. Use it to understand the code structure, then go beyond it — your job is to find what the reviewer MISSED.

COMPARTMENTALIZATION: Do NOT run any `gh` commands. Do NOT read any file named *digest* or *pr-comments*. Do NOT access GitHub in any way. Your only external context is the holistic review above.

MANDATE: Actively try to break the implementation. Construct failure scenarios. You have NO knowledge of what prior PR reviewers discussed — approach this with completely fresh eyes.

FOCUS AREAS:
1. CRITICAL — Extreme depth asymmetry: What happens with 1 vs 1000 depth branches? Does BFS budget blow up? Does memory usage explode? Does depth precedence still produce correct results?
2. Cascading diverge-then-reconverge: A diverges into B and C, then B and C each diverge further, then reconverge at different points. Does the meet-point finder handle nested diamonds?
3. TOCTOU retry loop in entity.rs: Is there a termination guarantee? Can concurrent writers cause infinite retry? What's the worst-case retry count?
4. Race conditions: Two threads calling apply_state simultaneously with events that reference each other's events. Can you construct a deadlock or inconsistent state?
5. Malicious/malformed events: What if an event claims parents that don't exist? What if the event graph has cycles? What if event IDs collide?
6. Resource exhaustion: Can an attacker craft a DAG topology that causes O(n²) or worse comparison time? What's the worst-case memory usage of chain construction?
7. Multiple properties with mixed backends (LWW + Yrs): concurrent updates to different properties on the same entity with different backends — any interaction bugs?
8. The "empty chain" edge case in LWW: when one branch never wrote to a property but the resolution still needs to handle it.

For each potential issue, trace through the actual code to determine if it's a real bug or handled correctly.

OUTPUT: Write to /Users/daniel/ak/ankurah-201/specs/concurrent-updates/review-matrix-B4-adversarial.md
```
