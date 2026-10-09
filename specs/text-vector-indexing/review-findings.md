# Spec Review Findings

Three independent reviewers evaluated the spec. Below are the consolidated findings by severity.

## Critical Issues

### 1. extract_entries() doesn't fit vector indexes
The trait returns `Vec<(Vec<u8>, EntityId)>` — forcing vectors into byte keys. HNSW graph traversal operates on raw f32 vectors, not encoded byte keys. This abstraction leaks.

**Resolution options:**
- A) Split the trait: composite/text indexes share `extract_entries()`, vector indexes have a separate interface
- B) Polymorphic return: `IndexEntry` enum with `Composite(Vec<u8>)` / `Text(String, EntityId)` / `Vector(Vec<f32>)`
- C) Don't force a unified trait — use `IndexSpec` enum with match arms that call index-type-specific methods directly

### 2. Concurrent text index writes are not atomic
Updating a text field with 100 tokens requires 100+ sled tree operations (removes + inserts). A crash mid-sequence leaves a corrupted index. Individual sled ops are atomic, but the sequence is not.

**Resolution:** Batch all index changes in a single sled transaction. Or accept that index can be stale and rebuild on startup.

### 3. NULL semantics unspecified
Spec says nothing about:
- `NULL CONTAINS 'x'` → NULL? false? error?
- `distance(NULL_embedding, query, 'cosine')` → NULL? error?

**Resolution:** Document: NULL propagates (SQL three-valued logic). In WHERE context, NULL predicates filter out the row.

## Major Issues

### 4. Plan enum needs TextIndex + VectorSearch variants
Every `match plan` in every backend (sled, indexeddb, postgres, sqlite) will need new arms. This is a cascade of ~5-10 new match arms per backend.

**Resolution:** Accept this — it's unavoidable when adding new plan types. Phase 1 (trait refactor) should add the variants with `unimplemented!()` stubs.

### 5. HashSet diff performance on large text fields
A document with 10K words → 10K hash insertions + 10K lookups + up to 10K tree operations per write. Write-heavy workloads with large text fields will see p99 latency spikes.

**Resolution:** Sorted Vec diff is O(N) with less allocation overhead. Or: batch sled writes. For MVP, HashSet diff is fine — optimize later if profiling shows it's a bottleneck.

### 6. Lazy backfill blocks first query
Creating a text index on a 1M-document collection → first query triggers full scan + tokenize + index build. Could block for seconds.

**Resolution:** Add a timeout/budget. Or: make index creation explicit (not lazy). Document that `#[index(text)]` triggers background build on node startup.

### 7. Planner needs 3 separate code paths
Current planner only handles composite key bounds. Text (token sets) and vector (kNN) are fundamentally different query shapes.

**Resolution:** Refactor planner to iterate candidate indexes and delegate to `index_type.infer_bounds_from_predicate()`. Each index type owns its own plan generation logic.

### 8. SQL backends have divergent index lifecycle
KV backends use extract_entries() + set diff. SQL backends use native `CREATE INDEX` DDL. No unified abstraction.

**Resolution:** Accept this as inherent — SQL and KV index lifecycles ARE different. Document clearly. The `IndexType` trait is for KV backends only. SQL backends handle index creation/maintenance via their own DDL paths.

## High Priority (Must Document)

### 9. OrderByItem only accepts PathExpr, not Expr
Current AST: `OrderByItem { path: PathExpr, direction }`. Must change to `{ expr: Expr, direction }` for `ORDER BY distance(...)`.

### 10. Empty string CONTAINS behavior
`field CONTAINS ''` is always true (empty string is substring of everything). Document or disallow.

### 11. Unicode normalization for ICONTAINS
`'café' ICONTAINS 'cafe'` — depends on Unicode normalization form (NFC vs NFD). Document: "ICONTAINS uses simple lowercase, no Unicode normalization. NFC recommended for storage."

### 12. Double computation of distance() in WHERE + ORDER BY
`WHERE distance(...) < 0.5 ORDER BY distance(...)` computes distance twice. Acceptable for MVP; add deduplication optimization later.

### 13. OR with text/vector predicates
`WHERE a CONTAINS 'x' OR b CONTAINS 'y'` — planner can't use indexes for OR across different fields. Falls back to full scan + post-filter. Document this limitation.

### 14. Grammar: FunctionCall vs PathExpr ambiguity
`distance(` must parse as function call, not path expression. Ensure FunctionCall rule has priority over PathExpr in pest grammar.

## Minor Issues

### 15. extract_entries() allocation overhead
10K allocations per write for large text fields. Use preallocated buffers if profiling shows this matters.

### 16. Offset + distance ORDER BY is meaningless for kNN
Document: "OFFSET with kNN ordering computes top-(limit+offset) then discards. Use keyset pagination for efficiency."

### 17. CONTAINS on nested JSON paths
Spec doesn't address `data.description CONTAINS 'text'`. Document: works if the path resolves to a string value.
