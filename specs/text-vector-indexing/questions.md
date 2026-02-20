# Open Questions

## Operator Design

1. ~~**How do distance/scoring expressions compose with boolean predicates?**~~
   **RESOLVED**: WHERE is boolean, ORDER BY is scoring. Function expressions (`distance()`, `relevance()`) work in both — WHERE adds threshold comparison (`< 0.5`), ORDER BY uses raw float value. Grammar needs `FunctionCall` and `VectorLiteral` rules. See `research-scoring-composition.md`.

2. ~~**How does text relevance ranking work?**~~
   **RESOLVED**: CONTAINS is purely boolean. For text relevance ranking, use `relevance(field, query)` function in ORDER BY. Explicit over implicit — matches Postgres `ts_rank()` pattern.

3. **Can we combine multiple index types in one query?**
   - E.g., `WHERE category = 'news' AND description CONTAINS 'urgent' ORDER BY distance(embedding, query) LIMIT 10`
   - Current planner picks ONE index. Rest goes to `remaining_predicate`.
   - Should we support bitmap index intersection (like Postgres)?
   - Or is "pick best index + post-filter" sufficient for now?

## Tokenization

4. **Fixed or pluggable tokenizer?**
   - MVP: whitespace + lowercase (simple, deterministic)
   - Future: language-specific stemming, stop words, custom tokenizers
   - Pluggable adds complexity to serialization (need to persist tokenizer config)

5. **Token normalization across query and index?**
   - Search string must be tokenized the same way as indexed text
   - If index uses lowercase, query must lowercase too
   - Normalization must be deterministic and identical on all backends

## Vector

6. **Which distance metrics to support initially?**
   - Postgres pgvector supports: L2, Cosine, Inner Product, L1
   - MVP: Cosine only? Or L2 + Cosine?
   - More metrics = more code paths but same architecture

7. **Vector dimensions — fixed or flexible per entity?**
   - Fixed per collection (schema metadata) is simpler and safer
   - Variable dimensions would require runtime checks and prevent indexing

## Architecture

8. **Plan ranking — pick first viable or estimate selectivity?**
   - Current: pick first viable plan
   - Better: estimate selectivity (text index likely more selective than table scan)
   - Full cost-based optimization is complex — defer?

9. **Index backfill — sync or async?**
   - Current: lazy backfill on first query (blocking)
   - Text indexes may be large — async with progress tracking?
   - Vector indexes with HNSW graph need bulk build

10. **Backward compatibility for IndexRecord serialization?**
    - Existing sled databases have `KeySpec`-only IndexRecords
    - Auto-upgrade to `IndexSpec::Composite` on load?
    - Version tag in serde format for future evolution?
