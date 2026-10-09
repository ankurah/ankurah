# Research: Scoring & Predicate Composition Design

## Problem Statement

ankQL needs to combine boolean predicates (WHERE) with continuous scoring/distance expressions (ORDER BY). How do other systems handle this, and what's the most elegant design?

## Patterns Across Major Systems

### Pattern 1: Separate Boolean from Scoring (PostgreSQL, SQLite)

PostgreSQL FTS: `@@` is boolean (WHERE), `ts_rank()` is scoring (ORDER BY):
```sql
WHERE to_tsvector('english', content) @@ to_tsquery('english', 'query')
ORDER BY ts_rank(to_tsvector('english', content), query) DESC
```

pgvector: distance operators work in both contexts:
```sql
WHERE (embedding <=> query_vec) < 0.5    -- boolean threshold
ORDER BY embedding <=> query_vec          -- distance sort
```

SQLite FTS5: `MATCH` is boolean, `rank`/`bm25()` is scoring:
```sql
WHERE docs MATCH 'query' ORDER BY rank
```

### Pattern 2: Filter vs Query Context (Elasticsearch)

- **filter** context: boolean, no scoring, cached — like WHERE
- **must/should** context: boolean AND scored — contributes to `_score`
- Distinct compilation paths: filters are cheap, queries compute relevance

```json
{
  "bool": {
    "filter": [{"term": {"category": "news"}}],
    "must": [{"match": {"content": "urgent"}}]
  }
}
```

### Pattern 3: Pre-filter then Score (Vector DBs)

Qdrant, Pinecone, Weaviate all:
- Apply metadata filters first (narrow candidate set)
- Score remaining candidates by vector similarity
- Filters never affect the score
- Hybrid search: combine multiple independent scores with fusion (RRF, weighted sum)

### Pattern 4: Implicit Score Field (MongoDB)

- `$meta: "vectorSearchScore"` — score is a metadata field you access
- Scores are computed by the search stage, not the filter stage
- Ordering is explicit: `{ $sort: { score: { $meta: "vectorSearchScore" } } }`

### Pattern 5: Scored Query vs Filter Query (Lucene/Solr)

- `q=` — scored query, contributes to relevance
- `fq=` — filter query, boolean only, cached
- Clear separation: filters narrow, queries score

## Common Principle

Every system separates two concerns:
1. **Filtering** — boolean, narrows candidates (WHERE)
2. **Scoring** — continuous, ranks candidates (ORDER BY)

Filters don't contribute to scores. Scores don't affect filtering. They compose via pipeline: filter first, then score the survivors.

## Recommended ankQL Design

### Core Principles

1. **All WHERE predicates are purely boolean** — they produce true/false, nothing more
2. **Scoring expressions live in ORDER BY** — `distance()`, `relevance()`, `bm25()` return floats
3. **Functions work in both contexts** — in WHERE, `distance(...) < 0.5` is a boolean threshold; in ORDER BY, `distance(...)` is a sort key
4. **CONTAINS is NOT a scoring operator** — it's boolean. For text relevance ranking, use `relevance()` in ORDER BY.
5. **Filters never affect scores** — WHERE narrows, ORDER BY ranks. Independent.

### Query Examples

```
-- Boolean-only (no scoring)
WHERE category = 'news' AND description ICONTAINS 'urgent'

-- Boolean filter + distance ordering
WHERE category = 'news'
ORDER BY distance(embedding, [0.1, 0.2, ...], 'cosine')
LIMIT 10

-- Distance threshold (boolean) + distance ordering
WHERE distance(embedding, [0.1, 0.2, ...], 'cosine') < 0.5
ORDER BY distance(embedding, [0.1, 0.2, ...], 'cosine')
LIMIT 10

-- Combined text filter + vector ordering
WHERE category = 'news' AND description ICONTAINS 'urgent'
ORDER BY distance(embedding, [0.1, 0.2, ...], 'cosine')
LIMIT 10

-- Weighted multi-signal scoring (future)
WHERE category = 'news'
ORDER BY distance(embedding, query, 'cosine') * 0.7 + relevance(description, 'search') * 0.3
LIMIT 10
```

### Execution Model

1. **WHERE phase**: Evaluate boolean predicates, narrow candidate set
   - Composite index: range scan
   - Text index: token lookup + intersect
   - Distance threshold: vector scan + threshold filter
2. **ORDER BY phase**: Compute scoring expressions on surviving candidates
   - `distance()`: compute vector distance per candidate
   - `relevance()`: compute text relevance per candidate
3. **LIMIT phase**: Take top-K from scored results

This matches how Postgres, Solr, and vector DBs all actually execute queries.

### Grammar Extension

Use **function expression syntax** (Option A from grammar research):
- Add `FunctionCall` rule: `Identifier "(" (Expr ("," Expr)*)? ")"`
- Add `VectorLiteral` rule: `"[" Float ("," Float)* "]"`
- Add `Expr::FunctionCall { name, args }` and `Literal::Vector(Vec<f64>)` to AST
- Extend `OrderByItem` to accept expressions, not just field paths

### Why This Design

- **Clarity**: No hidden scoring. WHERE = filter, ORDER BY = rank.
- **Composability**: Functions are first-class expressions. They work everywhere.
- **Performance**: Pre-filter before scoring matches real DB execution models.
- **Minimalism**: No special hybrid syntax. Just WHERE + ORDER BY + functions.
- **Extensibility**: New scoring functions (bm25, tfidf, custom) are just new functions.
