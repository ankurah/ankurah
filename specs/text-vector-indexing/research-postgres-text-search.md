# Research: PostgreSQL Text Search Capabilities

PostgreSQL is the tier-1 backend — its capabilities define the operator semantics.

## Two Distinct GIN-Based Approaches

PostgreSQL's GIN (Generalized Inverted Index) supports two text search strategies via different operator classes:

### 1. pg_trgm — Trigram Substring Matching

Breaks text into 3-character overlapping sequences. Best for `CONTAINS` (arbitrary substring) semantics.

**Setup:**
```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_name_trgm ON table USING GIN (name gin_trgm_ops);
```

**Query acceleration:**
```sql
-- These automatically use the GIN trigram index:
SELECT * FROM table WHERE name ILIKE '%pattern%';
SELECT * FROM table WHERE name LIKE '%pattern%';
```

**Operators:** `%` (similarity), `<%` (word similarity), `<->` (distance)

**Key advantage:** Works for `%middle%` patterns — no left-anchoring required (unlike B-tree).

### 2. tsvector/tsquery — Full-Text Search

Preprocesses documents into normalized lexemes with positions. Best for word-level search with linguistic features.

**Setup:**
```sql
CREATE INDEX idx_fts ON docs USING GIN (to_tsvector('english', content));
```

**Query:**
```sql
SELECT * FROM docs WHERE to_tsvector('english', content) @@ to_tsquery('english', 'word & phrase');
SELECT *, ts_rank(to_tsvector('english', content), query) AS rank FROM docs ORDER BY rank DESC;
```

**Features:** Stemming, stop words, phrase search (`<->`), Boolean operators (`& | !`), relevance ranking (`ts_rank`, `ts_rank_cd`).

## Mapping to ankQL Operators

| ankQL Operator | Semantics | Postgres Translation | Index |
|---|---|---|---|
| `CONTAINS 'text'` | Case-insensitive substring | `col ILIKE '%text%'` | GIN + gin_trgm_ops |
| `ICONTAINS 'text'` | Same (alias) | Same | Same |
| `MATCHES 'words'` (future) | Word-level FTS | `to_tsvector('simple', col) @@ to_tsquery('simple', 'words')` | GIN + tsvector_ops |

## SQL Builder Integration Points

**Current predicate pushdown** (`storage/postgres/src/sql_builder.rs`):
- `split_predicate_for_postgres()` classifies pushable vs post-filter
- `can_pushdown_expr()` checks expression types
- JSONB multi-step paths use `->` operator

**For CONTAINS:**
- Mark as pushdown-capable in `can_pushdown_expr()`
- Generate: `position(lower($1) in lower("col")) > 0` (ICONTAINS)
- Or: `"col" ILIKE '%' || $1 || '%'` (with proper escaping of `%` and `_` in the pattern)

## Co-existence

A single table can have multiple index types simultaneously:
```sql
CREATE INDEX idx_category ON docs USING BTREE (category);        -- scalar
CREATE INDEX idx_content ON docs USING GIN (to_tsvector('english', content));  -- FTS
CREATE INDEX idx_embedding ON docs USING HNSW (embedding vector_cosine_ops);   -- vector
```

Postgres query planner estimates selectivity per predicate and chooses the best index path.
