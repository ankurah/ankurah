# Research: PostgreSQL pgvector — Vector Search Capabilities

## Data Types

| Type | Max Dims | Storage | Use Case |
|------|----------|---------|----------|
| `vector(N)` | 2,000 | 32-bit floats | Standard embeddings |
| `halfvec(N)` | 4,000 (indexed) | 16-bit floats | Memory-constrained |
| `sparsevec` | 1,000 non-zero | Sparse format | High-dim sparse data |

Typical dimensions: OpenAI = 1,536; BERT = 768; newer models = 3,072.

## Distance Operators

| Operator | Metric | SQL Example |
|----------|--------|-------------|
| `<->` | L2 (Euclidean) | `ORDER BY embedding <-> '[0.1,0.2,...]' LIMIT 10` |
| `<=>` | Cosine distance | `ORDER BY embedding <=> '[0.1,0.2,...]' LIMIT 10` |
| `<#>` | Inner product (neg) | `ORDER BY embedding <#> '[0.1,0.2,...]' LIMIT 10` |
| `<+>` | L1 (Manhattan) | `ORDER BY embedding <+> '[0.1,0.2,...]' LIMIT 10` |

Distance operators return a float. Used in ORDER BY for kNN, or in WHERE with comparison for radius search.

## Index Types

**HNSW** (recommended):
- Multi-layer graph structure — greedy traversal
- Faster queries, higher recall, more memory
- No training required — works on empty tables
- v0.8.0+ iterative scanning prevents over-filtering

**IVFFlat:**
- Cluster centroids + flat scan within clusters
- Faster build, less memory
- Needs `REINDEX` as data changes significantly

**Threshold:** <10K vectors — brute force scan is fine, no index needed.

## Query Patterns

**Basic kNN:**
```sql
SELECT id, title FROM docs ORDER BY embedding <=> query_vector LIMIT 10;
```

**Filtered kNN:**
```sql
SELECT id FROM docs WHERE category = 'news' ORDER BY embedding <=> query LIMIT 10;
```

**Radius search (boolean):**
```sql
SELECT id FROM docs WHERE (embedding <=> query) < 0.5;
```

## Co-existence with Other Indexes

Common pattern — multiple indexes on one table:
```sql
CREATE INDEX idx_category ON docs USING BTREE (category);
CREATE INDEX idx_content ON docs USING GIN (to_tsvector('english', title));
CREATE INDEX idx_embedding ON docs USING HNSW (embedding vector_cosine_ops);
```

Postgres planner combines them via nested loops or bitmap scans.

## ankQL Integration

**Column type:** Add `PGValue::Vector(Vec<f32>)` variant, `postgres_type() → "vector(N)"`.

**DDL:** `ALTER TABLE "collection" ADD COLUMN "embedding" vector(1536)` — fits existing `add_missing_columns()` pattern.

**Query translation:** `distance(embedding, query, 'cosine')` → `embedding <=> $1` in Postgres SQL.

**Index creation:** `CREATE INDEX ... USING HNSW (embedding vector_cosine_ops)` — triggered by model annotation or explicit API.
