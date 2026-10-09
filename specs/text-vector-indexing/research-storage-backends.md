# Research: Per-Backend Text Search Implementation

## Backend Comparison

| Backend | Type | Native Text Search | Native Vector Search | Index Strategy |
|---------|------|-------------------|---------------------|----------------|
| **PostgreSQL** | SQL/relational | ILIKE, pg_trgm GIN, tsvector FTS | pgvector HNSW/IVFFlat | Native — CREATE INDEX |
| **SQLite** | SQL/embedded | instr(), LIKE, FTS5 | None | FTS5 virtual tables |
| **Sled** | KV/embedded | None | None | Custom inverted index trees |
| **IndexedDB** | KV/browser | None | None | Custom object stores |

## PostgreSQL

**CONTAINS translation:** `position(lower($1) in lower("col")) > 0` or `"col" ILIKE '%' || $1 || '%'`

**Index:** GIN + gin_trgm_ops for substring search. Native, managed by Postgres.

**Predicate pushdown:** Fully pushable — add to `can_pushdown_expr()` and `comparison_op_to_sql()` in `sql_builder.rs`.

**Key files:** `storage/postgres/src/sql_builder.rs`, `storage/postgres/src/lib.rs`

## SQLite

**CONTAINS translation:** `instr(lower("col"), lower(?)) > 0`

**Index options:** FTS5 virtual tables for word-level search, or no index for simple substring (instr is O(n)).

**Predicate pushdown:** Pushable — extend `split_predicate_for_sqlite()`.

**Schema:** State table stores entities with dynamic columns. DDL lock protects concurrent changes.

**Key files:** `storage/sqlite/src/sql_builder.rs`, `storage/sqlite/src/engine.rs`

## Sled

**CONTAINS evaluation:** Application-level `value.to_string().to_lowercase().contains(&needle.to_lowercase())`

**Index:** Custom inverted index tree — `text_index_{field}` with key = `[token_bytes][0x00][entity_id]`, value = empty.

**Maintenance:** Tokenize on write via `update_indexes_for_entity()`. Set diff for incremental updates.

**Query execution:** Planner generates `Plan::TextIndex`, scanner does token prefix scan using existing prefix-guard pattern.

**Existing index infrastructure:** Sophisticated — composite key indexes with lazy backfill, `IndexManager`, `SledIndexScanner`. Text index plugs into this directly.

**Key files:** `storage/sled/src/index.rs`, `storage/sled/src/scan_index.rs`, `storage/sled/src/collection.rs`

## IndexedDB-WASM

**CONTAINS evaluation:** Same Rust string ops as sled, running in WASM.

**Index:** Separate object store with compound keys `[collection, field, token, entity_id]`.

**Constraints:** Single-threaded JS event loop, no native C libraries, WASM binary size matters (~150KB compressed currently).

**Scanner:** `IdbIndexScanner` with IdbKeyRange + cursor-based traversal. Prefix guard terminates scan.

**Key files:** `storage/indexeddb-wasm/src/collection.rs`, `storage/indexeddb-wasm/src/scanner.rs`

## WASM-Compatible Crates

| Crate | Purpose | WASM Safe | Size Impact |
|-------|---------|-----------|-------------|
| `unicode-segmentation` | Tokenization | Yes | ~15KB |
| `rust-stemmers` | Stemming (future) | Yes | ~20KB/language |
| Manual f32 ops | Vector distance | Yes | 0 |
