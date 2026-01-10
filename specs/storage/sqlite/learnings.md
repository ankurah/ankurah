# SQLite Storage Engine - Implementation Learnings

## Deviations from Original Spec

### Predicate Splitting Not Extracted to Common
The original plan suggested extracting predicate splitting logic to `storage/common` for reuse by Postgres. Instead, the logic was implemented directly in `sql_builder.rs` as SQLite-specific. This can be refactored later if Postgres needs similar functionality.

### Test Structure
Tests are organized as flat files (`sqlite.rs`, `sqlite_undefined_column.rs`, `sqlite_json_semantics.rs`) rather than a module directory, matching the pattern used by other storage engine tests.

## Key Learnings

### JSONB Storage and Querying
SQLite's JSONB support requires careful handling:
- **Storage**: Use `jsonb(?)` function during INSERT to store as BLOB in native JSONB format
- **Querying**: Use `json_extract("column", '$.path')` for path traversal
- **Type-aware comparisons**: `json_extract()` returns SQL-native types (integers, floats, strings), not JSON strings, enabling proper numeric comparisons (e.g., `9 < 10` works correctly, not lexicographically)

This differs from naive approaches that might use the `->` operator or compare JSON strings directly.

### Async/Sync Bridge
rusqlite is fundamentally synchronous. The implementation bridges to async via:
- `Arc<tokio::sync::Mutex<Connection>>` for thread-safe access
- `spawn_blocking` for all database operations
- bb8 pool manages connection lifecycle, but actual operations are sync

### DDL Locking is Essential
Without explicit DDL locking, concurrent writes discovering new properties would race on `ALTER TABLE ADD COLUMN`. The implementation uses a `tokio::sync::Mutex` per collection, with a pattern of:
1. Acquire lock
2. Re-check columns (another task may have added them)
3. Add missing columns
4. Rebuild cache
5. Release lock

### Schema Cache for Missing Columns
Queries referencing columns that don't exist yet (before any data is written) need graceful handling. The implementation:
1. Maintains a schema cache of existing columns
2. Filters predicates to only include existing columns in SQL
3. Treats missing columns as NULL (which fails most comparisons, returning empty results)
4. Refreshes cache after writes that may add columns

### Connection Pooling for In-Memory Databases
In-memory SQLite databases are tied to their connection - closing the connection destroys the database. The implementation uses a single-connection pool for in-memory mode to keep the database alive while still using the same pool-based API.

## Gotchas

### Boolean Storage
SQLite has no native boolean type. Booleans are stored as INTEGER (0/1) for materialized query columns. This only affects the storage layer, not the application-level boolean semantics.

### Event Table Index
The implementation creates an index on `entity_id` for event tables, which the Postgres implementation is missing (filed as issue #215).

### Cross-Type JSON Comparisons
SQLite's `json_extract()` does type coercion in some cases (e.g., number 9 may equal string "9"). The test suite in `predicate_checks.rs` validates that cross-type comparisons behave consistently across all storage backends.
