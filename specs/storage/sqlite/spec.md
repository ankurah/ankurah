# SQLite Storage Engine - Requirements

## Overview

A SQLite-based storage engine for Ankurah, providing a lightweight embedded database option that sits between Sled (pure KV) and Postgres (full SQL server). SQLite offers:

- Single-file database (portable, easy backup)
- Full SQL query capabilities without external server
- Native support on all platforms including mobile (iOS, Android)
- WASM compatibility via sql.js or similar

## Motivation

- **Gap in current offerings**: Sled is pure KV (no server-side filtering), Postgres requires external server
- **Mobile-friendly**: SQLite is the standard embedded database for mobile apps
- **Development convenience**: No server setup, single file, easy to inspect
- **WASM potential**: Can run in browsers via sql.js (alternative to IndexedDB)

## Functional Requirements

### Core Storage Operations

1. **Implements `StorageEngine` and `StorageCollection` traits** from `ankurah_core::storage`
2. **File-based and in-memory modes**: Support both persistent file storage and in-memory databases (for testing)
3. **Dynamic table creation**: Create tables per collection on demand
4. **State persistence**: Store entity state buffers, heads (clocks), and attestations

### Query Capabilities

1. **Query pushdown**: Support AnkQL predicates converted to SQL WHERE clauses
2. **Materialized columns**: Dynamically add columns for queryable CRDT fields
3. **JSONB querying**: Support querying nested JSON properties via SQLite JSON functions (using `json_extract()` for path traversal and `jsonb()` function for storage, providing type-aware comparisons)

### Data Format Compatibility

1. **ULID storage**: TEXT (base64) matching Postgres format
2. **State buffer**: BLOB (bincode-serialized BTreeMap)
3. **Head/Clock**: JSON array of ULID strings
4. **Attestations**: BLOB (bincode-serialized Vec)
5. **JSON values**: Stored as BLOB (using SQLite JSONB format via `jsonb()` function), queried via `json_extract()` for type-aware comparisons

### Performance & Reliability

1. **WAL mode**: Enable Write-Ahead Logging for better concurrent read performance
2. **Connection pooling**: Use `bb8` for async connection management
3. **DDL locking**: Serialize schema modifications to prevent race conditions
4. **Performance PRAGMAs**: Additional SQLite optimizations (cache_size, mmap_size, temp_store)

### Event Storage

1. **Event tables**: Create `{collection}_event` tables for event sourcing
2. **Event schema**: Store event id, entity_id, operations (BLOB), parent (JSON), attestations (BLOB)
3. **Entity index**: Index on entity_id for efficient event retrieval

## Non-Functional Requirements

1. **Async compatibility**: Work with tokio async runtime (via `spawn_blocking` for synchronous rusqlite)
2. **Cross-platform**: Support all platforms where rusqlite works (desktop, mobile)
3. **Testing**: Comprehensive unit and integration tests mirroring Postgres patterns

## Out of Scope (Deferred)

1. **WASM support**: Not needed for initial implementation
