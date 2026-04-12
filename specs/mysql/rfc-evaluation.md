# Critical Evaluation: MySQL Storage & CDC RFC

Evaluated against the ankurah codebase at commit 80524690.
RFC source: `retailops-rs/migration-research/rfc-mysql-storage-and-cdc.md`

---

## Scope Clarification

The RFC conflates several concerns that have very different timelines. Per Daniel's direction, the scope is:

**Phase 1 (this spec): MySQL Storage Engine**
A MySQL implementation of `StorageEngine` and `StorageCollection`, equivalent to the existing Postgres implementation. Same ankurah state model: serialized StateBuffers blob, head (Clock), event tables. Standard EntityId (ULID). This is the immediate deliverable.

**Phase 2 (future): CDC via MySQL binlog replication + injectable connection resolver**
Detecting external writes via binlog, creating synthetic ankurah events. Plus multi-tenancy via an injectable connection resolver that receives PolicyAgent::ContextData. Designed after Phase 1 is working.

**Out of scope entirely (for now):**
- Integer PK <-> EntityId bridging
- Migration of existing tables (adding `_ankurah_*` columns)
- Per-column mapping of model fields to existing table columns (the RFC's "relational mapping mode")
- Field/column renaming (`#[model(column = "...")]`)

The spec should describe functional requirements without referencing any specific application.

---

## 1. MySQL Storage Engine (Phase 1)

This is a port of the Postgres implementation to MySQL. The RFC correctly identifies the key SQL dialect differences.

### 1.1 Connection pooling: use mysql_async's built-in pool

The RFC proposes `bb8` + `MysqlConnectionManager`. But `mysql_async` has a built-in connection pool. The Postgres engine uses `bb8` because `tokio-postgres` doesn't include pooling — that's not the case here.

> **Daniel**: Agreed. bb8 was used for Postgres because that's the idiomatic way for tokio-postgres. mysql_async doesn't need it.

### 1.2 SQL injection in GET_LOCK

MySQL doesn't have Postgres-style `pg_advisory_lock(hash)`. The RFC uses MySQL's `GET_LOCK(name, timeout)` for DDL serialization (ensuring only one connection creates/alters tables at a time). The RFC's code builds the lock name via string interpolation:

```rust
format!("SELECT GET_LOCK('{}', 30)", name)
```

where `name` is derived from the collection_id. If a collection_id contained a single quote, this would break the SQL. The Postgres engine mitigates this with `sane_name()` validation (alphanumeric + underscore only), which the MySQL engine should also use. Defense-in-depth says parameterize the query too.

**Resolution**: Use `sane_name()` validation (already required) AND parameterize the query. Minor implementation detail.

### 1.3 AUTO_INCREMENT on non-PK column

The event table needs a `commit_seq` column that auto-increments for ordering. The RFC puts the PRIMARY KEY on `id` (the EventId) and makes `commit_seq BIGINT UNSIGNED AUTO_INCREMENT UNIQUE`. MySQL requires AUTO_INCREMENT columns to be indexed — the UNIQUE constraint satisfies this. This works in InnoDB/MySQL 8 but is worth a quick verification during implementation.

**Resolution**: Implementation detail, verify during development.

### 1.4 Key SQL dialect differences (from RFC, verified correct)

| Postgres | MySQL | Notes |
|----------|-------|-------|
| `BYTEA` | `MEDIUMBLOB` | BLOB limited to 64KB; MEDIUMBLOB supports 16MB |
| `character(43)[]` | `JSON` | MySQL has no native array type; Clock stored as JSON array |
| `$1`, `$2` params | `?` params | Positional placeholders |
| `ON CONFLICT DO UPDATE` | `ON DUPLICATE KEY UPDATE` | UPSERT syntax |
| `pg_advisory_lock()` | `GET_LOCK()` | Named lock API |
| `"column"` quoting | `` `column` `` quoting | Identifier quoting |
| `RETURNING` clause | Not supported | Need separate SELECT or LAST_INSERT_ID() |
| Native boolean | `TINYINT(1)` | Boolean representation |

### 1.5 Clock storage as JSON

Postgres stores `Clock` (a `Vec<EventId>`) as `character(43)[]`. MySQL has no native array type. The RFC proposes JSON arrays of base64 strings, which is correct.

### 1.6 Missing: RETURNING clause workaround

The Postgres `set_state` uses a CTE with `RETURNING` to atomically check the old head value:

```sql
WITH old_state AS (SELECT "head" FROM ... WHERE "id" = $1)
INSERT INTO ... ON CONFLICT("id") DO UPDATE SET ...
RETURNING (SELECT "head" FROM old_state) as old_head
```

MySQL doesn't support `RETURNING`. The RFC proposes using a transaction (BEGIN, SELECT old head, UPSERT, COMMIT). This works but changes the locking behavior — need to ensure the isolation level prevents phantom reads between the SELECT and UPSERT.

---

## 2. Future: CDC / Binlog Listener (Phase 2)

These are notes for future design, not Phase 1 deliverables.

### 2.1 Head-field discriminator is sound

The discriminator logic (ankurah writes change `_ankurah_head`; external writes don't) is race-free because it examines before/after images in each individual binlog event, not a live query.

### 2.2 Crash recovery needs refinement

The listener must extract the parent clock from the **binlog event's before-image** of `_ankurah_head`, not from a live query. This ensures deterministic EventId generation across crash recovery replays.

### 2.3 Synthetic LWW operations are constructible

The CDC listener can construct LWWDiff from column diffs deterministically. bincode serialization of BTreeMaps is deterministic (sorted by key).

### 2.4 mysql_async binlog API needs verification

The RFC assumes specific API patterns. Verify against the actual crate version during Phase 2.

---

## 3. Future: Multi-Tenancy / Connection Resolver (Phase 2)

### 3.1 StorageEngine is completely context-unaware

`StorageEngine::collection()` receives only `&CollectionId`. There is no mechanism to pass `PA::ContextData` or any context to the storage layer. The connection resolver needs ContextData to route to the correct database.

### 3.2 Approaches for threading context to storage

Daniel's intent: "an optional connection resolver function/trait impl which is injected into the mysql storage engine (which receives the PA::ContextData)."

How ContextData reaches the storage engine is an open design question for Phase 2. Options identified:
- Add opaque context to StorageEngine trait
- Per-tenant Node instances
- Task-local context
- Scoped context on the storage engine

---

## 4. Out of Scope

These items are noted but explicitly deferred:

- **Integer PK <-> EntityId bridging**: Significant design question, deferred entirely.
- **Existing table migration**: Adding `_ankurah_*` columns to existing tables. Will be handled by a migration script, not the storage engine itself.
- **Per-column mapping**: Mapping model fields to existing MySQL columns individually (the RFC's "relational mapping mode"). Deferred until after Phase 1 and Phase 2.
- **Field/column renaming** (`#[model(column = "...")]`): De-scoped.
- **YrsString on shared tables**: YrsString backend cannot round-trip through individual columns (CRDT state is opaque binary, not a simple value). Only relevant if per-column mapping is implemented. Deferred with that feature.

---

## Resolved Questions

### Materialized columns

Auto-materialize columns for query pushdown, same as Postgres. The Postgres engine already does this: when `set_state` encounters a property with no corresponding column, it runs `ALTER TABLE ADD COLUMN`. The MySQL engine should do the same.

Additionally: the `_ankurah_head` and `_ankurah_entity_id` columns (which Postgres creates as part of the state table schema) can also be auto-materialized. MySQL 8 supports **invisible columns** (`ALTER TABLE ADD COLUMN ... INVISIBLE`) which are excluded from `SELECT *` by default. This keeps ankurah's bookkeeping columns out of the way for anyone inspecting the table casually. Investigate whether invisible columns work correctly with the queries the storage engine generates (explicit column references in SELECT lists should still work).

### YrsString and materialized columns

The RFC's analysis of YrsString being incompatible with per-column storage was wrong. Materialized columns are materialized from `PropertyBackend::property_values()`, which returns `Value` variants (strings, ints, etc.) regardless of backend. A YrsString field materializes as a plain string column. The CRDT state lives in the serialized StateBuffers blob and event operations — the materialized column is just a projection of the current value for query pushdown. No backend-specific concerns here.

### Foreign keys and Ref<T>

YrsString should never be used for foreign keys. FKs use `Ref<T>` or `Option<Ref<T>>`, which are backed by LWW internally. This was never in question for Phase 1, but noted for clarity.

---

## Open Questions (Phase 1)

(none remaining — ready to draft spec)
