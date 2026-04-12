# Connection Resolver and Multi-Tenancy

## Overview

Multi-tenant deployments need to route storage operations to different MySQL databases based on context (e.g., which tenant the current request is for). Ankurah provides the plumbing for this via two changes:

1. **StorageEngine trait change**: `collection()` receives `PA::ContextData` so the storage engine can make routing decisions based on the current context. The type binding is enforced at compile time. Context is always required (no `Option`) — system operations use a default context.
2. **Injectable connection resolver**: The MySQL storage engine accepts an application-provided resolver that maps context data to a MySQL connection pool. The resolver implementation is application-specific — ankurah defines the trait, the application provides the logic.

## ContextData Gets a Default Bound

The `ContextData` trait gains a `Default` bound:

```rust
pub trait ContextData: Send + Sync + Clone + Hash + Eq + Default + 'static {}
```

This provides a root/system context for operations that aren't user-initiated (system catalog loading, peer sync, subscription handlers). `SystemManager` and other internal code uses `C::default()` as context.

For single-tenant deployments with `()` as context, `Default` is already implemented. Multi-tenant applications implement `Default` to return a system/root context that resolves to the default pool.

## StorageEngine Trait Change

Currently, `StorageEngine::collection()` receives only a `CollectionId`:

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync {
    type Value;
    async fn collection(&self, id: &CollectionId)
        -> Result<Arc<dyn StorageCollection>, RetrievalError>;
}
```

### New Signature

Make `StorageEngine` generic over the context type, defaulting to `()`:

```rust
#[async_trait]
pub trait StorageEngine<C: Send + Sync + 'static = ()>: Send + Sync {
    type Value;
    async fn collection(&self, id: &CollectionId, context: &C)
        -> Result<Arc<dyn StorageCollection>, RetrievalError>;
    async fn delete_all_collections(&self) -> Result<bool, MutationError>;
}
```

Context is always required — no `Option`. Every code path provides context:
- `NodeAndContext` passes `&self.cdata` (user context).
- `SystemManager`, peer sync, subscription handlers pass `&C::default()` (root context).

### Type Binding Through the System

`Node<SE, PA>` binds the storage engine's context type to the policy agent's context data:

```rust
pub struct Node<SE, PA>(pub(crate) Arc<NodeInner<SE, PA>>)
where
    PA: PolicyAgent,
    SE: StorageEngine<PA::ContextData>;
```

`CollectionSet` forwards the context:

```rust
pub async fn get(
    &self,
    id: &CollectionId,
    context: &C,
) -> Result<StorageCollectionWrapper, RetrievalError>
```

Call sites that currently call `collections.get(id)` without context (e.g., `SystemManager::load_system_catalog`, `SystemManager::create`, peer sync in `node_applier.rs`, subscription handlers in `peer_subscription/server.rs`) are updated to pass `&C::default()`.

### Existing Storage Engines

Engines that don't use context (Postgres, SQLite, Sled, IndexedDB) implement `StorageEngine<C>` for all `C`:

```rust
#[async_trait]
impl<C: Send + Sync + 'static> StorageEngine<C> for Postgres {
    type Value = PGValue;

    async fn collection(
        &self,
        id: &CollectionId,
        _context: &C,
    ) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        // existing implementation unchanged — context ignored
    }
}
```

This is a mechanical change to each existing engine: add the generic parameter, add `_context` to `collection()`.

## Connection Resolver Trait

```rust
/// Application-provided connection resolver. Receives context data and
/// returns a MySQL connection pool for the appropriate database.
///
/// The implementation is entirely application-specific. Examples:
/// - Query a metadata database to map (client_id) -> connection string
/// - Read from a static configuration file
/// - Use environment variables for dev/test
#[async_trait]
pub trait ConnectionResolver: Send + Sync + 'static {
    /// The context type this resolver expects.
    /// Bound to PA::ContextData at the Node level.
    type Context: Send + Sync + 'static;

    /// Resolve a connection pool for the given context and collection.
    /// The resolver may use any information from the context (tenant ID,
    /// user ID, etc.) to determine which database to connect to.
    async fn resolve(
        &self,
        context: &Self::Context,
        collection: &CollectionId,
    ) -> Result<mysql_async::Pool, RetrievalError>;
}
```

## Multi-Tenant MySQL Storage Engine

```rust
pub struct MultiTenantMySQL<R: ConnectionResolver> {
    resolver: R,
}

#[async_trait]
impl<R: ConnectionResolver> StorageEngine<R::Context> for MultiTenantMySQL<R>
where R::Context: Default
{
    type Value = MySQLValue;

    async fn collection(
        &self,
        id: &CollectionId,
        context: &R::Context,
    ) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        let pool = self.resolver.resolve(context, id).await?;

        // Proceed with pool — same as single-tenant MySQL::collection()
        // (validate name, get schema, create bucket, DDL lock, create tables, etc.)
    }
}
```

The resolver receives context on every call, including system operations (which pass `R::Context::default()`). The resolver is responsible for mapping the default context to the appropriate system/default pool.

### Compile-Time Type Binding

When constructing a Node, the compiler enforces that the resolver's context type matches the policy agent's context data:

```rust
// Application code:
let resolver = MyAppResolver::new(/* ... */);
let engine = MultiTenantMySQL::new(resolver);
let node = Node::new(Arc::new(engine), my_policy_agent);
// Node<MultiTenantMySQL<MyAppResolver>, MyPolicyAgent>
//
// Compiler checks:
//   MultiTenantMySQL<MyAppResolver>: StorageEngine<MyPolicyAgent::ContextData>
//   which requires: MyAppResolver::Context == MyPolicyAgent::ContextData
```

If the resolver's context type doesn't match the policy agent's context data, the code fails to compile. No runtime downcasting.

## CollectionSet Caching

`CollectionSet` currently caches collections by `CollectionId`. With multi-tenancy, the same `CollectionId` (e.g., "order") may resolve to different databases depending on context.

The multi-tenant storage engine handles this internally — it returns different `MysqlBucket` instances backed by different pools for different contexts. Since `ContextData` already requires `Hash + Eq`, `CollectionSet` can include context in its cache key: `(CollectionId, C)`. For single-tenant engines where `C = ()`, this adds no overhead (all lookups use the same `()` key).

For multi-tenant engines, this means different tenant contexts get separate cached collections, which is correct — they're backed by different database pools.

## Pool Management

The resolver is expected to cache pools internally. Creating a new `mysql_async::Pool` for every `collection()` call would be expensive. A typical resolver implementation maintains a map of pools keyed on whatever routing key it derives from context.

Pool lifecycle management (creation, idle eviction, max connections per tenant) is the resolver's responsibility, not ankurah's.

Recommended defaults for resolver implementations:
- Max connections per tenant pool: 5
- Idle timeout: 5 minutes
- Pool-per-tenant with database in connection string (not `USE DATABASE`, which changes connection state and interacts poorly with pooling)

## Single-Tenant vs Multi-Tenant MySQL

Both engines coexist:
- `MySQL` — the simple Phase 1 engine. Implements `StorageEngine<C>` for all `C` (ignores context). For single-tenant or single-database deployments.
- `MultiTenantMySQL<R>` — wraps a `ConnectionResolver` for tenant-aware routing. Implements `StorageEngine<R::Context>`. Shares `MysqlBucket` and the SQL builder with `MySQL`.

## What Is NOT Specified Here

- **The concrete `PA::ContextData` type for any specific application.** That's application code.
- **The concrete `ConnectionResolver` implementation.** That's application code. An application using metadata-driven routing would implement a resolver that queries its metadata database. An application with static config would read a file. Ankurah doesn't dictate this.
- **The specific fields in `PA::ContextData`** (user ID, client ID, etc.). The resolver receives the full context and extracts whatever it needs.
