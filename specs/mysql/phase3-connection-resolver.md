# Connection Resolver and Multi-Tenancy

## Overview

Multi-tenant deployments need to route storage operations to different MySQL databases based on context (e.g., which tenant the current request is for). Ankurah provides the plumbing for this via two changes:

1. **StorageEngine trait change**: `collection()` receives `PA::ContextData` so the storage engine can make routing decisions based on the current context. The type binding is enforced at compile time.
2. **Injectable connection resolver**: The MySQL storage engine accepts an application-provided resolver that maps context data to a MySQL connection pool. The resolver implementation is application-specific — ankurah defines the trait, the application provides the logic.

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
    async fn collection(&self, id: &CollectionId, context: Option<&C>)
        -> Result<Arc<dyn StorageCollection>, RetrievalError>;
    async fn delete_all_collections(&self) -> Result<bool, MutationError>;
}
```

`Option<&C>` because some operations have no context (system-level operations, peer sync). The storage engine must have a sensible default or return an error if context is required but absent.

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
    context: Option<&C>,
) -> Result<StorageCollectionWrapper, RetrievalError>
```

`NodeAndContext` (which holds both `node` and `cdata`) passes `Some(&self.cdata)` when calling through to the collection set. Code paths without context (peer sync, system operations) pass `None`.

### Existing Storage Engines

Engines that don't use context (Postgres, SQLite, Sled, IndexedDB) implement `StorageEngine<C>` for all `C`:

```rust
#[async_trait]
impl<C: Send + Sync + 'static> StorageEngine<C> for Postgres {
    type Value = PGValue;

    async fn collection(
        &self,
        id: &CollectionId,
        _context: Option<&C>,
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
    /// Fallback pool for operations without context (system tables, etc.)
    default_pool: Option<mysql_async::Pool>,
}

#[async_trait]
impl<R: ConnectionResolver> StorageEngine<R::Context> for MultiTenantMySQL<R> {
    type Value = MySQLValue;

    async fn collection(
        &self,
        id: &CollectionId,
        context: Option<&R::Context>,
    ) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        let pool = match context {
            Some(ctx) => self.resolver.resolve(ctx, id).await?,
            None => {
                self.default_pool.clone()
                    .ok_or_else(|| RetrievalError::StorageError(
                        "No context provided and no default pool configured".into()
                    ))?
            }
        };

        // Proceed with pool — same as single-tenant MySQL::collection()
        // (validate name, get schema, create bucket, DDL lock, create tables, etc.)
    }
}
```

### Compile-Time Type Binding

When constructing a Node, the compiler enforces that the resolver's context type matches the policy agent's context data:

```rust
// Application code:
let resolver = MyAppResolver::new(/* ... */);
let engine = MultiTenantMySQL::new(resolver, None);
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

The multi-tenant storage engine handles this internally — it returns different `MysqlBucket` instances backed by different pools for different contexts. `CollectionSet`'s cache (keyed by `CollectionId` alone) would incorrectly return a cached collection for the wrong tenant.

Options:
- **Option A**: `CollectionSet` includes context in its cache key. Requires `C: Hash + Eq`.
- **Option B**: The multi-tenant storage engine manages its own per-tenant cache, and `CollectionSet` is bypassed or made context-aware.
- **Option C**: `CollectionSet` does not cache when context is `Some` (always delegates to the storage engine, which caches internally).

Option C is the simplest and avoids leaking tenancy concerns into the core. The storage engine's resolver is already expected to cache pools.

## Pool Management

The resolver is expected to cache pools internally. Creating a new `mysql_async::Pool` for every `collection()` call would be expensive. A typical resolver implementation maintains a map of pools keyed on whatever routing key it derives from context.

Pool lifecycle management (creation, idle eviction, max connections per tenant) is the resolver's responsibility, not ankurah's.

Recommended defaults for resolver implementations:
- Max connections per tenant pool: 5
- Idle timeout: 5 minutes
- Pool-per-tenant with database in connection string (not `USE DATABASE`, which changes connection state and interacts poorly with pooling)

## What Is NOT Specified Here

- **The concrete `PA::ContextData` type for any specific application.** That's application code.
- **The concrete `ConnectionResolver` implementation.** That's application code. An application using metadata-driven routing would implement a resolver that queries its metadata database. An application with static config would read a file. Ankurah doesn't dictate this.
- **The specific fields in `PA::ContextData`** (user ID, client ID, etc.). The resolver receives the full context and extracts whatever it needs.
