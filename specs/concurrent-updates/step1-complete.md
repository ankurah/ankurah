## Step 1 Complete: Traits and Structs Created

### ✅ Completed Structures

1. **ForwardView** (`core/src/causal_dag/forward_view.rs`)

   - Immutable forward subgraph (DAG slice) from meet to target heads
   - Streams ReadySets in topological order using Kahn's algorithm
   - Tags events as Primary (subject path) or Concurrency (concurrent with Primary)
   - Memory-efficient iterator-based design

2. **ReadySet** (`core/src/causal_dag/forward_view.rs`)

   - Causally-ready batch at a topological layer
   - All events have parents already in applied set
   - Events tagged with `EventRole::{Primary, Concurrency}`
   - Helper methods for filtering by role

3. **EventRole** (`core/src/causal_dag/forward_view.rs`)

   - Enum: `Primary` | `Concurrency`
   - Distinguishes subject path from concurrent branches

4. **PropertyTransaction** trait (`core/src/property/backend/transaction.rs`)

   - Ephemeral per-backend session for applying ForwardView
   - Methods: `apply_ready_set()`, `commit()`, `rollback()`
   - Atomic commit/rollback semantics

5. **LWWTransaction** stub (`core/src/property/backend/lww.rs`)

   - Struct with `base_registers` and `pending_winners`
   - Implements `PropertyTransaction` trait
   - TODO stubs for causal LWW tiebreaking logic

6. **YrsTransaction** stub (`core/src/property/backend/yrs.rs`)

   - Struct with forked Yrs doc and backend reference
   - Implements `PropertyTransaction` trait
   - Applies all operations blindly (Yrs ops are idempotent)

7. **PropertyBackend trait updated** (`core/src/property/backend/mod.rs`)
   - Added associated type `Transaction: PropertyTransaction`
   - Added `begin_transaction()` method
   - Removed `fork()` from trait (not object-safe with associated type)
   - Moved `fork()` to impl blocks for LWWBackend and YrsBackend

### ⚠️ Known Issues & TODOs

1. **Entity refactoring needed** (`core/src/entity.rs`)

   - Current: `Arc<dyn PropertyBackend>` storage
   - Issue: PropertyBackend with Transaction associated type is not fully object-safe
   - Options to fix:
     - Use enum dispatch for backends
     - Box<dyn PropertyTransaction>
     - Store concrete types instead of trait objects
   - Documented with TODO comments in Entity

2. **backend_from_string temporarily returns Arc<dyn Any>**

   - Workaround until Entity is refactored
   - Old code using this function will need updates

3. **Event must implement Clone**
   - ForwardView requires Event: Clone for ReadySet cloning
   - This is satisfied by ankurah_proto::Event

### Exports

Updated `core/src/causal_dag/mod.rs` to export:

- `ForwardView`
- `ReadySet`
- `ReadySetIterator`
- `EventRole`
- `EventAccumulator` (existing)
- `LineageVisitor` (existing)
- `CausalRelation` (existing)

### Next Steps (Step 2)

Implement ForwardView with isolated unit tests in `forward_view.rs`:

- Test topological ordering (Kahn's algorithm)
- Test Primary vs Concurrency tagging
- Test with linear histories
- Test with branching histories
- Test with merge nodes
- Test ReadySet iteration completeness
