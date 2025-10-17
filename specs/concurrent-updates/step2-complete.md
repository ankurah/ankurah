## Step 2 Complete: ForwardView Implementation and Integration

### Summary

Successfully implemented the complete ForwardView infrastructure for deterministic concurrent update resolution, replacing the previous forward_chain approach with a proper ReadySet streaming architecture.

### What Was Built

#### 1. Core ForwardView Types (`core/src/causal_dag/forward_view.rs`)

- **`ForwardView<E: TEvent>`**: Immutable DAG slice from meet to union of {subject, other} heads

  - Stores closed set of events between meet → subject and meet → other
  - Precomputes `primary_path` via reverse-BFS from subject_head to meet
  - Provides `iter_ready_sets()` for topological iteration

- **`ReadySet<E>`**: Causally-ready batch of events at a topological layer

  - Each event tagged as `Primary` (on subject path) or `Concurrency` (concurrent branch)
  - Iterators: `all_events()`, `primary_events()`, `concurrency_events()`

- **`ReadySetIterator`**: Streams ReadySets using Kahn's algorithm

  - Maintains in-degree counts for topological ordering
  - Emits batches of causally-ready events layer by layer

- **Unit tests**: 7 tests covering linear history, divergence, merges, complex DAGs, empty views

#### 2. Comparison Infrastructure Updates (`core/src/causal_dag/compare.rs`)

- **Event caching**: Added `events_by_id` and `parents_by_id` to cache all fetched events during traversal

  - Both subject and other sides cached for complete closed set construction
  - Enables efficient post-meet filtering without re-fetching

- **`build_forward_view()`**: Filters cached events to closed set via reverse-walk

  - Takes meet, subject_head, other_head as parameters
  - Reverse-walks from both heads back to meet to identify closed set
  - Returns ForwardView with filtered events

- **Updated `compare()` API**: Returns `RelationAndChain` with both:

  - `forward_chain: Vec<Event>` (legacy, backward compat, subject-only)
  - `forward_view: Option<ForwardView>` (new, complete closed set)
  - Built for `StrictDescends` and `DivergedSince`

- **Updated `compare_unstored_event()`**: Passes through ForwardView from parent comparison

#### 3. PropertyTransaction Protocol (`core/src/property/backend/transaction.rs`)

- **`PropertyTransaction<E: TEvent<Id = EventId>>`**: Generic trait for backend transactions

  - `apply_ready_set(&mut self, &ReadySet<E>)`: Apply a causally-ready batch
  - `commit(&mut self)`: Finalize changes (takes &mut for object safety)
  - `rollback(&mut self)`: Discard changes (takes &mut for object safety)

- **Object safety preserved**: `PropertyBackend::begin()` returns `Box<dyn PropertyTransaction<Event>>`

#### 4. Backend Implementations

- **`YrsTransaction`** (`core/src/property/backend/yrs.rs`):

  - ✅ Fully implemented: accumulates operations from all events, commits via backend.apply_operations
  - Idempotent: duplicates are safe due to Yrs CRDT properties

- **`LWWTransaction`** (`core/src/property/backend/lww.rs`):
  - Stub with `base_registers` and `pending_winners` fields
  - `last_writer_event_id` added to `ValueRegister` for causal tiebreaking
  - ⚠️ apply_ready_set and commit logic not yet implemented

#### 5. Entity Application (`core/src/entity.rs`)

- **DivergedSince handling**:
  - Begins PropertyTransaction for each backend
  - Iterates through ForwardView ReadySets, applying to all transactions
  - Commits all transactions atomically with CAS head check
  - Updates head to subject frontier (deterministic resolution result)
  - Legacy augment fallback if ForwardView unavailable (to be removed)

#### 6. EventBridge Collection (`core/src/node.rs`)

- **`collect_event_bridge()`**:
  - Uses ForwardView ReadySets instead of EventAccumulator
  - Collects only Primary events (subject-side path)
  - Cleaner implementation without conditional accumulation during traversal

### Integration Tests

- ✅ `test_forward_view_from_strict_descends`: ForwardView construction from comparison
- ✅ `test_forward_view_from_diverged`: Primary/Concurrency tagging verification
- ✅ `test_forward_view_with_complex_merge`: Merge node handling
- ✅ `test_forward_view_empty_for_equal`: Empty ForwardView for Equal relation
- ✅ All 100 existing tests pass (backward compatibility maintained)

### Architectural Decisions

1. **Closed set materialization**: Filter to exact closed set after meet is known (prevents over-collection)
2. **Primary path precomputation**: O(V+E) reverse-BFS once in ForwardView::new; ReadySetIterator stays cheap
3. **Generic PropertyTransaction**: Parameterized over Event type for test flexibility (ankurah_proto::Event in production)
4. **Mutable trait methods**: commit/rollback take &mut self for trait object compatibility
5. **Backward compatibility**: Keep forward_chain field alongside forward_view for gradual migration

### What's Next

Per the original plan order:

1. Add failing tests in lww.rs that directly construct ForwardViews with various histories
2. Implement Causal LWW tiebreaking in apply_ready_set (lineage precedence + lexicographic)
3. Implement LWWTransaction commit (diff, events table, register merge, serialization)
4. Remove legacy augment fallback and reverse-apply hack after verification
