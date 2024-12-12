pub mod collation;
pub mod comparision_index;
pub mod operation;
pub mod reactor;
pub mod test_server;

// Punch list:
// [ ] subscriptions can be registered
// [ ] all comparisons in the recursive predicate are registered against their respective indexes (index watchers)
// [ ] membership of a subscription is tracked explicitly - otherwise we would have to check all indexes for each record edit
// [ ] membership index is checked for each record edit (record watchers)
// [ ] index watchers are checked for each field being changed in a record edit
// [ ] record watchers are managed as records become matching or no longer matching

// Design plan
// - Implement a basic implementation of index watcher that is separate from the underlying index that uses one btree for each comparison (lt,gt,eq)
//   We will need this for test cases, and possibly the browser implementation. Index watchers should ideally be integrated into the index,
//   but this requires additional effort to build a special BTreeMap implementation. So we'll shortcut this for now by keeping them separate from the field index
// - Create/extend a trait for index watchers that can be implemented by each storage engine.
//   For instance: sled has its own implementation of watchers, and other backends will have their own too.
