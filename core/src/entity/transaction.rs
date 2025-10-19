use std::sync::{atomic::AtomicBool, Arc};

use ankurah_proto::EntityId;

use super::WeakEntitySet;

/// Tracks commit state for an entity (lightweight, stored in entity state)
pub struct CommitState {
    pub(crate) committed: AtomicBool,
    pub(crate) completion: tokio::sync::broadcast::Sender<()>,
}

impl std::fmt::Debug for CommitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitState").field("committed", &self.committed.load(std::sync::atomic::Ordering::Acquire)).finish()
    }
}

/// Coordinates commit/rollback for entities in a transaction
///
/// Tracks newly created Primary entities (not Branch or modified entities).
/// Commits by marking the shared CommitState - no need to update individual entities.
/// Rollback (on drop without commit): Removes created entities from WeakEntitySet, then wakes waiters.
///
/// TODO: Also manage locks on Primary entities for transaction isolation
pub struct EntityTransaction {
    pub id: ulid::Ulid,
    commit_state: Arc<CommitState>,
    created_entity_ids: Vec<EntityId>,
    weak_entity_set: WeakEntitySet,
}

impl std::fmt::Debug for EntityTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityTransaction")
            .field("id", &self.id)
            .field("created_entities", &self.created_entity_ids.len())
            .field("committed", &self.commit_state.is_committed())
            .finish()
    }
}

impl EntityTransaction {
    pub fn new(weak_entity_set: WeakEntitySet) -> Self {
        Self { id: ulid::Ulid::new(), commit_state: CommitState::new(), created_entity_ids: Vec::new(), weak_entity_set }
    }

    /// Get the shared commit state for this transaction
    pub fn commit_state(&self) -> Arc<CommitState> { self.commit_state.clone() }

    /// Add a newly created entity to this transaction
    /// IMPORTANT: Only call this for entities created with this transaction's commit_state
    pub fn add_created_entity_id(&mut self, id: EntityId) { self.created_entity_ids.push(id); }

    /// Commit all entities in this transaction
    ///
    /// Simply marks the shared CommitState as committed - entity.is_uncommitted()
    /// will return false after this, treating committed state the same as None.
    ///
    /// Note: Branch entities are NOT tracked here - they're handled separately in
    /// commit_local_trx where changes are propagated to upstream Primary entities.
    pub fn commit(&mut self) {
        // Mark as committed and wake all waiters
        self.commit_state.committed.store(true, std::sync::atomic::Ordering::Release);
        let _ = self.commit_state.completion.send(()); // Wake waiters

        // No need to iterate entities - is_uncommitted() checks the committed flag
    }
}

impl Drop for EntityTransaction {
    fn drop(&mut self) {
        // If not committed, this is a rollback - clean up before waking waiters
        if !self.commit_state.is_committed() {
            eprintln!("MARK EntityTransaction {} rolling back {} entities", self.id, self.created_entity_ids.len());

            // CRITICAL ORDER:
            // 1. Remove from WeakEntitySet (prevent new strong refs)
            {
                let mut entities = self.weak_entity_set.0.write().unwrap();
                for id in &self.created_entity_ids {
                    entities.remove(id);
                    eprintln!("MARK EntityTransaction {} removed entity {:#} from WeakEntitySet", self.id, id);
                }
            }

            // 2. Clear entity IDs (weak refs in WeakEntitySet will fail to upgrade)
            self.created_entity_ids.clear();

            // 3. Wake waiters (notify rollback complete)
            let _ = self.commit_state.completion.send(());
            eprintln!("MARK EntityTransaction {} rollback complete", self.id);
        }
    }
}

impl CommitState {
    pub fn new() -> Arc<Self> {
        let (tx, _rx) = tokio::sync::broadcast::channel(1);
        Arc::new(Self { committed: AtomicBool::new(false), completion: tx })
    }

    /// Check if committed (non-blocking)
    pub fn is_committed(&self) -> bool { self.committed.load(std::sync::atomic::Ordering::Acquire) }

    /// Wait for commit or rollback
    pub async fn await_completion(&self) -> bool {
        if self.is_committed() {
            return true;
        }
        let mut rx = self.completion.subscribe();
        let _ = rx.recv().await; // Ignore error (sender dropped = rolled back)
        self.is_committed()
    }
}
