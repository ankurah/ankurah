//! The storage commit that ends a local transaction or the application of a peer's update, run in
//! a task of its own so that it finishes once it takes reactor maintenance's commit gate, even if
//! its caller's future is dropped: a commit stopped between taking the gate and publishing would
//! leave maintenance unavailable for the whole node. The task stages the entity writes, then,
//! holding the gate, commits them, advances each resident entity and publishes the committed
//! entities to maintenance.
use crate::{
    changes::EntityChange,
    entity::RemoteTrxEntity,
    error::MutationError,
    node::Node,
    policy::PolicyAgent,
    reactor::ChangeNotification,
    retrieval::{LocalEventGetter, LocalStateGetter},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
};
use ankurah_proto::{Attested, Clock, EntityState, Event};
use tokio::sync::oneshot;

/// One entity's write: the stored head it replaces, the state and events it stores, and how the
/// resident entity follows once storage commits them.
pub(crate) struct EntityWrite {
    /// The stored head this write replaces; storage refuses the write if it has changed.
    pub(crate) expected_head: Clock,
    pub(crate) state: Attested<EntityState>,
    pub(crate) events: Vec<Attested<Event>>,
    pub(crate) resident: ResidentAdvance,
}

/// How the resident entity follows a committed write.
pub(crate) enum ResidentAdvance {
    /// Register the resident entity for a newly stored entity, unless a reader registered it first.
    Create,
    /// Commit the fork that adopted the stored snapshot; the change carries the events stored beside it.
    Adopt(RemoteTrxEntity),
    /// Commit the fork whose events produced the stored state; the change carries the events it applied.
    Apply(RemoteTrxEntity),
}

type CommitOutcome = Result<Option<Vec<EntityChange>>, MutationError>;

/// Entity writes staged in a storage transaction that their commit's task holds. Dropping this
/// before [`commit`](Self::commit) abandons the writes uncommitted; once `commit` releases them, the
/// task takes the commit gate and finishes without the caller.
pub(crate) struct GatedCommit {
    release: oneshot::Sender<()>,
    outcome: oneshot::Receiver<CommitOutcome>,
}

impl GatedCommit {
    /// Stage `writes` and report whether storage accepted them, so the caller can still act before
    /// committing them, as a local transaction relays its events to durable peers.
    pub(crate) async fn stage<SE, PA>(node: &Node<SE, PA>, writes: Vec<EntityWrite>) -> Result<Self, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let (commit, staging) = Self::spawn(node, writes);
        staging.await.expect("gated commit task panicked")?;
        Ok(commit)
    }

    /// Commit the staged writes, advance each resident entity and publish the committed entities to
    /// maintenance. Returns one change per write, in order, or `None` when storage refused the writes
    /// because a stored head was no longer the write's expected head.
    pub(crate) async fn commit(self) -> CommitOutcome {
        // Sending fails only if the task panicked, which its outcome reports.
        let _ = self.release.send(());
        self.outcome.await.expect("gated commit task panicked")
    }

    /// Start the task that stages `writes`, reports the staging result through the returned
    /// receiver, and commits them once released.
    fn spawn<SE, PA>(node: &Node<SE, PA>, writes: Vec<EntityWrite>) -> (Self, oneshot::Receiver<Result<(), MutationError>>)
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node = node.clone();
        let (staged, staging) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let (finished, outcome) = oneshot::channel();
        crate::task::spawn(async move {
            let mut storage_trx = node.storage.transaction();
            let result = stage_writes(&mut storage_trx, &writes).await;
            let failed = result.is_err();
            let _ = staged.send(result);
            // Released writes commit even if their caller has since been dropped; a caller dropped
            // before releasing them abandons them.
            if failed || released.await.is_err() {
                return;
            }
            let _ = finished.send(commit_staged(&node, storage_trx, writes).await);
        });
        (Self { release, outcome }, staging)
    }
}

impl EntityWrite {
    /// Stage and commit this write alone; `None` when storage refused it. Nothing waits between
    /// staging and committing it, so it is released at once and finishes even if its caller is
    /// dropped while it stages.
    pub(crate) async fn commit<SE, PA>(self, node: &Node<SE, PA>) -> Result<Option<EntityChange>, MutationError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let (commit, staging) = GatedCommit::spawn(node, vec![self]);
        // Sending fails only once the task has ended, which staging or the outcome reports.
        let _ = commit.release.send(());
        staging.await.expect("gated commit task panicked")?;
        let changes = commit.outcome.await.expect("gated commit task panicked")?;
        Ok(changes.map(|mut changes| changes.pop().expect("a gated commit returns one change per write")))
    }
}

async fn stage_writes(storage_trx: &mut impl StorageTransaction, writes: &[EntityWrite]) -> Result<(), MutationError> {
    for write in writes {
        storage_trx.add_events(&write.events).await?;
        storage_trx.set_state(&write.expected_head, &write.state).await?;
    }
    Ok(())
}

async fn commit_staged<SE, PA>(node: &Node<SE, PA>, storage_trx: SE::Transaction<'_>, writes: Vec<EntityWrite>) -> CommitOutcome
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut source = node.reactor.begin_commit().await;
    if let StorageCommitOutcome::Conflict { .. } = source.record_storage_outcome(storage_trx.commit().await)? {
        return Ok(None);
    }
    // Local storage holds all the history the advance reads, so it needs none of the caller's
    // getter. Holding the gate, no other commit moves a resident entity: each resident is still at
    // its write's expected head and takes the committed state without reading history, or already
    // holds that state because a reader loaded it from storage. Then a fork re-applies its events
    // to find them applied, reading only this write's events, now stored, and history that its own
    // application already read, which the caller's getter found locally or stored when fetching it.
    let history = LocalEventGetter::new(node.storage.clone(), node.durable);
    let mut changes = Vec::with_capacity(writes.len());
    for EntityWrite { state, events, resident, .. } in writes {
        changes.push(match resident {
            ResidentAdvance::Create => {
                let states = LocalStateGetter::new(node.storage.clone());
                let (_, entity) = node.entities.with_state(&states, &history, state.payload.entity_id, state.payload.state).await?;
                EntityChange::new(entity, events)
            }
            ResidentAdvance::Adopt(fork) => EntityChange::new(fork.commit(&node.entities, &history).await?.into_parts().0, events),
            ResidentAdvance::Apply(fork) => fork.commit(&node.entities, &history).await?,
        });
    }
    source.publish(changes.iter().map(|change| change.entity()));
    Ok(Some(changes))
}

#[cfg(test)]
mod tests;
