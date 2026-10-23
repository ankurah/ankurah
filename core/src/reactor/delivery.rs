//! A query's delivered event frontier is independent of the entity's current state.
//! Initial snapshots can advance the latter without consuming live notifications.
use crate::{
    error::RetrievalError,
    event_dag::{compare, AbstractCausalRelation, DEFAULT_BUDGET},
    retrieval::GetEvents,
};
use ankurah_proto as proto;

pub trait ReactorEvent: Clone + Send + 'static {
    fn causal_event(&self) -> Option<&proto::Event> { None }
}
impl ReactorEvent for proto::Attested<proto::Event> {
    fn causal_event(&self) -> Option<&proto::Event> { Some(&self.payload) }
}

#[derive(Debug)]
pub struct NotificationFailure {
    pub query: Option<proto::QueryId>,
    pub version: u32,
    pub error: RetrievalError,
}

pub(super) async fn filter<Ev: ReactorEvent>(
    history: Option<&(dyn GetEvents + Send + Sync)>,
    mut delivered: proto::Clock,
    events: &[Ev],
) -> Result<(proto::Clock, Vec<Ev>), RetrievalError> {
    let mut accepted = Vec::new();
    for item in events {
        let Some(event) = item.causal_event() else {
            accepted.push(item.clone());
            continue;
        };
        if delivered.contains(&event.id()) {
            continue;
        }
        let head = proto::Clock::singleton(event);
        if delivered.is_empty() || delivered == event.parent {
            delivered = head;
        } else {
            let history = history.ok_or_else(|| RetrievalError::Other("event delivery requires local causal history".into()))?;
            // Trace the incoming event back to the delivered frontier. An
            // initial snapshot can establish that frontier without its payloads.
            match compare(history, &head, &delivered, DEFAULT_BUDGET).await?.relation {
                AbstractCausalRelation::Equal | AbstractCausalRelation::StrictAscends => continue,
                AbstractCausalRelation::StrictDescends { .. } => delivered = head,
                AbstractCausalRelation::DivergedSince { .. } => {
                    // Retain concurrent tips while removing only proven ancestors.
                    let mut concurrent = Vec::new();
                    for (generation, id) in delivered.entries() {
                        let tip =
                            proto::Clock::new(vec![(*generation, id.clone())]).map_err(|error| RetrievalError::Other(error.to_string()))?;
                        match compare(history, &head, &tip, DEFAULT_BUDGET).await?.relation {
                            AbstractCausalRelation::Equal | AbstractCausalRelation::StrictDescends { .. } => {}
                            AbstractCausalRelation::DivergedSince { .. } => concurrent.push((*generation, id.clone())),
                            _ => return Err(RetrievalError::Other("cannot establish event delivery frontier".into())),
                        }
                    }
                    concurrent.extend_from_slice(head.entries());
                    delivered = proto::Clock::new(concurrent).map_err(|error| RetrievalError::Other(error.to_string()))?;
                }
                _ => return Err(RetrievalError::Other("cannot establish event delivery frontier".into())),
            }
        }
        accepted.push(item.clone());
    }
    Ok((delivered, accepted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        retrieval::LocalEventGetter,
        storage::{StorageEngine, StorageTransaction},
        test_utils::TestStorage,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn descendant_notification_needs_connecting_history_but_not_the_baseline_event() -> anyhow::Result<()> {
        let genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet::default());
        let baseline = proto::Clock::singleton(&genesis);
        let parent = proto::Event::update(genesis.entity_id, baseline.clone(), proto::AuthorId::Unknown, proto::OperationSet::default());
        let child = proto::Event::update(
            genesis.entity_id,
            proto::Clock::singleton(&parent),
            proto::AuthorId::Unknown,
            proto::OperationSet::default(),
        );
        let storage = Arc::new(TestStorage::default());
        let mut transaction = storage.transaction();
        transaction.add_events(&[parent.clone().into(), child.clone().into()]).await?;
        transaction.commit().await?.committed()?;
        // An initial snapshot establishes the baseline clock without supplying
        // its event payload. The later live child supplies the connecting history.
        assert!(storage.get_events(vec![genesis.id()]).await?.is_empty());
        let history = LocalEventGetter::new(storage, false);
        let (frontier, accepted) = filter(Some(&history), baseline, &[proto::Attested::from(child.clone())]).await?;
        assert_eq!(frontier, proto::Clock::singleton(&child));
        assert_eq!(accepted.len(), 1);
        let (frontier, accepted) =
            filter(Some(&history), frontier, &[proto::Attested::from(parent), proto::Attested::from(child.clone())]).await?;
        assert_eq!(frontier, proto::Clock::singleton(&child));
        assert!(accepted.is_empty(), "connecting history and duplicate live events remain silent");
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_notifications_keep_a_minimal_frontier_and_suppress_ancestor_replays() -> anyhow::Result<()> {
        let genesis = proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet::default());
        let baseline = proto::Clock::singleton(&genesis);
        let branch = |model: u8| {
            proto::Event::update(
                genesis.entity_id,
                baseline.clone(),
                proto::AuthorId::Unknown,
                proto::OperationSet(vec![proto::Operation::Membership(proto::Membership::Add(proto::ModelId::EntityId(
                    proto::EntityId::from_bytes([model; 32]),
                )))]),
            )
        };
        let left = branch(1);
        let right = branch(2);
        let merged = proto::Event::update(
            genesis.entity_id,
            proto::Clock::from_events([&left, &right]),
            proto::AuthorId::Unknown,
            proto::OperationSet::default(),
        );
        let storage = Arc::new(TestStorage::default());
        let mut transaction = storage.transaction();
        transaction
            .add_events(
                &[genesis.clone(), left.clone(), right.clone(), merged.clone()].into_iter().map(proto::Attested::from).collect::<Vec<_>>(),
            )
            .await?;
        transaction.commit().await?.committed()?;
        let history = LocalEventGetter::new(storage, false);
        let (frontier, accepted) =
            filter(Some(&history), baseline, &[proto::Attested::from(left.clone()), proto::Attested::from(right.clone())]).await?;
        assert_eq!(frontier, proto::Clock::from_events([&left, &right]));
        assert_eq!(accepted.len(), 2);
        let (frontier, accepted) = filter(
            Some(&history),
            frontier,
            &[
                proto::Attested::from(merged.clone()),
                proto::Attested::from(left),
                proto::Attested::from(right),
                proto::Attested::from(genesis),
            ],
        )
        .await?;
        assert_eq!(frontier, proto::Clock::singleton(&merged));
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].payload.id(), merged.id());
        Ok(())
    }
}
