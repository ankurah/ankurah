use crate::{
    reactor::{AbstractEntity, MembershipChange, ReactorSubscriptionId, ReactorUpdate, ReactorUpdateItem},
    resultset::EntityResultSet,
};
use ankurah_proto::{self as proto};
use futures::future;
use std::collections::{HashMap, HashSet};

type GapFillData<E> = (
    proto::QueryId,
    std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
    proto::CollectionId,
    ankql::ast::Selection,
    EntityResultSet<E>,
    Option<E>,
    usize,
);

/// State for a single predicate within a subscription
pub struct QueryState<E: AbstractEntity + ankql::selection::filter::Filterable> {
    // TODO make this a clonable PredicateSubscription and store it instead of the channel?
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) selection: ankql::ast::Selection,
    pub(crate) gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>, // For filling gaps when LIMIT is applied
    // I think we need to move these out of PredicateState and into WatcherState
    pub(crate) paused: bool, // When true, skip notifications (used during initialization and updates)
    pub(crate) resultset: EntityResultSet<E>,
    pub(crate) version: u32,
}

pub struct SubscriptionState<E: AbstractEntity + ankql::selection::filter::Filterable, Ev> {
    pub(crate) id: ReactorSubscriptionId,
    pub(crate) queries: HashMap<proto::QueryId, QueryState<E>>,
    /// The set of entities that are subscribed to by this subscription
    pub(crate) entity_subscriptions: HashSet<proto::EntityId>,
    // not sure if we actually need this
    pub(crate) entities: HashMap<proto::EntityId, E>,
    pub(crate) broadcast: ankurah_signals::broadcast::Broadcast<ReactorUpdate<E, Ev>>,
}

impl<E: AbstractEntity + ankql::selection::filter::Filterable + Send + 'static, Ev: Clone + Send + 'static> SubscriptionState<E, Ev> {
    pub fn notify(&self, update: ReactorUpdate<E, Ev>) {
        let gaps_to_fill = self.collect_gaps_to_fill();

        if gaps_to_fill.is_empty() {
            self.broadcast.send(update);
        } else {
            self.spawn_gap_filling_task(update, gaps_to_fill);
        }
    }

    fn collect_gaps_to_fill(&self) -> Vec<GapFillData<E>> {
        self.queries.iter().filter_map(|(query_id, query_state)| self.extract_gap_data(*query_id, query_state)).collect()
    }

    fn extract_gap_data(&self, query_id: proto::QueryId, query_state: &QueryState<E>) -> Option<GapFillData<E>> {
        let resultset = &query_state.resultset;

        if !resultset.is_gap_dirty() {
            return None;
        }

        let limit = resultset.get_limit()?;
        let current_len = resultset.len();

        if current_len >= limit {
            return None;
        }

        let gap_size = limit - current_len;
        let last_entity = resultset.last_entity();

        Some((
            query_id,
            query_state.gap_fetcher.clone(),
            query_state.collection_id.clone(),
            query_state.selection.clone(),
            resultset.clone(),
            last_entity,
            gap_size,
        ))
    }

    fn spawn_gap_filling_task(&self, update: ReactorUpdate<E, Ev>, gaps_to_fill: Vec<GapFillData<E>>) {
        let broadcast = self.broadcast.clone();
        let initial_items = update.items;
        let initialized_query = update.initialized_query;

        crate::task::spawn(async move {
            let mut all_items = initial_items;

            // Clear gap_dirty flags immediately for all queries
            for (_, _, _, _, ref resultset, _, _) in &gaps_to_fill {
                resultset.clear_gap_dirty();
            }

            // Process all gap fills concurrently
            let gap_fill_futures =
                gaps_to_fill.into_iter().map(|(query_id, gap_fetcher, collection_id, selection, resultset, last_entity, gap_size)| {
                    Self::process_gap_fill(query_id, gap_fetcher, collection_id, selection, resultset, last_entity, gap_size)
                });

            let gap_results = future::join_all(gap_fill_futures).await;

            // Collect all the new items from gap filling
            for gap_items in gap_results {
                all_items.extend(gap_items);
            }

            broadcast.send(ReactorUpdate { items: all_items, initialized_query });
        });
    }

    async fn process_gap_fill(
        query_id: proto::QueryId,
        gap_fetcher: std::sync::Arc<dyn crate::reactor::fetch_gap::GapFetcher<E>>,
        collection_id: proto::CollectionId,
        selection: ankql::ast::Selection,
        resultset: EntityResultSet<E>,
        last_entity: Option<E>,
        gap_size: usize,
    ) -> Vec<ReactorUpdateItem<E, Ev>> {
        tracing::debug!("Gap filling for query {} - need {} entities", query_id, gap_size);

        match gap_fetcher.fetch_gap(&collection_id, &selection, last_entity.as_ref(), gap_size).await {
            Ok(gap_entities) => {
                if !gap_entities.is_empty() {
                    tracing::debug!("Gap filling fetched {} entities for query {}", gap_entities.len(), query_id);

                    let mut write = resultset.write();
                    let mut gap_items = Vec::new();

                    for entity in gap_entities {
                        if write.add(entity.clone()) {
                            gap_items.push(ReactorUpdateItem {
                                entity,
                                events: vec![],
                                entity_subscribed: false,
                                predicate_relevance: vec![(query_id, MembershipChange::Add)],
                            });
                        }
                    }

                    gap_items
                } else {
                    tracing::debug!("Gap filling found no entities for query {}", query_id);
                    Vec::new()
                }
            }
            Err(e) => {
                tracing::warn!("Gap filling failed for query {}: {}", query_id, e);
                Vec::new()
            }
        }
    }
}
