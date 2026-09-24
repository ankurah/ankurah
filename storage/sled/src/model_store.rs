use ankql::ast::Resolved;
use ankql::selection::map_references;
use ankurah_storage_common::{ColumnPath, EngineColumns};
#[cfg(debug_assertions)]
use std::sync::atomic::AtomicBool;

use ankql::ast::Predicate;
use ankurah_core::indexing::KeySpec;
use ankurah_core::{
    error::RetrievalError,
    ModelId,
};
use ankurah_proto::PropertyId;
use ankurah_proto::{Attested, EntityState};
use ankurah_storage_common::{filtering::ValueSetStream, KeyBounds, OrderByComponents, Plan, Planner, PlannerConfig, ScanDirection};
use futures::Stream;
use std::sync::Arc;

use tokio::task;

use crate::entity::{SledEntityExt, SledEntityExtFromMats, SledEntityLookup};
use crate::materialization::ProjectedEntity;
use crate::scan_index::SledIndexScanner;
use crate::scan_model::{SledMaterializationKeyScanner, SledMaterializationLookup, SledMaterializationScanner};
use crate::{database::Database, property::planner_column_for_slot};
use ankurah_storage_common::traits::{EntityIdStream, EntityStateStream};

#[derive(Clone)]
/// Private handle state for one Sled model materialization.
pub struct SledModelStoreInner {
    /// Durable model represented by `tree`.
    pub model_id: ModelId,
    /// Shared canonical stores and engine-private registries.
    pub database: Arc<Database>,
    /// Projected rows for `model_id`.
    pub tree: sled::Tree,
    #[cfg(debug_assertions)]
    /// Runtime test switch for disabling open-ended scan prefix guards.
    pub prefix_guard_disabled: Arc<AtomicBool>,
}

/// Private Sled helper for one model's projected query surface.
pub struct SledModelStore(SledModelStoreInner);

impl SledModelStore {
    /// Construct a model materialization handle from engine-owned resources.
    pub fn new(
        model_id: ModelId,
        database: Arc<Database>,
        tree: sled::Tree,
        #[cfg(debug_assertions)] prefix_guard_disabled: Arc<AtomicBool>,
    ) -> Self {
        Self(SledModelStoreInner {
            model_id,
            database,
            tree,
            #[cfg(debug_assertions)]
            prefix_guard_disabled,
        })
    }
}

impl SledModelStore {
    /// Query this model's projection and hydrate matching canonical states.
    pub(crate) async fn fetch_states(
        &self,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let inner = self.0.clone();
        let selection = selection.clone();
        Ok(task::spawn_blocking(move || inner.fetch_states_blocking(selection)).await??)
    }
}

impl SledModelStoreInner {
    fn fetch_states_blocking(&self, selection: ankql::ast::Selection<Resolved>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let resolved_selection = selection.clone();
        let manager = &self.database.property_manager;

        // Pre-resolve every referenced identity to sled's numeric slot before
        // planning. The planner API carries string physical addresses, so the
        // slot is encoded losslessly at that boundary only.
        let referenced = selection.referenced_properties();
        let mut resolved_columns: std::collections::HashMap<PropertyId, String> = std::collections::HashMap::new();
        for pid in &referenced {
            if pid == &PropertyId::Id {
                resolved_columns.insert(pid.clone(), "id".to_owned());
            } else if let Some(slot) = manager.slot_of(pid)? {
                resolved_columns.insert(pid.clone(), planner_column_for_slot(slot));
            }
        }

        // Read-side absence, by durable identity: a referenced property this
        // engine never materialized (no column in the durable map) is absent,
        // with no fallback to a name. Fold those to NULL and drop any ORDER BY
        // key on one. `PropertyId::Id` is pinned to the "id"
        // column, so it is never absent.
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|pid| !resolved_columns.contains_key(pid)).collect();
        if selection.predicate.referenced_properties().iter().any(|property| absent.contains(property)) {
            // Keep missing-property comparisons fail-closed, including under NOT.
            let ids = SledMaterializationKeyScanner::new(&self.tree, &KeyBounds::empty(), ScanDirection::Forward)?;
            return self.filter_candidate_states(ids, &selection, false);
        }
        let selection = if absent.is_empty() { selection } else { selection.assume_null(&absent) };

        let selection = map_references(
            &selection,
            &|path| ColumnPath::new(resolved_columns[&path.property_id()].clone(), path.subpath.clone()),
            &|model| *model,
        );
        let plans = Planner::new(PlannerConfig::full_support()).plan(&selection, "id");
        let plan = plans.into_iter().next().ok_or_else(|| RetrievalError::Other("No plan generated".into()))?;

        // Execute the chosen plan using streaming pipeline architecture
        match plan {
            Plan::EmptyScan => Ok(Vec::new()),

            Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill } =>
            //
            {
                self.exec_index_scan_plan(index_spec, bounds, scan_direction, remaining_predicate, order_by_spill, &resolved_selection)
            }

            Plan::TableScan { bounds, scan_direction, remaining_predicate, order_by_spill } => {
                self.exec_table_scan_plan(bounds, scan_direction, remaining_predicate, order_by_spill, &resolved_selection)
            }
        }
    }
    fn exec_index_scan_plan(
        &self,
        mut index_spec: KeySpec<String>,
        mut bounds: KeyBounds,
        scan_direction: ScanDirection,
        remaining_predicate: Predicate<EngineColumns>,
        order_by_spill: OrderByComponents,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        use ankurah_core::{
            indexing::IndexKeyPart,
            value::{Value, ValueType},
        };
        use ankurah_storage_common::{Endpoint, KeyBoundComponent};
        // Sled orders entity IDs by bytes, not their base64 display strings.
        for keypart in &mut index_spec.keyparts {
            if keypart.key == "id" {
                keypart.value_type = ValueType::EntityId;
            }
        }
        let column = crate::materialization::MATERIALIZATION_COLUMN.to_owned();
        let membership = Endpoint::incl(Value::String(self.model_id.to_string()));
        index_spec.keyparts.insert(0, IndexKeyPart::asc(column.clone(), ValueType::String));
        bounds.keyparts.insert(0, KeyBoundComponent { column, low: membership.clone(), high: membership });

        // Debug flag for disabling equality-prefix guard (testing only)
        let prefix_guard_disabled = {
            #[cfg(debug_assertions)]
            {
                use std::sync::atomic::Ordering;
                self.prefix_guard_disabled.load(Ordering::Relaxed)
            }
            #[cfg(not(debug_assertions))]
            false
        };

        let (index, match_type) = self.database.index_manager.assure_index_exists(&index_spec, &self.database.db)?;

        let ids = SledIndexScanner::new(&index, &bounds, scan_direction, match_type, prefix_guard_disabled)?;
        if !selection.predicate.referenced_models().is_empty() {
            return self.filter_candidate_states(ids, selection, order_by_spill.is_satisfied());
        }
        let limit = selection.limit;

        if remaining_predicate == Predicate::True && order_by_spill.is_satisfied() {
            return futures::executor::block_on(ids.limit(limit).entities(&self.database.entities_tree).collect_states());
        }

        // Values path: ids → projected-value lookup → filter/sort/limit →
        // canonical entity lookup. Sled has no multi-get API, so the final
        // primary-tree point reads are the native index-to-record access path.
        let projected = SledMaterializationLookup::new(&self.tree, ids);
        self.finish_projected_scan(projected, remaining_predicate, order_by_spill, limit)
    }

    /// Reject nonmembers before loading state, preserving index order and LIMIT after filtering.
    fn filter_candidate_states(
        &self,
        ids: impl EntityIdStream,
        selection: &ankql::ast::Selection<Resolved>,
        ordered: bool,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        use ankurah_core::{entity::TemporaryEntity, selection::filter::evaluate_predicate};
        use ankurah_storage_common::materialization_plan::MaterializationPlan;
        use futures::{StreamExt, TryStreamExt};

        if selection.limit == Some(0) {
            return Ok(Vec::new());
        }
        futures::executor::block_on(async {
            let plan = MaterializationPlan::new(selection);
            let ids = ids.try_filter_map(|id| {
                futures::future::ready(
                    self.database
                        .memberships(id)
                        .map(|memberships| plan.may_match_memberships(|model| memberships.contains(model)).then_some(id)),
                )
            });
            let mut states = ids.entities(&self.database.entities_tree);
            if !ordered {
                return ankurah_storage_common::selection::select_states(states.collect_states().await?, selection);
            }
            let mut matches = Vec::new();
            while let Some(state) = states.next().await {
                let state = state?;
                let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
                if !evaluate_predicate(&entity, &selection.predicate).unwrap_or(false) {
                    continue;
                }
                matches.push(state);
                if selection.limit.is_some_and(|limit| matches.len() as u64 >= limit) {
                    break;
                }
            }
            Ok(matches)
        })
    }
    fn exec_table_scan_plan(
        &self,
        bounds: KeyBounds,
        scan_direction: ScanDirection,
        remaining_predicate: Predicate<EngineColumns>,
        order_by_spill: OrderByComponents,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        if !selection.predicate.referenced_models().is_empty() {
            let ids = SledMaterializationKeyScanner::new(&self.tree, &bounds, scan_direction)?;
            return self.filter_candidate_states(ids, selection, order_by_spill.is_satisfied());
        }
        let limit = selection.limit;
        if remaining_predicate == Predicate::True && order_by_spill.is_satisfied() {
            let ids = SledMaterializationKeyScanner::new(&self.tree, &bounds, scan_direction)?;
            let states = SledEntityLookup::new(&self.database.entities_tree, ids.limit(limit));
            return futures::executor::block_on(states.collect_states());
        }

        // Values path: scan the model's projected-value tree, then share the
        // same filter/sort/limit/hydration pipeline as an index scan.
        let scanner = SledMaterializationScanner::new(&self.tree, &bounds, scan_direction)?;
        self.finish_projected_scan(scanner, remaining_predicate, order_by_spill, limit)
    }

    fn finish_projected_scan<S>(
        &self,
        projected: S,
        remaining_predicate: Predicate<EngineColumns>,
        order_by_spill: OrderByComponents,
        limit: Option<u64>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError>
    where
        S: Stream<Item = ProjectedEntity> + Unpin,
    {
        let entities_tree = &self.database.entities_tree;
        let needs_spill = !order_by_spill.spill.is_empty();
        match remaining_predicate {
            Predicate::True => match (needs_spill, limit) {
                (true, Some(limit)) => {
                    futures::executor::block_on(projected.top_k(order_by_spill, limit as usize).entities(entities_tree).collect_states())
                }
                (true, None) => futures::executor::block_on(projected.sort_by(order_by_spill).entities(entities_tree).collect_states()),
                (false, limit) => futures::executor::block_on(projected.limit(limit).entities(entities_tree).collect_states()),
            },
            _ => {
                let filtered = projected.filter_predicate(&remaining_predicate);
                match (needs_spill, limit) {
                    (true, Some(limit)) => {
                        futures::executor::block_on(filtered.top_k(order_by_spill, limit as usize).entities(entities_tree).collect_states())
                    }
                    (true, None) => futures::executor::block_on(filtered.sort_by(order_by_spill).entities(entities_tree).collect_states()),
                    (false, limit) => futures::executor::block_on(filtered.limit(limit).entities(entities_tree).collect_states()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_core::{
        property::backend::{lww::LWWBackend, PropertyBackend},
        storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
        value::Value,
    };
    use ankurah_proto::{Attested, Clock, EntityId, EventId, PropertyId, State, StateBuffers};
    use std::collections::{BTreeMap, BTreeSet};

    fn entity_id(byte: u8) -> EntityId { EntityId::from_bytes([byte; EntityId::BYTE_LEN]) }

    fn state_with_strings(entity_id: EntityId, event_byte: u8, values: &[(PropertyId, &str)]) -> Attested<EntityState> {
        let backend = LWWBackend::new();
        for (property, value) in values {
            backend.set(*property, Some(Value::String((*value).to_owned())));
        }
        let operations = backend.to_operations().unwrap().expect("state has values");
        let event_id = EventId::from_bytes([event_byte; 32]);
        backend.apply_operations_with_event(&operations, event_id.clone()).unwrap();
        Attested::opt(
            EntityState {
                entity_id,
                state: State {
                    state_buffers: StateBuffers(BTreeMap::from([("lww".to_owned(), backend.to_state_buffer().unwrap())])),
                    memberships: BTreeSet::new(),
                    head: Clock::from(vec![event_id]),
                },
            },
            None,
        )
    }

    fn state_for_model(mut state: Attested<EntityState>, model: ModelId) -> Attested<EntityState> {
        state.payload.state.memberships = [model].into();
        state
    }

    fn state_for_models(mut state: Attested<EntityState>, models: impl IntoIterator<Item = ModelId>) -> Attested<EntityState> {
        state.payload.state.memberships = models.into_iter().collect();
        state
    }

    async fn commit_canonical_state(engine: &crate::SledStorageEngine, expected_head: Clock, state: Attested<EntityState>) {
        let mut transaction = engine.transaction();
        transaction.set_state(&expected_head, &state).await.unwrap();
        let outcome = transaction.commit().await.unwrap();
        assert!(matches!(outcome, StorageCommitOutcome::Committed(_)));
    }

    async fn commit_state(engine: &crate::SledStorageEngine, expected_head: Clock, model: ModelId, state: Attested<EntityState>) {
        commit_canonical_state(engine, expected_head, state_for_model(state, model)).await;
    }

    #[tokio::test]
    async fn write_refreshes_every_canonical_membership_materialization() {
        let engine = crate::SledStorageEngine::new_test().unwrap();
        let model_a = ModelId::EntityId(entity_id(0x11));
        let model_b = ModelId::EntityId(entity_id(0x12));
        let property_a = PropertyId::EntityId(entity_id(0x21));
        let property_b = PropertyId::EntityId(entity_id(0x22));
        let entity = entity_id(0x31);

        let initial = state_for_models(state_with_strings(entity, 1, &[(property_a, "a1"), (property_b, "b1")]), [model_a, model_b]);
        commit_canonical_state(&engine, Clock::default(), initial.clone()).await;
        let updated = state_for_models(state_with_strings(entity, 2, &[(property_a, "a2"), (property_b, "b2")]), [model_a, model_b]);
        commit_canonical_state(&engine, initial.payload.state.head, updated).await;

        let selection = ankql::ast::Selection {
            predicate: Predicate::Comparison {
                left: Box::new(ankql::ast::Expr::Path(property_b.into())),
                operator: ankql::ast::ComparisonOperator::Equal,
                right: Box::new(ankql::ast::Expr::Literal(Value::String("b2".into()))),
            },
            order_by: None,
            limit: None,
        };
        let found = engine.fetch_states(&selection.clone().and_member_of(model_b)).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].payload.entity_id, entity);
    }

    #[tokio::test]
    async fn shared_index_prefix_cannot_omit_another_models_entities() {
        use ankurah_core::{indexing::IndexKeyPart, value::ValueType};

        let engine = crate::SledStorageEngine::new_test().unwrap();
        let [a, b] = [1, 2].map(|byte| ModelId::EntityId(entity_id(byte)));
        let [p, q] = [3, 4].map(|byte| PropertyId::EntityId(entity_id(byte)));
        commit_state(&engine, Clock::default(), a, state_with_strings(entity_id(5), 1, &[(p, "x"), (q, "y")])).await;
        commit_state(&engine, Clock::default(), b, state_with_strings(entity_id(6), 2, &[(p, "x")])).await;
        {
            let database = engine.database.lock().unwrap();
            let spec = KeySpec::new(vec![
                IndexKeyPart::asc(crate::materialization::MATERIALIZATION_COLUMN, ValueType::String),
                IndexKeyPart::asc(planner_column_for_slot(database.property_manager.slot_of(&p).unwrap().unwrap()), ValueType::String),
                IndexKeyPart::asc(planner_column_for_slot(database.property_manager.slot_of(&q).unwrap().unwrap()), ValueType::String),
            ]);
            database.index_manager.assure_index_exists(&spec, &database.db).unwrap();
        }
        let selection = ankql::ast::Selection::from(Predicate::Comparison {
            left: Box::new(ankql::ast::Expr::Path(p.into())),
            operator: ankql::ast::ComparisonOperator::Equal,
            right: Box::new(ankql::ast::Expr::Literal(Value::String("x".into()))),
        })
        .and_member_of(b);
        let found = engine.fetch_states(&selection).await.unwrap();
        assert_eq!(found.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(), vec![entity_id(6)]);
    }

    #[tokio::test]
    async fn membership_rejection_does_not_read_entity_state() {
        use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Selection};

        let engine = crate::SledStorageEngine::new_test().unwrap();
        let [a, b] = [1, 2].map(|byte| ModelId::EntityId(entity_id(byte)));
        let property = PropertyId::EntityId(entity_id(3));
        commit_canonical_state(&engine, Clock::default(), state_for_models(state_with_strings(entity_id(5), 1, &[(property, "a")]), [a]))
            .await;
        commit_canonical_state(
            &engine,
            Clock::default(),
            state_for_models(state_with_strings(entity_id(6), 2, &[(property, "b")]), [a, b]),
        )
        .await;
        let comparison = Predicate::Comparison {
            left: Box::new(Expr::Path(property.into())),
            operator: ComparisonOperator::GreaterThanOrEqual,
            right: Box::new(Expr::Literal(Value::String("a".into()))),
        };
        let mut selection = Selection {
            predicate: comparison.clone(),
            order_by: Some(vec![OrderByItem { path: property.into(), direction: OrderDirection::Asc }]),
            limit: Some(1),
        }
        .and_member_of(b)
        .and_member_of(a);
        engine.fetch_states(&selection).await.unwrap();

        // Leave the index and membership records intact; reading this excluded entity must fail.
        engine.database.lock().unwrap().entities_tree.insert(entity_id(5).to_bytes(), &[0xff]).unwrap();
        for ordered in [true, false] {
            if !ordered {
                selection.order_by = None;
            }
            let states = engine.fetch_states(&selection).await.unwrap();
            assert_eq!(states.iter().map(|state| state.payload.entity_id).collect::<Vec<_>>(), vec![entity_id(6)]);
        }
        let union = Selection::from(Predicate::Or(Box::new(Predicate::MemberOf(b)), Box::new(comparison))).and_member_of(a);
        assert!(engine.fetch_states(&union).await.is_err(), "OR permits the unreadable entity, so it must still be read");
    }

    #[tokio::test]
    async fn stale_head_rolls_back_the_complete_batch() {
        let engine = crate::SledStorageEngine::new_test().unwrap();
        let model_a = ModelId::EntityId(entity_id(0x41));
        let model_b = ModelId::EntityId(entity_id(0x42));
        let property = PropertyId::EntityId(entity_id(0x43));
        let first_id = entity_id(0x44);
        let second_id = entity_id(0x45);
        let first = state_with_strings(first_id, 1, &[(property, "first-old")]);
        let second = state_with_strings(second_id, 2, &[(property, "second-old")]);
        commit_state(&engine, Clock::default(), model_a, first.clone()).await;
        commit_state(&engine, Clock::default(), model_a, second.clone()).await;

        let mut transaction = engine.transaction();
        transaction.set_state(
            &first.payload.state.head,
            &state_for_model(state_with_strings(first_id, 3, &[(property, "first-new")]), model_b),
        ).await.unwrap();
        transaction.set_state(
            &Clock::default(),
            &state_for_model(state_with_strings(second_id, 4, &[(property, "second-new")]), model_b),
        ).await.unwrap();
        let outcome = transaction.commit().await.unwrap();
        let StorageCommitOutcome::Conflict { observed } = outcome else {
            panic!("one stale expected head must reject the complete batch");
        };
        assert_eq!(observed[&first_id].as_ref().unwrap().payload.state.head, first.payload.state.head);
        assert_eq!(observed[&second_id].as_ref().unwrap().payload.state.head, second.payload.state.head);
        assert_eq!(engine.get_state(first_id).await.unwrap().payload.state.head, first.payload.state.head);
        assert_eq!(engine.get_state(second_id).await.unwrap().payload.state.head, second.payload.state.head);

        let all = ankql::ast::Selection::<Resolved> { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert!(
            engine.fetch_states(&all.clone().and_member_of(model_b)).await.unwrap().is_empty(),
            "a rejected batch must not publish associations or projections"
        );
    }
}
