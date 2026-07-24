#[cfg(debug_assertions)]
use std::sync::atomic::AtomicBool;

use ankql::ast::Predicate;
use ankurah_core::indexing::KeySpec;
use ankurah_core::{
    error::{MutationError, RetrievalError},
    ModelId,
};
use ankurah_proto::PropertyId;
use ankurah_proto::{Attested, EntityState, StateFragment};
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
    /// The injected catalog resolver is used only for canonical property value
    /// types during planning. Physical property addressing is the engine's
    /// durable PropertyId-to-slot registry.
    pub(crate) resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn ankurah_core::schema::CatalogResolver>>>>,
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
        resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn ankurah_core::schema::CatalogResolver>>>>,
        #[cfg(debug_assertions)] prefix_guard_disabled: Arc<AtomicBool>,
    ) -> Self {
        Self(SledModelStoreInner {
            model_id,
            database,
            tree,
            resolver,
            #[cfg(debug_assertions)]
            prefix_guard_disabled,
        })
    }
}

impl SledModelStore {
    /// Query this model's projection and hydrate matching canonical states.
    pub(crate) async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        // This engine can only address a property by identity. Refuse a
        // selection that still carries a name, before spawning any work.
        selection.check()?;
        let inner = self.0.clone();
        let selection = selection.clone();
        Ok(task::spawn_blocking(move || inner.fetch_states_blocking(selection)).await??)
    }
}

/// Extract one model's durable projected values from canonical state.
///
/// Property-slot allocation is engine metadata and may happen before the
/// multi-tree state transaction; no projected record becomes visible here.
pub(crate) fn project_state(
    database: &Database,
    resolver: &Arc<std::sync::RwLock<Option<std::sync::Weak<dyn ankurah_core::schema::CatalogResolver>>>>,
    model: &ModelId,
    state: &StateFragment,
) -> Result<Vec<(u32, ankurah_core::value::Value)>, MutationError> {
    use ankurah_proto::{PropertyId, SystemModel, SystemProperty};

    let projected: std::collections::BTreeSet<PropertyId> = match model {
        ModelId::System(SystemModel::System) => [SystemProperty::Item].into_iter().map(PropertyId::System).collect(),
        ModelId::System(SystemModel::Model) => [SystemProperty::Label, SystemProperty::Name].into_iter().map(PropertyId::System).collect(),
        ModelId::System(SystemModel::Property) => [
            SystemProperty::MintedFor,
            SystemProperty::Name,
            SystemProperty::Backend,
            SystemProperty::ValueType,
            SystemProperty::TargetModel,
        ]
        .into_iter()
        .map(PropertyId::System)
        .collect(),
        ModelId::System(SystemModel::ModelProperty) => {
            [SystemProperty::Model, SystemProperty::Property, SystemProperty::Optional].into_iter().map(PropertyId::System).collect()
        }
        ModelId::EntityId(_) => {
            let resolver = resolver
                .read()
                .unwrap()
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
                .ok_or_else(|| MutationError::General(format!("catalog resolver is unavailable for model {model}").into()))?;
            resolver.model_properties(model).map_err(|error| MutationError::General(error.to_string().into()))?.into_iter().collect()
        }
    };

    let mut materialized = Vec::new();
    let mut seen_slots = std::collections::HashSet::new();
    for (backend_name, state_buffer) in state.state.state_buffers.iter() {
        let backend = ankurah_core::property::backend::backend_from_string(backend_name, Some(state_buffer))?;
        for (property_id, value) in backend.property_values() {
            if !projected.contains(&property_id) {
                continue;
            }
            let slot = database.property_manager.slot_for(&property_id)?;
            if seen_slots.insert(slot) {
                if let Some(value) = value {
                    materialized.push((slot, value));
                }
            }
        }
    }
    Ok(materialized)
}

impl SledModelStoreInner {
    fn fetch_states_blocking(&self, selection: ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let resolver = {
            let resolver = self.resolver.read().unwrap();
            match resolver.as_ref() {
                None => None,
                Some(weak) => Some(
                    weak.upgrade()
                        .ok_or_else(|| RetrievalError::Other(format!("catalog resolver is unavailable for model {}", self.model_id)))?,
                ),
            }
        };
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
        // engine never materialized (no column in the durable map) is ABSENT --
        // there is NO fallback to a name (the #374 fix). Fold those to NULL and
        // drop any ORDER BY key on one. `PropertyId::Id` is pinned to the "id"
        // column, so it is never absent.
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|pid| !resolved_columns.contains_key(pid)).collect();
        let selection = if absent.is_empty() { selection } else { selection.assume_null(&absent)? };

        // Bind every surviving resolved property to its registered type, keyed
        // by sled's physical planner address. The planner re-casts predicate
        // bounds at execution and uses the same map for ORDER BY collation.
        let mut physical_types = std::collections::HashMap::new();
        for property_id in selection.referenced_properties() {
            let Some(column) = resolved_columns.get(&property_id).cloned() else { continue };
            let value_type = match property_id {
                PropertyId::Id => Some(ankurah_core::value::ValueType::EntityId),
                _ => resolver
                    .as_deref()
                    .map(|resolver| resolver.property_value_type(&property_id))
                    .transpose()
                    .map_err(|error| RetrievalError::Other(error.to_string()))?,
            };
            if let Some(value_type) = value_type {
                physical_types.insert(column, value_type);
            }
        }

        // Translate resolved identities to their engine-private numeric slots,
        // and supply each slot's canonical comparison type.
        let column_of = |property: &PropertyId| -> Option<String> { resolved_columns.get(property).cloned() };
        let physical_type_of = |column: &str| -> Option<ankurah_core::value::ValueType> { physical_types.get(column).copied() };

        // Generate query plans and choose the first non-empty one
        let plans = Planner::new(PlannerConfig::full_support()).plan_with_types(&selection, "id", &physical_type_of, &column_of);

        let plan = plans.into_iter().next().ok_or_else(|| RetrievalError::StorageError("No plan generated".into()))?;

        // Execute the chosen plan using streaming pipeline architecture
        match plan {
            Plan::EmptyScan => Ok(Vec::new()),

            Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill } =>
            //
            {
                self.exec_index_scan_plan(index_spec, bounds, scan_direction, remaining_predicate, order_by_spill, selection.limit)
            }

            Plan::TableScan { bounds, scan_direction, remaining_predicate, order_by_spill } => {
                self.exec_table_scan_plan(bounds, scan_direction, remaining_predicate, order_by_spill, selection.limit)
            }
        }
    }
    fn exec_index_scan_plan(
        &self,
        index_spec: KeySpec<String>,
        bounds: KeyBounds,
        scan_direction: ScanDirection,
        remaining_predicate: Predicate,
        order_by_spill: OrderByComponents,
        limit: Option<u64>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
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

        let (index, match_type) = self.database.index_manager.assure_index_exists(&self.model_id, &index_spec, &self.database.db)?;

        let ids = SledIndexScanner::new(&index, &bounds, scan_direction, match_type, prefix_guard_disabled)?;

        if remaining_predicate == Predicate::True && order_by_spill.is_satisfied() {
            return futures::executor::block_on(ids.limit(limit).entities(&self.database.entities_tree).collect_states());
        }

        // Values path: ids → projected-value lookup → filter/sort/limit →
        // canonical entity lookup. Sled has no multi-get API, so the final
        // primary-tree point reads are the native index-to-record access path.
        let projected = SledMaterializationLookup::new(&self.tree, &self.database.property_manager, ids);
        self.finish_projected_scan(projected, remaining_predicate, order_by_spill, limit)
    }
    fn exec_table_scan_plan(
        &self,
        bounds: KeyBounds,
        scan_direction: ScanDirection,
        remaining_predicate: Predicate,
        order_by_spill: OrderByComponents,
        limit: Option<u64>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        if remaining_predicate == Predicate::True && order_by_spill.is_satisfied() {
            let ids = SledMaterializationKeyScanner::new(&self.tree, &bounds, scan_direction)?;
            let states = SledEntityLookup::new(&self.database.entities_tree, ids.limit(limit));
            return futures::executor::block_on(states.collect_states());
        }

        // Values path: scan the model's projected-value tree, then share the
        // same filter/sort/limit/hydration pipeline as an index scan.
        let scanner = SledMaterializationScanner::new(&self.tree, &bounds, scan_direction, &self.database.property_manager)?;
        self.finish_projected_scan(scanner, remaining_predicate, order_by_spill, limit)
    }

    fn finish_projected_scan<S>(
        &self,
        projected: S,
        remaining_predicate: Predicate,
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
        schema::CatalogResolver,
        storage::{CommitBatchOutcome, PreparedEntityWrite, StorageEngine, StorageWriteBatch},
        value::{Value, ValueType},
    };
    use ankurah_proto::{Attested, Clock, EntityId, EventId, PropertyId, State, StateBuffers};
    use std::collections::BTreeMap;

    struct TestResolver {
        properties: BTreeMap<ModelId, Vec<PropertyId>>,
        names: BTreeMap<PropertyId, String>,
    }

    impl CatalogResolver for TestResolver {
        fn resolve_model_property(&self, model: &ModelId, name: &str) -> anyhow::Result<Option<PropertyId>> {
            Ok(self
                .properties
                .get(model)
                .into_iter()
                .flatten()
                .find_map(|property| (self.names.get(property).map(String::as_str) == Some(name)).then_some(*property)))
        }

        fn model_properties(&self, model: &ModelId) -> anyhow::Result<Vec<PropertyId>> {
            Ok(self.properties.get(model).cloned().unwrap_or_default())
        }

        fn model_name(&self, _model: &ModelId) -> anyhow::Result<String> { Ok("unused-by-sled".to_owned()) }

        fn property_name(&self, property: &PropertyId) -> anyhow::Result<String> {
            self.names.get(property).cloned().ok_or_else(|| anyhow::anyhow!("unknown property {property}"))
        }

        fn property_value_type(&self, property: &PropertyId) -> anyhow::Result<ValueType> {
            self.names.contains_key(property).then_some(ValueType::String).ok_or_else(|| anyhow::anyhow!("unknown property {property}"))
        }
    }

    fn entity_id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 16]) }

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
                    head: Clock::from(vec![event_id]),
                },
            },
            None,
        )
    }

    async fn commit_state(engine: &crate::SledStorageEngine, expected_head: Clock, model: ModelId, state: Attested<EntityState>) {
        let outcome =
            engine.commit_batch(StorageWriteBatch::new(vec![PreparedEntityWrite::new(expected_head, state, [model])])).await.unwrap();
        assert!(matches!(outcome, CommitBatchOutcome::Committed(_)));
    }

    #[tokio::test]
    async fn write_refreshes_every_associated_model_materialization() {
        let engine = crate::SledStorageEngine::new_test().unwrap();
        let model_a = ModelId::EntityId(entity_id(0x11));
        let model_b = ModelId::EntityId(entity_id(0x12));
        let property_a = PropertyId::EntityId(entity_id(0x21));
        let property_b = PropertyId::EntityId(entity_id(0x22));
        let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
            properties: BTreeMap::from([(model_a, vec![property_a]), (model_b, vec![property_b])]),
            names: BTreeMap::from([(property_a, "alpha".to_owned()), (property_b, "beta".to_owned())]),
        });
        engine.set_catalog_resolver(Arc::downgrade(&resolver));
        let entity = entity_id(0x31);

        let initial = state_with_strings(entity, 1, &[(property_a, "a1"), (property_b, "b1")]);
        commit_state(&engine, Clock::default(), model_a, initial.clone()).await;
        commit_state(&engine, initial.payload.state.head.clone(), model_b, initial.clone()).await;

        let updated = state_with_strings(entity, 2, &[(property_a, "a2"), (property_b, "b2")]);
        commit_state(&engine, initial.payload.state.head, model_a, updated).await;

        let selection = ankql::parser::parse_selection("beta = 'b2'").unwrap().resolve_names(&model_b, resolver.as_ref()).unwrap();
        let found = engine.fetch_states(&model_b, &selection).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].payload.entity_id, entity);
    }

    #[tokio::test]
    async fn stale_head_rolls_back_the_complete_batch() {
        let engine = crate::SledStorageEngine::new_test().unwrap();
        let model_a = ModelId::EntityId(entity_id(0x41));
        let model_b = ModelId::EntityId(entity_id(0x42));
        let property = PropertyId::EntityId(entity_id(0x43));
        let resolver: Arc<dyn CatalogResolver> = Arc::new(TestResolver {
            properties: BTreeMap::from([(model_a, vec![property]), (model_b, vec![property])]),
            names: BTreeMap::from([(property, "value".to_owned())]),
        });
        engine.set_catalog_resolver(Arc::downgrade(&resolver));
        let first_id = entity_id(0x44);
        let second_id = entity_id(0x45);
        let first = state_with_strings(first_id, 1, &[(property, "first-old")]);
        let second = state_with_strings(second_id, 2, &[(property, "second-old")]);
        commit_state(&engine, Clock::default(), model_a, first.clone()).await;
        commit_state(&engine, Clock::default(), model_a, second.clone()).await;

        let outcome = engine
            .commit_batch(StorageWriteBatch::new(vec![
                PreparedEntityWrite::new(
                    first.payload.state.head.clone(),
                    state_with_strings(first_id, 3, &[(property, "first-new")]),
                    [model_b],
                ),
                PreparedEntityWrite::new(Clock::default(), state_with_strings(second_id, 4, &[(property, "second-new")]), [model_b]),
            ]))
            .await
            .unwrap();
        let CommitBatchOutcome::Conflict { observed } = outcome else {
            panic!("one stale expected head must reject the complete batch");
        };
        assert_eq!(observed[&first_id].as_ref().unwrap().payload.state.head, first.payload.state.head);
        assert_eq!(observed[&second_id].as_ref().unwrap().payload.state.head, second.payload.state.head);
        assert_eq!(engine.get_state(first_id).await.unwrap().payload.state.head, first.payload.state.head);
        assert_eq!(engine.get_state(second_id).await.unwrap().payload.state.head, second.payload.state.head);

        let all = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        assert!(
            engine.fetch_states(&model_b, &all).await.unwrap().is_empty(),
            "a rejected batch must not publish associations, projections, or index-visible changes"
        );
    }
}
