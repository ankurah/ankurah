use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core::{error::RetrievalError, storage::StorageEngine};
use ankurah_proto::{Attested, EntityId, EntityState, PropertyId};
use ankurah_storage_common::{
    materialization_join::{MaterializationJoin, MaterializationTable},
    materialization_plan::MaterializationPlan,
    selection::select_states,
};

use super::{load_states, SqliteStorageEngine, ENTITY_TABLE};
use crate::{
    error::SqliteError,
    sql_builder::{split_predicate_for_sqlite, SqlBuilder},
};

/// A selection over joined materializations, with any unsupported predicate retained for Rust.
pub(super) struct Query {
    sql: String,
    params: Vec<rusqlite::types::Value>,
    remaining: Selection<Resolved>,
}

impl Query {
    pub async fn prepare(engine: &SqliteStorageEngine, selection: &Selection<Resolved>) -> Result<Self, RetrievalError> {
        let plan = MaterializationPlan::new(selection);
        let properties: Vec<_> = selection.referenced_properties().into_iter().collect();
        let existing = engine.list_materializations().await?;
        let mut models = plan.models.clone();
        if plan.needs_all_entities() && properties.iter().any(|property| *property != PropertyId::Id) {
            models.extend(existing.iter().copied());
            models.sort();
            models.dedup();
        }
        let mut tables = Vec::new();
        for model in models.into_iter().filter(|model| existing.contains(model)) {
            let table = engine.materialization(&model).await?;
            tables.push(MaterializationTable { model, table: table.table().to_owned(), columns: table.query_columns(&properties).await? });
        }
        let join = MaterializationJoin::new(tables);
        let mut split = split_predicate_for_sqlite(&selection.predicate);
        // Folding an absent comparison to false beneath NOT could grant a read.
        // Retain the original predicate for the fail-closed stored-state evaluator.
        if join.has_missing_properties(&split.sql_predicate) {
            split.sql_predicate = Predicate::True;
            split.remaining_predicate = selection.predicate.clone();
        }
        let pushed = Selection {
            predicate: split.sql_predicate.clone(),
            order_by: selection.order_by.clone(),
            limit: if split.needs_post_filter() { None } else { selection.limit },
        };
        let mut builder = SqlBuilder::new();
        builder.selection(&join.lower(&pushed)).map_err(SqliteError::from)?;
        let (predicate_sql, params) = builder.build_where_clause();
        Ok(Self {
            sql: format!(
                r#"SELECT "id" FROM ({}) AS materialized WHERE {predicate_sql}"#,
                join.sql(ENTITY_TABLE, &properties, &plan.models)
            ),
            params,
            remaining: Selection { predicate: split.remaining_predicate, order_by: None, limit: selection.limit },
        })
    }

    fn read_ids(&self, snapshot: &rusqlite::Transaction<'_>) -> Result<Vec<EntityId>, SqliteError> {
        let mut stmt = snapshot.prepare(&self.sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(&self.params), |row| row.get::<_, String>(0))?;
        rows.map(|row| EntityId::from_base64(&row?).map_err(|error| SqliteError::CorruptRecord(error.to_string()))).collect()
    }

    pub async fn states(self, engine: &SqliteStorageEngine) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let conn = engine.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        engine.ensure_shared_tables(&conn).await?;
        let states = conn
            .with_connection_mut(move |c| {
                let snapshot = c.transaction()?;
                let ids = self.read_ids(&snapshot)?;
                Ok((load_states(&snapshot, &ids)?, self.remaining))
            })
            .await?;
        select_states(states.0, &states.1)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, PropertyPath};
    use ankurah_core::{
        property::backend::{LWWBackend, PropertyBackend},
        storage::{StorageCommitOutcome, StorageTransaction},
        value::Value,
    };
    use ankurah_proto::{Clock, ModelId, State, StateBuffers};

    use super::*;

    fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([101; 32])) }

    fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([201; 32])) }

    fn compare(path: PropertyPath, operator: ComparisonOperator, value: Value) -> Predicate<Resolved> {
        Predicate::Comparison { left: Box::new(Expr::Path(path)), operator, right: Box::new(Expr::Literal(value)) }
    }

    fn and(left: Predicate<Resolved>, right: Predicate<Resolved>) -> Predicate<Resolved> { Predicate::And(Box::new(left), Box::new(right)) }

    /// Return SQLite's query plan steps for the SQL generated from `selection`.
    async fn plan(engine: &SqliteStorageEngine, selection: &Selection<Resolved>) -> anyhow::Result<Vec<String>> {
        let query = Query::prepare(engine, selection).await?;
        let conn = engine.pool.get().await?;
        Ok(conn
            .with_connection(move |c| {
                let mut statement = c.prepare(&format!("EXPLAIN QUERY PLAN {}", query.sql))?;
                let steps = statement.query_map(rusqlite::params_from_iter(&query.params), |row| row.get::<_, String>(3))?;
                Ok(steps.collect::<Result<Vec<_>, _>>()?)
            })
            .await?)
    }

    /// SQLite scans the id byte index for both range bounds and ORDER BY id, without a separate sort.
    /// Cover queries with no joins, with a component materialization join, and with that join
    /// plus a property condition.
    #[tokio::test]
    async fn an_id_range_ordered_by_id_is_searched_in_the_id_bytes_index() -> anyhow::Result<()> {
        let engine = SqliteStorageEngine::open_in_memory().await?;
        let mut ids = Vec::new();
        for byte in 1..=40u8 {
            let backend = LWWBackend::new();
            backend.set(property(), Some(Value::String(format!("{}", byte % 3))));
            let event = ankurah_core::test_helpers::commit_backend(&backend, byte)?;
            let id = EntityId::from_bytes([byte.wrapping_mul(37); 32]);
            let state = Attested::opt(
                EntityState {
                    entity_id: id,
                    state: State {
                        state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                        memberships: BTreeSet::from([model()]),
                        head: Clock::singleton(&event),
                    },
                },
                None,
            );
            let mut transaction = engine.transaction();
            transaction.set_state(&Clock::default(), &state).await?;
            assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
            ids.push(id);
        }
        let above = compare(PropertyPath::id(), ComparisonOperator::GreaterThan, Value::EntityId(ids[7]));
        let prefix = EntityId::base64_prefix_range(&ids[7].to_base64()[..2])?;
        let in_prefix = and(
            compare(PropertyPath::id(), ComparisonOperator::GreaterThanOrEqual, Value::EntityId(*prefix.start())),
            compare(PropertyPath::id(), ComparisonOperator::LessThanOrEqual, Value::EntityId(*prefix.end())),
        );
        let beside_property = and(in_prefix, compare(property().into(), ComparisonOperator::Equal, Value::String("1".into())));
        let cases = [
            ("id > X", Selection { predicate: above.clone(), order_by: None, limit: None }),
            ("id > X, of the model", Selection { predicate: above, order_by: None, limit: None }.and_member_of(model())),
            (
                "a prefix beside a property, of the model",
                Selection { predicate: beside_property, order_by: None, limit: None }.and_member_of(model()),
            ),
        ];
        for (case, mut selection) in cases {
            for direction in [OrderDirection::Asc, OrderDirection::Desc] {
                selection.order_by = Some(vec![OrderByItem { path: PropertyPath::id(), direction: direction.clone() }]);
                selection.limit = Some(5);
                let steps = plan(&engine, &selection).await?;
                assert!(
                    steps.iter().any(|step| step.contains("USING INDEX _ankurah_entity_id_bytes_idx (<expr>")),
                    "{case} ORDER BY id {direction:?}: {steps:?}"
                );
                assert!(!steps.iter().any(|step| step.contains("TEMP B-TREE")), "{case} ORDER BY id {direction:?}: {steps:?}");
            }
        }
        Ok(())
    }
}
