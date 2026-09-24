use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core::{error::RetrievalError, storage::StorageEngine};
use ankurah_proto::{Attested, EntityId, EntityState, PropertyId};
use ankurah_storage_common::{
    materialization_join::{MaterializationJoin, MaterializationTable},
    materialization_plan::MaterializationPlan,
    selection::select_states,
};

use super::{load_states, SqliteStorageEngine, ENTITY_TABLE};
use crate::{error::SqliteError, sql_builder::{split_predicate_for_sqlite, SqlBuilder}};

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
            sql: format!(r#"SELECT "id" FROM ({}) AS materialized WHERE {predicate_sql}"#, join.sql(ENTITY_TABLE, &properties, &plan.models)),
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
        let states = conn.with_connection_mut(move |c| {
            let snapshot = c.transaction()?;
            let ids = self.read_ids(&snapshot)?;
            Ok((load_states(&snapshot, &ids)?, self.remaining))
        }).await?;
        select_states(states.0, &states.1)
    }

    pub async fn ids(self, engine: &SqliteStorageEngine) -> Result<Vec<EntityId>, RetrievalError> {
        let conn = engine.pool.get().await.map_err(|error| SqliteError::Pool(error.to_string()))?;
        engine.ensure_shared_tables(&conn).await?;
        let (ids, states, remaining) = conn.with_connection_mut(move |c| {
            let snapshot = c.transaction()?;
            let ids = self.read_ids(&snapshot)?;
            let states = if matches!(self.remaining.predicate, Predicate::True) { None } else { Some(load_states(&snapshot, &ids)?) };
            Ok((ids, states, self.remaining))
        }).await?;
        match states {
            None => Ok(ids),
            Some(states) => Ok(select_states(states, &remaining)?.into_iter().map(|state| state.payload.entity_id).collect()),
        }
    }
}
