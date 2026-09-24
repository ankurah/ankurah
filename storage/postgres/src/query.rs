use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core::{error::RetrievalError, storage::StorageEngine};
use ankurah_proto::{Attested, EntityId, EntityState, PropertyId};
use ankurah_storage_common::{
    materialization_join::{MaterializationJoin, MaterializationTable},
    materialization_plan::MaterializationPlan,
    selection::select_states,
};
use tokio_postgres::types::ToSql;

use crate::{load_states, read_snapshot, sql_builder::{split_predicate_for_postgres, SqlBuilder}, Postgres, ENTITY_TABLE};

/// A selection over joined materializations, with any unsupported predicate retained for Rust.
pub(super) struct Query {
    sql: String,
    params: Vec<Box<dyn ToSql + Send + Sync>>,
    remaining: Selection<Resolved>,
}

impl Query {
    pub async fn prepare(engine: &Postgres, selection: &Selection<Resolved>) -> Result<Self, RetrievalError> {
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
        let mut split = split_predicate_for_postgres(&selection.predicate);
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
        builder.selection(&join.lower(&pushed))?;
        let (predicate_sql, params) = builder.build_where_clause();
        Ok(Self {
            sql: format!(r#"SELECT "id" FROM ({}) AS materialized WHERE {predicate_sql}"#, join.sql(ENTITY_TABLE, &properties, &plan.models)),
            params,
            remaining: Selection { predicate: split.remaining_predicate, order_by: None, limit: selection.limit },
        })
    }

    async fn read_ids(&self, snapshot: &tokio_postgres::Transaction<'_>) -> Result<Vec<EntityId>, RetrievalError> {
        let params: Vec<&(dyn ToSql + Sync)> = self.params.iter().map(|value| value.as_ref() as _).collect();
        snapshot.query(&self.sql, &params).await.map_err(RetrievalError::storage)?
            .into_iter().map(|row| row.try_get("id").map_err(RetrievalError::storage)).collect()
    }

    pub async fn states(self, engine: &Postgres) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let mut client = engine.pool.get().await.map_err(RetrievalError::storage)?;
        engine.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let snapshot = read_snapshot(&mut client).await?;
        let ids = self.read_ids(&snapshot).await?;
        select_states(load_states(&snapshot, &ids).await?, &self.remaining)
    }

    pub async fn ids(self, engine: &Postgres) -> Result<Vec<EntityId>, RetrievalError> {
        let mut client = engine.pool.get().await.map_err(RetrievalError::storage)?;
        engine.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let snapshot = read_snapshot(&mut client).await?;
        let ids = self.read_ids(&snapshot).await?;
        if matches!(self.remaining.predicate, Predicate::True) { return Ok(ids); }
        Ok(select_states(load_states(&snapshot, &ids).await?, &self.remaining)?
            .into_iter().map(|state| state.payload.entity_id).collect())
    }
}
