use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core::{error::RetrievalError, storage::StorageEngine};
use ankurah_proto::{Attested, EntityId, EntityState, PropertyId};
use ankurah_storage_common::{
    materialization_join::{MaterializationJoin, MaterializationTable},
    materialization_plan::MaterializationPlan,
    selection::select_states,
};
use tokio_postgres::types::ToSql;

use crate::{
    load_states, read_snapshot,
    sql_builder::{split_predicate_for_postgres, SqlBuilder},
    Postgres, ENTITY_TABLE,
};

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
            sql: format!(
                r#"SELECT "id" FROM ({}) AS materialized WHERE {predicate_sql}"#,
                join.sql(ENTITY_TABLE, &properties, &plan.models)
            ),
            params,
            remaining: Selection { predicate: split.remaining_predicate, order_by: None, limit: selection.limit },
        })
    }

    async fn read_ids(&self, snapshot: &tokio_postgres::Transaction<'_>) -> Result<Vec<EntityId>, RetrievalError> {
        let params: Vec<&(dyn ToSql + Sync)> = self.params.iter().map(|value| value.as_ref() as _).collect();
        snapshot
            .query(&self.sql, &params)
            .await
            .map_err(RetrievalError::storage)?
            .into_iter()
            .map(|row| row.try_get("id").map_err(RetrievalError::storage))
            .collect()
    }

    pub async fn states(self, engine: &Postgres) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let mut client = engine.pool.get().await.map_err(RetrievalError::storage)?;
        engine.ensure_shared_tables(&client).await.map_err(RetrievalError::storage)?;
        let snapshot = read_snapshot(&mut client).await?;
        let ids = self.read_ids(&snapshot).await?;
        select_states(load_states(&snapshot, &ids).await?, &self.remaining)
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
    use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

    use super::*;

    fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([101; 32])) }

    fn property() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([201; 32])) }

    fn compare(path: PropertyPath, operator: ComparisonOperator, value: Value) -> Predicate<Resolved> {
        Predicate::Comparison { left: Box::new(Expr::Path(path)), operator, right: Box::new(Expr::Literal(value)) }
    }

    fn and(left: Predicate<Resolved>, right: Predicate<Resolved>) -> Predicate<Resolved> { Predicate::And(Box::new(left), Box::new(right)) }

    /// Return PostgreSQL's plan nodes for the SQL generated from `selection`.
    /// Discourage sequential scans on this small fixture so the test can inspect an index plan.
    async fn plan(engine: &Postgres, selection: &Selection<Resolved>) -> anyhow::Result<Vec<serde_json::Value>> {
        let query = Query::prepare(engine, selection).await?;
        let mut client = engine.pool.get().await?;
        let transaction = client.transaction().await?;
        transaction.execute("SET LOCAL enable_seqscan = off", &[]).await?;
        let params: Vec<&(dyn ToSql + Sync)> = query.params.iter().map(|value| value.as_ref() as _).collect();
        let explained: serde_json::Value = transaction.query_one(&format!("EXPLAIN (FORMAT JSON) {}", query.sql), &params).await?.get(0);
        let mut nodes = Vec::new();
        let mut pending = vec![explained[0]["Plan"].clone()];
        while let Some(node) = pending.pop() {
            pending.extend(node["Plans"].as_array().into_iter().flatten().cloned());
            nodes.push(node);
        }
        Ok(nodes)
    }

    /// PostgreSQL uses the id byte index for both range bounds and ORDER BY id, without a separate sort.
    /// It scans backward for descending order. Cover queries with no joins, with a component
    /// materialization join, and with that join plus a property condition.
    #[tokio::test]
    async fn an_id_range_ordered_by_id_is_searched_in_the_id_bytes_index() -> anyhow::Result<()> {
        let container = postgres::Postgres::default().with_init_sql(include_bytes!("../tests/pg_init.sql").to_vec()).start().await?;
        let uri = format!(
            "host={} port={} user=postgres password=postgres dbname=postgres",
            container.get_host().await?,
            container.get_host_port_ipv4(5432).await?,
        );
        let engine = Postgres::open(&uri).await?;
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
        // Format the operator and id bytes as they appear in EXPLAIN's index condition.
        let bound = |operator: &str, id: &EntityId| format!("{operator} '\\x{}'", id.to_bytes().map(|byte| format!("{byte:02x}")).concat());
        let cases = [
            ("id > X", Selection { predicate: above.clone(), order_by: None, limit: None }, vec![bound(">", &ids[7])]),
            (
                "id > X, of the model",
                Selection { predicate: above, order_by: None, limit: None }.and_member_of(model()),
                vec![bound(">", &ids[7])],
            ),
            (
                "a prefix beside a property, of the model",
                Selection { predicate: beside_property, order_by: None, limit: None }.and_member_of(model()),
                vec![bound(">=", prefix.start()), bound("<=", prefix.end())],
            ),
        ];
        for (case, mut selection, bounds) in cases {
            for (direction, scan) in [(OrderDirection::Asc, "Forward"), (OrderDirection::Desc, "Backward")] {
                selection.order_by = Some(vec![OrderByItem { path: PropertyPath::id(), direction: direction.clone() }]);
                selection.limit = Some(5);
                let nodes = plan(&engine, &selection).await?;
                let context = format!("{case} ORDER BY id {direction:?}: {nodes:#?}");
                let index_scan = nodes.iter().find(|node| node["Index Name"] == "_ankurah_entity_id_bytes_idx").expect(&context);
                assert_eq!(index_scan["Scan Direction"], scan, "{context}");
                let condition = index_scan["Index Cond"].as_str().unwrap_or_default();
                assert!(bounds.iter().all(|bound| condition.contains(bound)), "{bounds:?} in {context}");
                assert!(!nodes.iter().any(|node| node["Node Type"].as_str().is_some_and(|kind| kind.contains("Sort"))), "{context}");
            }
        }
        Ok(())
    }
}
