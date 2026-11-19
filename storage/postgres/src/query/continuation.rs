//! Predicate checks and ordered refill through an already prepared query.

use std::collections::BTreeSet;

use super::*;

impl Query {
    /// Check only membership, without the selection's ORDER BY or LIMIT.
    pub(crate) async fn matching_ids_in<C: GenericClient + Sync>(
        &self,
        snapshot: &C,
        ids: &[EntityId],
    ) -> Result<Vec<EntityId>, RetrievalError> {
        let remaining = Selection { predicate: self.remaining.predicate.clone(), order_by: None, limit: None };
        let mut matched = BTreeSet::new();
        for ids in ids.chunks(256) {
            let mut builder = self.predicate_builder()?;
            builder.sql(" AND \"id\" = ANY(");
            builder.arg(ids.to_vec());
            builder.sql(")");
            let ids = self.ids_in(snapshot, builder).await?;
            let mut selected = StateSelection::new(&remaining);
            for ids in ids.chunks(256) {
                for state in load_states(snapshot, ids).await? {
                    selected.push(state)?;
                }
            }
            matched.extend(selected.finish().into_iter().map(|state| state.payload.entity_id));
        }
        Ok(ids.iter().filter(|id| matched.contains(id)).copied().collect())
    }

    /// Return at most `limit` members strictly after the captured order tuple.
    pub(crate) async fn select_after_in<C: GenericClient + Sync>(
        &self,
        snapshot: &C,
        after: Option<&OrderBoundary>,
        limit: NonZeroUsize,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let remaining = Selection {
            predicate: self.remaining.predicate.clone(),
            order_by: self.selection.order_by.clone(),
            limit: Some(limit.get() as u64),
        };
        let mut selected = StateSelection::new(&remaining).after(after)?;
        // Capping SQL before residual filtering could omit replacement members.
        // Fallback scans bounded ID pages and retains only the requested cap.
        let native = self.pushed.order_by.is_some()
            && matches!(remaining.predicate, Predicate::True)
            && after.is_none_or(|after| self.boundary_values(after).is_some());
        let page_limit = if native { limit.get() } else { 256 };
        let mut seek = None;
        loop {
            let builder = self.page_builder(after, seek, native, page_limit)?;
            let ids = self.ids_in(snapshot, builder).await?;
            for ids in ids.chunks(256) {
                for state in load_states(snapshot, ids).await? {
                    selected.push(state)?;
                }
            }
            if native || ids.len() < page_limit {
                break;
            }
            seek = ids.last().copied();
        }
        Ok(selected.finish())
    }

    /// Lowering removes properties absent from every materialization. Those
    /// are constant NULLs, so omit their boundary components only when the
    /// captured value is NULL too. Otherwise canonical fallback compares the
    /// constant with the supplied value before considering later components.
    fn boundary_values(&self, after: &OrderBoundary) -> Option<Vec<Option<ankurah_core_types::Value>>> {
        let order = self.pushed.order_by.as_ref()?;
        let mut values = Vec::new();
        for (part, value) in after.order().iter().zip(after.values()) {
            let column = property_column(&part.path.property_id());
            if order.iter().any(|part_| part_.path.column == column && part_.path.subpath == part.path.subpath) {
                values.push(value.clone());
            } else if value.is_some() {
                return None;
            }
        }
        Some(values)
    }

    fn predicate_builder(&self) -> Result<SqlBuilder, RetrievalError> {
        let mut builder = SqlBuilder::new();
        builder.order_types(self.order_types.clone());
        builder.sql("(");
        builder.predicate(&self.pushed.predicate)?;
        builder.sql(")");
        Ok(builder)
    }

    fn page_builder(
        &self,
        after: Option<&OrderBoundary>,
        seek: Option<EntityId>,
        ordered: bool,
        limit: usize,
    ) -> Result<SqlBuilder, RetrievalError> {
        let mut builder = self.predicate_builder()?;
        if let Some(after) = after {
            if let Some(values) = self.boundary_values(after) {
                builder.sql(" AND ");
                builder.after_order(self.pushed.order_by.as_ref().unwrap(), &values)?;
            }
        }
        if let Some(id) = seek {
            builder.sql(" AND ");
            builder.sql(crate::id_bytes::SQL);
            builder.sql(" > ");
            builder.arg(id.to_bytes().to_vec());
        }
        builder.sql(" ORDER BY ");
        if ordered {
            for (index, part) in self.pushed.order_by.iter().flatten().enumerate() {
                if index != 0 {
                    builder.sql(", ");
                }
                builder.order_by_item(part)?;
            }
        } else {
            builder.sql(crate::id_bytes::SQL);
            builder.sql(" ASC");
        }
        builder.sql(" LIMIT ");
        builder.arg(i64::try_from(limit).unwrap_or(i64::MAX));
        Ok(builder)
    }

    async fn ids_in<C: GenericClient + Sync>(&self, snapshot: &C, builder: SqlBuilder) -> Result<Vec<EntityId>, RetrievalError> {
        let (predicate, params) = builder.build_where_clause();
        let sql = format!(r#"SELECT "id" FROM ({}) AS materialized WHERE {predicate}"#, self.source);
        let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|value| value.as_ref() as _).collect();
        snapshot
            .query(&sql, &params)
            .await
            .map_err(RetrievalError::storage)?
            .into_iter()
            .map(|row| row.try_get("id").map_err(RetrievalError::storage))
            .collect()
    }
}
