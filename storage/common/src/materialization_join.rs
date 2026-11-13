use std::collections::BTreeMap;

use ankql::ast::{Comparison, ComparisonOperator, Predicate, Resolved, Selection};
use ankql::selection::map_references;
use ankurah_core_types::Value;
use ankurah_proto::{EntityId, ModelId, PropertyId};

use crate::{ColumnPath, EngineColumns};

/// An engine's physical table and the property columns actually present in it.
pub struct MaterializationTable {
    pub model: ModelId,
    pub table: String,
    pub columns: BTreeMap<PropertyId, String>,
}

/// Join materializations by entity identity, exposing property and membership
/// columns to the SQL predicate builder. Every table has at most one row per entity.
pub struct MaterializationJoin {
    tables: Vec<MaterializationTable>,
}

impl MaterializationJoin {
    pub fn new(tables: Vec<MaterializationTable>) -> Self { Self { tables } }

    pub fn has_missing_properties(&self, predicate: &Predicate<Resolved>) -> bool {
        predicate
            .referenced_properties()
            .iter()
            .any(|property| *property != PropertyId::Id && !self.tables.iter().any(|table| table.columns.contains_key(property)))
    }

    pub fn lower(&self, selection: &Selection<Resolved>) -> Selection<EngineColumns> {
        let absent: Vec<_> = selection
            .referenced_properties()
            .into_iter()
            .filter(|property| *property != PropertyId::Id && !self.tables.iter().any(|table| table.columns.contains_key(property)))
            .collect();
        map_references(
            &selection.assume_null(&absent),
            &|path| ColumnPath::new(property_column(&path.property_id()), path.subpath.clone()),
            &|model| *model,
        )
    }

    /// LEFT JOIN retains entities outside a materialization, so OR and NOT remain predicates.
    /// Property values are repeated in each of an entity's materializations; COALESCE
    /// reads them from whichever participating table has that entity.
    pub fn sql(&self, entity_table: &str, properties: &[PropertyId], models: &[ModelId]) -> String {
        let mut columns = vec![r#"e."id" AS "id""#.to_owned()];
        let mut joins = Vec::new();
        for model in models.iter().filter(|model| !self.tables.iter().any(|table| table.model == **model)) {
            columns.push(format!("FALSE AS {}", quote(&membership_column(model))));
        }
        for (index, table) in self.tables.iter().enumerate() {
            columns.push(format!(r#"(t{index}."id" IS NOT NULL) AS {}"#, quote(&membership_column(&table.model))));
            joins.push(format!(r#"LEFT JOIN {} t{index} ON t{index}."id" = e."id""#, quote(&table.table)));
        }
        for property in properties.iter().filter(|property| **property != PropertyId::Id) {
            let values: Vec<_> = self
                .tables
                .iter()
                .enumerate()
                .filter_map(|(index, table)| table.columns.get(property).map(|column| format!("t{index}.{}", quote(column))))
                .collect();
            let value = match values.as_slice() {
                [] => continue,
                [value] => value.clone(),
                _ => format!("COALESCE({})", values.join(", ")),
            };
            columns.push(format!("{value} AS {}", quote(&property_column(property))));
        }
        format!("SELECT {} FROM {} e {}", columns.join(", "), quote(entity_table), joins.join(" "))
    }
}

pub fn membership_column(model: &ModelId) -> String { format!("m_{model}") }

/// Extract an entity id range bound for SQL comparisons in byte order.
///
/// Matches `>`, `>=`, `<`, or `<=` between an id literal and the `id` column projected by
/// [`MaterializationJoin::sql`], in either operand order. Returns the operator with `id` on
/// the left and the literal on the right. Returns `None` for other comparisons.
pub fn entity_id_bound<'a>(comparison: Comparison<'a, EngineColumns>) -> Option<(ComparisonOperator, &'a EntityId)> {
    use ComparisonOperator::*;
    match comparison.path_op_lit()? {
        (path, operator @ (GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual), Value::EntityId(id))
            if path.is_simple() && path.column == "id" =>
        {
            Some((operator, id))
        }
        _ => None,
    }
}

fn property_column(property: &PropertyId) -> String { if *property == PropertyId::Id { "id".to_owned() } else { format!("p_{property}") } }

fn quote(identifier: &str) -> String { format!("\"{}\"", identifier.replace('"', "\"\"")) }

#[cfg(test)]
mod tests {
    use ankql::ast::Expr;

    use super::*;

    fn bound(
        left: Expr<EngineColumns>,
        operator: ComparisonOperator,
        right: Expr<EngineColumns>,
    ) -> Option<(ComparisonOperator, EntityId)> {
        entity_id_bound(Comparison { left: &left, operator, right: &right }).map(|(operator, id)| (operator, *id))
    }

    #[test]
    fn only_an_ordering_comparison_of_the_id_column_with_an_id_is_an_entity_id_bound() {
        use ComparisonOperator::*;
        let x = EntityId::from_bytes([7; 32]);
        let column = |name: &str| Expr::Path(ColumnPath::simple(name));
        let literal = || Expr::Literal(Value::EntityId(x));

        assert_eq!(bound(column("id"), GreaterThanOrEqual, literal()), Some((GreaterThanOrEqual, x)));
        assert_eq!(bound(literal(), GreaterThan, column("id")), Some((LessThan, x)));
        assert_eq!(bound(column("id"), Equal, literal()), None);
        // An id-valued property's column is not the entity's id
        assert_eq!(bound(column("p_parent"), GreaterThan, literal()), None);
        // A JSON path into the id column is not the entity's id
        assert_eq!(bound(Expr::Path(ColumnPath::new("id", vec!["x".to_owned()])), GreaterThan, literal()), None);
    }
}
