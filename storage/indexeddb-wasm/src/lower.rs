//! IndexedDB column lowering and materialization scoping.

use ankql::ast::{ComparisonOperator, Expr, Predicate, PropertyId, Resolved, Selection};
use ankql::selection::map_references;
use ankurah_core_types::Value;
use ankurah_storage_common::{ColumnPath, EngineColumns};
use std::collections::BTreeMap;

use crate::statics::MATERIALIZATION_KEY;

/// Lower through durable field assignments and restrict the shared store to this materialization.
pub(crate) fn lower(
    selection: &Selection<Resolved>,
    columns: &BTreeMap<PropertyId, String>,
    materialization: &str,
) -> Selection<EngineColumns> {
    let mut lowered = map_references(
        selection,
        &|path| {
            let column = if path.property_id() == PropertyId::Id { "id".to_owned() } else { columns[&path.property_id()].clone() };
            ColumnPath::new(column, path.subpath.clone())
        },
        &|model| *model,
    );
    let scope = Predicate::Comparison {
        left: Box::new(Expr::Path(ColumnPath::simple(MATERIALIZATION_KEY.to_string()))),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::String(materialization.to_owned()))),
    };
    lowered.predicate = Predicate::And(Box::new(scope), Box::new(lowered.predicate));
    lowered
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use ankql::ast::{OrderByItem, OrderDirection, SystemProperty};

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test::wasm_bindgen_test]
    fn builtin_property_columns_are_unchanged() {
        for property in [PropertyId::Id, PropertyId::System(SystemProperty::Item), PropertyId::System(SystemProperty::Name)] {
            let selection = Selection::<Resolved> {
                predicate: Predicate::True,
                order_by: Some(vec![OrderByItem { path: property.into(), direction: OrderDirection::Asc }]),
                limit: None,
            };
            let columns = [(property, property.to_string())].into();
            assert_eq!(lower(&selection, &columns, "key_paths").order_by.unwrap()[0].path.column, property.to_string());
        }
    }
}
