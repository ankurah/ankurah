//! IndexedDB property lowering and collection scoping.

use ankql::ast::{ComparisonOperator, Expr, Predicate, PropertyId, PropertyPath, Resolved, Selection};
use ankurah_core_types::Value;
use ankurah_storage_common::{lower_selection, ColumnPath, EngineColumns};

use crate::statics::COLLECTION_KEY;

/// Registered IDs need a native identifier: base64 can start with a digit or
/// contain '-'. '$' is not in base64, so the replacement is unambiguous.
pub(crate) fn property_column(property: PropertyId) -> String {
    match property {
        PropertyId::EntityId(id) => format!("p${}", id.to_string().replace('-', "$")),
        _ => property.to_string(),
    }
}

/// Map resolved properties to physical columns and restrict the shared store to this collection.
pub fn lower(selection: &Selection<Resolved>, collection: &ankurah_proto::CollectionId) -> Selection<EngineColumns> {
    let mut lowered =
        lower_selection(selection, &|path: &PropertyPath| ColumnPath::new(property_column(path.property_id()), path.subpath.clone()));

    let scope = Predicate::Comparison {
        left: Box::new(Expr::Path(ColumnPath::simple(COLLECTION_KEY.to_string()))),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::String(collection.to_string()))),
    };
    lowered.predicate = Predicate::And(Box::new(scope), Box::new(lowered.predicate));
    lowered
}
