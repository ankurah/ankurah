//! IndexedDB property lowering and collection scoping.

use ankql::ast::{ComparisonOperator, Expr, Predicate, PropertyPath, Resolved, Selection};
use ankurah_core_types::Value;
use ankurah_storage_common::{lower_selection, ColumnPath, EngineColumns};

use crate::statics::COLLECTION_KEY;

pub fn lower(selection: &Selection<Resolved>, collection: &ankurah_proto::CollectionId) -> Selection<EngineColumns> {
    let mut lowered =
        lower_selection(selection, &|path: &PropertyPath| ColumnPath::new(path.property_id().to_string(), path.subpath.clone()));

    let scope = Predicate::Comparison {
        left: Box::new(Expr::Path(ColumnPath::simple(COLLECTION_KEY.to_string()))),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::String(collection.to_string()))),
    };
    lowered.predicate = Predicate::And(Box::new(scope), Box::new(lowered.predicate));
    lowered
}
