//! Which field IndexedDB reads a resolved property from, and the collection
//! scoping this engine adds while lowering.

use ankql::ast::{ComparisonOperator, Expr, Predicate, PropertyPath, Resolved, Selection};
use ankurah_core_types::Value;
use ankurah_storage_common::{lower_selection, ColumnPath, EngineColumns};

use crate::statics::COLLECTION_KEY;

/// Rewrite a resolved selection into the fields this engine stores entities
/// under -- the property id's own rendering, with the `id` pseudo-property
/// rendering as `"id"` -- and scope it to `collection`.
///
/// The scoping conjunct is an ordinary comparison in this engine's own
/// vocabulary, indistinguishable to the planner from any other, which is why
/// it is added here rather than injected into the query the caller wrote:
/// `__collection` is a field of this engine's making and names no property
/// any catalog knows. `set_state` writes it under the same
/// [`COLLECTION_KEY`], and every collection shares one object store, so every
/// scan is bounded by it.
///
/// This is IndexedDB's own seam, deliberately not shared with the other
/// engines: the engine-side catalog resolver replaces this rendering with the
/// physical field name this engine assigned the property.
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
