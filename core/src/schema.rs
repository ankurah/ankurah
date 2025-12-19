use crate::property::PropertyError;
use crate::value::ValueType;
use ankql::ast::PathExpr;

/// Trait for providing schema information about collections
pub trait CollectionSchema {
    /// Get the ValueType for a given field path
    fn field_type(&self, path: &PathExpr) -> Result<ValueType, PropertyError>;
}
