use crate::property::PropertyError;
use crate::value::ValueType;
use ankql::ast::Identifier;

/// Trait for providing schema information about collections
pub trait CollectionSchema {
    /// Get the ValueType for a given field identifier
    fn field_type(&self, identifier: &Identifier) -> Result<ValueType, PropertyError>;
}
