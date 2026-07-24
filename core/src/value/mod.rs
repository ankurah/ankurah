mod collatable;

pub use ankurah_core_types::{CastError, Value, ValueParseError, ValueType};

impl From<CastError> for crate::property::PropertyError {
    fn from(error: CastError) -> Self { Self::NonCastable(error) }
}

impl From<ValueParseError> for crate::property::PropertyError {
    fn from(error: ValueParseError) -> Self {
        match error {
            ValueParseError::Json(error) => Self::SerializeError(Box::new(error)),
            ValueParseError::InvalidVariant { given, ty } => Self::InvalidVariant { given, ty },
            ValueParseError::InvalidValue { value, ty } => Self::InvalidValue { value, ty },
        }
    }
}
