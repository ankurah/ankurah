pub mod backend;
pub mod traits;
pub mod value;

use ankurah_proto::EntityId;

pub use traits::{FromActiveType, FromEntity, InitializeWith, PropertyError};
pub use value::{Json, Ref, YrsString};

use crate::value::Value;

pub type PropertyName = String;

pub trait Property: Sized {
    /// The NORMATIVE catalog value_type this Rust type carries on the wire: a
    /// lowercased `core::value::ValueType` variant name ("string", "i64",
    /// "entityid", ...) that MUST equal the `Value` variant [`into_value`]
    /// actually produces (RFC 4 in specs/model-property-metadata/rfc.md mapping table).
    ///
    /// IDENTITY-CRITICAL: `#[derive(Model)]` reads this const (at compile
    /// time, via the associated const) for field types outside the built-in
    /// table, and the value feeds the property-id derivation (RFC 5.1) --
    /// two nodes disagreeing on it mint different ids for the same field,
    /// and CHANGING it for a shipped type is a retype (mints a new property
    /// identity, RFC 5.8). Defaults to "string" because the JSON-in-a-string
    /// register is the catch-all serialization (`#[derive(Property)]` pins
    /// it explicitly); a hand-written impl producing any other variant must
    /// override this to match.
    const VALUE_TYPE: &'static str = "string";

    fn into_value(&self) -> Result<Option<Value>, PropertyError>;
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError>;

    /// The value an ABSENT REQUIRED property of this type reads back as under
    /// RFC 5.4 rule 3 (the #175 fix): the value type's default, fed to
    /// [`from_value`] by the compiled projection when the backend holds no
    /// entry AND the sibling gate is clear (see
    /// `LWWBackend::get_checked` and `property/value/lww.rs`).
    ///
    /// `None` means "no fabricable default" (`from_value(None)` then decides:
    /// a required scalar would error `Missing`, an `Option<T>` swallows it to
    /// `None`). This is the correct behavior for `EntityId`/`Ref<T>` (rule 3's
    /// flagged exception -- an entity id has no zero value to invent) and for
    /// arbitrary derived `Property` types (enums etc. have no natural default),
    /// so it is the trait default here. The primitive value types override it
    /// with their concrete default; `Json` overrides it with JSON null.
    ///
    /// Note this is keyed on the PROJECTED type, so an `Option<i64>` field
    /// resolves to `Option::<i64>::absent_default()` (the trait default `None`,
    /// hence `None`), and the inner `i64` default never fires: optionality
    /// short-circuits ahead of the required default, exactly as intended.
    fn absent_default() -> Option<Value> { None }
}

impl<T> Property for Option<T>
where T: Property
{
    // Optionality is a membership fact, not a value-type fact (RFC 4): the
    // wire type is the inner type's.
    const VALUE_TYPE: &'static str = T::VALUE_TYPE;
    fn into_value(&self) -> Result<Option<Value>, PropertyError> {
        match self {
            Some(value) => Ok(<T as Property>::into_value(value)?),
            None => Ok(None),
        }
    }
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
        match T::from_value(value) {
            Ok(value) => Ok(Some(value)),
            Err(PropertyError::Missing) => Ok(None),
            Err(err) => Err(err),
        }
    }
    // Inherit the trait default (`None`): an absent optional reads as `None`,
    // never a fabricated inner default (RFC 5.4 rule 2).
}

/// Property types with a fabricable required-absent default (RFC 5.4 rule 3).
macro_rules! into {
    ($ty:ty => $variant:ident, $value_type:literal) => {
        impl Property for $ty {
            const VALUE_TYPE: &'static str = $value_type;
            fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::$variant(self.clone()))) }
            fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
                match value {
                    Some(Value::$variant(value)) => Ok(value),
                    Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
                    None => Err(PropertyError::Missing),
                }
            }
            fn absent_default() -> Option<Value> { Some(Value::$variant(<$ty>::default())) }
        }
        impl From<$ty> for Value {
            fn from(value: $ty) -> Self { Value::$variant(value) }
        }
    };
}

/// Property types with NO fabricable default: absent required reads stay
/// `Missing` (RFC 5.4 rule 3's flagged exception -- e.g. `EntityId`, which has
/// no zero value to invent). Same wire mapping as [`into!`], but leaves
/// `absent_default` at the trait default (`None`).
macro_rules! into_no_default {
    ($ty:ty => $variant:ident, $value_type:literal) => {
        impl Property for $ty {
            const VALUE_TYPE: &'static str = $value_type;
            fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::$variant(self.clone()))) }
            fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
                match value {
                    Some(Value::$variant(value)) => Ok(value),
                    Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
                    None => Err(PropertyError::Missing),
                }
            }
        }
        impl From<$ty> for Value {
            fn from(value: $ty) -> Self { Value::$variant(value) }
        }
    };
}

into!(String => String, "string");
into!(i16 => I16, "i16");
into!(i32 => I32, "i32");
into!(i64 => I64, "i64");
into!(f64 => F64, "f64");
into!(bool => Bool, "bool");
into!(Vec<u8> => Binary, "binary");
// EntityId has no zero value to fabricate: absent required stays Missing.
into_no_default!(EntityId => EntityId, "entityid");

impl<'a> Property for std::borrow::Cow<'a, str> {
    const VALUE_TYPE: &'static str = "string";
    fn into_value(&self) -> Result<Option<Value>, PropertyError> { Ok(Some(Value::String(self.to_string()))) }

    fn from_value(value: Option<Value>) -> Result<Self, PropertyError> {
        match value {
            Some(Value::String(value)) => Ok(value.into()),
            Some(variant) => Err(PropertyError::InvalidVariant { given: variant, ty: stringify!($ty).to_owned() }),
            None => Err(PropertyError::Missing),
        }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self { Value::String(value.to_string()) }
}
