#![allow(clippy::wrong_self_convention)]

#[cfg(feature = "wasm")]
pub use serde_wasm_bindgen;
#[cfg(feature = "wasm")]
use wasm_bindgen::{JsCast, JsValue};

pub struct SerializationConfig {
    pub missing_as_null: bool,
    pub hashmap_as_object: bool,
    pub large_number_types_as_bigints: bool,
}

/// `Tsify` is a trait that allows you to convert a type to and from JavaScript.
/// Can be implemented manually if you need to customize the serialization or deserialization.
pub trait Tsify {
    #[cfg(feature = "wasm")]
    type JsType: JsCast;

    const DECL: &'static str;
    const SERIALIZATION_CONFIG: SerializationConfig =
        SerializationConfig { missing_as_null: false, hashmap_as_object: false, large_number_types_as_bigints: false };

    #[cfg(feature = "wasm")]
    #[inline]
    fn into_js(&self) -> Result<Self::JsType, serde_wasm_bindgen::Error>
    where Self: serde::Serialize {
        let config = <Self as Tsify>::SERIALIZATION_CONFIG;
        let serializer = serde_wasm_bindgen::Serializer::new()
            .serialize_missing_as_null(config.missing_as_null)
            .serialize_maps_as_objects(config.hashmap_as_object)
            .serialize_large_number_types_as_bigints(config.large_number_types_as_bigints);
        self.serialize(&serializer).map(JsCast::unchecked_from_js)
    }

    #[cfg(feature = "wasm")]
    #[inline]
    fn from_js<T: Into<JsValue>>(js: T) -> Result<Self, serde_wasm_bindgen::Error>
    where Self: serde::de::DeserializeOwned {
        serde_wasm_bindgen::from_value(js.into())
    }
}
