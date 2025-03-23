use ankurah::Model;

pub struct Album {
    pub name: String,
}

// use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub added: String,
    pub ip_address: String,
    pub node_id: String,
    pub complex: Complex,
}

use ankurah::Property;
use tsify::Tsify;
#[derive(Property, Serialize, Deserialize, Debug, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct Complex {
    name: String,
    value: i32,
    thing: Thing,
}

#[derive(Property, Serialize, Deserialize, Debug, Tsify)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub enum Thing {
    Alpha(String),
    Bravo { b: String, c: i32 },
}

// #[derive(Serialize, Deserialize, Debug)]
// pub struct TimeStamp(DateTime<Utc>);

// impl std::ops::Deref for TimeStamp {
//     type Target = DateTime<Utc>;
//     fn deref(&self) -> &Self::Target { &self.0 }
// }

// use ankurah::property::{Property, PropertyError, PropertyValue};
// impl Property for TimeStamp {
//     fn into_value(&self) -> Result<Option<ankurah::property::PropertyValue>, ankurah::property::PropertyError> {
//         Ok(Some(ankurah::property::PropertyValue::String(self.0.to_rfc3339())))
//     }

//     fn from_value(value: Option<ankurah::property::PropertyValue>) -> Result<Self, ankurah::property::PropertyError> {
//         let value = value.ok_or(ankurah::property::PropertyError::Missing)?;
//         let value = match value {
//             PropertyValue::String(value) => value,
//             _ => return Err(PropertyError::InvalidVariant { given: value, ty: "TimeStamp".to_string() }),
//         };
//         Ok(TimeStamp(
//             DateTime::parse_from_rfc3339(&value).map_err(|e| ankurah::property::PropertyError::DeserializeError(Box::new(e)))?.into(),
//         ))
//     }
// }
