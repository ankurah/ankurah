use ankurah::Model;
use serde::{Deserialize, Serialize};

// Control log generation on the server
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Flags {
    pub name: String,
    pub value: bool,
}

use ankurah::Property;
#[cfg(feature = "wasm")]
use tsify::Tsify;

#[derive(Property, Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "wasm", derive(Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Property, Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "wasm", derive(Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum Payload {
    Text(String),
    Json(serde_json::Value),
}

// Log entry structure
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    #[active_type(LWW)]
    pub timestamp: String, // ISO8601 timestamp
    pub level: Level, // Log level enum
    #[active_type(YrsString)]
    pub message: String, // Log message
    #[active_type(LWW)]
    pub source: String, // Service/component name
    #[active_type(LWW)]
    pub node_id: String, // Which server generated it
    pub payload: Payload, // Additional structured data
}

// #[derive(Serialize, Deserialize, Debug)]
// pub struct TimeStamp(DateTime<Utc>);

// impl std::ops::Deref for TimeStamp {
//     type Target = DateTime<Utc>;
//     fn deref(&self) -> &Self::Target { &self.0 }
// }

// use ankurah::property::{Property, PropertyError};
// use ankurah::value::Value;
// impl Property for TimeStamp {
//     fn into_value(&self) -> Result<Option<ankurah::value::Value>, ankurah::property::PropertyError> {
//         Ok(Some(ankurah::value::Value::String(self.0.to_rfc3339())))
//     }

//     fn from_value(value: Option<ankurah::value::Value>) -> Result<Self, ankurah::property::PropertyError> {
//         let value = value.ok_or(ankurah::property::PropertyError::Missing)?;
//         let value = match value {
//             Value::String(value) => value,
//             _ => return Err(PropertyError::InvalidVariant { given: value, ty: "TimeStamp".to_string() }),
//         };
//         Ok(TimeStamp(
//             DateTime::parse_from_rfc3339(&value).map_err(|e| ankurah::property::PropertyError::DeserializeError(Box::new(e)))?.into(),
//         ))
//     }
// }
