// #[cfg(feature = "wasm")]
// use wasm_bindgen::prelude::*;

// pub struct DateTime(chrono::DateTime<chrono::Utc>);

// pub struct LocalDateTime(chrono::DateTime<chrono::Local>);

// /// generic-erased DateTime suitable for use in JavaScript or Rust
// #[cfg_attr(feature = "wasm", wasm_bindgen)]
// impl DateTime {
//     pub fn now() -> Self { Self(chrono::Utc::now()) }
//     pub fn local(&self) -> LocalDateTime { LocalDateTime(self.0.with_timezone(&chrono::Local)) }
//     pub fn iso8601(&self) -> String { self.0.to_rfc3339() }
//     pub fn rfc3339(&self) -> String { self.0.to_rfc3339() }
// }

// impl std::ops::Deref for DateTime {
//     type Target = chrono::DateTime<chrono::Utc>;
//     fn deref(&self) -> &Self::Target { &self.0 }
// }
