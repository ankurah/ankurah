#![cfg(feature = "sqlite")]

mod common;

#[path = "sqlite/basic.rs"]
mod basic;
#[path = "sqlite/json_property.rs"]
mod json_property;
