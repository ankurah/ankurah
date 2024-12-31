pub mod collation;
pub mod comparision_index;
pub mod connector;
pub mod error;
pub mod event;
pub mod model;
pub mod node;
pub mod property;
pub mod reactor;
pub mod resultset;
pub mod storage;
pub mod transaction;

pub use model::Model;
pub use node::Node;

// Re-export the derive macro
#[cfg(feature = "derive")]
pub use ankurah_derive::*;

// TODO move this somewhere else - it's a dependency of the signal derive macro
pub trait GetSignalValue: reactive_graph::traits::Get {
    fn cloned(&self) -> Box<dyn GetSignalValue<Value = Self::Value>>;
}

// Add a blanket implementation for any type that implements Get + Clone
impl<T> GetSignalValue for T
where
    T: reactive_graph::traits::Get + Clone + 'static,
    T::Value: 'static,
{
    fn cloned(&self) -> Box<dyn GetSignalValue<Value = T::Value>> {
        Box::new(self.clone())
    }
}

// Re-export dependencies needed by derive macros
// #[cfg(feature = "derive")]
#[doc(hidden)]
pub mod derive_deps {
    pub use crate::GetSignalValue;
    pub use ::ankurah_react_signals;
    pub use ::js_sys;
    pub use ::reactive_graph; // Why does this fail with a Sized error: `the trait `GetSignalValue` cannot be made into an object the trait cannot be made into an object because it requires `Self: Sized``
                              // pub use reactive_graph::traits::Get as GetSignalValue; // and this one works fine?
    pub use ::ankurah_proto;
    pub use ::wasm_bindgen;
}
