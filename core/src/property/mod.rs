pub mod backend;
pub mod traits;
pub mod value;

pub use backend::Backends;
pub use traits::{PropertyError, FromActiveType, FromEntity, InitializeWith};
pub use value::YrsString;

pub type PropertyName = String;
