pub mod backend;
pub mod traits;
pub mod value;

pub use backend::Backends;
pub use traits::{InitializeWith, FromEntity};
pub use value::{ProjectedValue, YrsString};

pub type PropertyName = String;
