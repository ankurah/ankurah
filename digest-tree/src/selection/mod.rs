//! Whether and how a tree serves a Selection: the spec that defines a tree,
//! the request a Selection makes of one, the proof that a tree serves a
//! request, and the spans a request's limit selects.

mod limit;
mod request;
mod reuse;
mod spec;

pub(crate) use limit::{order_cut, TierWalk};
pub use request::{PropertyTypes, TreeRequest};
pub(crate) use reuse::ReusePlan;
pub use reuse::TreeMatch;
pub use spec::TreeSpec;
