pub mod compare;
pub mod misc;
pub mod relation;

pub(crate) use compare::{compare, compare_unstored_event, Comparison};
pub use relation::CausalRelation;

#[cfg(test)]
mod tests;
