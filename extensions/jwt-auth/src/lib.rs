// I'm skeptical that this recursion limit is right - most likely it
// got bumped up during a feature unification problem, and never reduced back
#![recursion_limit = "1024"]

mod agent;
mod agent_state;
mod authoring;
mod claims;
mod catalog;
mod bound_predicate;
mod bound_policy;
mod graph;
mod config;
mod context;
mod error;
mod keys;
mod model;
mod variables;
#[cfg(feature = "watcher")]
mod watcher;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

pub use agent::{AgentState, JwtAgent};
pub use catalog::PolicyCatalog;
pub use claims::{parse_claims_unverified, JwtClaims};
pub use config::{PolicyConfig, ScopeRule, ScopeRuleOp};
pub use context::JwtContext;
pub use error::AuthError;
pub use jwt_simple::prelude::Duration;
pub use keys::{JwtKeys, SigningKeys};
pub use model::*;
#[cfg(feature = "watcher")]
pub use watcher::PolicyWatcher;
