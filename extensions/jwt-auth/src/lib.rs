#![recursion_limit = "1024"]

mod agent;
mod agent_state;
mod claims;
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
pub use agent_state::start_ephemeral_policy_sync;
#[cfg(feature = "watcher")]
pub use agent_state::start_durable_policy_watcher;
pub use claims::{parse_claims_unverified, JwtClaims};
pub use config::{PolicyConfig, ScopeRule, ScopeRuleOp};
pub use context::JwtContext;
pub use error::AuthError;
pub use jwt_simple::prelude::Duration;
pub use keys::{JwtKeys, SigningKeys};
pub use model::*;
#[cfg(feature = "watcher")]
pub use watcher::PolicyWatcher;
