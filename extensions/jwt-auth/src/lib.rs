#![recursion_limit = "1024"]
//! JWT-based [`PolicyAgent`](ankurah_core::policy::PolicyAgent) for ankurah.
//!
//! `JwtAgent` is a data-path admission engine (the Firestore-security-rules /
//! Hasura-permissions category): it authenticates a request from a signed
//! token and admits or denies each event against a role → privilege →
//! collection-rule policy, with optional row-level scope predicates.
//!
//! # Write-scope enforcement freezes scope-keyed fields (supported pattern)
//!
//! A [`ScopeRule`] with `applies_to: Write` (or the default `ReadWrite`) is
//! enforced on **both sides** of an update: `check_event` runs the write-scope
//! check against `entity_before` (when it already has a head) and against
//! `entity_after`. The consequence is a deliberately useful state-freeze
//! primitive:
//!
//! - a scope filter such as `assignee = $jwt.sub` means a non-privileged
//!   writer must satisfy the scope on the **pre-state** to touch the row at
//!   all (you cannot grab a row that is not yours) **and** on the
//!   **post-state** (you cannot push a row out of your scope). For an equality
//!   scope keyed on the caller (`= $jwt.sub`), the only value that satisfies
//!   both sides is the caller's own — so the scope-keyed field is effectively
//!   frozen: reassignment and theft are both denied, in one rule.
//!
//! Limits, stated so callers do not over-trust it:
//!
//! - it gates by the **writing context**, not by field transition per se. A
//!   context that satisfies the scope on both sides may still change other
//!   fields, and may change the scope field to any *other* value that also
//!   satisfies the scope (only degenerate for `= $jwt.sub`, where exactly one
//!   value qualifies).
//! - it covers **scope-keyed** fields only. A free-valued field (one no scope
//!   rule references) is not frozen by this mechanism; freezing such a field
//!   is the subject of the deferred design note below.
//! - `entity_before` is the **live replica head**, not the event's causal
//!   parent — see the transition-guard note for why this rules out sound
//!   before→after transition predicates.
//!
//! # Design notes: considered and rejected
//!
//! Recorded so these are not re-proposed from scratch. Each was evaluated for
//! a concrete consumer and rejected on soundness or consumer count, not
//! category — per-write predicates and transition guards are canonical in this
//! class of engine; the objections are specific.
//!
//! **`deny_when` transition guards (`old.`/`new.` predicates on writes).**
//! Rejected as unsound today:
//! - at check time `entity_before` is the live replica head, not the event's
//!   causal parent `before(u)`; a guard evaluated against it races concurrent
//!   writes. This is the ankurah threat-model C4-21 boundary — a check that is
//!   not a deterministic function of the event and its causal-predecessor
//!   closure is not convergently enforceable.
//! - multi-commit laundering defeats edge guards: a forbidden A→C transition
//!   is walked A→B→C across two commits, each edge individually passing.
//! - reusing the predicate evaluator with deny polarity is fail-**open** on
//!   type mismatch: value comparison returns `false` for incompatible types,
//!   which is fail-closed for an allow-polarity scope filter but means "allow"
//!   for a deny rule.
//! - `old.` / `new.` prefixes collide with the existing dotted-path JSON
//!   traversal in path evaluation.
//!
//! **Tier-2 livequery-backed policy datasets** (predicates over a synced
//! entity set). Rejected: fail-open by construction when the dataset is cold
//! or stale (an empty set makes an `IN` test false, which for a deny rule
//! means allow); on browsers it is a trilemma (ship the dataset and leak it /
//! accept skew / make the rule server-only); and where genuinely needed it is
//! achievable in the consuming application by wrapping the agent.

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
pub use claims::{parse_claims_unverified, JwtClaims};
pub use config::{PolicyConfig, ScopeRule, ScopeRuleOp};
pub use context::JwtContext;
pub use error::AuthError;
pub use jwt_simple::prelude::Duration;
pub use keys::{JwtKeys, SigningKeys};
pub use model::*;
#[cfg(feature = "watcher")]
pub use watcher::PolicyWatcher;
