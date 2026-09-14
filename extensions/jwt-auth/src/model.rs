use ankurah::{Model, Property, Ref};
use ankurah_core_types::{ModelId, PropertyId};
use ankql::ast::{Predicate, Resolved};
use serde::{Deserialize, Serialize};

use crate::ScopeRuleOp;

#[derive(Model, Debug, Serialize, Deserialize)]
#[model(label = "jwtagent_role", no_ffi)]
pub struct Role {
    #[active_type(LWW)]
    pub name: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
#[model(label = "jwtagent_privilege", no_ffi)]
pub struct Privilege {
    #[active_type(LWW)]
    pub name: String,
}

/// A role's grant of one privilege.
#[derive(Model, Debug, Serialize, Deserialize)]
#[model(label = "jwtagent_role_privilege", no_ffi)]
pub struct RolePrivilege {
    pub role: Ref<Role>,
    pub privilege: Ref<Privilege>,
    #[active_type(LWW)]
    pub status: RuleStatus,
}

/// Rules for a source model label, bound to one durable model identity.
#[derive(Model, Debug, Serialize, Deserialize)]
#[model(label = "jwtagent_model_policy", no_ffi)]
pub struct ModelPolicy {
    #[active_type(LWW)]
    pub label: String,
    #[active_type(LWW)]
    pub model: Binding<ModelId>,
    pub read: Option<Ref<Privilege>>,
    pub retrieve: Option<Ref<Privilege>>,
    pub write: Option<Ref<Privilege>>,
    /// Do not grant access until this many active scope rows have arrived.
    pub scope_count: i32,
    /// Retiring this rule also retires its scopes.
    #[active_type(LWW)]
    pub status: RuleStatus,
}

/// A property referenced by scopes of one model policy.
#[derive(Model, Debug, Serialize, Deserialize)]
#[model(label = "jwtagent_policy_property", no_ffi)]
pub struct PolicyProperty {
    pub policy: Ref<ModelPolicy>,
    #[active_type(LWW)]
    pub label: String,
    #[active_type(LWW)]
    pub property: Binding<PropertyId>,
    /// Populated together with the property identity.
    #[active_type(LWW)]
    pub value_type: Option<String>,
}

#[derive(Model, Debug, Serialize, Deserialize)]
#[model(label = "jwtagent_policy_scope", no_ffi)]
pub struct PolicyScope {
    pub policy: Ref<ModelPolicy>,
    /// Original AnkQL filter, including any `$jwt.*` variables.
    #[active_type(LWW)]
    pub filter: String,
    pub unless_privilege: Option<Ref<Privilege>>,
    #[active_type(LWW)]
    pub applies_to: ScopeRuleOp,
    /// None means not yet bound, never an absent restriction.
    pub resolved: Option<Ref<ResolvedPolicyScope>>,
    #[active_type(LWW)]
    pub status: RuleStatus,
}

/// Property identities are fixed; claim values are supplied when the scope is evaluated.
#[derive(Model, Debug, Serialize, Deserialize)]
#[model(label = "jwtagent_resolved_policy_scope", no_ffi)]
pub struct ResolvedPolicyScope {
    #[active_type(LWW)]
    pub predicate: ScopePredicate,
}

/// One claim value to supply to a resolved scope's predicate.
#[derive(Model, Debug, Serialize, Deserialize)]
#[model(label = "jwtagent_claim_parameter", no_ffi)]
pub struct ClaimParameter {
    pub scope: Ref<ResolvedPolicyScope>,
    /// Zero-based placeholder position in predicate traversal order.
    pub position: i32,
    #[active_type(LWW)]
    pub variable: String,
    /// The required type when the parameter is compared with a property.
    #[active_type(LWW)]
    pub value_type: Option<String>,
}

/// Public verification material replicated to nodes; private signing keys stay local.
#[derive(Model, Debug, Serialize, Deserialize)]
#[model(label = "jwtagent_verification_key", no_ffi)]
pub struct JwtVerificationKey {
    #[active_type(LWW)]
    pub public_key_pem: String,
}

/// Whether a rule is configured, independently of whether its bindings are ready.
#[derive(Property, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[property(no_ffi)]
#[serde(rename_all = "snake_case")]
pub enum RuleStatus {
    Active,
    Retired,
}

/// A label's identity is filled once, until an explicit policy update replaces the binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Binding<T> {
    Pending,
    AtPolicySet(T),
    /// Selected by the first matching registration; retained as a warning for policy review.
    AtRegistration(T),
}

impl<T: Copy> Binding<T> {
    pub fn id(&self) -> Option<T> {
        match self {
            Self::Pending => None,
            Self::AtPolicySet(id) | Self::AtRegistration(id) => Some(*id),
        }
    }
}

impl<T: Serialize + serde::de::DeserializeOwned> Property for Binding<T> {
    const VALUE_TYPE: &'static str = "string";

    fn into_value(&self) -> Result<Option<ankurah::value::Value>, ankurah::property::PropertyError> {
        serde_json::to_string(self)?.into_value()
    }

    fn from_value(value: Option<ankurah::value::Value>) -> Result<Self, ankurah::property::PropertyError> {
        serde_json::from_str(&String::from_value(value)?)
            .map_err(|error| ankurah::property::PropertyError::DeserializeError(Box::new(error)))
    }
}

/// Property encoding for a resolved predicate with unfilled claim parameters.
#[derive(Property, Debug, Clone, Serialize, Deserialize)]
#[property(no_ffi)]
#[serde(transparent)]
pub struct ScopePredicate(pub Predicate<Resolved>);
