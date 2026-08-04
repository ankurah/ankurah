use ankurah_proto::CollectionId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The policy-schema version this build of the crate understands.
///
/// Bump this when the config shape gains a field or semantic that the agent
/// must actively consume to enforce policy correctly. A config declaring a
/// higher version is refused at load (see [`PolicyConfig::from_json`]): a
/// newer policy may rely on a restriction an older agent cannot apply, so
/// loading it anyway would be a silent fail-open under version skew — exactly
/// the class of problem this crate must not have as a data-path admission
/// engine.
pub const CURRENT_POLICY_VERSION: u32 = 1;

/// Error loading a [`PolicyConfig`] from JSON. Both variants are fail-closed:
/// an unrecognized key or a future version stops the config from loading
/// rather than loading a partially-understood policy.
#[derive(Debug, thiserror::Error)]
pub enum PolicyConfigError {
    /// JSON did not parse, or carried an unknown field (`deny_unknown_fields`).
    #[error("policy config did not parse: {0}")]
    Parse(#[from] serde_json::Error),
    /// The config declares a schema version newer than this build supports.
    #[error(
        "policy config declares version {found}, but this build supports at most {max}; \
         refusing to load a policy this agent may not fully enforce"
    )]
    UnsupportedVersion { found: u32, max: u32 },
}

/// Top-level policy configuration.
///
/// Maps roles to their granted privileges, and defines entity access rules
/// that map privileges to collection operations.
///
/// `deny_unknown_fields`: an unrecognized key fails the load rather than being
/// silently dropped. A policy authored against a newer schema must not appear
/// to load "successfully" on an older agent with the unknown restriction
/// quietly missing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    /// Config schema version. Absent means "legacy / unversioned", treated as
    /// [`CURRENT_POLICY_VERSION`] for back-compat with configs written before
    /// this field existed. A value greater than `CURRENT_POLICY_VERSION` is
    /// refused at load; see [`PolicyConfig::from_json`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,

    /// Role name → list of privilege names
    pub roles: HashMap<String, Vec<String>>,

    /// Collection access rules keyed by collection name
    pub collections: HashMap<String, CollectionRules>,
}

/// A conditional scope rule that injects a predicate filter into queries.
///
/// If `unless_privilege` is set, the filter is skipped when the user holds that privilege.
/// Multiple scope rules are AND-ed together (fail-closed).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeRule {
    /// AnkQL predicate string with $jwt.* variables (e.g., "assignee = $jwt.sub")
    pub filter: String,
    /// If user has this privilege, skip this filter (None = always apply)
    pub unless_privilege: Option<String>,
    /// Which operations this rule constrains. Defaults to both. A
    /// `"write"`-only rule gates what may be written without hiding rows
    /// from reads (e.g. "non-owners may not write owner-role rows, but may
    /// still see them"); a `"read"`-only rule filters visibility without
    /// constraining writes.
    #[serde(default)]
    pub applies_to: ScopeRuleOp,
}

/// The operations a [`ScopeRule`] applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeRuleOp {
    /// Applies to reads and writes (the default, and the pre-existing behavior).
    #[default]
    ReadWrite,
    /// Applies only when reading/filtering query results.
    Read,
    /// Applies only when validating writes.
    Write,
}

impl ScopeRuleOp {
    /// Whether a rule with this setting constrains read/query access.
    pub fn applies_to_reads(self) -> bool { matches!(self, Self::ReadWrite | Self::Read) }

    /// Whether a rule with this setting constrains writes.
    pub fn applies_to_writes(self) -> bool { matches!(self, Self::ReadWrite | Self::Write) }
}

/// Access rules for a single collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionRules {
    /// Privilege required to read entities in this collection (None = no access)
    pub read: Option<String>,

    /// Privilege required to write (create/update) entities in this collection (None = no access)
    pub write: Option<String>,

    /// Row-level scope rules — conditional predicate filters injected into queries
    #[serde(default)]
    pub scope: Vec<ScopeRule>,
}

impl PolicyConfig {
    /// Parse and validate a policy config from JSON. This is the load path
    /// every caller should use: it rejects unknown fields
    /// (`deny_unknown_fields`) and refuses a config whose declared version is
    /// newer than this build supports. Both failures are fail-closed — the
    /// config does not load — which is the correct posture for an admission
    /// engine facing version skew (e.g. a wasm client pinned to a different
    /// crate version than its server).
    pub fn from_json(json: &str) -> Result<Self, PolicyConfigError> {
        let config: PolicyConfig = serde_json::from_str(json)?;
        config.check_version()?;
        Ok(config)
    }

    /// Refuse a config that declares a schema version newer than this build
    /// supports. An absent version is treated as current (legacy configs).
    pub fn check_version(&self) -> Result<(), PolicyConfigError> {
        match self.version {
            Some(found) if found > CURRENT_POLICY_VERSION => {
                Err(PolicyConfigError::UnsupportedVersion { found, max: CURRENT_POLICY_VERSION })
            }
            _ => Ok(()),
        }
    }

    /// Check if any of the given roles can access a collection (read or write).
    pub fn can_access_collection(&self, roles: &[String], collection: &CollectionId) -> bool {
        for role in roles {
            if self.role_has_wildcard(role) {
                return true;
            }
        }

        let collection_name = collection.as_str();
        if let Some(rules) = self.collections.get(collection_name) {
            for role in roles {
                let privileges = self.privileges_for_role(role);
                if let Some(ref read_priv) = rules.read {
                    if privileges.contains(&read_priv.as_str()) {
                        return true;
                    }
                }
                if let Some(ref write_priv) = rules.write {
                    if privileges.contains(&write_priv.as_str()) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Check if any of the given roles can write to a collection.
    pub fn can_write_collection(&self, roles: &[String], collection: &CollectionId) -> bool {
        for role in roles {
            if self.role_has_wildcard(role) {
                return true;
            }
        }

        let collection_name = collection.as_str();
        if let Some(rules) = self.collections.get(collection_name) {
            for role in roles {
                let privileges = self.privileges_for_role(role);
                if let Some(ref write_priv) = rules.write {
                    if privileges.contains(&write_priv.as_str()) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Returns the scope rules for a given collection, or an empty slice if none.
    pub fn scope_rules_for_collection(&self, collection: &str) -> &[ScopeRule] {
        self.collections.get(collection).map_or(&[], |r| &r.scope)
    }

    /// Check if any of the given roles has a specific privilege (or wildcard).
    pub fn roles_have_privilege(&self, roles: &[String], privilege: &str) -> bool {
        for role in roles {
            if self.role_has_wildcard(role) {
                return true;
            }
            let privs = self.privileges_for_role(role);
            if privs.contains(&privilege) {
                return true;
            }
        }
        false
    }

    /// Check if a role has the wildcard privilege ("*"), granting full access.
    fn role_has_wildcard(&self, role: &str) -> bool { self.roles.get(role).map_or(false, |privs| privs.iter().any(|p| p == "*")) }

    /// Resolve a role to its set of privileges.
    fn privileges_for_role(&self, role: &str) -> Vec<&str> {
        self.roles.get(role).map(|privs| privs.iter().map(|s| s.as_str()).collect()).unwrap_or_default()
    }
}

impl Default for PolicyConfig {
    /// Default config: deny-all (no roles or collections defined).
    fn default() -> Self { Self { version: None, roles: HashMap::new(), collections: HashMap::new() } }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"{ "roles": {}, "collections": {} }"#;

    #[test]
    fn loads_a_versionless_config() {
        // Configs written before the `version` field must still load.
        let config = PolicyConfig::from_json(MINIMAL).expect("versionless config loads");
        assert_eq!(config.version, None);
    }

    #[test]
    fn loads_a_current_version_config() {
        let json = format!(r#"{{ "version": {CURRENT_POLICY_VERSION}, "roles": {{}}, "collections": {{}} }}"#);
        assert!(PolicyConfig::from_json(&json).is_ok());
    }

    #[test]
    fn refuses_a_future_version() {
        let json = format!(r#"{{ "version": {}, "roles": {{}}, "collections": {{}} }}"#, CURRENT_POLICY_VERSION + 1);
        match PolicyConfig::from_json(&json) {
            Err(PolicyConfigError::UnsupportedVersion { found, max }) => {
                assert_eq!(found, CURRENT_POLICY_VERSION + 1);
                assert_eq!(max, CURRENT_POLICY_VERSION);
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    #[test]
    fn rejects_an_unknown_top_level_key() {
        // A newer schema's key must not vanish silently on an older agent.
        let json = r#"{ "roles": {}, "collections": {}, "future_restriction": ["x"] }"#;
        assert!(matches!(PolicyConfig::from_json(json), Err(PolicyConfigError::Parse(_))));
    }

    #[test]
    fn rejects_an_unknown_nested_key() {
        // Unknown keys inside a scope rule are equally fail-closed.
        let json = r#"{
            "roles": {},
            "collections": {
                "task": {
                    "read": "view",
                    "write": "edit",
                    "scope": [{ "filter": "assignee = $jwt.sub", "deny_when": "old.locked" }]
                }
            }
        }"#;
        assert!(matches!(PolicyConfig::from_json(json), Err(PolicyConfigError::Parse(_))));
    }

    #[test]
    fn round_trips_a_realistic_config_including_applies_to() {
        // Serialize→parse of a config using every current field must succeed —
        // guards against deny_unknown_fields rejecting our own output.
        let json = r#"{
            "roles": { "admin": ["*"], "member": ["view", "edit"] },
            "collections": {
                "task": {
                    "read": "view",
                    "write": "edit",
                    "scope": [{ "filter": "assignee = $jwt.sub", "unless_privilege": "task:unscoped", "applies_to": "write" }]
                }
            }
        }"#;
        let config = PolicyConfig::from_json(json).expect("realistic config loads");
        let reserialized = serde_json::to_string(&config).expect("serializes");
        PolicyConfig::from_json(&reserialized).expect("own output round-trips");
    }
}
