use ankurah_proto::CollectionId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level policy configuration.
///
/// Maps roles to their granted privileges, and defines entity access rules
/// that map privileges to collection operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
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
    fn default() -> Self { Self { roles: HashMap::new(), collections: HashMap::new() } }
}
