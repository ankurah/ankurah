use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level policy configuration.
///
/// Maps roles to their granted privileges, and defines entity access rules
/// that map privileges to collection operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    /// Role name → list of privilege names. `"*"` satisfies any named privilege;
    /// it does not grant entity access without a configured operation rule.
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
#[derive(ankurah::Property, Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[property(no_ffi)]
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

    /// Privilege sufficient to RETRIEVE rows the caller can already name —
    /// a `Ref` follow and the by-id wire path (`Get{ids}`) — without
    /// admitting scans. Predicates are all scans for now, even id-shaped
    /// ones; admitting id-bounded predicates at this tier is a deliberate
    /// follow-up. None = retrieval requires `read`, the pre-existing
    /// behavior, so a policy written before this field existed means what
    /// it always meant. This exists for collections whose rows are public
    /// to whoever holds their ids while the roster stays private: "any
    /// holder of a user's id may retrieve that user; only the signed-in may
    /// list users." `read` or `write` always suffice for a retrieval, and
    /// row scope rules still bind per row (`check_read` is unchanged by
    /// this field).
    #[serde(default)]
    pub retrieve: Option<String>,

    /// Privilege required to write (create/update) entities in this collection (None = no access)
    pub write: Option<String>,

    /// Row-level scope rules — conditional predicate filters injected into queries
    #[serde(default)]
    pub scope: Vec<ScopeRule>,
}

impl PolicyConfig {
    /// Whether any role holds a configured `read`, `write`, or `retrieve` privilege.
    /// Row scopes still apply. Use [`Self::can_scan_collection`] to exclude retrieval-only grants.
    pub fn can_access_collection(&self, roles: &[String], collection_name: &str) -> bool {
        self.collections.get(collection_name).is_some_and(|rules| {
            [&rules.read, &rules.write, &rules.retrieve].into_iter().flatten()
                .any(|privilege| self.roles_have_privilege(roles, privilege))
        })
    }

    /// True when any role may run a SCAN — a predicate query, subscription,
    /// or listing — against the collection: `read` or `write` privilege
    /// (exactly what [`Self::can_access_collection`] meant before
    /// `retrieve` existed). `retrieve` deliberately does not count here:
    /// naming rows is the whole of what it grants.
    pub fn can_scan_collection(&self, roles: &[String], collection_name: &str) -> bool {
        self.collections.get(collection_name).is_some_and(|rules| {
            [&rules.read, &rules.write].into_iter().flatten()
                .any(|privilege| self.roles_have_privilege(roles, privilege))
        })
    }

    /// Check if any of the given roles can write to a collection.
    pub fn can_write_collection(&self, roles: &[String], collection_name: &str) -> bool {
        self.collections.get(collection_name).and_then(|rules| rules.write.as_ref())
            .is_some_and(|privilege| self.roles_have_privilege(roles, privilege))
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

    /// Whether this role satisfies every named privilege requirement.
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
