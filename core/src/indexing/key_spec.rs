use crate::value::ValueType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeySpec {
    pub keyparts: Vec<IndexKeyPart>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NullsOrder {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexKeyPart {
    pub column: String,
    /// Optional path within property value (for JSON, future Ref, etc.)
    pub sub_path: Option<Vec<String>>,
    pub direction: IndexDirection, // ASC/DESC
    pub value_type: ValueType,     // Expected type for this key component
    pub nulls: Option<NullsOrder>, // PG-style NULLS FIRST/LAST (optional)
    pub collation: Option<String>, // collation name/id if relevant (optional)
}

impl IndexKeyPart {
    pub fn asc<S: Into<String>>(col: S, value_type: ValueType) -> Self {
        Self { column: col.into(), sub_path: None, direction: IndexDirection::Asc, value_type, nulls: None, collation: None }
    }
    pub fn desc<S: Into<String>>(col: S, value_type: ValueType) -> Self {
        Self { column: col.into(), sub_path: None, direction: IndexDirection::Desc, value_type, nulls: None, collation: None }
    }

    /// Create from a PathExpr (handles multi-step paths)
    pub fn from_path(path: &ankql::ast::PathExpr, direction: IndexDirection, value_type: ValueType) -> Self {
        let (column, sub_path) = if path.steps.len() == 1 {
            (path.steps[0].clone(), None)
        } else {
            let column = path.steps[0].clone();
            let sub_path = path.steps[1..].to_vec();
            (column, Some(sub_path))
        };
        Self { column, sub_path, direction, value_type, nulls: None, collation: None }
    }

    /// Full path as a flat string (e.g., "context.session_id")
    pub fn full_path(&self) -> String {
        match &self.sub_path {
            None => self.column.clone(),
            Some(sub) => {
                let mut parts = vec![self.column.clone()];
                parts.extend(sub.clone());
                parts.join(".")
            }
        }
    }

    /// Create from a flat path string (e.g., "context.session_id")
    pub fn from_flat_path(path: &str, direction: IndexDirection, value_type: ValueType) -> Self {
        let parts: Vec<&str> = path.split('.').collect();
        let (column, sub_path) = if parts.len() == 1 {
            (parts[0].to_string(), None)
        } else {
            let column = parts[0].to_string();
            let sub_path: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();
            (column, Some(sub_path))
        };
        Self { column, sub_path, direction, value_type, nulls: None, collation: None }
    }

    /// Create ascending keypart from flat path
    pub fn asc_path(path: &str, value_type: ValueType) -> Self { Self::from_flat_path(path, IndexDirection::Asc, value_type) }

    /// Create descending keypart from flat path
    pub fn desc_path(path: &str, value_type: ValueType) -> Self { Self::from_flat_path(path, IndexDirection::Desc, value_type) }
}

impl IndexDirection {
    pub fn is_desc(&self) -> bool { matches!(self, IndexDirection::Desc) }
}

impl KeySpec {
    pub fn new(keyparts: Vec<IndexKeyPart>) -> Self { Self { keyparts } }

    /// Simple name generator similar to your existing helper.
    pub fn name_with(&self, prefix: &str, delim: &str) -> String {
        let fields: Vec<String> = self
            .keyparts
            .iter()
            .map(|k| {
                let dir = match k.direction {
                    IndexDirection::Asc => "asc",
                    IndexDirection::Desc => "desc",
                };
                let col_name = k.full_path();
                if k.collation.is_some() || k.nulls.is_some() {
                    // include extras only if present
                    let mut extras = Vec::new();
                    if let Some(c) = &k.collation {
                        extras.push(format!("collate={}", c));
                    }
                    if let Some(n) = &k.nulls {
                        extras.push(format!("nulls={:?}", n).to_lowercase());
                    }
                    format!("{} {}({})", col_name, dir, extras.join(","))
                } else {
                    format!("{} {}", col_name, dir)
                }
            })
            .collect();

        if prefix.is_empty() {
            fields.join(delim)
        } else {
            format!("{}{}{}", prefix, delim, fields.join(delim))
        }
    }

    /// Checks if this IndexSpec can be satisfied by another IndexSpec
    /// Returns Yes if this is a prefix subset of other
    /// Returns Inverse if this is a prefix subset of other with all directions flipped
    /// Returns No if neither condition is met
    pub fn matches(&self, other: &KeySpec) -> Option<IndexSpecMatch> {
        if self.keyparts.len() > other.keyparts.len() {
            return None;
        }

        let mut direct_match = true;
        let mut inverse_match = true;

        for (self_keypart, other_keypart) in self.keyparts.iter().zip(other.keyparts.iter()) {
            // Both column and sub_path must match
            if self_keypart.column != other_keypart.column || self_keypart.sub_path != other_keypart.sub_path {
                return None;
            }

            if self_keypart.direction != other_keypart.direction {
                direct_match = false;
            }

            if self_keypart.direction == other_keypart.direction {
                inverse_match = false;
            }
        }

        if direct_match {
            Some(IndexSpecMatch::Match)
        } else if inverse_match {
            Some(IndexSpecMatch::Inverse)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexSpecMatch {
    /// The index specs match
    Match,
    /// The index specs match, but scan direction must be inverted
    Inverse,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let spec1 = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::desc("b", ValueType::String)] };
        let spec2 = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::desc("b", ValueType::String)] };

        assert_eq!(spec1.matches(&spec2), Some(IndexSpecMatch::Match));
    }

    #[test]
    fn test_prefix_match() {
        // +a, -b matches +a, -b, +c
        let query_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::desc("b", ValueType::String)] };
        let index_spec = KeySpec {
            keyparts: vec![
                IndexKeyPart::asc("a", ValueType::String),
                IndexKeyPart::desc("b", ValueType::String),
                IndexKeyPart::asc("c", ValueType::String),
            ],
        };

        assert_eq!(query_spec.matches(&index_spec), Some(IndexSpecMatch::Match));
    }

    #[test]
    fn test_inverse_exact_match() {
        // +a, -b matches -a, +b (inverse)
        let query_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::desc("b", ValueType::String)] };
        let index_spec = KeySpec { keyparts: vec![IndexKeyPart::desc("a", ValueType::String), IndexKeyPart::asc("b", ValueType::String)] };

        assert_eq!(query_spec.matches(&index_spec), Some(IndexSpecMatch::Inverse));
    }

    #[test]
    fn test_inverse_prefix_match() {
        // +a, -b matches -a, +b, +c (inverse prefix)
        let query_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::desc("b", ValueType::String)] };
        let index_spec = KeySpec {
            keyparts: vec![
                IndexKeyPart::desc("a", ValueType::String),
                IndexKeyPart::asc("b", ValueType::String),
                IndexKeyPart::asc("c", ValueType::String),
            ],
        };

        assert_eq!(query_spec.matches(&index_spec), Some(IndexSpecMatch::Inverse));
    }

    #[test]
    fn test_user_example() {
        // "+a, -b matches +a, -b, any c AND -a, +b, any c"
        let query_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::desc("b", ValueType::String)] };

        // Test direct match: +a, -b, +c
        let index_spec1 = KeySpec {
            keyparts: vec![
                IndexKeyPart::asc("a", ValueType::String),
                IndexKeyPart::desc("b", ValueType::String),
                IndexKeyPart::asc("c", ValueType::String),
            ],
        };
        assert_eq!(query_spec.matches(&index_spec1), Some(IndexSpecMatch::Match));

        // Test inverse match: -a, +b, -c
        let index_spec2 = KeySpec {
            keyparts: vec![
                IndexKeyPart::desc("a", ValueType::String),
                IndexKeyPart::asc("b", ValueType::String),
                IndexKeyPart::desc("c", ValueType::String),
            ],
        };
        assert_eq!(query_spec.matches(&index_spec2), Some(IndexSpecMatch::Inverse));
    }

    #[test]
    fn test_no_match_different_fields() {
        let query_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::desc("b", ValueType::String)] };
        let index_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("x", ValueType::String), IndexKeyPart::desc("y", ValueType::String)] };

        assert_eq!(query_spec.matches(&index_spec), None);
    }

    #[test]
    fn test_no_match_partial_field_overlap() {
        // +a, -b does not match +a, +b (different direction on second field)
        let query_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::desc("b", ValueType::String)] };
        let index_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String), IndexKeyPart::asc("b", ValueType::String)] };

        assert_eq!(query_spec.matches(&index_spec), None);
    }

    #[test]
    fn test_no_match_query_longer_than_index() {
        // +a, -b, +c cannot match +a (query is longer than index)
        let query_spec = KeySpec {
            keyparts: vec![
                IndexKeyPart::asc("a", ValueType::String),
                IndexKeyPart::desc("b", ValueType::String),
                IndexKeyPart::asc("c", ValueType::String),
            ],
        };
        let index_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String)] };

        assert_eq!(query_spec.matches(&index_spec), None);
    }

    #[test]
    fn test_empty_specs() {
        let empty_spec = KeySpec { keyparts: vec![] };
        let non_empty_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String)] };

        // Empty spec matches any spec (empty prefix)
        assert_eq!(empty_spec.matches(&non_empty_spec), Some(IndexSpecMatch::Match));
        assert_eq!(empty_spec.matches(&empty_spec), Some(IndexSpecMatch::Match));

        // Non-empty spec does not match empty spec
        assert_eq!(non_empty_spec.matches(&empty_spec), None);
    }

    #[test]
    fn test_single_field_cases() {
        let asc_spec = KeySpec { keyparts: vec![IndexKeyPart::asc("a", ValueType::String)] };
        let desc_spec = KeySpec { keyparts: vec![IndexKeyPart::desc("a", ValueType::String)] };

        // Direct match
        assert_eq!(asc_spec.matches(&asc_spec), Some(IndexSpecMatch::Match));

        // Inverse match
        assert_eq!(asc_spec.matches(&desc_spec), Some(IndexSpecMatch::Inverse));
        assert_eq!(desc_spec.matches(&asc_spec), Some(IndexSpecMatch::Inverse));
    }

    #[test]
    fn test_complex_multi_field_scenarios() {
        // Test various combinations with 3+ fields
        let query_spec = KeySpec {
            keyparts: vec![
                IndexKeyPart::asc("a", ValueType::String),
                IndexKeyPart::desc("b", ValueType::String),
                IndexKeyPart::asc("c", ValueType::String),
            ],
        };

        // Exact match with additional fields
        let index_spec1 = KeySpec {
            keyparts: vec![
                IndexKeyPart::asc("a", ValueType::String),
                IndexKeyPart::desc("b", ValueType::String),
                IndexKeyPart::asc("c", ValueType::String),
                IndexKeyPart::desc("d", ValueType::String),
            ],
        };
        assert_eq!(query_spec.matches(&index_spec1), Some(IndexSpecMatch::Match));

        // Inverse match with additional fields
        let index_spec2 = KeySpec {
            keyparts: vec![
                IndexKeyPart::desc("a", ValueType::String),
                IndexKeyPart::asc("b", ValueType::String),
                IndexKeyPart::desc("c", ValueType::String),
                IndexKeyPart::asc("d", ValueType::String),
            ],
        };
        assert_eq!(query_spec.matches(&index_spec2), Some(IndexSpecMatch::Inverse));

        // No match - mixed directions that don't form inverse
        let index_spec3 = KeySpec {
            keyparts: vec![
                IndexKeyPart::asc("a", ValueType::String),
                IndexKeyPart::asc("b", ValueType::String),
                IndexKeyPart::desc("c", ValueType::String),
            ],
        };
        assert_eq!(query_spec.matches(&index_spec3), None);
    }

    #[test]
    fn test_helper_methods() {
        // Test IndexKeyPart helper methods
        let asc_keypart = IndexKeyPart::asc("test", ValueType::String);
        assert_eq!(asc_keypart.column, "test");
        assert_eq!(asc_keypart.direction, IndexDirection::Asc);
        assert_eq!(asc_keypart.nulls, None);
        assert_eq!(asc_keypart.collation, None);

        let desc_keypart = IndexKeyPart::desc("test", ValueType::String);
        assert_eq!(desc_keypart.column, "test");
        assert_eq!(desc_keypart.direction, IndexDirection::Desc);
        assert_eq!(desc_keypart.nulls, None);
        assert_eq!(desc_keypart.collation, None);
    }

    #[test]
    fn test_edge_case_behaviors() {
        // Test that matches works correctly with various edge cases
        let spec = KeySpec {
            keyparts: vec![
                IndexKeyPart::asc("a", ValueType::String),
                IndexKeyPart::desc("b", ValueType::String),
                IndexKeyPart::asc("c", ValueType::String),
            ],
        };

        // Self-match should always be Yes
        assert_eq!(spec.matches(&spec), Some(IndexSpecMatch::Match));

        // Empty spec matches any spec
        let empty = KeySpec { keyparts: vec![] };
        assert_eq!(empty.matches(&spec), Some(IndexSpecMatch::Match));
    }
}
