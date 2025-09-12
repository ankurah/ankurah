/// Index specification for storage engine index creation
#[derive(Debug, Clone, PartialEq)]
pub struct IndexSpec {
    /// Fields that make up this index
    pub fields: Vec<IndexField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexSpecMatch {
    /// The index specs do not match
    No,
    /// The index specs match
    Yes,
    /// The index specs match, but scan direction must be inverted
    Inverse,
}

impl IndexSpec {
    /// Checks if this IndexSpec can be satisfied by another IndexSpec
    /// Returns Yes if this is a prefix subset of other
    /// Returns Inverse if this is a prefix subset of other with all directions flipped
    /// Returns No if neither condition is met
    pub fn matches(&self, other: &IndexSpec) -> IndexSpecMatch {
        if self.fields.len() > other.fields.len() {
            return IndexSpecMatch::No;
        }

        let mut direct_match = true;
        let mut inverse_match = true;

        for (self_field, other_field) in self.fields.iter().zip(other.fields.iter()) {
            if self_field.name != other_field.name {
                return IndexSpecMatch::No;
            }

            if self_field.direction != other_field.direction {
                direct_match = false;
            }

            if self_field.direction == other_field.direction {
                inverse_match = false;
            }
        }

        if direct_match {
            IndexSpecMatch::Yes
        } else if inverse_match {
            IndexSpecMatch::Inverse
        } else {
            IndexSpecMatch::No
        }
    }
}

impl IndexField {
    /// Creates a new ascending IndexField
    pub fn asc(name: impl Into<String>) -> Self { Self { name: name.into(), direction: IndexDirection::Asc } }

    /// Creates a new descending IndexField
    pub fn desc(name: impl Into<String>) -> Self { Self { name: name.into(), direction: IndexDirection::Desc } }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let spec1 = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b")] };
        let spec2 = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b")] };

        assert_eq!(spec1.matches(&spec2), IndexSpecMatch::Yes);
    }

    #[test]
    fn test_prefix_match() {
        // +a, -b matches +a, -b, +c
        let query_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b")] };
        let index_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b"), IndexField::asc("c")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::Yes);
    }

    #[test]
    fn test_inverse_exact_match() {
        // +a, -b matches -a, +b (inverse)
        let query_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b")] };
        let index_spec = IndexSpec { fields: vec![IndexField::desc("a"), IndexField::asc("b")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::Inverse);
    }

    #[test]
    fn test_inverse_prefix_match() {
        // +a, -b matches -a, +b, +c (inverse prefix)
        let query_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b")] };
        let index_spec = IndexSpec { fields: vec![IndexField::desc("a"), IndexField::asc("b"), IndexField::asc("c")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::Inverse);
    }

    #[test]
    fn test_user_example() {
        // "+a, -b matches +a, -b, any c AND -a, +b, any c"
        let query_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b")] };

        // Test direct match: +a, -b, +c
        let index_spec1 = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b"), IndexField::asc("c")] };
        assert_eq!(query_spec.matches(&index_spec1), IndexSpecMatch::Yes);

        // Test inverse match: -a, +b, -c
        let index_spec2 = IndexSpec { fields: vec![IndexField::desc("a"), IndexField::asc("b"), IndexField::desc("c")] };
        assert_eq!(query_spec.matches(&index_spec2), IndexSpecMatch::Inverse);
    }

    #[test]
    fn test_no_match_different_fields() {
        let query_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b")] };
        let index_spec = IndexSpec { fields: vec![IndexField::asc("x"), IndexField::desc("y")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::No);
    }

    #[test]
    fn test_no_match_partial_field_overlap() {
        // +a, -b does not match +a, +b (different direction on second field)
        let query_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b")] };
        let index_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::asc("b")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::No);
    }

    #[test]
    fn test_no_match_query_longer_than_index() {
        // +a, -b, +c cannot match +a (query is longer than index)
        let query_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b"), IndexField::asc("c")] };
        let index_spec = IndexSpec { fields: vec![IndexField::asc("a")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::No);
    }

    #[test]
    fn test_empty_specs() {
        let empty_spec = IndexSpec { fields: vec![] };
        let non_empty_spec = IndexSpec { fields: vec![IndexField::asc("a")] };

        // Empty spec matches any spec (empty prefix)
        assert_eq!(empty_spec.matches(&non_empty_spec), IndexSpecMatch::Yes);
        assert_eq!(empty_spec.matches(&empty_spec), IndexSpecMatch::Yes);

        // Non-empty spec does not match empty spec
        assert_eq!(non_empty_spec.matches(&empty_spec), IndexSpecMatch::No);
    }

    #[test]
    fn test_single_field_cases() {
        let asc_spec = IndexSpec { fields: vec![IndexField::asc("a")] };
        let desc_spec = IndexSpec { fields: vec![IndexField::desc("a")] };

        // Direct match
        assert_eq!(asc_spec.matches(&asc_spec), IndexSpecMatch::Yes);

        // Inverse match
        assert_eq!(asc_spec.matches(&desc_spec), IndexSpecMatch::Inverse);
        assert_eq!(desc_spec.matches(&asc_spec), IndexSpecMatch::Inverse);
    }

    #[test]
    fn test_complex_multi_field_scenarios() {
        // Test various combinations with 3+ fields
        let query_spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b"), IndexField::asc("c")] };

        // Exact match with additional fields
        let index_spec1 =
            IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b"), IndexField::asc("c"), IndexField::desc("d")] };
        assert_eq!(query_spec.matches(&index_spec1), IndexSpecMatch::Yes);

        // Inverse match with additional fields
        let index_spec2 =
            IndexSpec { fields: vec![IndexField::desc("a"), IndexField::asc("b"), IndexField::desc("c"), IndexField::asc("d")] };
        assert_eq!(query_spec.matches(&index_spec2), IndexSpecMatch::Inverse);

        // No match - mixed directions that don't form inverse
        let index_spec3 = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::asc("b"), IndexField::desc("c")] };
        assert_eq!(query_spec.matches(&index_spec3), IndexSpecMatch::No);
    }

    #[test]
    fn test_helper_methods() {
        // Test IndexField helper methods
        let asc_field = IndexField::asc("test");
        assert_eq!(asc_field.name, "test");
        assert_eq!(asc_field.direction, IndexDirection::Asc);

        let desc_field = IndexField::desc("test");
        assert_eq!(desc_field.name, "test");
        assert_eq!(desc_field.direction, IndexDirection::Desc);

        // Test that helper methods work correctly
    }

    #[test]
    fn test_edge_case_behaviors() {
        // Test that matches works correctly with various edge cases
        let spec = IndexSpec { fields: vec![IndexField::asc("a"), IndexField::desc("b"), IndexField::asc("c")] };

        // Self-match should always be Yes
        assert_eq!(spec.matches(&spec), IndexSpecMatch::Yes);

        // Empty spec matches any spec
        let empty = IndexSpec { fields: vec![] };
        assert_eq!(empty.matches(&spec), IndexSpecMatch::Yes);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexField {
    /// Field name
    pub name: String,
    /// Direction of this field in the index structure
    pub direction: IndexDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexDirection {
    Asc,
    Desc,
    Any, // usable when we're doing an equality check on this field. can default to Asc when we need to create an index versus just matching
}
