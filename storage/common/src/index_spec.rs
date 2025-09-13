#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexSpec {
    pub keyparts: Vec<IndexKeyPart>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullsOrder {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKeyPart {
    pub column: String,
    pub direction: IndexDirection, // ASC/DESC
    pub nulls: Option<NullsOrder>, // PG-style NULLS FIRST/LAST (optional)
    pub collation: Option<String>, // collation name/id if relevant (optional)
}

impl IndexKeyPart {
    pub fn asc<S: Into<String>>(col: S) -> Self {
        Self { column: col.into(), direction: IndexDirection::Asc, nulls: None, collation: None }
    }
    pub fn desc<S: Into<String>>(col: S) -> Self {
        Self { column: col.into(), direction: IndexDirection::Desc, nulls: None, collation: None }
    }
}

impl IndexSpec {
    pub fn new(keyparts: Vec<IndexKeyPart>) -> Self { Self { keyparts } }

    /// Convenience if you don’t care about nulls/collation.
    pub fn from_names_asc<I, S>(cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self { keyparts: cols.into_iter().map(|c| IndexKeyPart::asc(c)).collect() }
    }

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
                if k.collation.is_some() || k.nulls.is_some() {
                    // include extras only if present
                    let mut extras = Vec::new();
                    if let Some(c) = &k.collation {
                        extras.push(format!("collate={}", c));
                    }
                    if let Some(n) = &k.nulls {
                        extras.push(format!("nulls={:?}", n).to_lowercase());
                    }
                    format!("{} {}({})", k.column, dir, extras.join(","))
                } else {
                    format!("{} {}", k.column, dir)
                }
            })
            .collect();

        if prefix.is_empty() { fields.join(delim) } else { format!("{}{}{}", prefix, delim, fields.join(delim)) }
    }

    /// Checks if this IndexSpec can be satisfied by another IndexSpec
    /// Returns Yes if this is a prefix subset of other
    /// Returns Inverse if this is a prefix subset of other with all directions flipped
    /// Returns No if neither condition is met
    pub fn matches(&self, other: &IndexSpec) -> IndexSpecMatch {
        if self.keyparts.len() > other.keyparts.len() {
            return IndexSpecMatch::No;
        }

        let mut direct_match = true;
        let mut inverse_match = true;

        for (self_keypart, other_keypart) in self.keyparts.iter().zip(other.keyparts.iter()) {
            if self_keypart.column != other_keypart.column {
                return IndexSpecMatch::No;
            }

            if self_keypart.direction != other_keypart.direction {
                direct_match = false;
            }

            if self_keypart.direction == other_keypart.direction {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexSpecMatch {
    /// The index specs do not match
    No,
    /// The index specs match
    Yes,
    /// The index specs match, but scan direction must be inverted
    Inverse,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let spec1 = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b")] };
        let spec2 = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b")] };

        assert_eq!(spec1.matches(&spec2), IndexSpecMatch::Yes);
    }

    #[test]
    fn test_prefix_match() {
        // +a, -b matches +a, -b, +c
        let query_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b")] };
        let index_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b"), IndexKeyPart::asc("c")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::Yes);
    }

    #[test]
    fn test_inverse_exact_match() {
        // +a, -b matches -a, +b (inverse)
        let query_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b")] };
        let index_spec = IndexSpec { keyparts: vec![IndexKeyPart::desc("a"), IndexKeyPart::asc("b")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::Inverse);
    }

    #[test]
    fn test_inverse_prefix_match() {
        // +a, -b matches -a, +b, +c (inverse prefix)
        let query_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b")] };
        let index_spec = IndexSpec { keyparts: vec![IndexKeyPart::desc("a"), IndexKeyPart::asc("b"), IndexKeyPart::asc("c")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::Inverse);
    }

    #[test]
    fn test_user_example() {
        // "+a, -b matches +a, -b, any c AND -a, +b, any c"
        let query_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b")] };

        // Test direct match: +a, -b, +c
        let index_spec1 = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b"), IndexKeyPart::asc("c")] };
        assert_eq!(query_spec.matches(&index_spec1), IndexSpecMatch::Yes);

        // Test inverse match: -a, +b, -c
        let index_spec2 = IndexSpec { keyparts: vec![IndexKeyPart::desc("a"), IndexKeyPart::asc("b"), IndexKeyPart::desc("c")] };
        assert_eq!(query_spec.matches(&index_spec2), IndexSpecMatch::Inverse);
    }

    #[test]
    fn test_no_match_different_fields() {
        let query_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b")] };
        let index_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("x"), IndexKeyPart::desc("y")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::No);
    }

    #[test]
    fn test_no_match_partial_field_overlap() {
        // +a, -b does not match +a, +b (different direction on second field)
        let query_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b")] };
        let index_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::asc("b")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::No);
    }

    #[test]
    fn test_no_match_query_longer_than_index() {
        // +a, -b, +c cannot match +a (query is longer than index)
        let query_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b"), IndexKeyPart::asc("c")] };
        let index_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a")] };

        assert_eq!(query_spec.matches(&index_spec), IndexSpecMatch::No);
    }

    #[test]
    fn test_empty_specs() {
        let empty_spec = IndexSpec { keyparts: vec![] };
        let non_empty_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a")] };

        // Empty spec matches any spec (empty prefix)
        assert_eq!(empty_spec.matches(&non_empty_spec), IndexSpecMatch::Yes);
        assert_eq!(empty_spec.matches(&empty_spec), IndexSpecMatch::Yes);

        // Non-empty spec does not match empty spec
        assert_eq!(non_empty_spec.matches(&empty_spec), IndexSpecMatch::No);
    }

    #[test]
    fn test_single_field_cases() {
        let asc_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a")] };
        let desc_spec = IndexSpec { keyparts: vec![IndexKeyPart::desc("a")] };

        // Direct match
        assert_eq!(asc_spec.matches(&asc_spec), IndexSpecMatch::Yes);

        // Inverse match
        assert_eq!(asc_spec.matches(&desc_spec), IndexSpecMatch::Inverse);
        assert_eq!(desc_spec.matches(&asc_spec), IndexSpecMatch::Inverse);
    }

    #[test]
    fn test_complex_multi_field_scenarios() {
        // Test various combinations with 3+ fields
        let query_spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b"), IndexKeyPart::asc("c")] };

        // Exact match with additional fields
        let index_spec1 =
            IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b"), IndexKeyPart::asc("c"), IndexKeyPart::desc("d")] };
        assert_eq!(query_spec.matches(&index_spec1), IndexSpecMatch::Yes);

        // Inverse match with additional fields
        let index_spec2 =
            IndexSpec { keyparts: vec![IndexKeyPart::desc("a"), IndexKeyPart::asc("b"), IndexKeyPart::desc("c"), IndexKeyPart::asc("d")] };
        assert_eq!(query_spec.matches(&index_spec2), IndexSpecMatch::Inverse);

        // No match - mixed directions that don't form inverse
        let index_spec3 = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::asc("b"), IndexKeyPart::desc("c")] };
        assert_eq!(query_spec.matches(&index_spec3), IndexSpecMatch::No);
    }

    #[test]
    fn test_helper_methods() {
        // Test IndexKeyPart helper methods
        let asc_keypart = IndexKeyPart::asc("test");
        assert_eq!(asc_keypart.column, "test");
        assert_eq!(asc_keypart.direction, IndexDirection::Asc);
        assert_eq!(asc_keypart.nulls, None);
        assert_eq!(asc_keypart.collation, None);

        let desc_keypart = IndexKeyPart::desc("test");
        assert_eq!(desc_keypart.column, "test");
        assert_eq!(desc_keypart.direction, IndexDirection::Desc);
        assert_eq!(desc_keypart.nulls, None);
        assert_eq!(desc_keypart.collation, None);
    }

    #[test]
    fn test_edge_case_behaviors() {
        // Test that matches works correctly with various edge cases
        let spec = IndexSpec { keyparts: vec![IndexKeyPart::asc("a"), IndexKeyPart::desc("b"), IndexKeyPart::asc("c")] };

        // Self-match should always be Yes
        assert_eq!(spec.matches(&spec), IndexSpecMatch::Yes);

        // Empty spec matches any spec
        let empty = IndexSpec { keyparts: vec![] };
        assert_eq!(empty.matches(&spec), IndexSpecMatch::Yes);
    }
}
