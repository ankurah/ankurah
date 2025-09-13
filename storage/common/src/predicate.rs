/// Extracts conjuncts that can be flattened from a predicate tree.
///
/// Conjuncts are predicates connected by AND operations that can be
/// extracted and used independently for index planning. OR operations
/// break conjunct chains since they require different evaluation logic.
///
/// Example: `(foo = 1 AND (? AND bar = 2)) AND (? OR (baz = 3 AND zed = 4))`
/// - `foo = 1` and `bar = 2` are conjuncts (can be flattened)  
/// - `baz = 3` and `zed = 4` are NOT conjuncts (blocked by OR)
///
/// Future optimization potential: Special handling for patterns like
/// `foo > 10 OR foo > 20` where both branches use the same field.
pub struct ConjunctFinder;

impl ConjunctFinder {
    /// Extract all top-level conjuncts from a predicate tree.
    /// Returns owned predicates in order of appearance.
    pub fn find(predicate: &ankql::ast::Predicate) -> Vec<ankql::ast::Predicate> {
        let mut conjuncts = Vec::new();
        Self::extract_conjuncts(predicate, &mut conjuncts);
        conjuncts
    }

    /// Recursively extract conjuncts, stopping at OR boundaries
    fn extract_conjuncts(predicate: &ankql::ast::Predicate, conjuncts: &mut Vec<ankql::ast::Predicate>) {
        match predicate {
            ankql::ast::Predicate::And(left, right) => {
                // Recursively extract from both sides of AND
                Self::extract_conjuncts(left, conjuncts);
                Self::extract_conjuncts(right, conjuncts);
            }
            ankql::ast::Predicate::Or(_, _) => {
                // OR breaks conjunct chains - treat the entire OR as a single conjunct
                conjuncts.push(predicate.clone());
            }
            _ => {
                // Base case: comparison, IsNull, Not, True, False, Placeholder
                conjuncts.push(predicate.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_derive::selection;

    #[test]
    fn test_single_comparison() {
        let predicate = selection!("age > 25").predicate;
        let conjuncts = ConjunctFinder::find(&predicate);
        assert_eq!(conjuncts.len(), 1);
        assert_eq!(conjuncts[0], predicate);
    }

    #[test]
    fn test_simple_and() {
        let predicate = selection!("age > 25 AND name = 'Alice'").predicate;
        let conjuncts = ConjunctFinder::find(&predicate);
        assert_eq!(conjuncts.len(), 2);

        // Should extract both comparisons
        assert_eq!(conjuncts[0], selection!("age > 25").predicate);
        assert_eq!(conjuncts[1], selection!("name = 'Alice'").predicate);
    }

    #[test]
    fn test_nested_and() {
        let predicate = selection!("(age > 25 AND name = 'Alice') AND score < 100").predicate;
        let conjuncts = ConjunctFinder::find(&predicate);
        assert_eq!(conjuncts.len(), 3);

        // Should flatten all AND operations
        assert_eq!(conjuncts[0], selection!("age > 25").predicate);
        assert_eq!(conjuncts[1], selection!("name = 'Alice'").predicate);
        assert_eq!(conjuncts[2], selection!("score < 100").predicate);
    }

    #[test]
    fn test_or_blocks_conjunct_extraction() {
        let predicate = selection!("age > 25 OR name = 'Alice'").predicate;
        let conjuncts = ConjunctFinder::find(&predicate);
        assert_eq!(conjuncts.len(), 1);

        // The entire OR should be treated as a single conjunct
        assert_eq!(conjuncts[0], predicate);
    }

    #[test]
    fn test_and_with_or_mixed() {
        let predicate = selection!("score = 100 AND (age > 25 OR name = 'Alice')").predicate;
        let conjuncts = ConjunctFinder::find(&predicate);
        assert_eq!(conjuncts.len(), 2);

        // Should extract the equality and treat the OR as a single conjunct
        assert_eq!(conjuncts[0], selection!("score = 100").predicate);
        assert_eq!(conjuncts[1], selection!("age > 25 OR name = 'Alice'").predicate);
    }

    #[test]
    fn test_complex_nested_example() {
        // Example from documentation: (foo = 1 AND bar = 2) AND (baz = 3 OR zed = 4)
        let predicate = selection!("(foo = 1 AND bar = 2) AND (baz = 3 OR zed = 4)").predicate;
        let conjuncts = ConjunctFinder::find(&predicate);
        assert_eq!(conjuncts.len(), 3);

        // foo = 1 and bar = 2 should be extracted as conjuncts
        assert_eq!(conjuncts[0], selection!("foo = 1").predicate);
        assert_eq!(conjuncts[1], selection!("bar = 2").predicate);
        // The OR should remain as a single conjunct
        assert_eq!(conjuncts[2], selection!("baz = 3 OR zed = 4").predicate);
    }

    #[test]
    fn test_non_comparison_predicates() {
        // Test with a simple AND of two comparisons since IS NULL isn't supported by selection! macro
        let predicate = selection!("age > 25 AND score = 100").predicate;
        let conjuncts = ConjunctFinder::find(&predicate);
        assert_eq!(conjuncts.len(), 2);

        assert_eq!(conjuncts[0], selection!("age > 25").predicate);
        assert_eq!(conjuncts[1], selection!("score = 100").predicate);
    }

    #[test]
    fn test_true_false_predicates() {
        // Test with Predicate::True and Predicate::False
        let conjuncts = ConjunctFinder::find(&ankql::ast::Predicate::True);
        assert_eq!(conjuncts.len(), 1);
        assert_eq!(conjuncts[0], ankql::ast::Predicate::True);

        let conjuncts = ConjunctFinder::find(&ankql::ast::Predicate::False);
        assert_eq!(conjuncts.len(), 1);
        assert_eq!(conjuncts[0], ankql::ast::Predicate::False);
    }
}
