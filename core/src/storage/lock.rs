//! The names a storage transaction locks, and the one order in which every
//! transaction takes them.

use std::fmt;

use crate::error::MutationError;

/// A name a storage transaction locks. It means nothing to the engine: what a
/// name guards is the business of whoever locks it. Names order by their
/// bytes.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LockName(Vec<u8>);

impl LockName {
    /// The lock named by these bytes.
    pub fn new(name: impl Into<Vec<u8>>) -> Self { Self(name.into()) }

    /// The bytes that name the lock.
    pub fn as_bytes(&self) -> &[u8] { &self.0 }

    /// Record `names` as the locks a storage transaction takes after `last`,
    /// the greatest name it has taken, if each comes after the one before it
    /// and the first after `last`: the greatest of them is then the last.
    /// Otherwise fail with [`MutationError::LockOutOfOrder`], recording none
    /// of them.
    ///
    /// This prevents cycles from acquiring these logical names in different
    /// orders. It does not rule out deadlocks on other resources or collisions
    /// in an engine's mapping of names to physical locks.
    pub(super) fn take_in_order(last: &mut Option<LockName>, names: &[LockName]) -> Result<(), MutationError> {
        let mut greatest = last.as_ref();
        for name in names {
            if let Some(after) = greatest.filter(|after| name <= *after) {
                return Err(MutationError::LockOutOfOrder { name: name.clone(), after: after.clone() });
            }

            greatest = Some(name);
        }

        if let Some(greatest) = names.last() {
            *last = Some(greatest.clone());
        }
        Ok(())
    }
}

impl fmt::Debug for LockName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "LockName(b\"{}\")", self.0.escape_ascii()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::map_conformance::fixtures::out_of_order;

    fn names(names: &[&str]) -> Vec<LockName> { names.iter().map(|name| LockName::new(*name)).collect() }

    #[test]
    fn names_ascending_across_calls_are_taken() {
        let mut last = None;

        LockName::take_in_order(&mut last, &names(&["a", "b"])).unwrap();
        LockName::take_in_order(&mut last, &[]).unwrap();
        LockName::take_in_order(&mut last, &names(&["ba", "c"])).unwrap();
        assert_eq!(last, Some(LockName::new("c")));
    }

    #[test]
    fn a_call_that_takes_no_name_leaves_the_last_name_taken() {
        let mut last = None;
        LockName::take_in_order(&mut last, &names(&["b"])).unwrap();
        LockName::take_in_order(&mut last, &[]).unwrap();

        assert_eq!(out_of_order(LockName::take_in_order(&mut last, &names(&["a"]))), (LockName::new("a"), LockName::new("b")));
    }

    #[test]
    fn a_name_taken_twice_fails_within_a_call_and_across_calls() {
        let mut last = None;
        assert_eq!(out_of_order(LockName::take_in_order(&mut last, &names(&["a", "a"]))), (LockName::new("a"), LockName::new("a")));

        LockName::take_in_order(&mut last, &names(&["b"])).unwrap();
        assert_eq!(out_of_order(LockName::take_in_order(&mut last, &names(&["b"]))), (LockName::new("b"), LockName::new("b")));
    }

    #[test]
    fn a_name_before_one_taken_fails_within_a_call_and_across_calls() {
        let mut last = None;
        assert_eq!(out_of_order(LockName::take_in_order(&mut last, &names(&["b", "a"]))), (LockName::new("a"), LockName::new("b")));

        LockName::take_in_order(&mut last, &names(&["c"])).unwrap();
        assert_eq!(out_of_order(LockName::take_in_order(&mut last, &names(&["b"]))), (LockName::new("b"), LockName::new("c")));
    }

    #[test]
    fn a_rejected_batch_leaves_the_last_name_unchanged() {
        let mut last = None;
        LockName::take_in_order(&mut last, &names(&["a"])).unwrap();
        out_of_order(LockName::take_in_order(&mut last, &names(&["b", "d", "c"])));

        assert_eq!(last, Some(LockName::new("a")));
        LockName::take_in_order(&mut last, &names(&["b"])).unwrap();
    }
}
