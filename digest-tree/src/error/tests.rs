use std::fmt;
use std::io;

use super::*;

/// A failure's cause comes back as its own type, and a failure asked for a
/// cause of another type comes back whole, as retryable as it was.
#[test]
fn a_failures_cause_comes_back_as_its_own_type_or_the_failure_whole() {
    assert!(StoreError::retryable(fmt::Error).downcast::<fmt::Error>().is_ok());

    let failure = StoreError::retryable(fmt::Error).downcast::<io::Error>().expect_err("a cause of another type");
    assert!(failure.is_retryable());
    assert!(failure.downcast::<fmt::Error>().is_ok());
}
