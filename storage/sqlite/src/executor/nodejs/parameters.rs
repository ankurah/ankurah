//! The refusal of a statement with a named parameter.

use crate::error::{Refusal, SqliteError};

/// Refuse a statement that has a named parameter, the first of which the
/// scan before preparing found (see `refuse_all_but_one_statement`), before it
/// runs: better-sqlite3 binds a named parameter only by name, which a
/// positional call cannot give, where rusqlite binds a call's values to every
/// parameter in turn. It is refused once SQLite has prepared the statement,
/// so that SQL SQLite cannot prepare fails with SQLite's own error, as it
/// does natively.
pub(super) fn refuse_a_named_parameter(named_parameter: Option<&str>) -> Result<(), SqliteError> {
    match named_parameter {
        None => Ok(()),
        Some(name) => Err(Refusal::NamedParameter { name: name.to_owned() }.into()),
    }
}
