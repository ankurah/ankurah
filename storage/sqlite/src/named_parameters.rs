//! The named parameters in a statement's SQL, which the node executor
//! refuses: node:sqlite binds no positional value to one, leaving it NULL,
//! where rusqlite binds a call's values to every parameter in turn.
//!
//! SQLite's tokenizer decides what is a parameter, and the scan follows it
//! (`sqlite3GetToken` in SQLite's tokenize.c) for every token that can hold
//! one of the characters a named parameter begins with: string literals,
//! quoted names, comments and identifiers. It scans SQL that SQLite has
//! already prepared, where each of those characters at the start of a token
//! begins a parameter. Natively it runs only in its test, which checks it
//! against the names SQLite gives each parameter.

/// The named parameters of `sql`, in the order they appear, as SQLite names
/// them: `:name`, `@name`, `#name` or `$name`, with the `::` and `(…)`
/// suffixes of SQLite's Tcl form. better-sqlite3's SQLite is built without
/// that form, and fails to prepare SQL that uses it.
pub(crate) fn named_parameters(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut parameters = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let rest = &bytes[at..];
        at += match rest {
            // SQLite reads SQL only up to a NUL.
            [0, ..] => break,
            [b'-', b'-', comment @ ..] => 2 + comment.iter().take_while(|&&byte| byte != b'\n' && byte != 0).count(),
            // `/*` with nothing after it is a slash, not a comment; a comment
            // with no `*/` runs to the end.
            [b'/', b'*', comment @ ..] if comment.first().is_some_and(|&byte| byte != 0) => {
                let comment = &comment[..comment.iter().position(|&byte| byte == 0).unwrap_or(comment.len())];
                2 + comment.windows(2).position(|pair| pair == b"*/").map_or(comment.len(), |end| end + 2)
            }
            [quote @ (b'\'' | b'"' | b'`'), ..] => quoted(rest, *quote),
            [b'[', ..] => match rest.iter().position(|&byte| byte == b']' || byte == 0) {
                Some(end) if rest[end] == b']' => end + 1,
                Some(end) => end,
                None => rest.len(),
            },
            // A UTF-8 byte order mark at the start of a token is whitespace.
            [0xef, 0xbb, 0xbf, ..] => 3,
            [b':' | b'@' | b'#' | b'$', ..] => {
                let length = parameter(rest);
                parameters.push(&sql[at..at + length]);
                length
            }
            // An identifier, a keyword or a number, which a parameter cannot
            // begin inside: `$` continues an identifier.
            [byte, ..] if is_identifier_byte(*byte) => rest.iter().take_while(|&&byte| is_identifier_byte(byte)).count(),
            _ => 1,
        };
    }
    parameters
}

/// The length of the quoted token at the start of `rest`, a string literal
/// or a quoted name: a doubled quote stands for one, and a token with no
/// closing quote runs to the end.
fn quoted(rest: &[u8], quote: u8) -> usize {
    let mut at = 1;
    while let Some(&byte) = rest.get(at).filter(|&&byte| byte != 0) {
        if byte == quote {
            if rest.get(at + 1) != Some(&quote) {
                return at + 1;
            }
            at += 1;
        }
        at += 1;
    }
    at
}

/// The length of the named parameter at the start of `rest`: its sigil, a
/// name of identifier bytes, any `::` within it, and a `(…)` suffix that
/// runs to a closing parenthesis, ending at a space or the end otherwise.
fn parameter(rest: &[u8]) -> usize {
    let mut at = 1;
    let mut named = false;
    while let Some(&byte) = rest.get(at).filter(|&&byte| byte != 0) {
        if is_identifier_byte(byte) {
            named = true;
        } else if byte == b'(' && named {
            at += 1 + rest[at + 1..].iter().take_while(|&&byte| byte != 0 && byte != b')' && !is_space(byte)).count();
            return at + usize::from(rest.get(at) == Some(&b')'));
        } else if byte == b':' && rest.get(at + 1) == Some(&b':') {
            at += 1;
        } else {
            break;
        }
        at += 1;
    }
    at
}

/// A byte SQLite allows in an identifier: an ASCII letter or digit, `_`,
/// `$`, or any byte of a character beyond ASCII.
fn is_identifier_byte(byte: u8) -> bool { byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80 }

/// A byte SQLite reads as a space within a token.
fn is_space(byte: u8) -> bool { matches!(byte, b'\t'..=b'\r' | b' ') }

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::collections::HashSet;

    use super::named_parameters;

    /// The tables the statements below read.
    const SCHEMA: &str = r#"CREATE TABLE t (a, "b:c", [d@e], `f$g`, "h""i", a$b, "x--y", "p/*q", `j``k`)"#;

    /// Statements whose parameters are named, positional, numbered and mixed,
    /// with the characters a named parameter begins with inside strings,
    /// quoted names, identifiers and comments, and the suffixes of SQLite's
    /// Tcl form.
    const STATEMENTS: &[&str] = &[
        "SELECT ?",
        "SELECT ?, ?, ?",
        "SELECT ?1, ?2, ?1",
        "SELECT ?3",
        "SELECT ?a",
        "SELECT :a",
        "SELECT @a",
        "SELECT $a",
        "SELECT #a",
        "SELECT:a",
        "SELECT :1",
        "SELECT :A, :a",
        "SELECT :a, ?, @b, ?5, $c, :a",
        "SELECT ?2, :a, ?1",
        "SELECT ?, :middle, ?",
        "SELECT ':a', '@b', '$c', '#d', 'it''s :e'",
        "SELECT 'a'':b'",
        "SELECT x'3a61', 1.5e3, .5, 0x1F, 1_000",
        r#"SELECT "b:c", [d@e], `f$g`, "h""i", `j``k` FROM t"#,
        "SELECT a$b FROM t",
        "SELECT [a:b] FROM (SELECT 1 AS [a:b])",
        "SELECT a FROM t -- :a @b $c #d",
        "SELECT a FROM t /* :a @b $c #d */",
        "SELECT a FROM t /* :a */ WHERE a = :b",
        "SELECT 1 /* comments /* do not nest :e */ , :f",
        "SELECT /**/ :c",
        "SELECT /*/ :d */ 1",
        "SELECT a FROM t WHERE a = :q /* a comment never closed :r",
        "SELECT 1 -- a comment with :s\n, :t",
        r#"SELECT "x--y", "p/*q" FROM t WHERE a = :z"#,
        "SELECT a FROM t WHERE a = '--' AND a = :n",
        "SELECT a FROM t WHERE a = '/*' AND a = :m -- */",
        "SELECT $a::b, $c(d), $e::f::g(h)",
        "SELECT :a::b, @c(d), #e::f",
        "SELECT $a()",
        "SELECT 1 WHERE 0 AND :optimized_away",
        "INSERT INTO t (a) VALUES (:v)",
        "WITH c(x) AS (SELECT @w) SELECT x FROM c",
        "SELECT a FROM t WHERE a IN (:l1, :l2) AND a=-:l3",
        "SELECT 'é:x', :é",
        "\u{FEFF}SELECT $bom",
        "SELECT 1, \u{FEFF}$after_bom",
        "SELECT :before_nul\0, :after_nul",
    ];

    /// The scan finds each statement's named parameters as SQLite names them,
    /// in the order SQLite numbers them, which is the order in which each
    /// name first appears.
    #[test]
    fn the_scan_finds_the_named_parameters_sqlite_finds() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        for sql in STATEMENTS {
            let statement = connection.prepare(sql).unwrap_or_else(|error| panic!("{sql:?} does not prepare: {error}"));
            let named: Vec<&str> = (1..=statement.parameter_count())
                .filter_map(|index| statement.parameter_name(index))
                .filter(|name| !name.starts_with('?'))
                .collect();
            let mut seen = HashSet::new();
            let mut scanned = named_parameters(sql);
            scanned.retain(|name| seen.insert(*name));
            assert_eq!(scanned, named, "{sql:?}");
        }
    }
}
