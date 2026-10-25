//! SQL text read as SQLite's tokenizer reads it, for what the executors need
//! to know before SQLite prepares a statement: whether some SQL holds no
//! statement, one, or a second after its first, what SQLite skips between
//! statements, a statement's first keyword, and its first named parameter.
//!
//! The scan follows SQLite's tokenizer (`sqlite3GetToken` in tokenize.c) for
//! every kind of token that can hold a semicolon, a keyword, or a character a
//! named parameter begins with: whitespace and comments, string literals,
//! quoted names, identifiers and parameters. It finds where a statement ends
//! as `sqlite3_complete` (complete.c) does, reading a CREATE TRIGGER statement
//! on through its body to the semicolon after the body's END. Its tests check
//! each reading against SQLite's own.

/// A token of SQL, as the scans tell tokens apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenKind {
    /// Whitespace, a comment or a UTF-8 byte order mark: what SQLite skips.
    Space,
    Semicolon,
    /// An identifier, a keyword or a number.
    Word,
    /// A named parameter: `:name`, `@name`, `#name` or `$name`.
    Parameter,
    /// Anything else: a string, a quoted name, a positional parameter, an
    /// operator.
    Other,
}

/// The tokens of `sql`, each with its text, up to the end or a NUL, where
/// SQLite stops reading SQL.
fn tokens(sql: &str) -> impl Iterator<Item = (TokenKind, &str)> {
    let mut at = 0;
    std::iter::from_fn(move || {
        let rest = &sql.as_bytes()[at..];
        let (kind, length) = match rest {
            [] | [0, ..] => return None,
            // A run of whitespace begins with one of five characters and goes
            // on through any of six: the vertical tab only continues one.
            [b' ' | b'\t' | b'\n' | b'\x0c' | b'\r', run @ ..] => {
                (TokenKind::Space, 1 + run.iter().take_while(|&&byte| is_space(byte)).count())
            }
            // A UTF-8 byte order mark at the start of a token is whitespace.
            [0xef, 0xbb, 0xbf, ..] => (TokenKind::Space, 3),
            [b'-', b'-', comment @ ..] => (TokenKind::Space, 2 + comment.iter().take_while(|&&byte| byte != b'\n' && byte != 0).count()),
            // `/*` with nothing after it is a slash, not a comment; a comment
            // with no `*/` runs to the end.
            [b'/', b'*', comment @ ..] if comment.first().is_some_and(|&byte| byte != 0) => {
                let comment = &comment[..comment.iter().position(|&byte| byte == 0).unwrap_or(comment.len())];
                (TokenKind::Space, 2 + comment.windows(2).position(|pair| pair == b"*/").map_or(comment.len(), |end| end + 2))
            }
            [b';', ..] => (TokenKind::Semicolon, 1),
            [quote @ (b'\'' | b'"' | b'`'), ..] => (TokenKind::Other, quoted(rest, *quote)),
            [b'[', ..] => (
                TokenKind::Other,
                match rest.iter().position(|&byte| byte == b']' || byte == 0) {
                    Some(end) if rest[end] == b']' => end + 1,
                    Some(end) => end,
                    None => rest.len(),
                },
            ),
            [b':' | b'@' | b'#' | b'$', ..] => (TokenKind::Parameter, parameter(rest)),
            [b'?', digits @ ..] => (TokenKind::Other, 1 + digits.iter().take_while(|byte| byte.is_ascii_digit()).count()),
            // An identifier, a keyword or a number, which a parameter cannot
            // begin inside: `$` continues an identifier.
            [byte, ..] if is_identifier_byte(*byte) => (TokenKind::Word, rest.iter().take_while(|&&byte| is_identifier_byte(byte)).count()),
            _ => (TokenKind::Other, 1),
        };
        let token = &sql[at..at + length];
        at += length;
        Some((kind, token))
    })
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

/// A byte SQLite reads as a space within a run of whitespace.
fn is_space(byte: u8) -> bool { matches!(byte, b'\t'..=b'\r' | b' ') }

/// The offset of `token`, a slice of `sql`, within it.
fn offset(sql: &str, token: &str) -> usize { token.as_ptr() as usize - sql.as_ptr() as usize }

/// Whether `sql` holds only what SQLite skips between statements:
/// whitespace, comments and semicolons, up to the end or a NUL.
pub(crate) fn holds_no_statement(sql: &str) -> bool { tokens(sql).all(|(kind, _)| matches!(kind, TokenKind::Space | TokenKind::Semicolon)) }

/// The statements some SQL holds, as the executors need to know them before
/// SQLite prepares it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Statements<'s> {
    /// No statement: only what SQLite skips between statements, up to the
    /// end or a NUL (see `holds_no_statement`).
    None,
    /// One statement, which ends at `end` (see `FirstStatement::end`) and is
    /// followed by no more than what SQLite skips, with the first named
    /// parameter it has, if any: `:name`, `@name`, `#name` or `$name`, with
    /// the `::` and `(…)` suffixes of SQLite's Tcl form, as SQLite names it.
    /// better-sqlite3's SQLite is built without that form, and fails to
    /// prepare SQL that uses it.
    One { end: usize, named_parameter: Option<&'s str> },
    /// A second statement after the first.
    Several,
}

/// The statements `sql` holds, found in one pass over it.
pub(crate) fn statements(sql: &str) -> Statements<'_> {
    let first = first_statement(sql);
    if !first.found {
        Statements::None
    } else if holds_no_statement(&sql[first.end..]) {
        Statements::One { end: first.end, named_parameter: first.named_parameter }
    } else {
        Statements::Several
    }
}

/// The first statement of some SQL, as one pass over its tokens finds it.
struct FirstStatement<'s> {
    /// Where it ends: just past the semicolon that ends it, or, when none
    /// does, just past its last token, so that whitespace and comments after
    /// it, a comment that runs to the end of the SQL among them, are no part
    /// of it. The whitespace, comments and semicolons SQLite skips before a
    /// statement are no part of it either, and a CREATE TRIGGER statement
    /// goes on through the semicolons of its body to the one after the
    /// body's END.
    end: usize,
    /// Whether there is a statement at all, rather than only what SQLite skips.
    found: bool,
    /// The statement's first named parameter.
    named_parameter: Option<&'s str>,
}

/// The first statement of `sql`, followed through `sqlite3_complete`'s
/// states to where it ends.
fn first_statement(sql: &str) -> FirstStatement<'_> {
    let mut state = State::Start;
    let mut first = FirstStatement { end: 0, found: false, named_parameter: None };
    for (kind, text) in tokens(sql) {
        let token = match kind {
            TokenKind::Space => continue,
            TokenKind::Semicolon => CompletionToken::Semicolon,
            TokenKind::Word => CompletionToken::of_word(text),
            TokenKind::Parameter => {
                first.named_parameter.get_or_insert(text);
                CompletionToken::Other
            }
            TokenKind::Other => CompletionToken::Other,
        };
        first.end = offset(sql, text) + text.len();
        state = NEXT[state as usize][token as usize];
        if token == CompletionToken::Semicolon && state == State::Start && first.found {
            return first;
        }
        first.found |= token != CompletionToken::Semicolon;
    }
    first
}

/// `sql` past what SQLite skips before its first statement.
#[cfg(any(test, all(target_arch = "wasm32", feature = "nodejs")))]
pub(crate) fn skip_to_statement(sql: &str) -> &str {
    let start = tokens(sql)
        .find(|(kind, _)| !matches!(kind, TokenKind::Space | TokenKind::Semicolon))
        .map_or(sql.len(), |(_, token)| offset(sql, token));
    &sql[start..]
}

/// The keyword a statement begins with, past what SQLite skips before it, or
/// an empty string when it begins with no word.
#[cfg(any(test, all(target_arch = "wasm32", feature = "nodejs")))]
pub(crate) fn first_keyword(sql: &str) -> &str {
    match tokens(skip_to_statement(sql)).next() {
        Some((TokenKind::Word, word)) => word,
        _ => "",
    }
}

/// The states of `sqlite3_complete`, which follows a statement to its end:
/// complete.c's START, NORMAL, EXPLAIN, CREATE, TRIGGER, SEMI and END. Its
/// INVALID state differs from START only in calling empty SQL incomplete,
/// which `first_statement` tells apart itself.
#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Start,
    Normal,
    Explain,
    Create,
    Trigger,
    Semi,
    End,
}

/// The tokens `sqlite3_complete` tells apart, past whitespace and comments,
/// which change no state.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CompletionToken {
    Semicolon,
    Other,
    Explain,
    Create,
    Temp,
    Trigger,
    End,
}

impl CompletionToken {
    /// The token a word is, its keyword read without regard to ASCII case,
    /// as SQLite reads keywords, and without copying the word.
    fn of_word(word: &str) -> Self {
        const KEYWORDS: [(&str, CompletionToken); 6] = [
            ("explain", CompletionToken::Explain),
            ("create", CompletionToken::Create),
            ("temp", CompletionToken::Temp),
            ("temporary", CompletionToken::Temp),
            ("trigger", CompletionToken::Trigger),
            ("end", CompletionToken::End),
        ];
        KEYWORDS.iter().find(|(keyword, _)| word.eq_ignore_ascii_case(keyword)).map_or(CompletionToken::Other, |&(_, token)| token)
    }
}

/// `sqlite3_complete`'s transitions, as complete.c lays them out.
#[rustfmt::skip]
const NEXT: [[State; 7]; 7] = {
    use State::*;
    [
        //               Semicolon Other    Explain  Create   Temp     Trigger  End
        /* Start   */ [Start,    Normal,  Explain, Create,  Normal,  Normal,  Normal],
        /* Normal  */ [Start,    Normal,  Normal,  Normal,  Normal,  Normal,  Normal],
        /* Explain */ [Start,    Explain, Normal,  Create,  Normal,  Normal,  Normal],
        /* Create  */ [Start,    Normal,  Normal,  Normal,  Create,  Trigger, Normal],
        /* Trigger */ [Semi,     Trigger, Trigger, Trigger, Trigger, Trigger, Trigger],
        /* Semi    */ [Semi,     Trigger, Trigger, Trigger, Trigger, Trigger, End],
        /* End     */ [Start,    Trigger, Trigger, Trigger, Trigger, Trigger, Trigger],
    ]
};

/// SQL on either side of holding no statement, on which `holds_no_statement`
/// is checked against SQLite's own reading, natively and on node:sqlite:
/// whitespace, semicolons and comments, ended or not, byte order marks and
/// SQL past a NUL, beside characters and statements SQLite does not skip.
#[cfg(test)]
const NO_STATEMENT_CANDIDATES: &[&str] = &[
    "",
    " \t\n\x0c\r",
    " \x0b",
    "\x0b",
    ";",
    " ; ;",
    " ; ;; ",
    "-- c",
    "-- c\n;",
    "-",
    "/* c */",
    "/**/",
    "/*/",
    "/* unended",
    "/*",
    "/",
    "\u{FEFF}",
    " \u{FEFF};\u{FEFF} ",
    "\0 SELECT 1",
    " SELECT 1",
    "x",
];

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::{collections::HashSet, ffi::CString};

    use rusqlite::{ffi, Batch, Connection};

    use super::{
        first_keyword, first_statement, holds_no_statement, skip_to_statement, statements, tokens, Statements, TokenKind,
        NO_STATEMENT_CANDIDATES,
    };

    /// The tables the statements below read.
    const SCHEMA: &str = r#"CREATE TABLE t (a, "b:c", [d@e], `f$g`, "h""i", a$b, "x--y", "p/*q", `j``k`); CREATE TABLE x (a)"#;

    /// SQL of one statement and more, with semicolons inside strings, quoted
    /// names and comments, triggers with semicolons, CASE … END and END in
    /// their bodies, keywords as names, and runs of semicolons and
    /// whitespace. A byte order mark appears only where `sqlite3_complete`,
    /// which reads one as a word, ends a statement where SQLite does.
    const STATEMENTS: &[&str] = &[
        "SELECT 1",
        "SELECT 1;",
        "SELECT 1; SELECT 2",
        ";;  ; SELECT 1; SELECT 2",
        "SELECT 1;;; SELECT 2",
        "SELECT 1 ;\t\n\x0c\r SELECT 2",
        "SELECT ';' || \";\" || [;] || `;`; SELECT 2",
        r#"SELECT "a;b", 'c;d', 'it''s; here' FROM "x;y"; SELECT 2"#,
        "SELECT 1 -- a comment with ;\n; SELECT 2",
        "SELECT 1 /* ; */ ; SELECT 2",
        "SELECT 1 /* a comment never closed ; SELECT 2",
        "SELECT 'a string never closed ; SELECT 2",
        "SELECT 1 -- a comment at the end ;",
        "SELECT :a; SELECT 2",
        "WITH a AS (SELECT 1) SELECT * FROM a; SELECT 2",
        "BEGIN; SELECT 1; END;",
        "CREATE TABLE trigger (end); SELECT 1",
        "CREATE VIEW v AS SELECT 1; SELECT 2",
        "CREATE TEMP VIEW v AS SELECT 1; SELECT 2",
        "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; SELECT 2; END; SELECT 3",
        "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END",
        "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END; END;",
        "CREATE TEMP TRIGGER t AFTER INSERT ON x BEGIN UPDATE x SET a = CASE WHEN a THEN 1 ELSE 2 END; END; SELECT 3",
        "create temporary trigger t after insert on x begin select 1; end ; select 2",
        "EXPLAIN CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; END; SELECT 2",
        "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT ';END;'; END; SELECT 2",
        "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1; /* ; END; */ END; SELECT 2",
        "CREATE TRIGGER t AFTER INSERT ON x BEGIN SELECT 1;\n-- END;\nEND; SELECT 2",
        "CREATE TRIGGER IF NOT EXISTS t BEFORE DELETE ON x FOR EACH ROW WHEN old.a > 0 BEGIN DELETE FROM x WHERE a = old.a; \
         INSERT INTO x VALUES (old.a); END; SELECT 2",
        "\u{FEFF}SELECT 1; SELECT 2",
        "SELECT 1 \u{FEFF}; SELECT 2",
        "SELECT 1;\u{FEFF}SELECT 2",
    ];

    /// `first_statement` ends each statement where `sqlite3_complete` first
    /// calls the SQL complete, past what SQLite skips before it, and one that
    /// no semicolon ends where SQLite's parser finds no statement in the rest.
    #[test]
    fn statements_end_where_sqlite3_complete_ends_them() {
        let connection = Connection::open_in_memory().unwrap();
        let complete = |sql: &str| unsafe { ffi::sqlite3_complete(CString::new(sql).unwrap().as_ptr()) == 1 };
        let no_statement = |sql: &str| matches!(Batch::new(&connection, sql).next(), Ok(None));
        for sql in STATEMENTS {
            let start = sql.len() - skip_to_statement(sql).len();
            let expected = (start + 1..=sql.len())
                .find(|&end| sql.as_bytes()[end - 1] == b';' && complete(&sql[..end]))
                .or_else(|| (start..=sql.len()).find(|&end| sql.is_char_boundary(end) && no_statement(&sql[end..])));
            assert_eq!(Some(first_statement(sql).end), expected, "{sql:?}");
        }
    }

    /// Where `sqlite3_complete`, which reads a byte order mark as a word and a
    /// vertical tab as an operator, would read a trigger otherwise than SQLite
    /// does, `first_statement` ends it where SQLite's parser does.
    #[test]
    fn statements_end_where_the_parser_ends_them() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        for sql in [
            "\u{FEFF}CREATE TRIGGER t1 AFTER INSERT ON x BEGIN SELECT 1; END; SELECT 2",
            "CREATE TRIGGER t2 AFTER INSERT ON x BEGIN SELECT 1; \u{FEFF}END; SELECT 2",
            "CREATE \x0bTRIGGER t3 AFTER INSERT ON x BEGIN SELECT 1; END; SELECT 2",
            ";\u{FEFF};CREATE TRIGGER t4 AFTER INSERT ON x BEGIN SELECT 1;\x0c END ; SELECT 2",
        ] {
            // SAFETY: the connection outlives the statement, which is
            // finalized at once, and `tail` points into `sql`.
            let parsed = unsafe {
                let mut statement = std::ptr::null_mut();
                let mut tail = std::ptr::null();
                let code = ffi::sqlite3_prepare_v2(connection.handle(), sql.as_ptr().cast(), sql.len() as i32, &mut statement, &mut tail);
                ffi::sqlite3_finalize(statement);
                assert_eq!(code, ffi::SQLITE_OK, "{sql:?}");
                tail.cast::<u8>().offset_from(sql.as_ptr()) as usize
            };
            assert_eq!(first_statement(sql).end, parsed, "{sql:?}");
        }
    }

    /// `first_keyword` reads past what SQLite skips before a statement, a byte
    /// order mark and semicolons among it, and finds no keyword where a
    /// statement begins with something else.
    #[test]
    fn the_first_keyword_comes_after_what_sqlite_skips() {
        assert_eq!(first_keyword("\u{FEFF}; -- c\n/* d */ PRAGMA busy_timeout"), "PRAGMA");
        assert_eq!(first_keyword(" explain\tSELECT 1"), "explain");
        assert_eq!(first_keyword("(SELECT 1)"), "");
        assert_eq!(first_keyword(";;"), "");
    }

    /// `holds_no_statement` holds of exactly the SQL in which SQLite's parser
    /// finds no statement, and `statements` finds none in exactly that SQL.
    #[test]
    fn sql_holds_no_statement_where_sqlite_finds_none() {
        let connection = Connection::open_in_memory().unwrap();
        for sql in NO_STATEMENT_CANDIDATES {
            let no_statement = matches!(Batch::new(&connection, sql).next(), Ok(None));
            assert_eq!(holds_no_statement(sql), no_statement, "{sql:?}");
            assert_eq!(statements(sql) == Statements::None, no_statement, "{sql:?}");
        }
    }

    /// Statements whose parameters are named, positional, numbered and mixed,
    /// with the characters a named parameter begins with inside strings,
    /// quoted names, identifiers and comments, and the suffixes of SQLite's
    /// Tcl form.
    const PARAMETERS: &[&str] = &[
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

    /// The scan reads each statement's named parameters as SQLite names them,
    /// in the order SQLite numbers them, which is the order in which each
    /// name first appears, and `statements` gives the first of them.
    #[test]
    fn the_scan_finds_the_named_parameters_sqlite_finds() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        for sql in PARAMETERS {
            let statement = connection.prepare(sql).unwrap_or_else(|error| panic!("{sql:?} does not prepare: {error}"));
            let named: Vec<&str> = (1..=statement.parameter_count())
                .filter_map(|index| statement.parameter_name(index))
                .filter(|name| !name.starts_with('?'))
                .collect();
            let mut seen = HashSet::new();
            let mut scanned: Vec<&str> =
                tokens(sql).filter(|(kind, _)| *kind == TokenKind::Parameter).map(|(_, parameter)| parameter).collect();
            scanned.retain(|name| seen.insert(*name));
            assert_eq!(scanned, named, "{sql:?}");
            assert!(
                matches!(statements(sql), Statements::One { named_parameter, .. } if named_parameter == named.first().copied()),
                "{sql:?}"
            );
        }
    }
}

#[cfg(all(test, target_arch = "wasm32", feature = "nodejs"))]
mod node_sqlite_tests {
    use js_sys::{Function, Reflect};
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_test::wasm_bindgen_test;

    use super::{holds_no_statement, NO_STATEMENT_CANDIDATES};

    /// `holds_no_statement` holds of exactly what SQLite skips between
    /// statements on node:sqlite's SQLite too: SQL it holds of prepares there
    /// to no statement, and any other SQL to a statement or an error.
    #[wasm_bindgen_test]
    fn sql_holds_no_statement_where_node_sqlite_finds_none() {
        let construct = Function::new_no_args("return new (process.getBuiltinModule('node:sqlite').DatabaseSync)(':memory:');");
        let sqlite = construct.call0(&JsValue::NULL).unwrap();
        let prepare: Function = Reflect::get(&sqlite, &"prepare".into()).unwrap().dyn_into().unwrap();
        // node:sqlite can crash the process, in a later sqlite3_step, when a
        // statement prepared from SQL that holds no statement is collected
        // while its database is open, as Node.js 24.13.0 does. So the test
        // holds every statement it prepares until it has closed the database.
        let mut statements = Vec::new();
        let mut prepares_to_no_statement = |sql: &str| match prepare.call1(&sqlite, &sql.into()) {
            // node:sqlite hands SQL that holds no statement back as a statement
            // already finalized, whose source cannot be read.
            Ok(statement) => {
                let finalized = Reflect::get(&statement, &"sourceSQL".into()).is_err();
                statements.push(statement);
                finalized
            }
            Err(_) => false,
        };
        for sql in NO_STATEMENT_CANDIDATES {
            assert_eq!(holds_no_statement(sql), prepares_to_no_statement(sql), "{sql:?}");
        }
        let close: Function = Reflect::get(&sqlite, &"close".into()).unwrap().dyn_into().unwrap();
        close.call0(&sqlite).unwrap();
        drop(statements);
    }
}
