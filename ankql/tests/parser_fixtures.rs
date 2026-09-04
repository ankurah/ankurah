//! Parser fixture tests for ankql.
//!
//! The AST fixtures in `proto/test_fixtures/ankql_ast.bin` pin how an AST *encodes*.
//! They say nothing about which AST a given query text produces. This file closes
//! that gap: it runs a corpus of query strings through `ankql::parser::parse_selection`
//! and records, for each one, exactly what came out.
//!
//! Every case is written down as whatever the parser actually does, not as what SQL
//! would do. Several of these differ from SQL — `AND` does not bind tighter than `OR`,
//! `NOT IN` silently becomes `IN`, negative and fractional literals do not parse at
//! all — and pinning those divergences is the point: a port must reproduce this
//! parser, and it cannot do that from a SQL reference.
//!
//! Cases are sorted into `accept` and `reject` by what the parser returns, not by what
//! this file predicts. If a change flips a query from one to the other, the fixture
//! changes and verify mode fails.
//!
//! Each accepted case carries four assertions for a port to make:
//!   1. `ast_json`        - the tree the parser built.
//!   2. `ast_bincode_hex` - that tree's bincode encoding. A port that builds a
//!                          semantically-equal but structurally-different tree passes
//!                          (1) and fails here.
//!   3. `predicate_sql`   - `generate_selection_sql(&selection.predicate, None)`.
//!   4. `roundtrip_sql`   - `format!("{}", selection)`, which is the predicate SQL plus
//!                          the ORDER BY and LIMIT clauses.
//!
//! - If `OVERWRITE_FIXTURES` env var is set: write the fixture.
//! - If NOT set: read it and assert it matches exactly.
//!
//! Run with `OVERWRITE_FIXTURES=1 cargo test -p ankql --test parser_fixtures` to regenerate.
//!
//! See `ankql/test_fixtures/README.md` for the schema and the behaviour notes.
use std::fs;
use std::path::PathBuf;

use ankql::error::ParseError;
use ankql::parser::parse_selection;
use ankql::selection::sql::generate_selection_sql;

const ENCODING: &str = "bincode 1.3 `serialize` defaults: fixed-width integers, little-endian, \
     u64 sequence/string length prefixes, u32 enum variant tags, 1-byte Option tag, \
     no length prefix on fixed-size arrays";

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_fixtures")
        .join(name)
}

fn check_or_write_bytes(name: &str, data: &[u8]) {
    let path = fixture_path(name);
    let overwrite = std::env::var("OVERWRITE_FIXTURES").is_ok();

    if overwrite {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!("Wrote fixture: {} ({} bytes)", path.display(), data.len());
    } else if !path.exists() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, data).unwrap();
        eprintln!(
            "Generated missing fixture: {} ({} bytes)",
            path.display(),
            data.len()
        );
    } else {
        let expected = fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "Failed to read fixture {}: {}. Run with OVERWRITE_FIXTURES=1 to generate.",
                path.display(),
                e
            )
        });
        assert_eq!(data, &expected[..], "Fixture mismatch for {}", name);
    }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// The `ParseError` variant name, which is a stable contract a port can implement.
fn error_variant(err: &ParseError) -> &'static str {
    match err {
        ParseError::SyntaxError(_) => "SyntaxError",
        ParseError::EmptyExpression => "EmptyExpression",
        ParseError::UnexpectedRule { .. } => "UnexpectedRule",
        ParseError::InvalidPredicate(_) => "InvalidPredicate",
        ParseError::MissingOperand(_) => "MissingOperand",
    }
}

/// The error text, for every variant except `SyntaxError`.
///
/// `SyntaxError` wraps pest's rendered diagnostic - a multi-line block with the input
/// echoed back, a caret, and a list of expected rule names. That string is pest's, not
/// ankurah's: it changes with the pest version and no port could reproduce it. Pinning
/// it would make the fixture brittle and teach a consumer nothing, so `SyntaxError`
/// cases record only the variant. Which distinction still matters: a `SyntaxError` says
/// the grammar rejected the text, while every other variant says the grammar accepted
/// it and AST construction then refused it - two different things for a port to get
/// right, and the corpus below contains both.
fn error_message(err: &ParseError) -> Option<String> {
    match err {
        ParseError::SyntaxError(_) => None,
        other => Some(other.to_string()),
    }
}

struct Case {
    query: &'static str,
    note: &'static str,
}

const fn c(query: &'static str, note: &'static str) -> Case {
    Case { query, note }
}

/// The corpus. Ordered by theme; the generator preserves this order within each of the
/// `accept` and `reject` arrays, so a diff stays readable.
const CORPUS: &[Case] = &[
    // ---- comparison operators, in surface syntax ----
    c("name = 'test'", "baseline equality"),
    c("name <> 'test'", "not-equal, SQL spelling"),
    c(
        "name != 'test'",
        "not-equal, C spelling - must produce the identical AST to the <> case",
    ),
    c("score > 5", "greater than"),
    c(
        "score >= 5",
        "greater or equal - the grammar tries GtEq before Gt, so this is one operator, not '>' followed by '='",
    ),
    c("score < 5", "less than"),
    c("score <= 5", "less or equal"),
    c("status IN ('a', 'b')", "IN with a two-element row"),
    c("status IN ('a')", "IN with a one-element row"),
    c(
        "status NOT IN ('a', 'b')",
        "NOT IN - the grammar's In rule swallows an optional leading NOT, so watch which operator comes out",
    ),
    c(
        "score BETWEEN 1 AND 10",
        "BETWEEN - present in the grammar and in ComparisonOperator; check whether the parser builds it",
    ),
    // ---- integer literal width: which Literal variant does the parser choose? ----
    c("n = 0", "zero"),
    c("n = 1", "one"),
    c("n = 32767", "i16::MAX - does the parser ever emit Literal::I16?"),
    c("n = 32768", "i16::MAX + 1"),
    c("n = 2147483646", "i32::MAX - 1"),
    c(
        "n = 2147483647",
        "i32::MAX exactly - parse_number's bound is a strict <, so this is the boundary that decides I32 vs I64",
    ),
    c("n = 2147483648", "i32::MAX + 1"),
    c(
        "n = 9007199254740993",
        "2^53 + 1 - the first integer a JS number cannot hold exactly",
    ),
    c("n = 9223372036854775807", "i64::MAX"),
    c(
        "n = 9223372036854775808",
        "i64::MAX + 1 - beyond what parse::<i64>() accepts",
    ),
    c("n = -1", "negative integer"),
    c("n = +1", "explicitly signed positive integer"),
    c("n = 1.5", "decimal"),
    c("n = 1e3", "scientific notation"),
    // ---- boolean and null literals ----
    c("TRUE", "bare TRUE as the whole predicate"),
    c("true", "lowercase true"),
    c("FALSE", "bare FALSE"),
    c("flag = true", "true on the right of a comparison"),
    c("flag = NULL", "NULL literal - present in the grammar; is it in the AST?"),
    // ---- string literals ----
    c("name = 'hello'", "plain string"),
    c(
        "name = ''''",
        "four quote characters - OnlyQuotesSequence is tried before AnythingButQuotesSequence, \
         so record what content this yields and how it renders back to SQL",
    ),
    c("name = ''", "empty string"),
    c(
        "name = 'it''s'",
        "SQL-style doubled quote as an escape - check whether the grammar treats it as one literal",
    ),
    c(
        "name = 'café 日本語 🚀 مرحبا'",
        "non-ASCII string contents: 2-, 3- and 4-byte UTF-8 and RTL script",
    ),
    c("name = 'a b  c'", "interior whitespace inside a string must survive"),
    c("name = 'ORDER BY'", "a reserved word inside a string is just text"),
    // ---- identifiers and paths ----
    c("licensing.territory = 'US'", "two-step dotted path in the predicate"),
    c("a.b.c = 1", "three-step dotted path"),
    c("\"quoted\" = 1", "double-quoted identifier"),
    c("MixedCase = 1", "identifier case must be preserved verbatim"),
    c("snake_case = 1", "underscore in an identifier"),
    c(
        "kebab-case = 1",
        "hyphen in an identifier - the grammar's IdentifierNonDigit includes '-', so this is one name, not a subtraction",
    ),
    c(
        "имя = 1",
        "Cyrillic identifier - the grammar explicitly admits the Cyrillic ranges",
    ),
    c(
        "名前 = 1",
        "CJK identifier - not in any of the grammar's identifier ranges",
    ),
    c(
        "option = 1",
        "a Reserved word as a column name - IdentifierInner's negative lookahead fires when a \
         reserved word is followed by an open paren, whitespace, a comma or end of input",
    ),
    c("left = 1", "another Reserved word as a column name"),
    c(
        "optional = 1",
        "a column name that merely starts with the reserved word option - the lookahead should \
         not fire, because the next character is not one of those boundaries",
    ),
    c("leftish = 1", "the same, extending the reserved word left"),
    c(
        "limit = 1",
        "limit reaches Reserved only through the whole LimitClause rule, which also requires a \
         value, so a bare limit as a column name may be fine",
    ),
    c("order = 1", "the same for order, which reaches Reserved via OrderByClause"),
    // ---- AND / OR precedence: the case that separates a correct port from a wrong one ----
    c("a = 1 AND b = 2", "single AND"),
    c("a = 1 OR b = 2", "single OR"),
    c(
        "a = 1 OR b = 2 AND c = 3",
        "unparenthesized OR then AND - in SQL, AND binds tighter; record what this parser does",
    ),
    c(
        "a = 1 AND b = 2 OR c = 3",
        "unparenthesized AND then OR - the mirror of the case above",
    ),
    c("(a = 1 OR b = 2) AND c = 3", "explicit parentheses around the OR"),
    c("a = 1 AND (b = 2 OR c = 3)", "explicit parentheses around the right operand"),
    c("a = 1 AND b = 2 AND c = 3", "three-term AND chain - which way does it associate?"),
    c("a = 1 OR b = 2 OR c = 3", "three-term OR chain"),
    c("and_field = 1", "an identifier that merely starts with 'and' is not the AND operator"),
    // ---- NOT and IS NULL ----
    c("NOT (a = 1)", "unary NOT over a parenthesized expression"),
    c("NOT a = 1", "unary NOT over a bare comparison"),
    c(
        "NOT (a = 1) AND b = 2",
        "NOT with more expression after it - parse_expr's UnaryNot branch returns as soon as \
         it has the negated operand, so check whether the trailing conjunct survives",
    ),
    c(
        "a = 1 AND NOT (b = 2)",
        "NOT in the right-hand position rather than the leading one - only the leading \
         UnaryNot of ExprAtomValue is reached by that branch",
    ),
    c(
        "nothing = 1",
        "an identifier that merely starts with the letters n-o-t - NotFlag carries no \
         IDENT_CONT word-boundary guard, unlike And and Or",
    ),
    c("not_field = 1", "the same, with an underscore right after those letters"),
    c("a IS NULL", "IS NULL postfix"),
    c("a IS NOT NULL", "IS NOT NULL postfix"),
    c(
        "a isnull",
        "IS and NULL with no space between them - IsNullPostfix is an ordinary rule, not an \
         atomic one, so pest skips implicit whitespace between its parts, including none",
    ),
    c("licensing.territory IS NULL", "IS NULL on a dotted path"),
    c("a IS NULL AND b = 1", "IS NULL combined with AND"),
    // ---- arithmetic against comparison ----
    c(
        "qty * 3 = 9",
        "multiplication inside a comparison - the grammar has ArithInfixOp; does the AST builder?",
    ),
    c("a + 1 = 2", "addition inside a comparison"),
    c("a - 1 = 2", "subtraction - note this competes with '-' being legal inside identifiers"),
    // ---- placeholders ----
    c("?", "a bare placeholder as the whole predicate"),
    c("a = ?", "placeholder on the right of a comparison"),
    c("a = ? AND b = ?", "two placeholders"),
    // ---- ORDER BY ----
    c("a = 1 ORDER BY created_at", "ORDER BY with the direction omitted"),
    c("a = 1 ORDER BY created_at DESC", "explicit DESC"),
    c("a = 1 ORDER BY created_at ASC", "explicit ASC"),
    c("a = 1 ORDER BY created_at desc", "lowercase direction keyword"),
    c("a = 1 ORDER BY x ASC, y DESC", "two ORDER BY items with different directions"),
    c("a = 1 order by x", "lowercase ORDER BY keyword"),
    c(
        "a = 1 ORDER BY licensing.territory",
        "dotted path in ORDER BY - the grammar's OrderByItem takes an Identifier, not a PathExpr",
    ),
    // ---- LIMIT ----
    c("a = 1 LIMIT 0", "limit zero, which is not the same as no limit"),
    c("a = 1 LIMIT 10", "ordinary limit"),
    c("a = 1 LIMIT 4294967296", "limit above u32::MAX - the field is u64"),
    c("a = 1 LIMIT 18446744073709551615", "u64::MAX"),
    c("a = 1 LIMIT 18446744073709551616", "u64::MAX + 1"),
    c("a = 1 LIMIT -1", "negative limit - LimitClause takes Unsigned"),
    c("a = 1 ORDER BY x DESC LIMIT 5", "both clauses, in the grammar's order"),
    c("a = 1 LIMIT 5 ORDER BY x", "both clauses in the wrong order"),
    // ---- whitespace and degenerate input ----
    c("", "empty input - parse_selection short-circuits before touching the grammar"),
    c("   ", "whitespace-only input"),
    c("a\n=\n1", "newlines between tokens"),
    c(
        "a\r\n=\r\n1",
        "CRLF line endings - the WHITESPACE rule lists the two-character CRLF sequence",
    ),
    c(
        "a\r=\r1",
        "a lone CR - WHITESPACE lists space, tab, LF and CRLF, but not a bare CR",
    ),
    c("a  =  1", "extra spaces between tokens"),
    c("a = 1;", "trailing semicolon"),
    // ---- syntax rejects ----
    c("name = ", "trailing operator with no right operand"),
    c("= 1", "leading operator with no left operand"),
    c("(a = 1", "unbalanced open parenthesis"),
    c("a = 1)", "unbalanced close parenthesis"),
    c("name = 'unterminated", "unterminated string literal"),
    c("a === 1", "an operator that does not exist"),
];

#[test]
fn test_parse_cases_fixture() {
    let mut accept = Vec::new();
    let mut reject = Vec::new();

    for case in CORPUS {
        match parse_selection(case.query) {
            Ok(selection) => {
                let mut item = serde_json::Map::new();
                item.insert("query".into(), case.query.into());
                item.insert("note".into(), case.note.into());
                item.insert(
                    "ast_json".into(),
                    serde_json::to_value(&selection).unwrap(),
                );
                item.insert(
                    "ast_bincode_hex".into(),
                    to_hex(&bincode::serialize(&selection).unwrap()).into(),
                );
                match generate_selection_sql(&selection.predicate, None) {
                    Ok(sql) => {
                        item.insert("predicate_sql".into(), sql.into());
                        item.insert("predicate_sql_error".into(), serde_json::Value::Null);
                    }
                    Err(e) => {
                        item.insert("predicate_sql".into(), serde_json::Value::Null);
                        item.insert("predicate_sql_error".into(), e.to_string().into());
                    }
                }
                item.insert("roundtrip_sql".into(), format!("{}", selection).into());
                accept.push(serde_json::Value::Object(item));
            }
            Err(err) => {
                let mut item = serde_json::Map::new();
                item.insert("query".into(), case.query.into());
                item.insert("note".into(), case.note.into());
                item.insert("error_variant".into(), error_variant(&err).into());
                item.insert(
                    "error_message".into(),
                    match error_message(&err) {
                        Some(m) => m.into(),
                        None => serde_json::Value::Null,
                    },
                );
                reject.push(serde_json::Value::Object(item));
            }
        }
    }

    // Every accepted case must survive a bincode round-trip. This is not part of the
    // fixture; it is a guard that the hex we publish really is the tree we published.
    for item in &accept {
        let hex = item["ast_bincode_hex"].as_str().unwrap();
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let decoded: ankql::ast::Selection = bincode::deserialize(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            item["ast_json"],
            "bincode round-trip diverged for query {}",
            item["query"]
        );
    }

    let mut root = serde_json::Map::new();
    root.insert("fixture".into(), "parse_cases.json".into());
    root.insert("encoding".into(), ENCODING.into());
    root.insert(
        "produced_by".into(),
        "ankql::parser::parse_selection, ankql::selection::sql::generate_selection_sql, \
         and Display for ankql::ast::Selection"
            .into(),
    );
    root.insert("accept_count".into(), accept.len().into());
    root.insert("reject_count".into(), reject.len().into());
    root.insert("accept".into(), serde_json::Value::Array(accept));
    root.insert("reject".into(), serde_json::Value::Array(reject));

    let mut text = serde_json::to_string_pretty(&serde_json::Value::Object(root)).unwrap();
    text.push('\n');
    check_or_write_bytes("parse_cases.json", text.as_bytes());
}
