# ankql parser fixtures

`parse_cases.json` records what `ankql::parser::parse_selection` does to a corpus of
query strings. It exists so a port can reimplement the parser against observed
behaviour rather than against SQL intuition — and the two differ in several places that
matter.

The AST fixtures in `proto/test_fixtures/ankql_ast.bin` pin how an AST *encodes*. They
say nothing about which AST a given query produces. This file is the other half.

## Regenerating

```bash
OVERWRITE_FIXTURES=1 cargo test -p ankql --test parser_fixtures
```

Without the flag the same command verifies and fails on drift. The corpus lives in
`ankql/tests/parser_fixtures.rs`; each case is classified into `accept` or `reject` by
what the parser actually returns, not by what the test predicts, so a change that flips
a query from one to the other shows up as a fixture diff.

`bincode` was added to `ankql`'s **dev**-dependencies for this file (same 1.3.3 the
proto and core crates already use). Nothing else changed.

## Schema

```json
{
  "fixture": "parse_cases.json",
  "encoding": "bincode 1.3 `serialize` defaults: …",
  "produced_by": "…",
  "accept_count": 74,
  "reject_count": 31,
  "accept": [ … ],
  "reject": [ … ]
}
```

### `accept[]`

| field | meaning |
| --- | --- |
| `query` | the input string, verbatim |
| `note` | what this case is probing |
| `ast_json` | `serde_json` of the resulting `Selection`. The tree the parser built. |
| `ast_bincode_hex` | that same `Selection` bincode-encoded, lowercase hex. A port that builds a semantically-equal but structurally-different tree matches `ast_json` loosely and fails here. |
| `predicate_sql` | `generate_selection_sql(&selection.predicate, None)` on success, else `null` |
| `predicate_sql_error` | the `SqlGenerationError` text when the above fails, else `null` |
| `roundtrip_sql` | `format!("{}", selection)` — the predicate SQL plus `ORDER BY` and `LIMIT`. **Not always valid SQL**: `Display for Predicate` swallows generation errors and prints `"SQL Error: …"` in their place, which is what the placeholder cases show. |

### `reject[]`

| field | meaning |
| --- | --- |
| `query` | the input string |
| `note` | what this case is probing |
| `error_variant` | the `ParseError` variant name |
| `error_message` | the error text, or `null` for `SyntaxError` |

`error_message` is `null` for `SyntaxError` on purpose. That variant wraps pest's
rendered diagnostic — a multi-line block echoing the input with a caret and a list of
expected rule names. That text is pest's, changes with the pest version, and no port
could reproduce it; pinning it would make the fixture brittle and teach a consumer
nothing. The variant itself still carries real information, and it is the distinction
worth implementing: **`SyntaxError` means the grammar refused the text; every other
variant means the grammar accepted it and AST construction then refused it.** The
corpus contains both kinds, and a port should land on the same side of that line.

## Behaviour worth knowing before porting

Everything below was found by running the corpus, not by reading the grammar. Each one
has at least one fixture case.

### Integer literals

**The parser never produces `Literal::I16`.** `n = 32767` yields `I32(32767)`.

**`2147483647` is `I64`, but `2147483646` is `I32`.** `parse_number` chooses the width
with strict inequalities:

```rust
if num < i32::MAX as i64 && num > i32::MIN as i64 { … I32 … }
```

so `i32::MAX` itself falls through to `I64`. This is an off-by-one at exactly the
boundary a port is most likely to implement as `<=`. Cases `n = 2147483646`,
`n = 2147483647` and `n = 2147483648` pin all three.

`n = 9223372036854775808` (`i64::MAX + 1`) is an `InvalidPredicate`, not a syntax error:
the grammar matches the digits and `parse::<i64>()` then fails.

### Number forms that do not parse at all

`n = -1`, `n = +1`, `n = 1.5` and `n = 1e3` all **reject**. The grammar has `Integer`,
`Decimal` and `Double` rules, but `parse_atomic_expr` has arms only for `Unsigned`, so
they surface as `UnexpectedRule { expected: "atomic expression", got: Integer | Decimal
| Double }`. Consequently **`Literal::F64` is unreachable from query text** — it only
enters the AST through the preparation pass or through code that builds literals
directly.

`flag = NULL` rejects the same way (`got: Null`): there is no null literal in the AST.

### Operators the parser cannot build

- **`BETWEEN` rejects** (`UnexpectedRule { got: Between }`). The grammar has the rule and
  `ComparisonOperator::Between` exists, but `create_comparison` has no arm for it, so
  that variant is unreachable from query text.
- **All arithmetic rejects.** `qty * 3 = 9`, `a + 1 = 2` and `a - 1 = 2` fail with
  `got: Multiply | Add | Subtract`. **`Expr::InfixExpr` and all four `InfixOperator`
  variants are unreachable from query text.**
- **`NOT` only works over parentheses, and only in the leading position.**
  `NOT (a = 1)` parses; `NOT a = 1` rejects with
  `UnexpectedRule { expected: "ExpressionInParentheses", got: PathExpr }`. See the next
  section for the three further ways `NOT` misbehaves.

### `NOT` drops the rest of the expression

`parse_expr` handles a leading `UnaryNot` with an early `return`, before the loop that
consumes the remaining infix operators. So everything after the negated operand is
**silently discarded**:

> `NOT (a = 1) AND b = 2` parses to `Not(a = 1)`. The `AND b = 2` is gone — no error, no
> warning. **Byte-identity check: its `ast_bincode_hex` is identical to the bare
> `NOT (a = 1)` case.** A port that keeps the conjunct will differ.

`NOT` in any position other than the front is a hard reject: `a = 1 AND NOT (b = 2)`
fails with `UnexpectedRule { expected: "atomic expression", got: UnaryNot }`, because
only `ExprAtomValue`'s own leading `UnaryNot*` is reachable from that branch.

### `NotFlag` has no word boundary

`And` and `Or` are guarded (`@{ ^"and" ~ !IDENT_CONT }`); `NotFlag` is not
(`NotFlag = { ^"not" }`). So the first three letters of an ordinary identifier are lexed
as the `NOT` operator and the remainder as a path:

| query | read as | result |
| --- | --- | --- |
| `nothing = 1` | `NOT hing = 1` | `UnexpectedRule { expected: "ExpressionInParentheses", got: PathExpr }` |
| `not_field = 1` | `NOT _field = 1` | the same error |

**Identity check: both produce the exact same `error_variant` and `error_message` as
`NOT a = 1`** — which is the tell that they went down the unary-NOT path rather than
being rejected as bad syntax. No identifier may begin with `not`.

### `NOT IN` silently becomes `IN`

The grammar is `In = { NotFlag? ~ ^"in" }` — the optional `NOT` is consumed inside the
`In` rule and never reaches the AST builder. `status NOT IN ('a', 'b')` produces an AST
**byte-identical** to `status IN ('a', 'b')`, and renders back as `"status" IN ('a', 'b')`.
A port that implements `NOT IN` correctly will disagree with this parser.

### Single-element `IN` rejects

`status IN ('a')` fails with `InvalidPredicate("Expression is not a predicate")`, while
`status IN ('a', 'b')` succeeds. `AtomicExpr` tries `ExpressionInParentheses` before
`Row`, so a parenthesized group with no comma is parsed as a grouped expression rather
than a one-element list, and a bare string literal is not a predicate. The comma is what
makes it a list.

### `AND` does not bind tighter than `OR`

Precedence is pure left-to-right, because the grammar's `Expr` is a flat
`atom (op atom)*` and `parse_expr` left-folds it:

| query | resulting tree | what SQL would give |
| --- | --- | --- |
| `a = 1 OR b = 2 AND c = 3` | `And(Or(a=1, b=2), c=3)` | `Or(a=1, And(b=2, c=3))` |
| `a = 1 AND b = 2 OR c = 3` | `Or(And(a=1, b=2), c=3)` | same, by luck of ordering |
| `a = 1 AND b = 2 AND c = 3` | `And(And(a=1, b=2), c=3)` | left-associative, agrees |

The first row is the one that diverges. Note also that
`a = 1 OR b = 2 AND c = 3` and `(a = 1 OR b = 2) AND c = 3` produce the *same* tree and
the same `roundtrip_sql` — so re-rendering is not round-trip-stable in the direction a
reader expects: the printed form is correct for the tree, but the tree is not what the
original text means in SQL.

### `IS NOT NULL` is `Not(IsNull(...))`

There is no dedicated variant. `a IS NOT NULL` renders back as `NOT ("a" IS NULL)`.

`IsNullPostfix` is an ordinary rule rather than an atomic one, so pest applies implicit
whitespace skipping *between* `^"is"` and `^"null"` — and skipping zero whitespace is
allowed. **`a isnull` therefore parses, and its `ast_bincode_hex` is identical to
`a IS NULL`.** The space is optional, not required.

### Identifiers

- Case is preserved verbatim; `MixedCase` stays `MixedCase`.
- `-` is a legal identifier character, so `kebab-case = 1` is one path step named
  `kebab-case`, not a subtraction. (Which is also why `a - 1 = 2` is a `Subtract`
  rejection rather than an identifier: the space matters.)
- Cyrillic parses (`имя = 1`) because the grammar lists `'А'..'Я'` and `'а'..'я'`
  explicitly. **CJK does not**: `名前 = 1` is a `SyntaxError`. The identifier character
  set is ASCII letters, `-`, `_`, digits after the first character, and Cyrillic.
- **A `Reserved` word cannot be a column name, but only at a boundary.**
  `IdentifierInner`'s negative lookahead is `!(Reserved ~ ("(" | WHITESPACE | "," | EOF))`,
  so it fires only when the reserved word is *followed* by one of those. `option = 1`
  and `left = 1` are `SyntaxError`s; `optional = 1` and `leftish = 1` parse as ordinary
  identifiers, because the next character is a letter rather than a boundary.
  **`limit = 1` and `order = 1` also parse**, even though both words appear in
  `Reserved` — they get there only through the whole `LimitClause` / `OrderByClause`
  rules, which need more than the bare keyword to match, so the lookahead never fires on
  the word alone.
- **A double-quoted identifier keeps its quotes.** `"quoted" = 1` produces a path step
  whose text is `"quoted"` — quotes included — and SQL generation then emits
  `""quoted"" = 1`. Recorded as-is.

### Strings

- `name = ''` gives the empty string.
- **`name = 'it''s'` rejects.** SQL's doubled-quote escape is not supported by the
  grammar. There is no escape mechanism for a quote inside a string literal.
- Non-ASCII string *contents* are fine (`'café 日本語 🚀 مرحبا'`); only *identifiers* are
  restricted.
- A reserved word inside quotes is just text: `name = 'ORDER BY'` parses.
- **Four quotes is a legal literal whose content is two quote characters.**
  `AtomicExpr` tries `OnlyQuotesSequence = ("'" ~ "'")+` before
  `AnythingButQuotesSequence`, so `''''` matches as one token; `parse_string_literal`
  then strips the outermost character from each end, leaving `''`. **Identity check: it
  is *not* byte-identical to `name = ''`** — that one yields the empty string. The
  rendered SQL is `"name" = ''''''`, six quotes, because the generator re-escapes each
  of the two content quotes and adds the surrounding pair. This is the only way to get a
  quote character into a string literal, and it does not behave like SQL's escape: the
  quotes stay in the value rather than collapsing.

### ORDER BY

- **The direction defaults to `Asc`** when omitted: `ORDER BY created_at` yields
  `direction: "Asc"` and renders back as `ORDER BY created_at ASC`. The AST has no way
  to express "unspecified".
- Keyword and direction are case-insensitive (`order by x`, `... desc`).
- **Dotted paths reject.** `ORDER BY licensing.territory` is a `SyntaxError`:
  `OrderByItem` takes an `Identifier`, not a `PathExpr`, so the `.` is left unconsumed
  and `EOI` fails. (`parse_order_by_item` also contains an explicit
  `InvalidPredicate("Dotted identifiers are not supported in ORDER BY clauses")` check,
  but the grammar rejects first, so that branch is unreachable.)

### LIMIT

- `LIMIT 0` is a real limit of zero and is not the same as no limit — `limit: 0` versus
  `limit: null`.
- The field is `u64`: `LIMIT 4294967296` and `LIMIT 18446744073709551615` both parse.
  `LIMIT 18446744073709551616` is an `InvalidPredicate`.
- `LIMIT -1` is a `SyntaxError` — the clause takes `Unsigned`.
- **Clause order is fixed.** `... ORDER BY x DESC LIMIT 5` parses; `... LIMIT 5 ORDER BY x`
  is a `SyntaxError`.

### Degenerate input

- **Empty and whitespace-only input yield `Predicate::True`**, with no `ORDER BY` and no
  `LIMIT`. `parse_selection` short-circuits before invoking the grammar at all.
- A trailing semicolon **rejects**: `a = 1;` is a `SyntaxError`, despite the grammar
  defining an `EOF = { EOI | ";" }` rule (which is only used inside a negative lookahead
  in `IdentifierInner` and never terminates a selection).
- **A lone carriage return is not whitespace.** `WHITESPACE` is
  `{ " " | "\t" | "\n" | "\r\n" }` — space, tab, LF and the two-character CRLF sequence,
  with no bare `\r` alternative. So `a\r\n=\r\n1` parses and `a\r=\r1` is a
  `SyntaxError`. **Byte-identity check: the CRLF form is identical to `a\n=\n1` and to
  `a  =  1`** — all three produce the same `ast_bincode_hex`. A port that normalizes line
  endings before lexing, or that treats `\r` as generic whitespace, will accept input
  this parser rejects.

### Placeholders

`?` and `a = ?` parse. SQL generation then *fails* for them —
`SqlGenerationError::InvalidExpression("Placeholder must be transformed before SQL
generation")` — so those cases carry `predicate_sql: null`, the message in
`predicate_sql_error`, and a `roundtrip_sql` that reads `"SQL Error: …"` because
`Display for Predicate` prints the error rather than propagating it.

## Not covered

- The `wasm` feature's `TryFrom<JsValue> for Expr` conversion, which maps JS `null` to
  the sentinel string `"NULL_IMPROBABLE_VALUE"`. It needs a wasm target to exercise.
- `Selection::assume_null` and `Selection::referenced_columns`, which transform an
  already-parsed AST rather than parse text. They are worth their own fixtures if the
  port implements them.
- Deliberate error *positions*. See the `error_message` note above.
