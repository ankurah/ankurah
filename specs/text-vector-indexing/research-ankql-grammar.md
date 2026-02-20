# Research: ankQL Grammar, AST, and Operator Pipeline

## Parser Technology

ankQL uses a **PEG parser** via the `pest` crate.

- **Grammar file:** `ankql/src/ankql.pest` (63 lines)
- **Parser struct:** `ankql/src/grammar.rs` (derive wrapper)

## AST Core Types

**`ankql/src/ast.rs`:**

| Type | Lines | Purpose |
|------|-------|---------|
| `Expr` | 24-32 | Literal, Path, Predicate, InfixExpr, ExprList, Placeholder |
| `Literal` | 34-50 | I16, I32, I64, F64, Bool, String, EntityId, Object, Binary, Json |
| `PathExpr` | 52-70 | Dot-separated field paths (e.g., `licensing.territory`) |
| `Predicate` | 133-143 | Comparison, IsNull, And, Or, Not, True, False, Placeholder |
| `ComparisonOperator` | 412-422 | Equal, NotEqual, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, In, Between |
| `Selection` | 76-81 | Top-level: predicate + order_by + limit |

## Existing Operators

Currently supported: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`
Defined but not implemented: `BETWEEN`
Not present: Any text or pattern matching operators

## Operator Pipeline (end-to-end)

1. **Grammar parsing** (`ankql.pest:16-23`) — tokenizes into operator rules
2. **AST parsing** (`parser.rs:107-143`) — `create_comparison()` maps rules to `ComparisonOperator` enum
3. **SQL generation** (`selection/sql.rs:169-180`) — `comparison_op_to_sql()` converts to SQL strings
4. **Runtime evaluation** (`core/src/selection/filter.rs:144-199`) — `evaluate_predicate()` dispatches per operator

## Predicate Evaluation Architecture

Dual-layer system:
- **SQL pushdown**: Each backend (postgres, sqlite) translates predicates to native SQL WHERE clauses
- **Post-filtering**: `evaluate_predicate()` handles whatever can't be pushed down (used by all backends, primary for sled/indexeddb)

## Key Extension Points for New Operators

| Step | File | Function | What to add |
|------|------|----------|-------------|
| Grammar | `ankql/src/ankql.pest:16` | `CmpInfixOp` rule | New operator keywords |
| AST | `ankql/src/ast.rs:412` | `ComparisonOperator` | New enum variants |
| Parser | `ankql/src/parser.rs:128` | `create_comparison()` | New rule → operator mapping |
| SQL | `ankql/src/selection/sql.rs:169` | `comparison_op_to_sql()` | SQL translation per backend |
| Evaluation | `core/src/selection/filter.rs:150` | `evaluate_predicate()` | Runtime matching logic |

## String Handling Notes

- Single-quoted strings only: `'value'`
- Escaping: `'O''Brien'` → `O'Brien` (SQL-style doubled quotes)
- Placeholder support: `?` tokens populated via `Predicate::populate(values)`
- Type casting: implicit numeric family casting in comparisons (i32 ↔ i64 ↔ f64)
- JSON paths: multi-step paths (`licensing.territory`) trigger JSON traversal via `Value::extract_at_path()`
