## Phase 1a: Parser — Basic Dot-Path Navigation

### Goal

Extend AnkQL to parse multi-step dot paths like `licensing.territory`. This is the minimal parser change needed for structured property queries.

**Deferred to later phases:**
- `[filter]` syntax (for multi-valued traversals)
- `->role` syntax (for relation-entities)
- `^Model.field` syntax (for inbound navigation)

### Tasks

#### 1. Grammar Extension (`ankql/src/ankql.pest`)

Replace the current identifier handling:

```pest
// BEFORE:
// IdentifierWithOptionalContinuation = { Identifier ~ (ReferenceContinuation)? }
//     ReferenceContinuation = { "." ~ Identifier }

// AFTER:
PathExpr = { Identifier ~ ("." ~ Identifier)* }
```

Integrate `PathExpr` where `IdentifierWithOptionalContinuation` was used in `AtomicExpr`.

#### 2. AST Types (`ankql/src/ast.rs`)

```rust
// Remove Identifier enum entirely
// pub enum Identifier {
//     Property(String),
//     CollectionProperty(String, String),
// }

/// A dot-separated path like `name` or `licensing.territory`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathExpr {
    pub steps: Vec<String>,
}

impl PathExpr {
    pub fn simple(name: impl Into<String>) -> Self {
        Self { steps: vec![name.into()] }
    }
    
    pub fn is_simple(&self) -> bool {
        self.steps.len() == 1
    }
    
    pub fn first(&self) -> &str {
        &self.steps[0]
    }
}

// Update Expr
pub enum Expr {
    Literal(Literal),
    Path(PathExpr),      // was: Identifier(Identifier)
    Predicate(Predicate),
    InfixExpr { left: Box<Expr>, operator: InfixOperator, right: Box<Expr> },
    ExprList(Vec<Expr>),
    Placeholder,
}

// Update OrderByItem
pub struct OrderByItem {
    pub path: PathExpr,  // was: identifier: Identifier
    pub direction: OrderDirection,
}
```

#### 3. Parser Updates (`ankql/src/parser.rs`)

```rust
fn build_path_expr(pair: pest::iterators::Pair<Rule>) -> Result<PathExpr, ParseError> {
    let steps: Vec<String> = pair
        .into_inner()
        .map(|p| p.as_str().trim().to_string())
        .collect();
    Ok(PathExpr { steps })
}
```

#### 4. Update Downstream Code

All code that matched on `Identifier::Property` / `Identifier::CollectionProperty` needs updating:

| File | Change |
|------|--------|
| `ankql/src/selection/sql.rs` | Use `path.steps.join(".")` or just `path.first()` for single-step |
| `core/src/selection/filter.rs` | Handle multi-step paths (Phase 1c) |
| `core/src/reactor/watcherset.rs` | Use `path.first()` for field watching |
| `storage/common/src/sorting.rs` | Use `path.first()` for sort field |
| `storage/sled/src/collection.rs` | Use `path.first()` |
| `storage/postgres/src/sql_builder.rs` | Use `path.first()` or error on multi-step |
| `derive/src/selection.rs` | Update macro codegen |
| `core/src/value/cast_predicate.rs` | Use `path.first()` |

**Strategy**: For Phase 1a, multi-step paths in contexts that don't support them yet should error clearly: "Multi-step paths not yet supported in ORDER BY" etc.

### Tests

```rust
#[test]
fn test_parse_simple_path() {
    let sel = parse_selection("name = ?").unwrap();
    if let Predicate::Comparison { left, .. } = &sel.predicate {
        if let Expr::Path(path) = left.as_ref() {
            assert_eq!(path.steps, vec!["name"]);
        }
    }
}

#[test]
fn test_parse_dotted_path() {
    let sel = parse_selection("licensing.territory = ?").unwrap();
    if let Predicate::Comparison { left, .. } = &sel.predicate {
        if let Expr::Path(path) = left.as_ref() {
            assert_eq!(path.steps, vec!["licensing", "territory"]);
        }
    }
}

#[test]
fn test_parse_deep_path() {
    let sel = parse_selection("a.b.c.d = ?").unwrap();
    // Should parse to 4-step path
}
```

### Acceptance Criteria

1. Single identifiers parse as single-step `PathExpr`
2. Dotted identifiers parse as multi-step `PathExpr`
3. All existing tests pass (with mechanical updates)
4. Multi-step paths in unsupported contexts produce clear errors

### Estimated Effort

1-2 days

