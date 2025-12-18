## Phase 2: Ref Traversal

### Goal

Enable queries that navigate across entity references, e.g., `artist.name = ?` from an Album to its Artist.

### Prerequisites

- Phase 1 complete (parser understands paths, Json type exists)

**Note**: This phase does NOT require full schema registry. Minimal ref metadata (field → target collection mapping) is sufficient.

### Open Questions (to resolve before starting)

1. **How is `Ref<T>` represented today?** 
   - Current models use `EntityId` directly, not a typed `Ref<T>`
   - Do we need a `Ref<T>` wrapper type, or can we work with `EntityId` + schema metadata?

2. **Storage representation**
   - Is the referenced EntityId stored as a property value?
   - How does `Filterable::value()` return it?

3. **Cross-entity fetch during filtering**
   - Filtering happens per-entity; how do we access referenced entities?
   - Need `Filterable` to gain entity fetch capability, or pass a context

### Tasks

#### 3.1 Ref Type Decision

Choose one:

**Option A**: Add `Ref<T>` wrapper type
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ref<T: Model>(EntityId, PhantomData<T>);

impl<T: Model> Property for Ref<T> {
    fn into_value(&self) -> Option<Value> { Some(Value::EntityId(self.0)) }
    fn from_value(value: Option<Value>) -> Result<Self, PropertyError> { ... }
}
```

**Option B**: Use `EntityId` with schema annotation
```rust
// In model:
#[model(ref_to = "Artist")]
pub artist: EntityId,

// Schema describes it as FieldKind::Ref { target_model: "Artist" }
```

- [ ] Decide approach
- [ ] Implement chosen approach
- [ ] Update example models

#### 3.2 Resolver Module (`ankql/src/resolver.rs`)

- [ ] Create resolver module
- [ ] Implement `resolve_path()` function
- [ ] Walk path steps and populate `resolved` field
- [ ] Handle Ref steps by switching schema context

```rust
pub struct Resolver<'a> {
    registry: &'a SchemaRegistry,
}

impl<'a> Resolver<'a> {
    pub fn resolve_path(
        &self,
        path: &mut PathExpr,
        root_model: &str,
    ) -> Result<(), ResolveError> {
        let mut current_schema = self.registry.get(root_model)
            .ok_or(ResolveError::UnknownModel(root_model.to_string()))?;
        
        for step in &mut path.steps {
            let field = current_schema.fields.iter()
                .find(|f| f.name == step.name)
                .ok_or(ResolveError::UnknownField(step.name.clone()))?;
            
            step.resolved = Some(ResolvedStep {
                property_id: Some(field.property_id),
                kind: match &field.kind {
                    FieldKind::Scalar(_) => StepKind::Scalar,
                    FieldKind::Structured(_) => StepKind::Structured,
                    FieldKind::Ref { target_model } => {
                        current_schema = self.registry.get(target_model)?;
                        StepKind::Ref
                    }
                    FieldKind::MultiRef { target_model } => {
                        current_schema = self.registry.get(target_model)?;
                        StepKind::MultiRef
                    }
                },
                target_model: match &field.kind {
                    FieldKind::Ref { target_model } |
                    FieldKind::MultiRef { target_model } => Some(target_model.clone()),
                    _ => None,
                },
                ..Default::default()
            });
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("unknown model: {0}")]
    UnknownModel(String),
    #[error("unknown field: {0}")]
    UnknownField(String),
    #[error("traversal filter on single-valued step")]
    FilterOnSingleRef,
    #[error("role selection on non-relation step")]
    RoleOnNonRelation,
}
```

#### 3.3 Extended Filterable Trait

- [ ] Add method for cross-entity value resolution
- [ ] Or: add context parameter to `evaluate_predicate`

```rust
// Option: Add entity fetcher to evaluation context
pub struct FilterContext<'a> {
    pub entity_fetcher: &'a dyn Fn(EntityId, &str) -> Option<Box<dyn Filterable>>,
}

pub fn evaluate_predicate_with_context<I: Filterable>(
    item: &I,
    predicate: &Predicate,
    ctx: &FilterContext,
) -> Result<bool, Error>;
```

#### 3.4 Path Evaluation in Filter

- [ ] Update `evaluate_expr()` to handle `Identifier::Path`
- [ ] Implement step-by-step value resolution
- [ ] For Ref steps: fetch referenced entity, continue on it

```rust
fn evaluate_path_expr<I: Filterable>(
    item: &I,
    path: &PathExpr,
    ctx: &FilterContext,
) -> Result<ExprOutput<Value>, Error> {
    let mut current: Box<dyn Filterable> = Box::new(item.clone()); // or ref
    
    for (i, step) in path.steps.iter().enumerate() {
        let is_last = i == path.steps.len() - 1;
        
        match &step.resolved.as_ref().map(|r| &r.kind) {
            Some(StepKind::Scalar) | Some(StepKind::Structured) if is_last => {
                return Ok(ExprOutput::Value(
                    current.value(&step.name)
                        .ok_or(Error::PropertyNotFound(step.name.clone()))?
                ));
            }
            Some(StepKind::Ref) => {
                let entity_id = current.value(&step.name)
                    .and_then(|v| v.as_entity_id())
                    .ok_or(Error::PropertyNotFound(step.name.clone()))?;
                let target_model = step.resolved.as_ref()
                    .and_then(|r| r.target_model.as_ref())
                    .ok_or(Error::UnsupportedExpression("unresolved ref"))?;
                current = (ctx.entity_fetcher)(entity_id, target_model)
                    .ok_or(Error::PropertyNotFound(format!("entity {}", entity_id)))?;
            }
            // MultiRef handled in Phase 5
            _ => return Err(Error::UnsupportedExpression("unresolved path")),
        }
    }
    
    Err(Error::UnsupportedExpression("path did not terminate"))
}
```

#### 3.5 Integration with Node/Context

- [ ] Implement entity fetcher using `Node::get_entity`
- [ ] Wire into predicate evaluation during fetch/query
- [ ] Consider caching fetched entities within a single evaluation

### Tests

```rust
// Requires test models with Ref relationships
#[derive(Model)]
pub struct Artist {
    pub name: String,
}

#[derive(Model)]
pub struct Album {
    pub name: String,
    pub year: String,
    pub artist: EntityId,  // or Ref<Artist>
}

#[tokio::test]
async fn test_ref_traversal_query() {
    let node = setup_node().await;
    let ctx = node.context();
    
    // Create artist
    let trx = ctx.begin();
    let radiohead = trx.create(&Artist { name: "Radiohead".to_string() }).await?;
    trx.commit().await?;
    
    // Create album referencing artist
    let trx = ctx.begin();
    let album = trx.create(&Album {
        name: "OK Computer".to_string(),
        year: "1997".to_string(),
        artist: radiohead.id(),
    }).await?;
    trx.commit().await?;
    
    // Query album by artist name
    let albums: Vec<AlbumView> = ctx.fetch("artist.name = ?", "Radiohead").await?;
    assert_eq!(albums.len(), 1);
    assert_eq!(albums[0].name(), "OK Computer");
}
```

### Acceptance Criteria

1. `Ref<T>` or equivalent is defined and usable in models
2. Schema describes ref relationships
3. Resolver populates path steps with target model info
4. Filter evaluation fetches referenced entities
5. Queries across refs work end-to-end

### Files Modified/Created

- `core/src/property/ref.rs` (new, if Option A)
- `ankql/src/resolver.rs` (new)
- `core/src/selection/filter.rs` (extend)
- `examples/model/src/lib.rs` (add ref examples)
- `tests/tests/ref_query.rs` (new)

### Estimated Effort

3-4 days

### Risks

- **Performance**: Fetching referenced entities during filter evaluation could be slow for large result sets
- **Mitigation**: Phase 1 implementation is correctness-focused; optimize with caching later
- **Circular refs**: Need to handle or detect cycles in ref chains

