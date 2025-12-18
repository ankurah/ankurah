## Phase 4: Inbound Navigation

### Goal

Enable queries that traverse references in reverse: find entities that are referenced BY others.

Example: Find Artists that have Albums with a specific name.

```rust
ctx.fetch::<ArtistView>("^Album.artist[name = ?]", "OK Computer")
```

### Prerequisites

- Phase 1 complete (parser, Json type, structured queries)
- Phase 2 complete (ref traversal)
- Phase 3 complete (schema registry - needed to validate inbound refs)

**Note**: Inbound navigation requires schema registry to validate that the referenced field actually points to the root model type.

### Semantics

`^Album.artist` means:
- From the current root entity (an Artist)
- Find Album entities whose `artist` field points to this Artist
- The result is a **set** of Albums (always multi-valued)

Combined with filter: `^Album.artist[name = "OK Computer"]`
- Filter the set of Albums to those with name = "OK Computer"
- If ANY such Album exists, the Artist matches

### Tasks

#### 4.1 Resolver: Inbound Step Handling

- [ ] Detect inbound prefix in PathStep
- [ ] Validate source model and field exist
- [ ] Validate field is ref-like (Ref or MultiRef pointing to root model)
- [ ] Set `StepKind::Inbound` and record source model

```rust
// In resolver
if let Some(source_model_name) = &step.inbound_source {
    let source_schema = self.registry.get(source_model_name)
        .ok_or(ResolveError::UnknownModel(source_model_name.clone()))?;
    
    let field = source_schema.fields.iter()
        .find(|f| f.name == step.name)
        .ok_or(ResolveError::UnknownField(step.name.clone()))?;
    
    // Validate field points to current context model
    let target = match &field.kind {
        FieldKind::Ref { target_model } => target_model,
        FieldKind::MultiRef { target_model } => target_model,
        _ => return Err(ResolveError::NotARefField(step.name.clone())),
    };
    
    // Target should match our current context (the root model)
    // ... validation logic
    
    step.resolved = Some(ResolvedStep {
        property_id: Some(field.property_id),
        kind: StepKind::Inbound,
        source_model: Some(source_model_name.clone()),
        target_model: Some(target.clone()),
        ..Default::default()
    });
    
    // Switch context to source model for subsequent steps
    current_schema = source_schema;
}
```

#### 4.2 Inbound Query Evaluation

Two implementation strategies:

**Strategy A: Index-based (preferred)**
- Requires index on `(collection, ref_field, target_entity_id)`
- Lookup: "Albums where artist = <this artist's id>"
- Fast but requires index infrastructure

**Strategy B: Collection scan (fallback)**
- Scan entire source collection
- Filter by ref field pointing to root entity
- Slow but always works

- [ ] Implement Strategy B first for correctness
- [ ] Add index support later as optimization

```rust
fn evaluate_inbound_step(
    root_entity_id: EntityId,
    step: &PathStep,
    ctx: &FilterContext,
) -> Result<Vec<Box<dyn Filterable>>, Error> {
    let source_model = step.resolved.as_ref()
        .and_then(|r| r.source_model.as_ref())
        .ok_or(Error::UnsupportedExpression("unresolved inbound"))?;
    
    let ref_field = &step.name;
    
    // Strategy B: Scan source collection
    let all_sources = (ctx.collection_scanner)(source_model)?;
    
    let matching: Vec<_> = all_sources
        .into_iter()
        .filter(|entity| {
            entity.value(ref_field)
                .and_then(|v| v.as_entity_id())
                .map(|id| id == root_entity_id)
                .unwrap_or(false)
        })
        .collect();
    
    Ok(matching)
}
```

#### 4.3 Traversal Filter on Inbound

- [ ] Apply `[predicate]` filter to inbound result set
- [ ] Use ANY semantics: root matches if any filtered source matches

```rust
fn evaluate_inbound_with_filter(
    root_entity_id: EntityId,
    step: &PathStep,
    remaining_path: &[PathStep],
    ctx: &FilterContext,
) -> Result<bool, Error> {
    let sources = evaluate_inbound_step(root_entity_id, step, ctx)?;
    
    // Apply traversal filter if present
    let filtered: Vec<_> = if let Some(filter) = &step.filter {
        sources.into_iter()
            .filter(|s| evaluate_predicate(s.as_ref(), filter).unwrap_or(false))
            .collect()
    } else {
        sources
    };
    
    if remaining_path.is_empty() {
        // Inbound step is terminal - just check if any exist
        return Ok(!filtered.is_empty());
    }
    
    // Continue evaluating remaining path on filtered sources
    // ANY semantics: return true if any source satisfies remaining path
    for source in filtered {
        if evaluate_remaining_path(source.as_ref(), remaining_path, ctx)? {
            return Ok(true);
        }
    }
    
    Ok(false)
}
```

#### 4.4 FilterContext Extensions

- [ ] Add collection scanner to FilterContext
- [ ] Or: Add to Filterable trait

```rust
pub struct FilterContext<'a> {
    pub entity_fetcher: &'a dyn Fn(EntityId, &str) -> Option<Box<dyn Filterable>>,
    pub collection_scanner: &'a dyn Fn(&str) -> Result<Vec<Box<dyn Filterable>>, Error>,
}
```

#### 4.5 Index Infrastructure (deferred optimization)

- [ ] Design index key format for inbound lookups
- [ ] Integrate with existing indexing (sled_index_plan.md)
- [ ] Fall back to scan when index unavailable

### Tests

```rust
#[tokio::test]
async fn test_inbound_simple() {
    let (node, ctx) = setup().await;
    
    // Create artist
    let trx = ctx.begin();
    let radiohead = trx.create(&Artist { name: "Radiohead".to_string() }).await?;
    let coldplay = trx.create(&Artist { name: "Coldplay".to_string() }).await?;
    trx.commit().await?;
    
    // Create albums
    let trx = ctx.begin();
    trx.create(&Album { name: "OK Computer".to_string(), artist: radiohead.id() }).await?;
    trx.create(&Album { name: "Kid A".to_string(), artist: radiohead.id() }).await?;
    trx.create(&Album { name: "Parachutes".to_string(), artist: coldplay.id() }).await?;
    trx.commit().await?;
    
    // Find artists with album named "OK Computer"
    let artists: Vec<ArtistView> = ctx.fetch("^Album.artist[name = ?]", "OK Computer").await?;
    assert_eq!(artists.len(), 1);
    assert_eq!(artists[0].name(), "Radiohead");
}

#[tokio::test]
async fn test_inbound_no_match() {
    // ... setup ...
    
    // No artist has album named "XYZ"
    let artists: Vec<ArtistView> = ctx.fetch("^Album.artist[name = ?]", "XYZ").await?;
    assert!(artists.is_empty());
}

#[tokio::test]
async fn test_inbound_multiple_matches() {
    // Artist with multiple albums, filter matches multiple
    // Should still return artist once (not duplicated)
}
```

### Acceptance Criteria

1. `^Model.field` syntax parses correctly
2. Resolver validates inbound references
3. Inbound evaluation finds referencing entities
4. Traversal filters work with ANY semantics
5. Integration tests pass

### Files Modified/Created

- `ankql/src/resolver.rs` (extend)
- `core/src/selection/filter.rs` (add inbound evaluation)
- `tests/tests/inbound_query.rs` (new)

### Estimated Effort

3-4 days

### Risks

- **Performance**: Collection scan is O(n) for source collection
- **Mitigation**: Defer index optimization; ensure scan works correctly first
- **Edge cases**: Self-referential models, missing refs, null refs

