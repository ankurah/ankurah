use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;

use crate::error::{MutationError, RetrievalError};
use ankurah_proto::{Attested, CollectionId, EntityId, EntityState, Event, EventId, SystemRootProof};

/// Result of the storage-wide atomic system-root claim operation.
///
/// The claim is engine metadata, not a collection entity. It serializes root
/// creation across independent Node/SystemManager instances and, for engines
/// that support it, across processes sharing the same database.
#[derive(Debug, Clone, PartialEq)]
pub enum SystemRootClaim {
    /// This caller atomically installed the candidate root.
    Claimed,
    /// A claim already existed; its root is returned without modification.
    Existing(SystemRootProof),
}

pub fn state_name(name: &str) -> String { format!("{}_state", name) }

pub fn event_name(name: &str) -> String { format!("{}_event", name) }

/// The model-definition id a storage bucket stamps on wire envelopes it
/// reconstructs from stored fragments (#330): well-known system/catalog ids
/// first (answerable stone-cold, which is how the catalog itself warms from
/// storage), then the injected catalog resolver. An error means a
/// user-collection envelope is being reconstructed before the catalog warmed;
/// readiness gating makes that unreachable in steady state, and failing loud
/// beats stamping a wrong id.
pub fn bucket_model_id(
    collection: &CollectionId,
    resolver: Option<&dyn crate::property::PropertyResolver>,
) -> Result<EntityId, RetrievalError> {
    crate::schema::well_known_model_id(collection.as_str())
        .or_else(|| resolver.and_then(|r| r.model_id_for(collection.as_str())))
        .ok_or_else(|| RetrievalError::Other(format!("no model id known for collection '{collection}' (catalog cold?)")))
}

/// Rewrite a resolved selection into an engine's COLUMN SPACE: every property
/// reference (a resolved `Identifier` in the predicate, a name-based order-by
/// path) is translated to the column name the engine's durable id-to-column
/// map assigned, so everything downstream of `fetch_states` -- SQL builders,
/// the index planner, in-memory sort comparators, projected-row lookups --
/// addresses columns by one consistent string.
///
/// This is what makes reads id-addressed (the ratified design): a renamed
/// property still reads its original (sticky) column, a collision-deduped
/// property reads `{name}_{suffix}` rather than silently reading the other
/// lineage's column, and a fallback-named `p_{suffix}` column is fully
/// queryable.
///
/// Translation rules, per reference:
/// - Predicate `Identifier`: `column_of(property_id)`, else keep its display
///   name (a map miss means this engine never materialized that property, so
///   the name matches the column it WOULD create -- and the missing-column
///   handling downstream answers NULL exactly as today). The identifier keeps
///   its property id: the post-filter tier still evaluates by id.
/// - Order-by path (name-only): resolve the name through the injected catalog
///   resolver, then the map, with the same miss rule. The `id`
///   pseudo-property and engine-internal `__`-prefixed fields pass through
///   untouched, as does everything when no resolver is available (a bare
///   engine, or the catalog is gone).
/// - Bare predicate `Path`s get the same order-by treatment: system/catalog
///   collection queries (which never resolve) and engine-internal fields pass
///   through name-addressed, exactly as before.
pub fn selection_to_column_space(
    collection: &str,
    selection: &ankql::ast::Selection,
    resolver: Option<&dyn crate::property::PropertyResolver>,
    column_of: &dyn Fn(&EntityId) -> Option<String>,
) -> ankql::ast::Selection {
    use ankql::ast::{Expr, OrderByItem, Predicate};

    let translate_name = |name: &str| -> Option<String> {
        if name == "id" || name.starts_with("__") {
            return None;
        }
        let id = resolver?.resolve(collection, name)?;
        column_of(&id)
    };

    fn walk_expr(expr: &Expr, translate_id: &dyn Fn(&EntityId, &str) -> String, translate_name: &dyn Fn(&str) -> Option<String>) -> Expr {
        match expr {
            Expr::Identifier(identifier) => {
                let column = translate_id(&EntityId::from_bytes(identifier.property), &identifier.name);
                Expr::Identifier(ankql::ast::Identifier {
                    property: identifier.property,
                    name: column,
                    subpath: identifier.subpath.clone(),
                })
            }
            Expr::Path(path) => match path.steps.split_first() {
                Some((first, rest)) => match translate_name(first) {
                    Some(column) => {
                        let mut steps = Vec::with_capacity(path.steps.len());
                        steps.push(column);
                        steps.extend(rest.iter().cloned());
                        Expr::Path(ankql::ast::PathExpr { steps })
                    }
                    None => expr.clone(),
                },
                None => expr.clone(),
            },
            Expr::ExprList(exprs) => Expr::ExprList(exprs.iter().map(|e| walk_expr(e, translate_id, translate_name)).collect()),
            Expr::Predicate(p) => Expr::Predicate(walk_predicate(p, translate_id, translate_name)),
            Expr::InfixExpr { left, operator, right } => Expr::InfixExpr {
                left: Box::new(walk_expr(left, translate_id, translate_name)),
                operator: operator.clone(),
                right: Box::new(walk_expr(right, translate_id, translate_name)),
            },
            Expr::Literal(_) | Expr::Placeholder => expr.clone(),
        }
    }

    fn walk_predicate(
        predicate: &Predicate,
        translate_id: &dyn Fn(&EntityId, &str) -> String,
        translate_name: &dyn Fn(&str) -> Option<String>,
    ) -> Predicate {
        match predicate {
            Predicate::Comparison { left, operator, right } => Predicate::Comparison {
                left: Box::new(walk_expr(left, translate_id, translate_name)),
                operator: operator.clone(),
                right: Box::new(walk_expr(right, translate_id, translate_name)),
            },
            Predicate::And(a, b) => Predicate::And(
                Box::new(walk_predicate(a, translate_id, translate_name)),
                Box::new(walk_predicate(b, translate_id, translate_name)),
            ),
            Predicate::Or(a, b) => Predicate::Or(
                Box::new(walk_predicate(a, translate_id, translate_name)),
                Box::new(walk_predicate(b, translate_id, translate_name)),
            ),
            Predicate::Not(inner) => Predicate::Not(Box::new(walk_predicate(inner, translate_id, translate_name))),
            Predicate::IsNull(expr) => Predicate::IsNull(Box::new(walk_expr(expr, translate_id, translate_name))),
            Predicate::True | Predicate::False | Predicate::Placeholder => predicate.clone(),
        }
    }

    let translate_id = |id: &EntityId, display: &str| -> String { column_of(id).unwrap_or_else(|| display.to_string()) };

    let order_by = selection.order_by.as_ref().map(|items| {
        items
            .iter()
            .map(|item| match item.path.steps.split_first() {
                Some((first, rest)) => match translate_name(first) {
                    Some(column) => {
                        let mut steps = Vec::with_capacity(item.path.steps.len());
                        steps.push(column);
                        steps.extend(rest.iter().cloned());
                        OrderByItem { path: ankql::ast::PathExpr { steps }, direction: item.direction.clone() }
                    }
                    None => item.clone(),
                },
                None => item.clone(),
            })
            .collect()
    });

    ankql::ast::Selection {
        predicate: walk_predicate(&selection.predicate, &translate_id, &translate_name),
        order_by,
        limit: selection.limit,
    }
}

#[cfg(test)]
mod column_space_tests {
    use super::selection_to_column_space;
    use crate::property::PropertyResolver;
    use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, OrderByItem, OrderDirection, PathExpr, Predicate, Selection};
    use ankurah_proto::EntityId;

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        EntityId::from_bytes(bytes)
    }

    struct OneField {
        name: &'static str,
        id: EntityId,
    }
    impl PropertyResolver for OneField {
        fn resolve(&self, _collection: &str, name: &str) -> Option<EntityId> { (name == self.name).then_some(self.id) }
        fn name_for(&self, id: &EntityId) -> Option<String> { (*id == self.id).then(|| self.name.to_string()) }
    }

    fn title_selection(property: EntityId, order_step: &str) -> Selection {
        Selection {
            predicate: Predicate::Comparison {
                left: Box::new(Expr::Identifier(Identifier { property: property.to_bytes(), name: "title".into(), subpath: vec![] })),
                operator: ComparisonOperator::Equal,
                right: Box::new(Expr::Literal(Literal::String("x".into()))),
            },
            order_by: Some(vec![OrderByItem { path: PathExpr::simple(order_step.to_string()), direction: OrderDirection::Asc }]),
            limit: None,
        }
    }

    fn predicate_column(selection: &Selection) -> String {
        match &selection.predicate {
            Predicate::Comparison { left, .. } => match left.as_ref() {
                Expr::Identifier(identifier) => identifier.name.clone(),
                other => panic!("unexpected left expr {other:?}"),
            },
            other => panic!("unexpected predicate {other:?}"),
        }
    }

    #[test]
    fn identifier_and_order_by_translate_through_the_map() {
        // The collision/rename shape: the map assigned a deduped column.
        let property = id(0x11);
        let resolver = OneField { name: "title", id: property };
        let out = selection_to_column_space("record", &title_selection(property, "title"), Some(&resolver), &|queried| {
            (*queried == property).then(|| "title_IpDQ".to_string())
        });
        assert_eq!(predicate_column(&out), "title_IpDQ", "identifier addresses the assigned column");
        assert_eq!(out.order_by.unwrap()[0].path.first(), "title_IpDQ", "order-by resolves name->id->column");
    }

    #[test]
    fn map_miss_keeps_the_display_name() {
        // Never-materialized property: the name matches the column a write
        // WOULD create, and missing-column handling answers NULL as today.
        let property = id(0x11);
        let resolver = OneField { name: "title", id: property };
        let out = selection_to_column_space("record", &title_selection(property, "title"), Some(&resolver), &|_| None);
        assert_eq!(predicate_column(&out), "title");
        assert_eq!(out.order_by.unwrap()[0].path.first(), "title");
    }

    #[test]
    fn pseudo_and_internal_fields_pass_through() {
        let property = id(0x11);
        let resolver = OneField { name: "title", id: property };
        // `id` pseudo-property and engine-internal `__collection` are never
        // translated, resolver or not.
        let out = selection_to_column_space("record", &title_selection(property, "id"), Some(&resolver), &|_| Some("never".to_string()));
        assert_eq!(out.order_by.unwrap()[0].path.first(), "id");
        let out = selection_to_column_space("record", &title_selection(property, "__collection"), Some(&resolver), &|_| {
            Some("never".to_string())
        });
        assert_eq!(out.order_by.unwrap()[0].path.first(), "__collection");
    }

    #[test]
    fn no_resolver_leaves_names_untouched() {
        // A bare engine (no injected catalog): order-by/path names pass
        // through; identifiers still translate by id (the map needs no names).
        let property = id(0x11);
        let out = selection_to_column_space("record", &title_selection(property, "title"), None, &|queried| {
            (*queried == property).then(|| "title_IpDQ".to_string())
        });
        assert_eq!(predicate_column(&out), "title_IpDQ");
        assert_eq!(out.order_by.unwrap()[0].path.first(), "title");
    }
}

/// Storage-identifier naming policy for the engine-owned durable id-to-name
/// maps (tables from model-definition ids, columns from property-definition
/// ids). Pure string policy: the engine owns the map and its persistence;
/// these helpers only pick names. The invariants they encode (ratified on
/// #289/#305):
///
/// - Names are SEEDS, ids are identity: a friendly name comes from the catalog
///   resolver at materialization time, correctness never depends on it (reads
///   translate ids through the engine's map).
/// - Collisions dedupe as `{name}_{suffix}` where the suffix is the trailing
///   4+ characters of an injective base32 encoding of the full definition id,
///   widening to all 256 bits until unique.
/// - An id whose name the resolver cannot supply falls back to a synthetic
///   `{prefix}_{suffix}` name from the id alone -- the belt-and-suspenders
///   net for the intra-node descriptor race. Callers log loudly when it
///   fires; with descriptor shipping it should effectively never fire.
pub mod naming {
    use ankurah_proto::EntityId;

    // PostgreSQL's shortest cross-engine identifier limit. Staying within it
    // avoids silent server-side truncation turning two names into one.
    const MAX_IDENTIFIER_LEN: usize = 63;
    // Synthetic fully-qualified names live here. `sanitize` prevents a
    // catalog display name from entering the namespace unqualified.
    const QUALIFIED_PREFIX: &str = "__a_";

    /// Sanitize a display name into a storage identifier seed: every char
    /// outside `[A-Za-z0-9_]` becomes `_`, and a leading digit is prefixed
    /// with `_`. Engines still quote identifiers; this exists so the same
    /// display name yields the same column name on every engine (and so the
    /// postgres/sqlite `sane_name` whitelist accepts it).
    pub fn sanitize(name: &str) -> String {
        let mut out: String = name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
        if out.is_empty() || out.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            out.insert(0, '_');
        }
        if out.starts_with(QUALIFIED_PREFIX) {
            out.insert(0, '_');
        }
        out.truncate(MAX_IDENTIFIER_LEN);
        out
    }

    /// Identifier-safe RFC 4648 base32 (lowercase, no padding). Unlike
    /// filtered base64, this is injective over all 32 id bytes.
    fn id_token(id: &EntityId) -> String {
        const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
        let mut out = String::with_capacity(52);
        let mut accumulator = 0u16;
        let mut bits = 0u8;
        for byte in id.as_bytes() {
            accumulator = (accumulator << 8) | u16::from(*byte);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                out.push(ALPHABET[((accumulator >> bits) & 0x1f) as usize] as char);
                accumulator &= (1u16 << bits) - 1;
            }
        }
        if bits != 0 {
            out.push(ALPHABET[((accumulator << (5 - bits)) & 0x1f) as usize] as char);
        }
        debug_assert_eq!(out.len(), 52);
        out
    }

    /// The trailing `len` characters of the injective id token.
    fn id_suffix(id: &EntityId, len: usize) -> String {
        let token = id_token(id);
        token[token.len().saturating_sub(len)..].to_owned()
    }

    fn suffixed(seed: &str, id: &EntityId, len: usize) -> String {
        let suffix = id_suffix(id, len);
        let seed_len = seed.len().min(MAX_IDENTIFIER_LEN - 1 - suffix.len());
        format!("{}_{}", &seed[..seed_len], suffix)
    }

    fn fully_qualified(kind: &str, id: &EntityId) -> String {
        let kind_len = kind.len().min(4);
        format!("{QUALIFIED_PREFIX}{}_{}", &kind[..kind_len], id_token(id))
    }

    /// Pick the storage name for `desired` (an already-[`sanitize`]d seed):
    /// `desired` itself when free, else `{desired}_{trailing 4+ of id}`,
    /// widening the suffix until `is_taken` clears. `is_taken` closes over the
    /// engine's map and reserved names (fixed columns like `id`,
    /// `state_buffer`, `head`, `attestations` count as taken).
    pub fn dedupe(desired: &str, id: &EntityId, is_taken: impl Fn(&str) -> bool) -> String {
        if !is_taken(desired) {
            return desired.to_string();
        }
        for len in 4..=52 {
            let candidate = suffixed(desired, id, len);
            if !is_taken(&candidate) {
                return candidate;
            }
        }
        // A plain or previously shortened name can theoretically occupy every
        // friendly candidate. The reserved namespace plus the complete token
        // is injective by id, so correctly formed maps cannot collide here.
        fully_qualified("d", id)
    }

    /// Synthetic name for an id the resolver cannot name: `{prefix}_{suffix}`
    /// through the same widening dedup. `prefix` is `p` for properties, `m`
    /// for models, keeping the fallback visibly synthetic in raw data.
    pub fn fallback(prefix: &str, id: &EntityId, is_taken: impl Fn(&str) -> bool) -> String {
        for len in 4..=52 {
            let candidate = suffixed(prefix, id, len);
            if !is_taken(&candidate) {
                return candidate;
            }
        }
        fully_qualified(prefix, id)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn id(byte: u8) -> EntityId {
            let mut bytes = [0u8; 32];
            bytes[0] = byte;
            bytes[31] = byte.wrapping_add(1);
            EntityId::from_bytes(bytes)
        }

        #[test]
        fn sanitize_maps_invalid_chars_and_leading_digit() {
            assert_eq!(sanitize("title"), "title");
            assert_eq!(sanitize("my-field.x"), "my_field_x");
            assert_eq!(sanitize("9lives"), "_9lives");
            assert_eq!(sanitize(""), "_");
            assert_eq!(sanitize("__a_reserved"), "___a_reserved");
            assert_eq!(sanitize(&"x".repeat(80)).len(), MAX_IDENTIFIER_LEN);
        }

        #[test]
        fn dedupe_returns_desired_when_free() {
            assert_eq!(dedupe("title", &id(1), |_| false), "title");
        }

        #[test]
        fn dedupe_suffixes_with_trailing_id_chars_and_widens() {
            let property = id(1);
            let suffix4 = id_suffix(&property, 4);
            // Plain name taken -> first widening step.
            assert_eq!(dedupe("title", &property, |n| n == "title"), format!("title_{suffix4}"));
            // That taken too -> widens to 5.
            let candidate4 = format!("title_{suffix4}");
            let five = dedupe("title", &property, |n| n == "title" || n == candidate4);
            assert!(five.starts_with("title_") && five.len() > format!("title_{suffix4}").len());
        }

        #[test]
        fn long_common_suffixes_widen_to_an_injective_full_id_token() {
            let first = EntityId::from_bytes([0; 32]);
            let mut second_bytes = [0; 32];
            second_bytes[0] = 1;
            let second = EntityId::from_bytes(second_bytes);
            assert_eq!(id_token(&first).len(), 52);
            assert_ne!(id_token(&first), id_token(&second));

            let mut taken = std::collections::HashSet::from(["title".to_owned()]);
            for len in 4..=52 {
                taken.insert(suffixed("title", &first, len));
            }
            let assigned = dedupe("title", &second, |candidate| taken.contains(candidate));
            assert!(!taken.contains(&assigned));
            assert!(assigned.len() <= MAX_IDENTIFIER_LEN);
        }

        #[test]
        fn exhausted_friendly_names_use_the_reserved_full_id_namespace() {
            let assigned = fallback("p", &id(9), |candidate| !candidate.starts_with(QUALIFIED_PREFIX));
            assert!(assigned.starts_with("__a_p_"));
            assert!(assigned.len() <= MAX_IDENTIFIER_LEN);
        }

        #[test]
        fn fallback_is_visibly_synthetic() {
            let name = fallback("p", &id(7), |_| false);
            assert!(name.starts_with("p_") && name.len() >= 6, "got {name}");
        }
    }
}

#[async_trait]
pub trait StorageEngine: Send + Sync {
    type Value;
    // Opens and/or creates a storage collection.
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError>;
    // Delete all collections and their data from the storage engine
    async fn delete_all_collections(&self) -> Result<bool, MutationError>;

    /// Atomically install `candidate` iff no system-root claim exists.
    async fn claim_system_root(&self, candidate: &SystemRootProof) -> Result<SystemRootClaim, MutationError>;

    /// Read the currently persisted system-root claim, if any.
    async fn system_root_claim(&self) -> Result<Option<SystemRootProof>, RetrievalError>;

    /// Atomically remove the claim only when it still equals `expected`.
    /// Used to clean up a failed create without deleting a winner installed by
    /// another caller.
    async fn release_system_root_claim(&self, expected: &SystemRootProof) -> Result<bool, MutationError>;

    /// List the collections that already have durable storage, WITHOUT
    /// creating any. Used by the catalog manager to warm only the catalog
    /// collections that actually exist, so a schema-less node never
    /// materializes empty `_ankurah_*` trees. The default returns empty,
    /// which is always safe: a caller then simply skips its existence-gated
    /// warm (falling back to live subscription updates) rather than
    /// misbehaving. Engines override this with their real list.
    async fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> { Ok(Vec::new()) }

    /// Inject the catalog's property resolver. Engines that maintain
    /// human-named materialized structures (postgres/sqlite/indexeddb columns,
    /// sled property slots) seed their DURABLE id-to-name maps from it at
    /// materialization time; the maps stay engine-owned, the resolver is only
    /// the name source. Called once from `Node` construction, after the
    /// catalog exists -- the engine object is constructed before the node, so
    /// this cannot be a constructor argument. Weak: the engine must not keep
    /// the catalog (and thus the node) alive. Default no-op for engines with
    /// no human-named structures (memory/test engines).
    fn set_property_resolver(&self, resolver: std::sync::Weak<dyn crate::property::PropertyResolver>) { let _ = resolver; }
}

#[async_trait]
pub trait StorageCollection: Send + Sync {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError>;
    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError>;

    // Fetch raw entity states matching a selection (predicate + order by + limit)
    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    async fn set_states(&self, states: Vec<Attested<EntityState>>) -> Result<(), MutationError> {
        for state in states {
            self.set_state(state).await?;
        }
        Ok(())
    }

    async fn get_states(&self, ids: Vec<EntityId>) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let mut states = Vec::new();
        for id in ids {
            match self.get_state(id).await {
                Ok(state) => states.push(state),
                Err(RetrievalError::EntityNotFound(_)) => {
                    warn!("Entity not found: {:?}", id);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(states)
    }

    async fn add_event(&self, entity_event: &Attested<Event>) -> Result<bool, MutationError>;

    /// Retrieve a list of events
    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError>;

    /// Retrieve all events from the collection
    async fn dump_entity_events(&self, id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError>;
}

/// Manages the storage and state of the collection without any knowledge of the model type
#[derive(Clone)]
pub struct StorageCollectionWrapper(pub(crate) Arc<dyn StorageCollection>);

/// Storage interface for a collection
impl StorageCollectionWrapper {
    pub fn new(bucket: Arc<dyn StorageCollection>) -> Self { Self(bucket) }
}

impl std::ops::Deref for StorageCollectionWrapper {
    type Target = Arc<dyn StorageCollection>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
