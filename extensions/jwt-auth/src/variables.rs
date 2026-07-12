use ankql::ast::{Expr, Literal, Predicate};
use ankurah_core::policy::AccessDenied;
use ankurah_proto::EntityId;

use crate::claims::JwtClaims;

/// Resolve a `$jwt.*` variable reference to its string value from the claims.
fn resolve_variable<'a>(var: &str, claims: &'a JwtClaims) -> Result<String, AccessDenied> {
    match var {
        "$jwt.sub" => Ok(claims.sub.clone()),
        "$jwt.email" => Ok(claims.email.clone()),
        "$jwt.name" => claims.name.clone().ok_or(AccessDenied::ByPolicy("Missing name claim for $jwt.name")),
        _ if var.starts_with("$jwt.custom.") => {
            let field = &var["$jwt.custom.".len()..];
            match claims.custom.get(field) {
                Some(serde_json::Value::String(s)) => Ok(s.clone()),
                Some(_) => Err(AccessDenied::ByPolicy("Non-string custom claim values are not supported in scope filters")),
                None => Err(AccessDenied::ByPolicy("Missing claim for variable")),
            }
        }
        _ => Err(AccessDenied::ByPolicy("Unknown variable")),
    }
}

/// Convert a resolved claim value into a typed literal expression.
///
/// Values that parse as base64 EntityIds become `Literal::EntityId` so that
/// comparisons against Ref fields collate identically to the stored property
/// values in the reactor's watcher index — string literals collate as text
/// there and never match commit-time lookups (ankurah#259). Everything else
/// becomes a String literal.
///
/// This is a value-shape heuristic, not schema knowledge: a scope filter
/// comparing a plain String field against a claim value that happens to parse
/// as an EntityId will collate as EntityId and stop matching (fails closed).
/// Once #259 is fixed at the watcher index, this can return plain String
/// literals unconditionally.
fn typed_expr(value: String) -> Expr {
    match EntityId::from_base64(&value) {
        Ok(id) => Expr::from(&id),
        Err(_) => Expr::Literal(Literal::String(value)),
    }
}

/// Parse a filter string containing `$jwt.*` variables and substitute them
/// with claim values. Returns a `Predicate` ready to AND into the query.
///
/// Each `$jwt.*` token is replaced with a `?` placeholder before parsing, and
/// the resolved claim values are then populated into the parsed AST as
/// literals. Claim values never appear in the parsed query text, so they
/// cannot alter the filter's structure regardless of content (quotes and
/// other AnkQL metacharacters are inert).
///
/// Placeholder/value count mismatches — e.g. a literal `?` in the filter
/// string — fail closed as a parse error.
///
/// Example: `"technician = $jwt.sub"` parses as `technician = ?` and is
/// populated with `claims.sub`, typed per [`typed_expr`].
pub fn parse_and_substitute(filter_str: &str, claims: &JwtClaims) -> Result<Predicate, AccessDenied> {
    let mut values = Vec::new();
    let mut query = String::with_capacity(filter_str.len());
    let mut rest = filter_str;

    // Find each "$jwt." occurrence and extract the full variable token
    // (alphanumeric + dots + underscores after the initial '$').
    while let Some(start) = rest.find("$jwt.") {
        query.push_str(&rest[..start]);
        let tail = &rest[start..];
        let token_len = tail
            .char_indices()
            .skip(1) // skip the '$'
            .find(|(_, c)| !c.is_alphanumeric() && *c != '.' && *c != '_')
            .map(|(i, _)| i)
            .unwrap_or(tail.len());

        values.push(resolve_variable(&tail[..token_len], claims)?);
        query.push('?');
        rest = &tail[token_len..];
    }
    query.push_str(rest);

    let selection = ankql::parser::parse_selection(&query).map_err(AccessDenied::ParseError)?;

    selection.predicate.populate(values.into_iter().map(typed_expr)).map_err(AccessDenied::ParseError)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_claims(sub: &str) -> JwtClaims {
        JwtClaims {
            sub: sub.to_string(),
            roles: vec![],
            email: "test@example.com".to_string(),
            name: Some("Test User".to_string()),
            custom: serde_json::Map::new(),
        }
    }

    fn test_claims_with_custom(sub: &str, key: &str, value: &str) -> JwtClaims {
        let mut custom = serde_json::Map::new();
        custom.insert(key.to_string(), serde_json::Value::String(value.to_string()));
        JwtClaims {
            sub: sub.to_string(),
            roles: vec![],
            email: "test@example.com".to_string(),
            name: Some("Test User".to_string()),
            custom,
        }
    }

    /// Extract the right-hand literal of a single comparison predicate.
    fn rhs_literal(predicate: &Predicate) -> &Literal {
        match predicate {
            Predicate::Comparison { right, .. } => match right.as_ref() {
                Expr::Literal(lit) => lit,
                other => panic!("Expected literal rhs, got {:?}", other),
            },
            other => panic!("Expected comparison, got {:?}", other),
        }
    }

    #[test]
    fn test_substitute_jwt_sub() {
        let claims = test_claims("user123");
        let result = parse_and_substitute("author = $jwt.sub", &claims).unwrap();
        assert_eq!(rhs_literal(&result), &Literal::String("user123".to_string()));
    }

    #[test]
    fn test_substitute_jwt_email() {
        let claims = test_claims("user123");
        let result = parse_and_substitute("contact = $jwt.email", &claims).unwrap();
        assert_eq!(rhs_literal(&result), &Literal::String("test@example.com".to_string()));
    }

    #[test]
    fn test_substitute_jwt_custom() {
        let claims = test_claims_with_custom("user123", "field_office", "NYC");
        let result = parse_and_substitute("office = $jwt.custom.field_office", &claims).unwrap();
        assert_eq!(rhs_literal(&result), &Literal::String("NYC".to_string()));
    }

    #[test]
    fn test_missing_custom_claim() {
        let claims = test_claims("user123");
        let result = parse_and_substitute("office = $jwt.custom.nonexistent", &claims);
        assert!(result.is_err(), "Should fail for missing custom claim");
    }

    #[test]
    fn test_unknown_variable() {
        let claims = test_claims("user123");
        let result = parse_and_substitute("x = $jwt.unknown_field", &claims);
        assert!(result.is_err(), "Should fail for unknown variable");
    }

    #[test]
    fn test_substitute_jwt_name() {
        let claims = test_claims("user123");
        let result = parse_and_substitute("author_name = $jwt.name", &claims).unwrap();
        assert_eq!(rhs_literal(&result), &Literal::String("Test User".to_string()));
    }

    #[test]
    fn test_substitute_jwt_name_missing_fails() {
        let mut claims = test_claims("user123");
        claims.name = None;
        let result = parse_and_substitute("author_name = $jwt.name", &claims);
        assert!(result.is_err(), "Missing name claim should fail-closed");
    }

    #[test]
    fn test_substitute_special_chars_in_sub() {
        // Values with hyphens, underscores, and @ are common in user IDs
        let claims = test_claims("user-123_test@example.com");
        let result = parse_and_substitute("author = $jwt.sub", &claims).unwrap();
        assert_eq!(rhs_literal(&result), &Literal::String("user-123_test@example.com".to_string()));
    }

    #[test]
    fn test_injection_payload_becomes_inert_literal() {
        // Claim values are populated into the parsed AST, never spliced into
        // the query text — metacharacters cannot alter the filter's structure.
        let payload = "'; DROP TABLE posts; --";
        let claims = test_claims(payload);
        let result = parse_and_substitute("author = $jwt.sub", &claims).unwrap();
        assert_eq!(rhs_literal(&result), &Literal::String(payload.to_string()));
    }

    #[test]
    fn test_apostrophe_value_is_inert() {
        // Regression: string splicing previously failed closed on any value
        // containing a quote (AnkQL has no escape syntax). Population makes
        // apostrophes ordinary characters.
        let mut claims = test_claims("user123");
        claims.name = Some("Miles O'Brien".to_string());
        let result = parse_and_substitute("author_name = $jwt.name", &claims).unwrap();
        assert_eq!(rhs_literal(&result), &Literal::String("Miles O'Brien".to_string()));
    }

    #[test]
    fn test_entity_id_value_becomes_typed_literal() {
        // Values that parse as EntityIds must produce typed literals so Ref
        // comparisons collate correctly in the watcher index (ankurah#259).
        let id = ankurah_proto::EntityId::from_bytes([0xA5; 32]);
        let claims = test_claims(&id.to_base64());
        let result = parse_and_substitute("owner = $jwt.sub", &claims).unwrap();
        assert_eq!(rhs_literal(&result), &Literal::EntityId(id.to_bytes()));
    }

    #[test]
    fn test_literal_placeholder_in_filter_fails_closed() {
        // A raw `?` in the filter has no corresponding claim value; the
        // placeholder/value count mismatch must be rejected.
        let claims = test_claims("user123");
        let result = parse_and_substitute("author = $jwt.sub AND status = ?", &claims);
        assert!(result.is_err(), "Literal placeholder in filter must fail-closed");
    }

    #[test]
    fn test_non_string_custom_claim_rejected() {
        let mut claims = test_claims("user123");
        claims.custom.insert("count".to_string(), serde_json::Value::Number(42.into()));
        let result = parse_and_substitute("x = $jwt.custom.count", &claims);
        assert!(result.is_err(), "Non-string custom claim values must be rejected");
    }

    #[test]
    fn test_substitute_multiple_variables() {
        let mut custom = serde_json::Map::new();
        custom.insert("office".to_string(), serde_json::Value::String("NYC".to_string()));
        let claims = JwtClaims { sub: "user123".to_string(), roles: vec![], email: "test@example.com".to_string(), name: None, custom };
        let result = parse_and_substitute("author = $jwt.sub AND office = $jwt.custom.office", &claims).unwrap();
        let display = format!("{}", result);
        assert!(display.contains("user123"), "Expected 'user123' in: {}", display);
        assert!(display.contains("NYC"), "Expected 'NYC' in: {}", display);
    }
}
