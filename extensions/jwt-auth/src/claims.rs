use crate::error::AuthError;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// JWT claims carried in every token issued by the auth system.
///
/// Note: `sub` is stored in the standard JWT `subject` field (not in custom claims)
/// to avoid serde conflicts with `jwt_simple`'s `#[serde(flatten)]` on custom claims.
/// The `sub` field here is populated by `SigningKeys::verify()` from the standard claim.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JwtClaims {
    /// User entity ID (ankurah EntityId serialized as string).
    /// When signing, this is placed in the standard JWT `sub` claim.
    /// When deserializing custom claims, this is skipped (populated from standard claims).
    #[serde(skip)]
    pub sub: String,

    /// User's roles (e.g. ["Admin"], ["Dispatcher", "Technician"])
    pub roles: Vec<String>,

    /// User's email
    pub email: String,

    /// User's display name
    #[serde(default)]
    pub name: Option<String>,

    /// Arbitrary custom claims from the identity provider.
    /// Captures any JSON fields not explicitly defined above.
    #[serde(flatten)]
    pub custom: serde_json::Map<String, serde_json::Value>,
}

/// Total equality is honest: `serde_json` cannot represent NaN or
/// infinity (parsing rejects them and `Number::from_f64` refuses them),
/// so the reflexivity hole in `Value`'s `PartialEq` is unreachable.
impl Eq for JwtClaims {}

/// Full-value hash agreeing with `Eq` (the `ContextData` contract).
/// `serde_json::Value` implements no `Hash`, so the `custom` map is
/// hashed by a recursive walk — objects in sorted key order, because
/// map equality is order-independent.
impl Hash for JwtClaims {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.sub.hash(state);
        self.roles.hash(state);
        self.email.hash(state);
        self.name.hash(state);
        self.custom.len().hash(state);
        let mut keys: Vec<&String> = self.custom.keys().collect();
        keys.sort();
        for key in keys {
            key.hash(state);
            hash_json_value(&self.custom[key], state);
        }
    }
}

/// Hash a JSON value such that values equal under `serde_json`'s own
/// `PartialEq` hash equal: arm-tagged, arrays in order, objects in
/// sorted key order. Numbers hash by their canonical arm (unsigned,
/// signed, then float bits), mirroring `serde_json::Number` equality,
/// which never compares across arms.
fn hash_json_value<H: Hasher>(value: &serde_json::Value, state: &mut H) {
    use serde_json::Value;
    match value {
        Value::Null => 0u8.hash(state),
        Value::Bool(b) => {
            1u8.hash(state);
            b.hash(state);
        }
        Value::Number(n) => {
            2u8.hash(state);
            if let Some(u) = n.as_u64() {
                0u8.hash(state);
                u.hash(state);
            } else if let Some(i) = n.as_i64() {
                1u8.hash(state);
                i.hash(state);
            } else {
                // serde_json number equality is IEEE (-0.0 == 0.0), so
                // the hash must not split zero by sign bit.
                2u8.hash(state);
                let float = n.as_f64().expect("a serde_json Number is one of u64/i64/f64");
                let float = if float == 0.0 { 0.0 } else { float };
                float.to_bits().hash(state);
            }
        }
        Value::String(s) => {
            3u8.hash(state);
            s.hash(state);
        }
        Value::Array(items) => {
            4u8.hash(state);
            items.len().hash(state);
            for item in items {
                hash_json_value(item, state);
            }
        }
        Value::Object(map) => {
            5u8.hash(state);
            map.len().hash(state);
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for key in keys {
                key.hash(state);
                hash_json_value(&map[key], state);
            }
        }
    }
}

/// Parse a JWT token without verifying the signature.
/// Extracts claims from the payload section (base64url-decoded).
/// Useful on clients that only need to read claims without access to the signing key.
pub fn parse_claims_unverified(token: &str) -> Result<JwtClaims, AuthError> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(AuthError::Verification("invalid JWT format: expected 3 dot-separated segments".into()));
    }

    let payload_bytes =
        URL_SAFE_NO_PAD.decode(parts[1]).map_err(|e| AuthError::Verification(format!("failed to decode JWT payload: {e}")))?;

    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::Verification(format!("failed to parse JWT payload JSON: {e}")))?;

    let sub = payload
        .get("sub")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AuthError::Verification("missing 'sub' claim in JWT payload".into()))?
        .to_string();

    let roles = payload
        .get("roles")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let email = payload.get("email").and_then(|v| v.as_str()).unwrap_or_default().to_string();

    let name = payload.get("name").and_then(|v| v.as_str()).map(String::from);

    // Extract custom claims: all fields not in the known set
    let known_keys: &[&str] = &["sub", "roles", "email", "name", "iat", "exp", "nbf", "iss", "aud", "jti"];
    let custom = if let Some(obj) = payload.as_object() {
        obj.iter().filter(|(k, _)| !known_keys.contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect()
    } else {
        serde_json::Map::new()
    };

    Ok(JwtClaims { sub, roles, email, name, custom })
}
