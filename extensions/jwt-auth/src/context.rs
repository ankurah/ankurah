use crate::claims::JwtClaims;
use ankurah_core::node::ContextData;
use ankurah_core::policy::AccessDenied;
use ankurah_proto as proto;
use async_trait::async_trait;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime};

/// The context data extracted from a validated JWT, used for all policy checks.
/// `Root` represents a privileged system context that bypasses all RBAC checks.
/// `NoUser` represents an unauthenticated context (e.g. for reading the policy collection).
#[derive(Debug, Clone)]
pub enum JwtContext {
    User { claims: JwtClaims, token: String, expires_at: Option<SystemTime> },
    Root,
    NoUser,
}

impl JwtContext {
    pub fn from_claims(claims: JwtClaims, token: String) -> Self {
        let expires_at = token_expiration(&token);
        JwtContext::User { claims, token, expires_at }
    }

    pub fn system() -> Self { JwtContext::Root }

    pub fn is_privileged(&self) -> bool { matches!(self, JwtContext::Root) }

    /// Returns the raw token as `AuthData` for forwarding to peers.
    /// Root contexts cannot produce auth data (they are local-only).
    pub fn auth_data(&self) -> Result<proto::AuthData, AccessDenied> {
        match self {
            JwtContext::User { token, .. } => Ok(proto::AuthData(token.as_bytes().to_vec())),
            JwtContext::Root => Err(AccessDenied::ByPolicy("Root context cannot be serialized to auth data")),
            JwtContext::NoUser => Ok(proto::AuthData(Vec::new())),
        }
    }

    pub fn user_id(&self) -> Option<&str> {
        match self {
            JwtContext::User { claims, .. } => Some(&claims.sub),
            JwtContext::Root | JwtContext::NoUser => None,
        }
    }

    pub fn roles(&self) -> &[String] {
        match self {
            JwtContext::User { claims, .. } => &claims.roles,
            JwtContext::Root | JwtContext::NoUser => &[],
        }
    }

    pub fn email(&self) -> Option<&str> {
        match self {
            JwtContext::User { claims, .. } => Some(&claims.email),
            JwtContext::Root | JwtContext::NoUser => None,
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            JwtContext::User { claims, .. } => claims.name.as_deref(),
            JwtContext::Root | JwtContext::NoUser => None,
        }
    }

    pub fn token_bytes(&self) -> Option<&[u8]> {
        match self {
            JwtContext::User { token, .. } => Some(token.as_bytes()),
            JwtContext::Root | JwtContext::NoUser => None,
        }
    }

    pub fn expires_at(&self) -> Option<SystemTime> {
        match self {
            JwtContext::User { expires_at, .. } => *expires_at,
            JwtContext::Root | JwtContext::NoUser => None,
        }
    }
}

fn token_expiration(token: &str) -> Option<SystemTime> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    let seconds = claims.get("exp")?.as_u64()?;
    SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(seconds))
}

impl PartialEq for JwtContext {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (JwtContext::User { claims: a, .. }, JwtContext::User { claims: b, .. }) => a.sub == b.sub,
            (JwtContext::Root, JwtContext::Root) => true,
            (JwtContext::NoUser, JwtContext::NoUser) => true,
            _ => false,
        }
    }
}

impl Eq for JwtContext {}

impl Hash for JwtContext {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            JwtContext::User { claims, .. } => claims.sub.hash(state),
            JwtContext::Root | JwtContext::NoUser => {}
        }
    }
}

#[async_trait]
impl ContextData for JwtContext {}
