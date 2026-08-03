use crate::claims::JwtClaims;
use ankurah_core::node::ContextData;
use ankurah_core::policy::AccessDenied;
use ankurah_proto as proto;
use async_trait::async_trait;

/// The context data extracted from a validated JWT, used for all policy checks.
/// `Root` represents a privileged system context that bypasses all RBAC checks.
/// `NoUser` represents an unauthenticated context (e.g. for reading the policy collection).
///
/// Equality and hash are full-value — claims and token — because
/// `ContextData`'s contract is operational identity and `Session` update
/// delivery gates on it: a token refresh compares unequal and
/// re-permissions; an identical update is a no-op.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JwtContext {
    User { claims: JwtClaims, token: String },
    Root,
    NoUser,
}

impl JwtContext {
    pub fn from_claims(claims: JwtClaims, token: String) -> Self { JwtContext::User { claims, token } }

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
}

#[async_trait]
impl ContextData for JwtContext {}
