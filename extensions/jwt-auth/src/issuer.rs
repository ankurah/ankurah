use crate::{AuthError, JwtClaims};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use jwt_simple::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration as StdDuration,
};

const CURRENT_DESCRIPTOR_VERSION: u32 = 1;
#[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
const ISSUER_HTTP_CONNECT_TIMEOUT: StdDuration = StdDuration::from_secs(5);
#[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
const ISSUER_HTTP_REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);
#[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
const UNKNOWN_KID_REFRESH_COOLDOWN: StdDuration = StdDuration::from_secs(30);

#[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
struct RefreshGate {
    pending: std::sync::atomic::AtomicBool,
    next_miss_refresh: std::sync::Mutex<std::time::Instant>,
}

#[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
impl RefreshGate {
    fn new() -> Self {
        Self { pending: std::sync::atomic::AtomicBool::new(false), next_miss_refresh: std::sync::Mutex::new(std::time::Instant::now()) }
    }

    fn try_start_miss(&self) -> bool {
        use std::sync::atomic::Ordering;
        if self.pending.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return false;
        }
        let next = *self.next_miss_refresh.lock().unwrap_or_else(|e| e.into_inner());
        if std::time::Instant::now() < next {
            self.pending.store(false, Ordering::Release);
            return false;
        }
        true
    }

    fn try_start_periodic(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.pending.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }

    fn finish(&self) {
        use std::sync::atomic::Ordering;
        *self.next_miss_refresh.lock().unwrap_or_else(|e| e.into_inner()) = std::time::Instant::now() + UNKNOWN_KID_REFRESH_COOLDOWN;
        self.pending.store(false, Ordering::Release);
    }

    fn cancel(&self) { self.pending.store(false, std::sync::atomic::Ordering::Release); }
}

/// Serializable issuer trust root distributed alongside the authorization policy.
///
/// `jwks_uri` is the value resolved from the issuer's discovery document. Durable
/// verifiers should use [`IssuerVerifier::discover`] rather than constructing this
/// descriptor with a pinned JWKS URL.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuerTrustDescriptor {
    pub version: u32,
    pub issuer: String,
    pub jwks_uri: String,
    pub required_audiences: HashSet<String>,
    pub required_typ: String,
    pub required_token_use: String,
    pub default_roles: Vec<String>,
    #[serde(with = "duration_seconds")]
    pub refresh_ttl: StdDuration,
    #[serde(with = "duration_seconds")]
    pub time_tolerance: StdDuration,
}

impl IssuerTrustDescriptor {
    pub fn validate(&self) -> Result<(), AuthError> {
        if self.version != CURRENT_DESCRIPTOR_VERSION {
            return Err(AuthError::IssuerTrust(format!("unsupported issuer trust descriptor version {}", self.version)));
        }
        validate_https_url("issuer", &self.issuer)?;
        validate_https_url("jwks_uri", &self.jwks_uri)?;
        if self.required_audiences.is_empty() {
            return Err(AuthError::IssuerTrust("required_audiences must not be empty".into()));
        }
        if self.required_typ.is_empty() || self.required_token_use.is_empty() {
            return Err(AuthError::IssuerTrust("required_typ and required_token_use must not be empty".into()));
        }
        if self.default_roles.is_empty() || self.default_roles.iter().any(String::is_empty) {
            return Err(AuthError::IssuerTrust("default_roles must contain at least one non-empty role".into()));
        }
        if self.refresh_ttl.is_zero() {
            return Err(AuthError::IssuerTrust("refresh_ttl must be greater than zero".into()));
        }
        Ok(())
    }
}

mod duration_seconds {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(value.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        Ok(Duration::from_secs(u64::deserialize(deserializer)?))
    }
}

#[derive(Clone)]
pub struct IssuerVerifier {
    descriptor: IssuerTrustDescriptor,
    keys: Arc<RwLock<HashMap<String, RS256PublicKey>>>,
    #[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
    refresh_tx: Option<tokio::sync::mpsc::Sender<()>>,
    #[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
    refresh_gate: Arc<RefreshGate>,
}

impl IssuerVerifier {
    pub fn descriptor(&self) -> &IssuerTrustDescriptor { &self.descriptor }

    /// Construct a verifier from an injected JWKS snapshot. This is intended
    /// for deterministic/offline environments; production durable agents
    /// should resolve trust with [`Self::discover`].
    pub fn from_jwks_json(descriptor: IssuerTrustDescriptor, jwks_json: &str) -> Result<Self, AuthError> {
        descriptor.validate()?;
        let document = serde_json::from_str(jwks_json).map_err(|e| AuthError::KeyParsing(e.to_string()))?;
        let keys = Arc::new(RwLock::new(parse_jwks(document)?));
        Ok(Self {
            descriptor,
            keys,
            #[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
            refresh_tx: None,
            #[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
            refresh_gate: Arc::new(RefreshGate::new()),
        })
    }

    /// Atomically install a newly parsed non-empty JWKS snapshot, retaining
    /// the current keys if parsing or validation fails.
    pub fn replace_jwks_json(&self, jwks_json: &str) -> Result<(), AuthError> {
        let document = serde_json::from_str(jwks_json).map_err(|e| AuthError::KeyParsing(e.to_string()))?;
        let new_keys = parse_jwks(document)?;
        *self.keys.write().unwrap_or_else(|e| e.into_inner()) = new_keys;
        Ok(())
    }

    /// Verify from a snapshot of the cached key selected by the untrusted `kid`.
    /// This method never waits for network I/O. An unknown `kid` schedules one
    /// bounded, coalesced refresh and rejects the current request.
    pub fn verify(&self, token: &str) -> Result<JwtClaims, AuthError> {
        let metadata = Token::decode_metadata(token).map_err(|e| AuthError::Verification(e.to_string()))?;
        if metadata.algorithm() != "RS256" {
            return Err(AuthError::Verification("JWT alg must be RS256".into()));
        }
        if metadata.critical().is_some() {
            return Err(AuthError::Verification("JWT crit headers are not supported".into()));
        }
        let kid =
            metadata.key_id().filter(|kid| !kid.is_empty()).ok_or_else(|| AuthError::Verification("missing JWT kid header".into()))?;
        if metadata.signature_type() != Some(self.descriptor.required_typ.as_str()) {
            return Err(AuthError::Verification(format!("JWT typ must be {}", self.descriptor.required_typ)));
        }

        let key = self.keys.read().unwrap_or_else(|e| e.into_inner()).get(kid).cloned();
        let Some(key) = key else {
            self.trigger_refresh();
            return Err(AuthError::Verification(format!("unknown JWT kid: {kid}")));
        };

        let options = VerificationOptions {
            required_key_id: Some(kid.to_string()),
            required_signature_type: Some(self.descriptor.required_typ.clone()),
            allowed_issuers: Some(HashSet::from([self.descriptor.issuer.clone()])),
            allowed_audiences: Some(self.descriptor.required_audiences.clone()),
            time_tolerance: Some(Duration::from_secs(self.descriptor.time_tolerance.as_secs())),
            ..Default::default()
        };
        let result = key.verify_token::<JwtClaims>(token, Some(options)).map_err(|e| AuthError::Verification(e.to_string()))?;
        if result.expires_at.is_none() {
            return Err(AuthError::Verification("missing expiration claim".into()));
        }
        let sub = result.subject.ok_or_else(|| AuthError::Verification("missing subject claim".into()))?;
        let mut claims = result.custom;
        match claims.custom.get("token_use").and_then(serde_json::Value::as_str) {
            Some(value) if value == self.descriptor.required_token_use => {}
            _ => return Err(AuthError::Verification(format!("token_use must be {}", self.descriptor.required_token_use))),
        }
        claims.sub = sub;
        claims.roles = self.descriptor.default_roles.clone();
        Ok(claims)
    }

    fn trigger_refresh(&self) {
        #[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
        if let Some(refresh_tx) = &self.refresh_tx {
            if self.refresh_gate.try_start_miss() {
                match refresh_tx.try_send(()) {
                    Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                        self.refresh_gate.cancel();
                    }
                }
            }
        }
    }

    #[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
    pub async fn discover(
        discovery_uri: &str,
        issuer: impl Into<String>,
        required_audiences: HashSet<String>,
        default_roles: Vec<String>,
        refresh_ttl: StdDuration,
    ) -> Result<Self, AuthError> {
        let issuer = issuer.into();
        validate_https_url("discovery_uri", discovery_uri)?;
        validate_https_url("issuer", &issuer)?;
        if required_audiences.is_empty() {
            return Err(AuthError::IssuerTrust("required_audiences must not be empty".into()));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(ISSUER_HTTP_CONNECT_TIMEOUT)
            .timeout(ISSUER_HTTP_REQUEST_TIMEOUT)
            .build()
            .map_err(|e| AuthError::IssuerHttp(e.to_string()))?;
        let discovery: DiscoveryDocument = fetch_json(&client, discovery_uri).await?;
        if discovery.issuer != issuer {
            return Err(AuthError::IssuerTrust(format!("discovery issuer mismatch: expected {issuer}, got {}", discovery.issuer)));
        }
        let descriptor = IssuerTrustDescriptor {
            version: CURRENT_DESCRIPTOR_VERSION,
            issuer,
            jwks_uri: discovery.jwks_uri,
            required_audiences,
            required_typ: "at+jwt".into(),
            required_token_use: "access".into(),
            default_roles,
            refresh_ttl,
            time_tolerance: StdDuration::from_secs(60),
        };
        descriptor.validate()?;
        let initial = fetch_jwks(&client, &descriptor.jwks_uri).await?;
        let keys = Arc::new(RwLock::new(initial));
        let (refresh_tx, refresh_rx) = tokio::sync::mpsc::channel(1);
        let refresh_gate = Arc::new(RefreshGate::new());
        spawn_refresher(client, descriptor.clone(), Arc::clone(&keys), refresh_rx, Arc::clone(&refresh_gate));
        Ok(Self { descriptor, keys, refresh_tx: Some(refresh_tx), refresh_gate })
    }
}

#[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
#[derive(Deserialize)]
struct DiscoveryDocument {
    issuer: String,
    jwks_uri: String,
}

#[derive(Deserialize)]
struct JwksDocument {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kid: Option<String>,
    kty: String,
    alg: Option<String>,
    #[serde(rename = "use")]
    key_use: Option<String>,
    n: Option<String>,
    e: Option<String>,
}

fn parse_jwks(document: JwksDocument) -> Result<HashMap<String, RS256PublicKey>, AuthError> {
    let mut result = HashMap::new();
    for jwk in document.keys {
        if jwk.kty != "RSA" || jwk.alg.as_deref().is_some_and(|alg| alg != "RS256") || jwk.key_use.as_deref().is_some_and(|u| u != "sig") {
            continue;
        }
        let (Some(kid), Some(n), Some(e)) = (jwk.kid, jwk.n, jwk.e) else { continue };
        if kid.is_empty() || result.contains_key(&kid) {
            return Err(AuthError::IssuerTrust("JWKS contains an empty or duplicate kid".into()));
        }
        let n = URL_SAFE_NO_PAD.decode(n).map_err(|e| AuthError::KeyParsing(format!("invalid JWKS modulus: {e}")))?;
        let e = URL_SAFE_NO_PAD.decode(e).map_err(|e| AuthError::KeyParsing(format!("invalid JWKS exponent: {e}")))?;
        let significant = n.iter().skip_while(|byte| **byte == 0).copied().collect::<Vec<_>>();
        let modulus_bits = significant.first().map(|first| significant.len() * 8 - first.leading_zeros() as usize).unwrap_or(0);
        if modulus_bits < 2048 {
            return Err(AuthError::KeyParsing(format!("RSA modulus for kid {kid} is only {modulus_bits} bits")));
        }
        let key = RS256PublicKey::from_components(&n, &e).map_err(|e| AuthError::KeyParsing(e.to_string()))?;
        result.insert(kid, key);
    }
    if result.is_empty() {
        return Err(AuthError::IssuerTrust("JWKS contains no usable RS256 signing keys".into()));
    }
    Ok(result)
}

fn validate_https_url(field: &str, value: &str) -> Result<(), AuthError> {
    let parsed = url::Url::parse(value).map_err(|e| AuthError::IssuerTrust(format!("invalid {field}: {e}")))?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(AuthError::IssuerTrust(format!(
            "{field} must be an HTTPS URL with a host and without credentials, query, or fragment"
        )));
    }
    Ok(())
}

#[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
async fn fetch_json<T: serde::de::DeserializeOwned>(client: &reqwest::Client, uri: &str) -> Result<T, AuthError> {
    let response = client.get(uri).send().await.map_err(|e| AuthError::IssuerHttp(e.to_string()))?;
    if !response.status().is_success() {
        return Err(AuthError::IssuerHttp(format!("issuer endpoint returned HTTP {}", response.status())));
    }
    response.json().await.map_err(|e| AuthError::IssuerHttp(e.to_string()))
}

#[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
async fn fetch_jwks(client: &reqwest::Client, uri: &str) -> Result<HashMap<String, RS256PublicKey>, AuthError> {
    parse_jwks(fetch_json(client, uri).await?)
}

#[cfg(all(feature = "issuer-http", not(target_arch = "wasm32")))]
fn spawn_refresher(
    client: reqwest::Client,
    descriptor: IssuerTrustDescriptor,
    keys: Arc<RwLock<HashMap<String, RS256PublicKey>>>,
    mut refresh_rx: tokio::sync::mpsc::Receiver<()>,
    refresh_gate: Arc<RefreshGate>,
) {
    ankurah_core::task::spawn(async move {
        let mut interval = tokio::time::interval(descriptor.refresh_ttl);
        interval.tick().await;
        loop {
            let should_refresh = tokio::select! {
                _ = interval.tick() => refresh_gate.try_start_periodic(),
                value = refresh_rx.recv() => {
                    if value.is_none() { break }
                    true
                },
            };
            if !should_refresh {
                continue;
            }
            match fetch_jwks(&client, &descriptor.jwks_uri).await {
                Ok(new_keys) => *keys.write().unwrap_or_else(|e| e.into_inner()) = new_keys,
                Err(error) => tracing::warn!(%error, "issuer JWKS refresh failed; retaining last good keys"),
            }
            refresh_gate.finish();
        }
    });
}

#[cfg(all(test, feature = "issuer-http", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn repeated_unknown_kid_triggers_are_bounded_and_coalesced() {
        let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::channel(1);
        let refresh_gate = Arc::new(RefreshGate::new());
        let verifier = IssuerVerifier {
            descriptor: IssuerTrustDescriptor {
                version: 1,
                issuer: "https://issuer.example".into(),
                jwks_uri: "https://issuer.example/jwks".into(),
                required_audiences: HashSet::from(["client".into()]),
                required_typ: "at+jwt".into(),
                required_token_use: "access".into(),
                default_roles: vec!["user".into()],
                refresh_ttl: StdDuration::from_secs(900),
                time_tolerance: StdDuration::from_secs(60),
            },
            keys: Arc::new(RwLock::new(HashMap::new())),
            refresh_tx: Some(refresh_tx),
            refresh_gate: Arc::clone(&refresh_gate),
        };
        let unknown_kid = "eyJhbGciOiJSUzI1NiIsImtpZCI6Im5ldy1rZXkiLCJ0eXAiOiJhdCtqd3QifQ.e30.invalid";
        assert!(verifier.verify(unknown_kid).is_err());
        assert!(verifier.verify(unknown_kid).is_err());
        assert!(refresh_rx.try_recv().is_ok());
        assert!(verifier.verify(unknown_kid).is_err());
        assert!(verifier.verify(unknown_kid).is_err());
        assert!(matches!(refresh_rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
        refresh_gate.finish();
        assert!(verifier.verify(unknown_kid).is_err());
        assert!(verifier.verify(unknown_kid).is_err());
        assert!(matches!(refresh_rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn discovery_uri_is_validated_before_network_io() {
        let error = match IssuerVerifier::discover(
            "http://127.0.0.1:1/.well-known/openid-configuration",
            "https://issuer.example",
            HashSet::from(["client".into()]),
            vec!["user".into()],
            StdDuration::from_secs(900),
        )
        .await
        {
            Ok(_) => panic!("insecure discovery URI was accepted"),
            Err(error) => error,
        };
        assert!(matches!(error, AuthError::IssuerTrust(message) if message.contains("discovery_uri must be an HTTPS URL")));
    }
}
