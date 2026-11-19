//! Per-team, short-lived, request-bound control credentials.
//!
//! The main API alone holds the Ed25519 private key. The executor receives a public verifier,
//! so compromising an executor cannot mint control credentials or authenticate to the main API.
//! Claims bind the actor, team, release, generation, exact request and current configuration.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use aws_lc_rs::pkcs8::Document;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Control protocol, independent of the public API's normal HS256 login tokens.
pub const PROTOCOL: u32 = 1;
/// Maximum lifetime of a newly issued control credential.
pub const TOKEN_SECONDS: i64 = 30;
/// Bounded clock skew when accepting an issuance time.
pub const CLOCK_SKEW_SECONDS: i64 = 5;
/// Bound parsing before allocating or verifying a hostile token.
pub const MAX_TOKEN_BYTES: usize = 32 * 1024;
/// Maximum live replay records per executor (full caches refuse rather than evict live records).
pub const MAX_REPLAYS: usize = 4096;
const HEADER: &[u8] = br#"{"alg":"EdDSA","typ":"mrsm-executor+jwt"}"#;

/// The identity the main API has authenticated and authorized for this operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Actor {
    /// Canonical owner label: a username or `token:<id>`, selected by the main API.
    pub owner: String,
    /// The token's owning user, when any.
    pub username: Option<String>,
    /// Effective permissions, already intersected with current user and token scopes.
    pub permissions: BTreeSet<String>,
    /// Whether the caller is a full administrator of the main API.
    pub administrator: bool,
}

impl Actor {
    fn validate(&self) -> Result<(), ControlError> {
        let valid = |value: &str, max| {
            !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
        };
        if !valid(&self.owner, 256)
            || self.username.as_deref().is_some_and(|s| !valid(s, 256))
            || self.permissions.len() > 128
            || self.permissions.iter().any(|s| !valid(s, 256))
        {
            return Err(ControlError::Claims);
        }
        Ok(())
    }
}

/// One authenticated operation, including the configuration fingerprint used for its scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Claims {
    /// Control protocol version.
    pub protocol: u32,
    /// Exact Meerschaum release of the main API.
    pub version: String,
    /// The main API's stable identity.
    pub issuer: Uuid,
    /// The one team this executor serves.
    pub team_id: Uuid,
    /// Credential generation, changed when an executor is replaced or revoked.
    pub generation: Uuid,
    /// Issued at, Unix seconds.
    pub issued_at: i64,
    /// Expires at, Unix seconds.
    pub expires_at: i64,
    /// Single-use request id.
    pub nonce: Uuid,
    /// Uppercase HTTP method.
    pub method: String,
    /// Exact encoded path and query, excluding scheme/authority.
    pub path: String,
    /// SHA-256 of the request body.
    pub body_sha256: String,
    /// SHA-256 fingerprint of the current filtered configuration and grants.
    pub configuration_sha256: String,
    /// The main API's caller identity.
    pub actor: Actor,
}

/// Request bytes to which a control credential is bound.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    /// Uppercase HTTP method.
    pub method: &'a str,
    /// Exact encoded path and query.
    pub path: &'a str,
    /// Complete body, verified before performing an operation.
    pub body: &'a [u8],
}

impl Request<'_> {
    fn validate(&self) -> Result<(), ControlError> {
        if self.method.is_empty()
            || self.method.len() > 10
            || !self.method.bytes().all(|b| b.is_ascii_uppercase())
            || !self.path.starts_with('/')
            || self.path.starts_with("//")
            || self.path.len() > 4096
            || self.path.contains('#')
            || self.path.chars().any(char::is_control)
        {
            return Err(ControlError::Request);
        }
        Ok(())
    }
}

/// Failures do not include credentials, signatures, request bodies or configuration values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ControlError {
    /// Invalid private/public key material.
    #[error("Invalid executor control key.")]
    Key,
    /// Wrong framing, token type or encoding.
    #[error("Invalid executor control credential.")]
    Token,
    /// Signature does not match this executor's pinned public key.
    #[error("Executor control signature verification failed.")]
    Signature,
    /// Unsupported protocol, release, team, issuer or generation.
    #[error("Executor control identity, generation or version does not match.")]
    Identity,
    /// Invalid claim values.
    #[error("Invalid executor control claims.")]
    Claims,
    /// Expired, premature, or overly long credential lifetime.
    #[error("Executor control credential is expired or not yet valid.")]
    Time,
    /// Request bytes differ from the authorized operation.
    #[error("Executor control credential does not authorize this request.")]
    Request,
    /// Replayed request.
    #[error("Executor control credential has already been used.")]
    Replay,
    /// Refuse new requests while all replay slots are live.
    #[error("Executor control request capacity is exhausted; retry shortly.")]
    Capacity,
}

/// Main-API signing authority for one team. Debug output never prints key material.
pub struct Authority {
    key: Ed25519KeyPair,
    issuer: Uuid,
    team_id: Uuid,
    generation: Uuid,
}

impl std::fmt::Debug for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authority")
            .field("issuer", &self.issuer)
            .field("team_id", &self.team_id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl Authority {
    /// Generate an authority and its private PKCS#8 document. Persist the document only in the
    /// main API's owner-only secret storage; an executor gets only [`Self::verifier`].
    pub fn generate(
        issuer: Uuid,
        team_id: Uuid,
        generation: Uuid,
    ) -> Result<(Self, Document), ControlError> {
        let document =
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).map_err(|_| ControlError::Key)?;
        let authority = Self::from_pkcs8(issuer, team_id, generation, document.as_ref())?;
        Ok((authority, document))
    }

    /// Restore the main API's private key without generating another credential.
    pub fn from_pkcs8(
        issuer: Uuid,
        team_id: Uuid,
        generation: Uuid,
        bytes: &[u8],
    ) -> Result<Self, ControlError> {
        Ok(Self {
            key: Ed25519KeyPair::from_pkcs8(bytes).map_err(|_| ControlError::Key)?,
            issuer,
            team_id,
            generation,
        })
    }

    /// Public bootstrap material, safe to deliver to the one executor it names.
    pub fn verifier(&self) -> Verifier {
        Verifier {
            protocol: PROTOCOL,
            version: env!("CARGO_PKG_VERSION").into(),
            issuer: self.issuer,
            team_id: self.team_id,
            generation: self.generation,
            public_key: URL_SAFE_NO_PAD.encode(self.key.public_key().as_ref()),
        }
    }

    /// Sign an operation using the caller's current permissions and filtered configuration.
    pub fn issue(
        &self,
        actor: Actor,
        request: Request<'_>,
        configuration_sha256: &str,
        now: i64,
    ) -> Result<String, ControlError> {
        request.validate()?;
        actor.validate()?;
        if !valid_digest(configuration_sha256) {
            return Err(ControlError::Claims);
        }
        let claims = Claims {
            protocol: PROTOCOL,
            version: env!("CARGO_PKG_VERSION").into(),
            issuer: self.issuer,
            team_id: self.team_id,
            generation: self.generation,
            issued_at: now,
            expires_at: now.checked_add(TOKEN_SECONDS).ok_or(ControlError::Time)?,
            nonce: Uuid::new_v4(),
            method: request.method.into(),
            path: request.path.into(),
            body_sha256: body_digest(request.body),
            configuration_sha256: configuration_sha256.into(),
            actor,
        };
        let payload = serde_json::to_vec(&claims).map_err(|_| ControlError::Claims)?;
        let input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(HEADER),
            URL_SAFE_NO_PAD.encode(payload)
        );
        let token = format!(
            "{input}.{}",
            URL_SAFE_NO_PAD.encode(self.key.sign(input.as_bytes()).as_ref())
        );
        if token.len() > MAX_TOKEN_BYTES {
            return Err(ControlError::Claims);
        }
        Ok(token)
    }
}

/// The only control key material in an executor: a pinned public key and expected identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verifier {
    /// Control protocol version.
    pub protocol: u32,
    /// Exact release required at both ends.
    pub version: String,
    /// Main API identity.
    pub issuer: Uuid,
    /// The one team served.
    pub team_id: Uuid,
    /// Current credential generation.
    pub generation: Uuid,
    /// Base64url-encoded Ed25519 public key.
    pub public_key: String,
}

impl Verifier {
    /// Check bootstrap material before listening for operations.
    pub fn validate(&self) -> Result<(), ControlError> {
        if self.protocol != PROTOCOL || self.version != env!("CARGO_PKG_VERSION") {
            return Err(ControlError::Identity);
        }
        if URL_SAFE_NO_PAD
            .decode(&self.public_key)
            .map_err(|_| ControlError::Key)?
            .len()
            != 32
        {
            return Err(ControlError::Key);
        }
        Ok(())
    }

    /// Authenticate before any mutation, consuming a nonce only after every check succeeds.
    pub fn authenticate(
        &self,
        token: &str,
        request: Request<'_>,
        now: i64,
        replays: &ReplayCache,
    ) -> Result<Claims, ControlError> {
        self.authenticate_head(token, request.method, request.path, now)?
            .finish(request.body, now, replays)
    }

    /// Verify the signature and request target before buffering an authenticated body. This
    /// does not admit an operation: [`AuthenticatedHead::finish`] must verify the complete body
    /// and consume the nonce before any routing or mutation.
    pub fn authenticate_head(
        &self,
        token: &str,
        method: &str,
        path: &str,
        now: i64,
    ) -> Result<AuthenticatedHead, ControlError> {
        self.validate()?;
        let request = Request {
            method,
            path,
            body: &[],
        };
        request.validate()?;
        if token.len() > MAX_TOKEN_BYTES {
            return Err(ControlError::Token);
        }
        let parts: Vec<_> = token.split('.').collect();
        if parts.len() != 3 || parts[0] != URL_SAFE_NO_PAD.encode(HEADER) {
            return Err(ControlError::Token);
        }
        let key = URL_SAFE_NO_PAD
            .decode(&self.public_key)
            .map_err(|_| ControlError::Key)?;
        let signature = URL_SAFE_NO_PAD
            .decode(parts[2])
            .map_err(|_| ControlError::Token)?;
        let input = format!("{}.{}", parts[0], parts[1]);
        UnparsedPublicKey::new(&ED25519, key)
            .verify(input.as_bytes(), &signature)
            .map_err(|_| ControlError::Signature)?;
        let payload = URL_SAFE_NO_PAD
            .decode(parts[1])
            .map_err(|_| ControlError::Token)?;
        let claims: Claims = serde_json::from_slice(&payload).map_err(|_| ControlError::Claims)?;
        if claims.protocol != self.protocol
            || claims.version != self.version
            || claims.issuer != self.issuer
            || claims.team_id != self.team_id
            || claims.generation != self.generation
        {
            return Err(ControlError::Identity);
        }
        if claims.expires_at <= now
            || claims.issued_at > now.saturating_add(CLOCK_SKEW_SECONDS)
            || claims.issued_at > claims.expires_at
            || claims.expires_at.saturating_sub(claims.issued_at) > TOKEN_SECONDS
        {
            return Err(ControlError::Time);
        }
        claims.actor.validate()?;
        if !valid_digest(&claims.configuration_sha256) {
            return Err(ControlError::Claims);
        }
        if claims.method != request.method || claims.path != request.path {
            return Err(ControlError::Request);
        }
        Ok(AuthenticatedHead(claims))
    }
}

/// Signature-verified headers. The private field prevents callers manufacturing admission.
pub struct AuthenticatedHead(Claims);

impl AuthenticatedHead {
    /// Finish authentication with the complete body; refuse credentials that expired while
    /// receiving it and consume the single-use nonce only after all checks have passed.
    pub fn finish(
        self,
        body: &[u8],
        now: i64,
        replays: &ReplayCache,
    ) -> Result<Claims, ControlError> {
        let claims = self.0;
        if claims.expires_at <= now {
            return Err(ControlError::Time);
        }
        if claims.body_sha256 != body_digest(body) {
            return Err(ControlError::Request);
        }
        replays.consume(claims.nonce, claims.expires_at, now)?;
        Ok(claims)
    }
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// SHA-256 for request/configuration binding, with no secret key material.
pub fn body_digest(body: &[u8]) -> String {
    Sha256::digest(body)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A bounded replay guard. It never evicts a still-valid nonce to make room for another.
pub struct ReplayCache {
    entries: Mutex<HashMap<Uuid, i64>>,
    capacity: usize,
}

impl Default for ReplayCache {
    fn default() -> Self {
        Self::new(MAX_REPLAYS)
    }
}

impl ReplayCache {
    /// Bound the cache to at least one and at most [`MAX_REPLAYS`] live operations.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity: capacity.clamp(1, MAX_REPLAYS),
        }
    }

    fn consume(&self, nonce: Uuid, expires_at: i64, now: i64) -> Result<(), ControlError> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        entries.retain(|_, expiry| *expiry > now);
        if entries.contains_key(&nonce) {
            return Err(ControlError::Replay);
        }
        if entries.len() >= self.capacity {
            return Err(ControlError::Capacity);
        }
        entries.insert(nonce, expires_at);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority() -> Authority {
        Authority::generate(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4())
            .unwrap()
            .0
    }

    fn actor() -> Actor {
        Actor {
            owner: "member".into(),
            username: Some("member".into()),
            permissions: BTreeSet::from(["pipes:read".into(), "jobs:execute".into()]),
            administrator: false,
        }
    }

    const REQUEST: Request<'_> = Request {
        method: "POST",
        path: "/executor/operations?stream=true",
        body: b"{\"operation\":\"sync\"}",
    };

    #[test]
    fn valid_request_preserves_the_authenticated_actor_and_scope() {
        let authority = authority();
        let fingerprint = body_digest(b"filtered configuration");
        let token = authority
            .issue(actor(), REQUEST, &fingerprint, 1000)
            .unwrap();
        let verifier = authority.verifier();
        let claims = verifier
            .authenticate(&token, REQUEST, 1001, &ReplayCache::default())
            .unwrap();
        assert_eq!(claims.actor, actor());
        assert_eq!(claims.team_id, verifier.team_id);
        assert_eq!(claims.configuration_sha256, fingerprint);
        assert_eq!(claims.expires_at - claims.issued_at, TOKEN_SECONDS);
        let json = serde_json::to_string(&verifier).unwrap();
        assert!(!json.contains("private"));
        assert_eq!(
            URL_SAFE_NO_PAD.decode(&verifier.public_key).unwrap().len(),
            32
        );
    }

    #[test]
    fn another_executor_key_or_identity_cannot_accept_the_request() {
        let issuer = authority();
        let token = issuer
            .issue(actor(), REQUEST, &body_digest(b"config"), 1000)
            .unwrap();
        assert_eq!(
            authority()
                .verifier()
                .authenticate(&token, REQUEST, 1001, &ReplayCache::default()),
            Err(ControlError::Signature)
        );
        let mut verifier = issuer.verifier();
        verifier.team_id = Uuid::new_v4();
        assert_eq!(
            verifier.authenticate(&token, REQUEST, 1001, &ReplayCache::default()),
            Err(ControlError::Identity)
        );
        verifier = issuer.verifier();
        verifier.generation = Uuid::new_v4();
        assert_eq!(
            verifier.authenticate(&token, REQUEST, 1001, &ReplayCache::default()),
            Err(ControlError::Identity)
        );
        verifier = issuer.verifier();
        verifier.version = "0.0.0".into();
        assert_eq!(
            verifier.authenticate(&token, REQUEST, 1001, &ReplayCache::default()),
            Err(ControlError::Identity)
        );
    }

    #[test]
    fn method_path_query_and_body_are_bound_before_nonce_consumption() {
        let issuer = authority();
        let token = issuer
            .issue(actor(), REQUEST, &body_digest(b"config"), 1000)
            .unwrap();
        let verifier = issuer.verifier();
        let replays = ReplayCache::default();
        for wrong in [
            Request {
                method: "GET",
                ..REQUEST
            },
            Request {
                path: "/executor/operations",
                ..REQUEST
            },
            Request {
                body: b"changed",
                ..REQUEST
            },
        ] {
            assert_eq!(
                verifier.authenticate(&token, wrong, 1001, &replays),
                Err(ControlError::Request)
            );
        }
        assert!(
            verifier
                .authenticate(&token, REQUEST, 1001, &replays)
                .is_ok()
        );
        assert_eq!(
            verifier.authenticate(&token, REQUEST, 1001, &replays),
            Err(ControlError::Replay)
        );
    }

    #[test]
    fn expiration_prematurity_and_normal_api_tokens_are_refused() {
        let issuer = authority();
        let token = issuer
            .issue(actor(), REQUEST, &body_digest(b"config"), 1000)
            .unwrap();
        let verifier = issuer.verifier();
        assert_eq!(
            verifier.authenticate(&token, REQUEST, 1030, &ReplayCache::default()),
            Err(ControlError::Time)
        );
        assert_eq!(
            verifier.authenticate(&token, REQUEST, 994, &ReplayCache::default()),
            Err(ControlError::Time)
        );
        let ordinary = crate::auth::jwt_encode(&meerschaum_core::Map::new(), b"main API secret");
        assert_eq!(
            verifier.authenticate(&ordinary, REQUEST, 1001, &ReplayCache::default()),
            Err(ControlError::Token)
        );
        let mut claims_token = token.into_bytes();
        let index = claims_token.iter().position(|b| *b == b'.').unwrap() + 10;
        claims_token[index] = if claims_token[index] == b'A' {
            b'B'
        } else {
            b'A'
        };
        assert_eq!(
            verifier.authenticate(
                std::str::from_utf8(&claims_token).unwrap(),
                REQUEST,
                1001,
                &ReplayCache::default()
            ),
            Err(ControlError::Signature)
        );
    }

    #[test]
    fn live_replays_are_not_evicted_at_capacity_and_expired_entries_are_reclaimed() {
        let issuer = authority();
        let verifier = issuer.verifier();
        let replays = ReplayCache::new(1);
        let a = issuer
            .issue(actor(), REQUEST, &body_digest(b"config"), 1000)
            .unwrap();
        let b = issuer
            .issue(actor(), REQUEST, &body_digest(b"config"), 1001)
            .unwrap();
        assert!(verifier.authenticate(&a, REQUEST, 1001, &replays).is_ok());
        assert_eq!(
            verifier.authenticate(&b, REQUEST, 1002, &replays),
            Err(ControlError::Capacity)
        );
        assert_eq!(
            verifier.authenticate(&a, REQUEST, 1002, &replays),
            Err(ControlError::Replay)
        );
        assert!(verifier.authenticate(&b, REQUEST, 1030, &replays).is_ok());
    }

    #[test]
    fn a_persisted_key_restores_the_same_public_identity() {
        let (first, document) =
            Authority::generate(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()).unwrap();
        let restored = Authority::from_pkcs8(
            first.issuer,
            first.team_id,
            first.generation,
            document.as_ref(),
        )
        .unwrap();
        assert_eq!(first.verifier(), restored.verifier());
    }
}
