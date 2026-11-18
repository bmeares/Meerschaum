//! Exact Main-plugin archive capabilities. These credentials never become a Principal.

use std::io::Read;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{callbacks, plugins};
use crate::AppState;
use crate::error::{ApiError, ApiResult};

const SCHEME: &str = "Mrsm-Plugin";
const DOMAIN: &[u8] = b"mrsm-plugin-delivery-v1\0";
const TOKEN_SECONDS: i64 = 30;
const MAX_TOKEN_BYTES: usize = 8192;
pub const MAX_ARCHIVE_BYTES: usize = 32 * 1024 * 1024;
static READS: std::sync::LazyLock<std::sync::Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| std::sync::Arc::new(tokio::sync::Semaphore::new(4)));

struct ArchiveBytes {
    bytes: Vec<u8>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl AsRef<[u8]> for ArchiveBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    authorization: String,
    pub expires_at: i64,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginDeliveryCredential")
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl Credential {
    /// Only for the trusted repository transport or a signed control delivery payload.
    pub fn authorization(&self) -> &str {
        &self.authorization
    }
}

/// A separate signed control payload. Rotating credentials are never configuration or public
/// metadata, so renewing them cannot change the immutable configuration fingerprint.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delivery {
    pub catalog: plugins::Catalog,
    pub repository_origin: String,
    pub credentials: std::collections::BTreeMap<String, Credential>,
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginDelivery")
            .field("team_id", &self.catalog.team_id)
            .field("generation", &self.catalog.generation)
            .finish_non_exhaustive()
    }
}

impl Delivery {
    pub fn validate(
        &self,
        team_id: Uuid,
        generation: Uuid,
        reserved_actions: &std::collections::BTreeSet<String>,
    ) -> Result<plugins::Validated, String> {
        super::environment::validate_callback_url(&self.repository_origin)?;
        let grants = self.catalog.plugins.keys().cloned().collect();
        if self
            .credentials
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            != grants
        {
            return Err("Plugin delivery credentials do not match its exact source set.".into());
        }
        let now = chrono::Utc::now().timestamp();
        for credential in self.credentials.values() {
            if credential.authorization.len() > MAX_TOKEN_BYTES
                || !credential.authorization.starts_with("Mrsm-Plugin ")
                || credential.expires_at <= now
                || credential.expires_at > now.saturating_add(TOKEN_SECONDS + 5)
            {
                return Err("Plugin delivery credentials are expired or invalid.".into());
            }
        }
        plugins::Validated::decode(
            &serde_json::to_vec(&self.catalog).map_err(|_| "Invalid plugin catalog.")?,
            plugins::Expected {
                team_id,
                generation,
                grants: &grants,
                reserved_actions,
            },
        )
        .map_err(|error| error.to_string())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    version: u16,
    release: String,
    grant_id: Uuid,
    launch_id: Uuid,
    team_id: Uuid,
    generation: Uuid,
    plugin: String,
    archive_sha256: String,
    issued_at: i64,
    expires_at: i64,
}

#[derive(Clone)]
struct Authorized {
    credential_hash: [u8; 32],
    claims: Claims,
}

tokio::task_local! { static REQUEST: Authorized; }

fn refused() -> ApiError {
    ApiError::unauthorized(
        "Plugin delivery authority is expired, revoked or invalid.",
        None,
    )
}

async fn eligible(state: &AppState, claims: &Claims) -> Result<(), ApiError> {
    let identity = callbacks::load_identity(state, claims.grant_id, claims.launch_id).await?;
    let caller = identity.caller();
    let team = identity.team.as_ref().ok_or_else(refused)?;
    if identity.grant.team_id != Some(claims.team_id)
        || identity.grant.generation != Some(claims.generation)
        || team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) != Some(claims.team_id)
        || !identity
            .grant
            .source_ceiling
            .as_ref()
            .is_some_and(|ceiling| ceiling.permits_archive(&claims.plugin, &claims.archive_sha256))
        || !team
            .granted_plugins()
            .iter()
            .any(|name| name == &claims.plugin)
        || !identity.grant.required.iter().any(|scope| {
            matches!(
                scope.as_str(),
                "jobs:execute" | "actions:execute" | "pipes:write" | "pipes:register"
            ) || (scope == "plugins:read" && identity.grant.purpose == callbacks::Purpose::Command)
        })
        || !meerschaum_core::access::can_use_plugin(&caller.principal, &claims.plugin)
        || !identity
            .source_access()
            .principal
            .permits_team(Some(&team.name))
    {
        return Err(refused());
    }
    Ok(())
}

/// The trusted Main launch supervisor issues this only after its source/command admission.
/// This reads ACTIVE original proof and captures one exact source, never ambient restart-user
/// permissions or an expanded ordinary callback purpose.
pub async fn issue(
    state: &AppState,
    grant_id: Uuid,
    launch_id: Uuid,
    team_id: Uuid,
    generation: Uuid,
    plugin: &str,
    archive_sha256: &str,
) -> Result<Credential, ApiError> {
    if state.config.no_auth
        || team_id.is_nil()
        || generation.is_nil()
        || !plugins::valid_name(plugin)
        || !plugins::valid_digest(archive_sha256)
    {
        return Err(refused());
    }
    let now = chrono::Utc::now().timestamp();
    let claims = Claims {
        version: 1,
        release: env!("CARGO_PKG_VERSION").into(),
        grant_id,
        launch_id,
        team_id,
        generation,
        plugin: plugin.into(),
        archive_sha256: archive_sha256.into(),
        issued_at: now,
        expires_at: now + TOKEN_SECONDS,
    };
    eligible(state, &claims).await?;
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).map_err(|_| refused())?);
    let mut mac = Hmac::<Sha256>::new_from_slice(state.secret()).map_err(|_| refused())?;
    mac.update(DOMAIN);
    mac.update(payload.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(Credential {
        authorization: format!("{SCHEME} {payload}.{signature}"),
        expires_at: claims.expires_at,
    })
}

/// Recognize the separate credential namespace before any ordinary user/JWT auth helpers.
pub fn recognizes(headers: &HeaderMap) -> bool {
    headers.get_all(header::AUTHORIZATION).iter().any(|value| {
        value
            .to_str()
            .ok()
            .and_then(|value| value.split_ascii_whitespace().next())
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case(SCHEME))
    })
}

fn authorization(headers: &HeaderMap) -> Result<&str, ApiError> {
    if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
        return Err(refused());
    }
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(refused)?;
    let (scheme, token) = authorization.split_once(' ').ok_or_else(refused)?;
    if !scheme.eq_ignore_ascii_case(SCHEME)
        || token.len() > MAX_TOKEN_BYTES
        || token.bytes().any(|c| c.is_ascii_whitespace())
    {
        return Err(refused());
    }
    Ok(authorization)
}

fn decode(state: &AppState, authorization: &str) -> Result<Claims, ApiError> {
    let token = authorization.split_once(' ').ok_or_else(refused)?.1;
    let (payload, signature) = token.split_once('.').ok_or_else(refused)?;
    let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| refused())?;
    let mut mac = Hmac::<Sha256>::new_from_slice(state.secret()).map_err(|_| refused())?;
    mac.update(DOMAIN);
    mac.update(payload.as_bytes());
    mac.verify_slice(&signature).map_err(|_| refused())?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).map_err(|_| refused())?;
    let claims: Claims = serde_json::from_slice(&bytes).map_err(|_| refused())?;
    let now = chrono::Utc::now().timestamp();
    if claims.version != 1
        || claims.release != env!("CARGO_PKG_VERSION")
        || claims.issued_at > now + 5
        || claims.expires_at <= now
        || claims.expires_at <= claims.issued_at
        || claims.expires_at.saturating_sub(claims.issued_at) > TOKEN_SECONDS
        || claims.team_id.is_nil()
        || claims.generation.is_nil()
        || !plugins::valid_name(&claims.plugin)
        || !plugins::valid_digest(&claims.archive_sha256)
    {
        return Err(refused());
    }
    Ok(claims)
}

/// Retain ordinary route allowlists/rate limits; only the exact plugin handler consumes this
/// task-scoped proof. No user, admin or execution principal is installed for the request.
pub async fn guard(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let checked = async {
        if state.config.no_auth
            || method != Method::GET
            || uri.query().is_some()
            || headers.contains_key(header::COOKIE)
        {
            return Err(refused());
        }
        let name = uri
            .path()
            .strip_prefix("/plugins/")
            .filter(|name| plugins::valid_name(name))
            .ok_or_else(refused)?;
        let authorization = authorization(&headers)?;
        let claims = decode(&state, authorization)?;
        if claims.plugin != name {
            return Err(refused());
        }
        eligible(&state, &claims).await?;
        Ok(Authorized {
            credential_hash: Sha256::digest(authorization.as_bytes()).into(),
            claims,
        })
    }
    .await;
    match checked {
        Ok(proof) => REQUEST.scope(proof, next.run(request)).await,
        Err(error) => error.into_response(),
    }
}

pub async fn archive(state: &AppState, headers: &HeaderMap, name: &str) -> ApiResult {
    let authorization = authorization(headers)?;
    let hash: [u8; 32] = Sha256::digest(authorization.as_bytes()).into();
    let proof = REQUEST.try_with(Clone::clone).map_err(|_| refused())?;
    if proof.credential_hash != hash || proof.claims.plugin != name {
        return Err(refused());
    }
    let permit = READS
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::unavailable("Plugin archive delivery capacity is exhausted."))?;
    let path = state
        .config
        .plugins_archives_path
        .join(format!("{name}.tar.gz"));
    let (bytes, digest) = tokio::task::spawn_blocking(move || {
        #[cfg(unix)]
        let file = std::fs::File::from(
            rustix::fs::open(
                &path,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| ())?,
        );
        #[cfg(not(unix))]
        let file = {
            let metadata = std::fs::symlink_metadata(&path).map_err(|_| ())?;
            if metadata.is_symlink() {
                return Err(());
            }
            std::fs::File::open(&path).map_err(|_| ())?
        };
        let metadata = file.metadata().map_err(|_| ())?;
        if !metadata.is_file() || metadata.len() > MAX_ARCHIVE_BYTES as u64 {
            return Err(());
        }
        let mut bytes = Vec::new();
        file.take(MAX_ARCHIVE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ())?;
        if bytes.len() > MAX_ARCHIVE_BYTES {
            return Err(());
        }
        let digest = plugins::sha256(&bytes);
        Ok::<_, ()>((bytes, digest))
    })
    .await
    .map_err(|_| ApiError::unavailable("Plugin archive delivery was interrupted."))?
    .map_err(|_| ApiError::unavailable("The exact plugin archive is unavailable."))?;
    if digest != proof.claims.archive_sha256 {
        return Err(ApiError::new(
            409,
            "The plugin archive changed; refresh its approved catalog.",
        ));
    }
    if proof.claims.expires_at <= chrono::Utc::now().timestamp() {
        return Err(refused());
    }
    eligible(state, &proof.claims).await?;
    // Keep the slot until the actual response buffer (including transport clones) is freed,
    // so a slow receiver cannot queue an unbounded number of complete archive allocations.
    let mut response = bytes::Bytes::from_owner(ArchiveBytes {
        bytes,
        _permit: permit,
    })
    .into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, "application/gzip".parse().unwrap());
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    Ok(response)
}
