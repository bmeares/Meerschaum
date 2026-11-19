//! Native self-API service authority. All credential material stays on Main.
//!
//! The trusted selection layer must first prove this actual configured API target is a
//! constructor-bound alias for this exact native Main connection. This module neither
//! guesses a physical target from URLs nor gives that SQL label to the original caller.

use crate::{
    AppState,
    access::{self, Access},
    auth::{self, Principal, Scopes},
    error::ApiError,
};
use axum::http::{HeaderMap, header};
use base64::Engine;
use meerschaum_connector::api_target::ServiceCredential;
use meerschaum_core::{Value, users::User};
use std::collections::BTreeSet;

#[derive(Clone, PartialEq)]
enum Subject {
    NoAuth,
    User {
        name: String,
        id: Value,
    },
    Token {
        id: uuid::Uuid,
        owner: Option<Value>,
        creation: Option<chrono::DateTime<chrono::Utc>>,
        hash: Option<String>,
    },
}
impl Subject {
    fn of(access: &Access) -> Result<Self, ApiError> {
        if access.no_auth {
            return Ok(Self::NoAuth);
        }
        match access.auth.as_ref() {
            Some(Principal::User(user)) => Ok(Self::User {
                name: user.username.clone(),
                id: user
                    .user_id
                    .clone()
                    .ok_or_else(ApiError::invalid_credentials)?,
            }),
            Some(Principal::Token(token)) => Ok(Self::Token {
                id: token.id.ok_or_else(ApiError::invalid_credentials)?,
                owner: token.user_id.clone(),
                creation: token.creation,
                hash: token.secret_hash.clone(),
            }),
            // A rotating caller/executor bearer must never be adopted as the stored service.
            _ => Err(ApiError::invalid_credentials()),
        }
    }
}

#[derive(Clone)]
enum Authentication {
    NoAuth,
    BoundPassword {
        username: String,
        credential: String,
    },
    Key(HeaderMap),
}

/// No Debug/serialization: holds Main-only registered subject and service authentication.
#[derive(Clone)]
pub struct ServiceAuthority {
    authentication: Authentication,
    subject: Subject,
    held: BTreeSet<String>,
}

fn denied() -> ApiError {
    ApiError::new(
        403,
        "The configured API service authority is unavailable or no longer permits this operation.",
    )
}

impl ServiceAuthority {
    pub(super) async fn capture(
        state: &AppState,
        binding: &super::selection::SelfApiBinding,
    ) -> Result<Self, ApiError> {
        if state.config.no_auth {
            return Ok(Self {
                authentication: Authentication::NoAuth,
                subject: Subject::NoAuth,
                held: meerschaum_core::access::ALL_PERMISSIONS
                    .iter()
                    .map(|name| (*name).into())
                    .collect(),
            });
        }
        let (authentication, access) = match &binding.target().credential {
            ServiceCredential::None => return Err(denied()),
            ServiceCredential::Password { username, password } => {
                let user = User::new(username, None);
                let hash = state
                    .instance()
                    .get_user_password_hash(&user)
                    .await
                    .map_err(|_| {
                        ApiError::unavailable(
                            "Could not read the configured API service authority.",
                        )
                    })?
                    .ok_or_else(denied)?;
                let credential = auth::credential_fingerprint(state.secret(), &hash);
                {
                    let mut verified = binding.verified_password.lock().await;
                    if verified.as_ref() != Some(&credential) {
                        let start = std::time::Instant::now();
                        if !meerschaum_connector::passwords::verify(password, &hash)
                            .await
                            .unwrap_or(false)
                        {
                            return Err(denied());
                        }
                        tracing::debug!(target: "meerschaum_server::engine_runtime", elapsed_ms = start.elapsed().as_millis(), "Verified the configured service password");
                        *verified = Some(credential.clone());
                    }
                }
                let principal =
                    auth::load_from_bound_subject(state, username, Some("user"), Some(&credential))
                        .await?
                        .ok_or_else(denied)?;
                let access =
                    access::access_of_fresh(state, principal, &Scopes::List(vec!["*".into()]))
                        .await?;
                (
                    Authentication::BoundPassword {
                        username: username.clone(),
                        credential,
                    },
                    access,
                )
            }
            ServiceCredential::ApiKey { value } => {
                let headers = key_headers(value)?;
                let access = access::require_fresh(state, &headers, &[]).await?;
                (Authentication::Key(headers), access)
            }
            ServiceCredential::Client { id, secret } => {
                let value = format!(
                    "mrsm-key:{}",
                    base64::engine::general_purpose::STANDARD.encode(format!("{id}:{secret}"))
                );
                let headers = key_headers(&value)?;
                let access = access::require_fresh(state, &headers, &[]).await?;
                (Authentication::Key(headers), access)
            }
        };
        let subject = Subject::of(&access)?;
        Ok(Self {
            authentication,
            subject,
            held: access.principal.permissions,
        })
    }

    /// Call within the same Main native transaction after its real writer lock. Configured
    /// password material was verified once, and its hash binding + registered ID must still
    /// match; role/scopes/ownership are read fresh. No JWT is minted, forwarded or extended.
    pub async fn fresh(&self, state: &AppState, required: &[&str]) -> Result<Access, ApiError> {
        let mut current = match &self.authentication {
            Authentication::NoAuth if state.config.no_auth => Access::everything(),
            Authentication::NoAuth => return Err(denied()),
            Authentication::BoundPassword {
                username,
                credential,
            } => {
                let principal =
                    auth::load_from_bound_subject(state, username, Some("user"), Some(credential))
                        .await?
                        .ok_or_else(denied)?;
                access::access_of_fresh(
                    state,
                    principal,
                    &Scopes::List(self.held.iter().cloned().collect()),
                )
                .await?
            }
            Authentication::Key(headers) => access::require_fresh(state, headers, &[]).await?,
        };
        if Subject::of(&current)? != self.subject {
            return Err(denied());
        }
        current
            .principal
            .permissions
            .retain(|scope| self.held.contains(scope));
        current.require(required, &[])?;
        Ok(current)
    }

    /// Run synchronously AFTER the last awaited actor/object/definition check. Stored API-key
    /// JWT expiry and expiring token credentials remain effective through a long write wait.
    pub fn finish(&self, state: &AppState, current: &Access) -> Result<(), ApiError> {
        if Subject::of(current)? != self.subject {
            return Err(denied());
        }
        if let Authentication::Key(headers) = &self.authentication {
            let value = headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(denied)?;
            if !value.contains("mrsm-key:") {
                auth::bearer_scopes(state, headers)?;
            }
            if let Some(Principal::Token(token)) = &current.auth
                && (!token.is_valid
                    || token
                        .expiration
                        .is_some_and(|time| time <= chrono::Utc::now()))
            {
                return Err(denied());
            }
        }
        Ok(())
    }
}

fn key_headers(value: &str) -> Result<HeaderMap, ApiError> {
    if value.is_empty() || value.len() > 16 * 1024 || value.chars().any(char::is_control) {
        return Err(denied());
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        format!("Bearer {value}").parse().map_err(|_| denied())?,
    );
    Ok(headers)
}
