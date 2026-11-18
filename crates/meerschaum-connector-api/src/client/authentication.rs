//! Request-specific authentication for executor control and restricted API execution contexts.
//!
//! These clients never log in with stored usernames/passwords, follow redirects, or inherit a
//! proxy from their environment. Normal API connectors retain their existing behavior.

use std::sync::Arc;

use reqwest::header::{AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION};

use super::APIConnector;
use crate::traits::{ConnectorError, ConnectorResult};

/// Bound a buffered signed request, including multipart plugin archives.
pub const MAX_AUTHENTICATED_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// Short-lived user credential persisted by a restricted command's main-API supervisor.
/// Rotation replaces the owner-only file atomically; clients read it for every request.
pub use crate::models::ExecutionCredential as FileCredential;

/// Rotating user credential source. Debug omits the private path and credential value.
pub struct CredentialFile(std::path::PathBuf);

impl std::fmt::Debug for CredentialFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CredentialFile([private])")
    }
}

impl CredentialFile {
    /// The supervisor's owner-only credential file.
    pub fn new(path: std::path::PathBuf) -> Self {
        Self(path)
    }
}

impl RequestAuthentication for CredentialFile {
    fn authorization(&self, _: Request<'_>) -> ConnectorResult<String> {
        use std::io::Read;
        let invalid = || {
            ConnectorError::Invalid(
                "The command's API credential is unavailable, expired or invalid.".into(),
            )
        };
        let metadata = std::fs::symlink_metadata(&self.0).map_err(|_| invalid())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid());
        }
        let file = std::fs::File::open(&self.0).map_err(|_| invalid())?;
        let metadata = file.metadata().map_err(|_| invalid())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(invalid());
            }
        }
        if !metadata.is_file() || metadata.len() > 32 * 1024 {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        file.take(32 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid())?;
        if bytes.len() > 32 * 1024 {
            return Err(invalid());
        }
        let credential: FileCredential = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if credential.expires_at <= chrono::Utc::now().timestamp()
            || credential.token.is_empty()
            || credential.token.len() > 16 * 1024
            || credential
                .token
                .bytes()
                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        {
            return Err(invalid());
        }
        Ok(format!("Bearer {}", credential.token))
    }
}

/// Exact request bytes available to an authenticator after HTTP encoding.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    /// Uppercase HTTP method.
    pub method: &'a str,
    /// Exact encoded path and query.
    pub path: &'a str,
    /// Encoded body. `None` means an unbuffered body, not an empty body.
    pub body: Option<&'a [u8]>,
}

/// A host-provided credential source. Implementations must not print credential values in
/// errors. Body-bound signers must refuse `body: None`; file token providers may ignore it.
pub trait RequestAuthentication: Send + Sync {
    /// Full Authorization value, for example `Bearer ...`.
    fn authorization(&self, request: Request<'_>) -> ConnectorResult<String>;
}

impl APIConnector {
    /// Use per-request credentials and bounded response buffering. The host supplies current
    /// authorization; a failed request does not trigger a password login or a write retry.
    pub fn with_request_authentication(
        mut self,
        authentication: Arc<dyn RequestAuthentication>,
        response_limit: usize,
    ) -> ConnectorResult<Self> {
        if response_limit == 0 {
            return Err(ConnectorError::Invalid(
                "An authenticated API response limit must be positive.".into(),
            ));
        }
        let timeout = meerschaum_core::connect::connect_timeout(
            self.attributes
                .get("connect_timeout")
                .map(super::attr_str)
                .as_deref(),
        );
        self.http = reqwest::Client::builder()
            .connect_timeout(timeout)
            .danger_accept_invalid_certs(!self.verify)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| {
                ConnectorError::Backend("Could not create the authenticated API transport.".into())
            })?;
        self.request_authentication = Some(authentication);
        self.response_limit = Some(response_limit);
        Ok(self)
    }

    pub(crate) fn custom_authorization(
        &self,
        method: &str,
        url: &reqwest::Url,
        body: Option<&[u8]>,
    ) -> ConnectorResult<Option<String>> {
        let Some(authentication) = &self.request_authentication else {
            return Ok(None);
        };
        let mut path = url.path().to_string();
        if let Some(query) = url.query() {
            path.push('?');
            path.push_str(query);
        }
        authentication
            .authorization(Request {
                method,
                path: &path,
                body,
            })
            .map(Some)
    }

    pub(crate) fn authenticate_request(
        &self,
        request: &mut reqwest::Request,
    ) -> ConnectorResult<()> {
        if self.request_authentication.is_none() {
            return Ok(());
        }
        let body = request
            .body()
            .map_or(Some(&[][..]), reqwest::Body::as_bytes);
        if body.is_some_and(|bytes| bytes.len() > MAX_AUTHENTICATED_REQUEST_BYTES) {
            return Err(ConnectorError::Invalid(
                "An authenticated API request exceeds its size limit.".into(),
            ));
        }
        let authorization = self
            .custom_authorization(request.method().as_str(), request.url(), body)?
            .ok_or_else(|| ConnectorError::Invalid("Missing request authentication.".into()))?;
        let headers = request.headers_mut();
        for name in [AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION] {
            headers.remove(name);
        }
        headers.insert(
            AUTHORIZATION,
            authorization.parse().map_err(|_| {
                ConnectorError::Invalid("Invalid request authentication header.".into())
            })?,
        );
        Ok(())
    }
}
