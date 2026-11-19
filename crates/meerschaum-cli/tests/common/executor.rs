//! Shared signed executor control-plane authenticator for integration tests.
#![allow(dead_code)]

use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
use meerschaum_connector_api::traits::{ConnectorError, ConnectorResult};
use meerschaum_server::executor::control::{Actor, Authority};

/// Signs outbound executor control requests with an Ed25519 `Authority` and fixed actor/revision.
pub struct ControlSigner {
    pub authority: Authority,
    pub revision: String,
    pub actor: Actor,
}

impl ControlSigner {
    pub fn operator(authority: Authority, revision: impl Into<String>) -> Self {
        Self {
            authority,
            revision: revision.into(),
            actor: Actor {
                owner: "operator".into(),
                username: Some("operator".into()),
                permissions: Default::default(),
                administrator: false,
            },
        }
    }
}

impl RequestAuthentication for ControlSigner {
    fn authorization(&self, request: Request<'_>) -> ConnectorResult<String> {
        self.authority
            .issue(
                self.actor.clone(),
                meerschaum_server::executor::control::Request {
                    method: request.method,
                    path: request.path,
                    body: request.body.ok_or_else(|| {
                        ConnectorError::Invalid("Unbuffered fixture request.".into())
                    })?,
                },
                &self.revision,
                chrono::Utc::now().timestamp(),
            )
            .map(|token| format!("Bearer {token}"))
            .map_err(|_| ConnectorError::Invalid("Could not sign fixture request.".into()))
    }
}
