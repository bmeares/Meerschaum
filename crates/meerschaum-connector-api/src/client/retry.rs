//! `retry_connect()` for an API instance (the `api` branch of v4's `_wrap_retry_connect`).
//!
//! An attempt asks the instance whether it allows chaining (`GET /chaining`, which sends the
//! login token and so logs in first) and then logs in. The chaining status is asked until the
//! instance answers it, and not again after that.

use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::retry::{Attempt, AttemptCx, RetryProbe};

use super::APIConnector;

/// The [`RetryProbe`] of an API connector.
pub struct ApiRetryProbe {
    connector: Arc<APIConnector>,
    /// `str(connector)`, as the messages name it.
    keys: String,
    /// `enforce_chaining`: an instance that does not allow chaining ends the loop.
    enforce_chaining: bool,
    /// `enforce_login`: the attempt must log in (otherwise reaching the instance is enough).
    enforce_login: bool,
    /// Whether a failed login may warn (`warn`, and the connector's own `_warn` is not `False`).
    login_warns: bool,
    /// The instance's answer to `/chaining`, once it gave one.
    chaining: Option<bool>,
}

impl ApiRetryProbe {
    /// A probe over `connector`; `keys` is what its messages call it.
    pub fn new(
        connector: Arc<APIConnector>,
        keys: impl Into<String>,
        enforce_chaining: bool,
        enforce_login: bool,
        login_warns: bool,
    ) -> ApiRetryProbe {
        ApiRetryProbe {
            connector,
            keys: keys.into(),
            enforce_chaining,
            enforce_login,
            login_warns,
            chaining: None,
        }
    }
}

impl RetryProbe for ApiRetryProbe {
    async fn attempt(&mut self, cx: &mut AttemptCx<'_>) -> Attempt {
        let keys = &self.keys;
        // If the remote instance does not allow chaining, don't even try logging in.
        if self.chaining.is_none() {
            match self.connector.get_chaining_status().await {
                // No answer (unreachable, an error status, not a boolean): try again later.
                None => return Attempt::Failed,
                Some(false) => {
                    if self.enforce_chaining {
                        cx.warn_user(&format!(
                            "Meerschaum instance '{keys}' does not allow chaining and cannot \
                             be used as the parent for this instance."
                        ));
                        return Attempt::Refused;
                    }
                    // Allow is the option to ignore the chaining status.
                    self.chaining = Some(true);
                }
                Some(true) => self.chaining = Some(true),
            }
        }

        let connected = if self.enforce_login {
            self.connector
                .login_with(self.login_warns && cx.warns())
                .await
                .success
        } else {
            true
        };
        if connected {
            return Attempt::Connected;
        }
        cx.warn_user(&format!("Unable to login to '{keys}'!"));
        Attempt::Failed
    }
}

impl APIConnector {
    /// The longest an attempt of [`ApiRetryProbe`] may take: two requests (`/chaining`, then
    /// `/login`), each bounded by the connection timeout, and a second of grace.
    pub fn retry_attempt_timeout(&self) -> Duration {
        self.connect_timeout()
            .saturating_mul(2)
            .saturating_add(Duration::from_secs(1))
    }
}
