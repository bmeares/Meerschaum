//! `login()`, the token property and `test_connection()` (`_login.py`, `_APIConnector.py`).

use chrono::{Duration, NaiveDateTime, Utc};
use meerschaum_core::SuccessTuple;

use crate::routes;
use crate::traits::ConnectorResult;

use super::{APIConnector, Body, RequestOptions};

/// Parse the `/login` response's naive `expires` timestamp.
///
/// PARITY: v4 parses with `'%Y-%m-%dT%H:%M:%S.%f'`, which raises when the server omits the
/// fraction (a whole-second expiration); this accepts both forms.
pub fn parse_expires(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").ok()
}

impl APIConnector {
    /// `login()`: request a JWT (password or client-credentials grant), or validate the API key.
    pub async fn login(&self) -> SuccessTuple {
        let scheme = self.login_scheme();
        if scheme == "api_key" {
            let api_key = self.attr("api_key").unwrap_or_default();
            let res = self
                .post(
                    &format!("{}/validate", routes::TOKENS),
                    RequestOptions {
                        headers: vec![("Authorization".into(), format!("Bearer {api_key}"))],
                        use_token: false,
                        ..Default::default()
                    },
                )
                .await;
            return match res {
                Ok(r) if r.ok() => SuccessTuple::ok("API key is valid."),
                _ => SuccessTuple::fail("API key is not valid."),
            };
        }
        let login_data: Vec<(String, String)> = match scheme {
            "password" => match (self.attr("username"), self.attr("password")) {
                (Some(u), Some(p)) => vec![("username".into(), u), ("password".into(), p)],
                _ => Vec::new(),
            },
            _ => match (self.attr("client_id"), self.attr("client_secret")) {
                (Some(i), Some(s)) => vec![("client_id".into(), i), ("client_secret".into(), s)],
                _ => Vec::new(),
            },
        };
        if login_data.is_empty() {
            return SuccessTuple::fail(format!("Please login with the command `login {self}`."));
        }
        // PARITY: the scheme is compared with `'username'`, never true, so the message never
        // names the user; the success message keeps v4's stray quote (`_login.py:55`).
        let login_scheme_msg = "";
        let response = self
            .post(
                routes::LOGIN,
                RequestOptions {
                    body: Body::Form(login_data),
                    use_token: false,
                    ..Default::default()
                },
            )
            .await;
        let failure = format!(
            "Failed to log into '{self}'{login_scheme_msg}.\n    Please verify login details for connector '{self}'."
        );
        let response = match response {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(format!("{failure}\n{e}")),
        };
        if !response.ok() {
            let mut auth = self.auth.lock().await;
            if !auth.emitted_warning {
                super::warnings::warn(&failure);
                auth.emitted_warning = true;
            }
            return SuccessTuple::fail(failure);
        }
        let parsed = response.json().ok().and_then(|j| {
            let token = j.get("access_token")?.as_str()?.to_string();
            let expires = j
                .get("expires")
                .and_then(|e| e.as_str())
                .and_then(parse_expires);
            Some((token, expires))
        });
        let Some((token, expires)) = parsed else {
            return SuccessTuple::fail(failure);
        };
        let mut auth = self.auth.lock().await;
        auth.token = Some(token);
        auth.expires = expires;
        SuccessTuple::ok(format!(
            "Successfully logged into '{self}'{login_scheme_msg}'."
        ))
    }

    /// The `token` property: the API key, or a valid JWT (logging in when missing or expiring
    /// within a minute). `None` if login fails (warned once).
    pub async fn token(&self) -> Option<String> {
        if self.login_scheme() == "api_key" {
            return self.attr("api_key");
        }
        {
            let auth = self.auth.lock().await;
            let expired = match auth.expires {
                None => true,
                Some(exp) => exp < Utc::now().naive_utc() + Duration::minutes(1),
            };
            if auth.token.is_some() && !expired {
                return auth.token.clone();
            }
        }
        let res = self.login().await;
        let mut auth = self.auth.lock().await;
        if !res.success && !auth.emitted_warning {
            super::warnings::warn(&res.message);
            auth.emitted_warning = true;
        }
        auth.token.clone()
    }

    /// Set the session token directly (e.g. a token obtained elsewhere).
    pub async fn set_token(&self, token: Option<String>, expires: Option<NaiveDateTime>) {
        let mut auth = self.auth.lock().await;
        auth.token = token;
        auth.expires = expires;
    }

    /// `test_connection()`: whether the API answers `/healthcheck` (v4 uses `retry_connect` with
    /// one attempt, which checks the version and login).
    pub async fn test_connection_inner(&self) -> ConnectorResult<bool> {
        Ok(self
            .get(
                routes::HEALTHCHECK,
                RequestOptions::default().without_token(),
            )
            .await
            .map(|r| r.ok())
            .unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expires_formats() {
        assert!(parse_expires("2026-10-12T17:09:44.933360").is_some());
        assert!(parse_expires("2026-10-12T17:09:44").is_some());
        assert!(parse_expires("garbage").is_none());
    }
}
