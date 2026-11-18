//! Public caller context for one sealed private extension call; never authentication.
//!
//! Operation admission binds the same subject and scope projection before and after waits.
//! It carries no password/credential, token secret,
//! signing material, cookie, grant ID, launch ID, or header.

use crate::{
    AppState,
    access::{self, Access},
    auth::Principal,
    error::ApiError,
};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Subject {
    User {
        username: String,
        user_id: Value,
        user_type: Option<String>,
    },
    Token {
        token_id: uuid::Uuid,
        label: Option<String>,
        user_id: Value,
        expiration: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Caller {
    pub principal: Subject,
    pub scopes: BTreeSet<String>,
    /// Only Main's verified console-session adapter may set this. A public request body,
    /// cookie string, header, environment variable or executor result cannot select it.
    pub console_session: bool,
}

fn refused() -> ApiError {
    ApiError::new(403, "The exact public extension caller is unavailable.")
}

impl Caller {
    /// Capture the registered origin, retaining the effective source's parent/Team narrowing.
    pub fn capture(access: &Access, console_session: bool) -> Result<Self, ApiError> {
        let source = match &access.auth {
            Some(Principal::Execution(identity)) => identity.source_access(),
            _ => access,
        };
        let id = |id: &Option<meerschaum_core::Value>| -> Result<Value, ApiError> {
            match id {
                Some(id) => serde_json::to_value(id).map_err(|_| refused()),
                None => Ok(Value::Null),
            }
        };
        let principal = match source.auth.as_ref() {
            Some(Principal::User(user)) => Subject::User {
                username: user.username.clone(),
                user_id: id(&user.user_id)?,
                user_type: if access.full_admin() {
                    Some("admin".into())
                } else {
                    user.user_type.clone().filter(|typ| typ != "admin")
                },
            },
            Some(Principal::Token(token)) => Subject::Token {
                token_id: token.id.ok_or_else(refused)?,
                label: token.label.clone(),
                user_id: id(&token.user_id)?,
                expiration: token
                    .expiration
                    .as_ref()
                    .map(crate::json::pydantic_datetime),
            },
            _ => return Err(refused()),
        };
        let scopes = meerschaum_core::access::ALL_PERMISSIONS
            .iter()
            .filter(|permission| access.has(permission))
            .map(|permission| permission.to_string())
            .collect();
        let caller = Self {
            principal,
            scopes,
            console_session,
        };
        caller.validate()?;
        Ok(caller)
    }

    pub fn validate(&self) -> Result<(), ApiError> {
        let text = |text: &str| {
            !text.is_empty() && text.len() <= 256 && !text.chars().any(char::is_control)
        };
        let id = |id: &Value| match id {
            Value::Number(number) => number.is_i64() || number.is_u64(),
            Value::String(value) => text(value),
            _ => false,
        };
        match &self.principal {
            Subject::User {
                username,
                user_id,
                user_type,
            } => {
                if !text(username)
                    || !id(user_id)
                    || user_type.as_deref().is_some_and(|typ| !text(typ))
                {
                    return Err(refused());
                }
            }
            Subject::Token {
                token_id,
                label,
                user_id,
                expiration,
            } => {
                if token_id.is_nil()
                    || label.as_deref().is_some_and(|label| !text(label))
                    || (!user_id.is_null() && !id(user_id))
                    || expiration.as_deref().is_some_and(|value| !text(value))
                    || self.console_session
                {
                    return Err(refused());
                }
            }
        }
        if self
            .scopes
            .iter()
            .any(|scope| !meerschaum_core::access::ALL_PERMISSIONS.contains(&scope.as_str()))
            || serde_json::to_vec(self).map_err(|_| refused())?.len() > 8192
        {
            return Err(refused());
        }
        Ok(())
    }

    /// Keep the admitted scope snapshot. A later role gain cannot broaden it; a lost scope
    /// invalidates this short call. Admission must separately bind the immutable initiating
    /// grant, since fresh stored-owner rights cannot reconstruct a narrowed JWT or parent.
    pub fn matches(&self, fresh: &Access) -> Result<(), ApiError> {
        let current = Self::capture(fresh, self.console_session)?;
        let same = match (&self.principal, &current.principal) {
            (
                Subject::User {
                    username,
                    user_id,
                    user_type,
                },
                Subject::User {
                    username: fresh_name,
                    user_id: fresh_id,
                    user_type: fresh_type,
                },
            ) => {
                username == fresh_name
                    && user_id == fresh_id
                    && user_type
                        .as_ref()
                        .is_none_or(|kind| Some(kind) == fresh_type.as_ref())
            }
            _ => current.principal == self.principal,
        };
        if same && self.scopes.is_subset(&current.scopes) {
            Ok(())
        } else {
            Err(refused())
        }
    }

    /// Same shape the native API/MCP SDK already accepts, with no authority-bearing fields.
    pub fn principal_json(&self) -> Result<Value, ApiError> {
        self.validate()?;
        serde_json::to_value(&self.principal).map_err(|_| refused())
    }
}

/// Original request credentials and verified console-session identity stay on Main. Only
/// `Caller` may enter the sealed operation; this type has no Serialize or Debug implementation.
pub(crate) struct RequestSource {
    headers: HeaderMap,
    caller: Arc<Access>,
    public: Caller,
    console: Option<String>,
    credential: Option<String>,
    administrator: bool,
}

impl RequestSource {
    pub(crate) async fn resolve(
        state: &AppState,
        headers: &HeaderMap,
        scopes: &[&str],
    ) -> Result<Self, ApiError> {
        let bearer = headers.contains_key(axum::http::header::AUTHORIZATION);
        let browser = state.engine_streams.browser_auth();
        let mut console = None;
        let caller = if bearer {
            let caller = access::require_fresh(state, headers, scopes).await?;
            // A bearer does not become a console session. The same actual user must also
            // have a verified live cookie; a token never adopts a cookie's user identity.
            if matches!(&caller.auth, Some(Principal::User(_)))
                && let Some(browser) = browser
                && let Some(Ok((session_headers, session))) =
                    browser.authorize(state, headers).await
            {
                let session_source = access::require_fresh(state, &session_headers, &[]).await?;
                if let (Some(Principal::User(user)), Some(Principal::User(session_user))) =
                    (&caller.auth, &session_source.auth)
                    && user.username == session_user.username
                    && user.user_id == session_user.user_id
                    && caller.credential == session_source.credential
                {
                    console = Some(session);
                }
            }
            // Browser resolution may wait on the store: refresh the original bearer last.
            access::require_fresh(state, headers, scopes).await?
        } else {
            let browser = browser.ok_or_else(ApiError::invalid_credentials)?;
            let (session_headers, session) = browser
                .authorize(state, headers)
                .await
                .ok_or_else(ApiError::invalid_credentials)?
                .map_err(|status| {
                    ApiError::new(status.as_u16(), "The console session is unavailable.")
                })?;
            let caller = access::require_fresh(state, &session_headers, scopes).await?;
            if !matches!(&caller.auth, Some(Principal::User(_))) {
                return Err(ApiError::invalid_credentials());
            }
            console = Some(session);
            caller
        };
        let public = Caller::capture(&caller, console.is_some())?;
        let credential = caller.credential.clone();
        let administrator = caller.full_admin();
        Ok(Self {
            headers: headers.clone(),
            caller: Arc::new(caller),
            public,
            console,
            credential,
            administrator,
        })
    }

    pub(crate) fn access(&self) -> Arc<Access> {
        self.caller.clone()
    }
    pub(crate) fn console_session(&self) -> bool {
        self.console.is_some()
    }

    /// Repeat the original bearer/cookie checks after a wait, retaining its immutable scope
    /// ceiling and console registration. A later role gain cannot enlarge this request.
    pub(crate) async fn refresh(
        &self,
        state: &AppState,
        scopes: &[&str],
    ) -> Result<Arc<Access>, ApiError> {
        let fresh = Self::resolve(state, &self.headers, scopes).await?;
        if fresh.console != self.console || fresh.credential != self.credential {
            return Err(ApiError::invalid_credentials());
        }
        self.public.matches(&fresh.caller)?;
        let mut caller = (*fresh.caller).clone();
        caller
            .principal
            .permissions
            .retain(|scope| self.public.scopes.contains(scope));
        caller.principal.is_admin &= self.administrator;
        caller.require(scopes, &[])?;
        Ok(Arc::new(caller))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_caller_never_accepts_credential_fields_or_an_unbound_console_token() {
        let user = serde_json::json!({"principal":{"kind":"user","username":"alice","user_id":1,"user_type":null},"scopes":["pipes:read"],"console_session":false});
        let decoded: Caller = serde_json::from_value(user.clone()).unwrap();
        decoded.validate().unwrap();
        let mut forged = user;
        forged["principal"]["credential"] = "not-authority".into();
        assert!(serde_json::from_value::<Caller>(forged).is_err());
        let caller = Caller {
            principal: Subject::Token {
                token_id: uuid::Uuid::new_v4(),
                label: None,
                user_id: Value::Null,
                expiration: None,
            },
            scopes: BTreeSet::new(),
            console_session: true,
        };
        assert!(caller.validate().is_err());
    }
}
