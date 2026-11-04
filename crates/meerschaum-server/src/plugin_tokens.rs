//! Tokens a native plugin mints and revokes for the caller of one of its routes, console pages or
//! MCP entries (the plugin SDK's `host::mint_token` and `host::revoke_token`): a provisioning
//! route mints a device's token, and the device's own requests rotate it.
//!
//! - **Who.** Only an authenticated caller, read again from the instance: the token's owner is
//!   the calling user, or the owner of the calling token (a device rotating its own token).
//!   Anonymous callers, and every caller under `--no-auth` (which has no identities), mint and
//!   revoke nothing.
//! - **Scopes.** At least one, each a custom scope the calling plugin itself declares
//!   ([`plugin_scopes`]: never `*`, a core permission or any scope in a core permission's
//!   namespace), and each held by the caller, as `/tokens/register` requires (refused, never
//!   trimmed).
//! - **Revocation** deletes a token of the same owner that carries nothing but the plugin's own
//!   scopes. Any other id (another user's token, one with a core scope or another plugin's scope,
//!   one that does not exist) gets the same refusal, so a plugin learns nothing about tokens it
//!   may not touch. Every request reads its token from the instance, so the next request with a
//!   revoked token is refused (`401`), as are the later host calls of a request already running
//!   with it.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use meerschaum_core::SuccessTuple;
use meerschaum_core::users::{Expiration, Token, User};
use serde_json::{Value as Json, json};
use uuid::Uuid;

use crate::auth::{Principal, grantable_scopes, load_token, load_user};
use crate::json::pydantic_datetime;
use crate::state::AppState;

/// Whether `scope` is the core API's: `*`, a permission (`pipes:read`, ...), or any scope in the
/// namespace of one (`pipes:*`, `users:anything`). A plugin never puts these on a token.
pub fn is_core_scope(scope: &str) -> bool {
    use meerschaum_core::access::{ALL_PERMISSIONS, ALL_SCOPE};
    if scope == ALL_SCOPE
        || ALL_PERMISSIONS.contains(&scope)
        || meerschaum_core::static_config()
            .scope_names()
            .iter()
            .any(|s| s == scope)
    {
        return true;
    }
    let namespace = |s: &str| s.split(':').next().unwrap_or_default().to_ascii_lowercase();
    let namespace_of_scope = namespace(scope);
    ALL_PERMISSIONS
        .iter()
        .any(|permission| namespace(permission) == namespace_of_scope)
}

/// The scopes a plugin's tokens may carry: the custom scopes among `declared` (the `scopes` of
/// its routes and MCP entries), without `*`, core scopes ([`is_core_scope`]) or empty names.
pub fn plugin_scopes<S: AsRef<str>>(declared: impl IntoIterator<Item = S>) -> BTreeSet<String> {
    declared
        .into_iter()
        .map(|s| s.as_ref().trim().to_string())
        .filter(|s| !s.is_empty() && !is_core_scope(s))
        .collect()
}

/// A token a plugin minted. `secret` and `api_key` are shown here only: the instance keeps a
/// hash of the secret.
#[derive(Clone, PartialEq, Eq)]
pub struct MintedToken {
    pub id: Uuid,
    pub label: String,
    pub scopes: Vec<String>,
    pub expiration: Option<DateTime<Utc>>,
    pub secret: String,
    pub api_key: String,
}

impl std::fmt::Debug for MintedToken {
    /// Without the secret and the API key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MintedToken")
            .field("id", &self.id)
            .field("label", &self.label)
            .field("scopes", &self.scopes)
            .field("expiration", &self.expiration)
            .finish_non_exhaustive()
    }
}

impl MintedToken {
    /// The token as `POST /tokens/register` returns it (`label`, `secret`, `id`, `api_key`,
    /// `expiration`), with its `scopes`.
    pub fn to_json(&self) -> Json {
        json!({
            "id": self.id.to_string(),
            "label": self.label,
            "scopes": self.scopes,
            "expiration": self.expiration.as_ref().map(pydantic_datetime),
            "secret": self.secret,
            "api_key": self.api_key,
        })
    }
}

/// The caller, read again from the instance, and the user who owns the tokens it mints: the
/// caller itself, or a calling token's owner.
async fn owner(
    state: &AppState,
    caller: Option<&Principal>,
    what: &str,
) -> Result<(Principal, User), String> {
    if state.config.no_auth {
        return Err(format!(
            "A plugin cannot {what} under `--no-auth`: a token needs an owner, and the server \
             has no identities."
        ));
    }
    let Some(caller) = caller else {
        return Err(format!("An anonymous caller may not {what}."));
    };
    // Reloaded as the kind it already is (a revoked token or deleted user stops here).
    let reloaded = match caller {
        Principal::User(user) => load_user(state, &user.username).await,
        Principal::Token(token) => {
            let id = token.id.map(|id| id.to_string()).unwrap_or_default();
            load_token(state, &id).await
        }
    }
    .map_err(|e| e.detail_text())?
    .ok_or_else(|| "Invalid or revoked credentials.".to_string())?;
    let user = match &reloaded {
        Principal::User(user) => user.clone(),
        Principal::Token(token) => {
            let no_owner = || format!("The calling token has no owner, so it may not {what}.");
            let user_id = token
                .user_id
                .clone()
                .filter(|id| id.truthy())
                .ok_or_else(no_owner)?;
            let conn = state.instance();
            let username = conn
                .get_username(&user_id)
                .await
                .map_err(|e| format!("Could not read the calling token's owner: {e}"))?
                .ok_or_else(no_owner)?;
            let mut user = User::new(&meerschaum_core::pyfmt::py_str(&username), None);
            user.user_id = Some(user_id);
            user.instance_keys = Some(conn.connector_keys());
            user
        }
    };
    Ok((reloaded, user))
}

/// `'a', 'b'`.
fn quoted(names: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    names
        .into_iter()
        .map(|s| format!("'{}'", s.as_ref()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Mint a token labeled `label` with `scopes` for the caller of `plugin`'s route, page or MCP
/// entry (`caller`, holding `granted` in this request), owned by the caller's user. `allowed` is
/// what the plugin may grant ([`plugin_scopes`]). `expiration` `None` never expires (as
/// `/tokens/register`). An `Err` names the refusal.
#[allow(clippy::too_many_arguments)]
pub async fn mint(
    state: &AppState,
    caller: Option<&Principal>,
    granted: &[String],
    plugin: &str,
    allowed: &BTreeSet<String>,
    label: &str,
    scopes: &[String],
    expiration: Option<DateTime<Utc>>,
) -> Result<MintedToken, String> {
    let label = label.trim();
    if label.is_empty() {
        return Err("A minted token needs a label (a device's id, for example).".into());
    }
    let mut wanted: Vec<String> = Vec::new();
    for scope in scopes {
        let scope = scope.trim();
        if !scope.is_empty() && !wanted.iter().any(|s| s == scope) {
            wanted.push(scope.to_string());
        }
    }
    if wanted.is_empty() {
        return Err(format!(
            "A minted token needs at least one scope that plugin '{plugin}' declares."
        ));
    }
    let refused: Vec<&String> = wanted.iter().filter(|s| !allowed.contains(*s)).collect();
    if !refused.is_empty() {
        let own = if allowed.is_empty() {
            "it declares none".to_string()
        } else {
            quoted(allowed)
        };
        return Err(format!(
            "Plugin '{plugin}' may not mint a token with {}: a plugin's tokens carry only the \
             custom scopes its own routes and MCP entries declare ({own}), never `*` or a core \
             scope.",
            quoted(refused)
        ));
    }
    if let Some(at) = expiration
        && at <= Utc::now()
    {
        return Err(format!(
            "A minted token's expiration must be in the future (got {}).",
            pydantic_datetime(&at)
        ));
    }
    let (principal, user) = owner(state, caller, "mint tokens").await?;
    // The caller holds each scope it grants, in this request and on the instance now.
    let cap = grantable_scopes(state, Some(&principal))
        .await
        .map_err(|e| e.detail_text())?;
    let unheld: Vec<&String> = wanted
        .iter()
        .filter(|scope| {
            let in_request = granted.iter().any(|g| g == "*" || g == *scope);
            let on_instance = cap.as_ref().is_none_or(|cap| cap.contains(scope));
            !(in_request && on_instance)
        })
        .collect();
    if !unheld.is_empty() {
        return Err(format!(
            "The caller may not grant scopes it does not hold: {}. A token carries at most its \
             owner's scopes.",
            quoted(unheld)
        ));
    }
    let conn = state.instance();
    let mut token = Token::new(
        Some(label),
        match expiration {
            Some(at) => Expiration::At(at),
            None => Expiration::Never,
        },
        Some(wanted.clone()),
        state.config.token_default_expiration_days,
        Utc::now(),
    );
    token.instance_keys = Some(conn.connector_keys());
    token.user_id = user.user_id.clone();
    token.user = Some(user);
    let (id, secret) = token
        .generate_credentials()
        .map_err(|e| format!("Could not generate the token's credentials: {e}"))?;
    let registered = conn.register_token(&mut token).await;
    if !registered.success {
        return Err(format!(
            "Could not register the new token:\n{}",
            registered.message
        ));
    }
    tracing::info!("Plugin '{plugin}' minted token {id} ('{label}').");
    Ok(MintedToken {
        id,
        label: label.to_string(),
        scopes: wanted,
        expiration: token.expiration,
        api_key: token.get_api_key(),
        secret,
    })
}

/// Revoke (delete) token `id` for the caller of `plugin`'s route, page or MCP entry: only a token
/// of the caller's user whose scopes are all among `allowed` ([`plugin_scopes`]). Any other id is
/// refused with the same message, whether or not it exists.
pub async fn revoke(
    state: &AppState,
    caller: Option<&Principal>,
    plugin: &str,
    allowed: &BTreeSet<String>,
    id: &str,
) -> Result<SuccessTuple, String> {
    let (_, user) = owner(state, caller, "revoke tokens").await?;
    let refused = || {
        format!(
            "Plugin '{plugin}' may not revoke token '{}': a plugin revokes only its caller's own \
             tokens that carry nothing but the plugin's own scopes.",
            id.trim()
        )
    };
    let Ok(uuid) = Uuid::parse_str(id.trim()) else {
        return Err(refused());
    };
    let conn = state.instance();
    let model = conn
        .get_token_model(&uuid)
        .await
        .map_err(|e| format!("Could not read tokens from the instance: {e}"))?;
    let revocable = model.is_some_and(|model| {
        let owned = model
            .user_id
            .as_ref()
            .filter(|owner| owner.truthy())
            .is_some_and(|owner| Some(owner) == user.user_id.as_ref());
        owned && !model.scopes.is_empty() && model.scopes.iter().all(|s| allowed.contains(s))
    });
    if !revocable {
        return Err(refused());
    }
    let mut token = Token::new(None, Expiration::Never, None, 366, Utc::now());
    token.id = Some(uuid);
    let deleted = conn.delete_token(&token).await;
    if deleted.success {
        tracing::info!("Plugin '{plugin}' revoked token {uuid}.");
        return Ok(SuccessTuple::ok(format!("Revoked token '{uuid}'.")));
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_scopes_and_their_namespaces_are_never_a_plugins() {
        for core in [
            "*",
            "pipes:read",
            "users:delete",
            "webterm:use",
            "pipes:anything",
            "Jobs:new",
            "actions",
        ] {
            assert!(is_core_scope(core), "{core}");
        }
        for custom in ["devices:ingest", "devices", "example:report"] {
            assert!(!is_core_scope(custom), "{custom}");
        }
        assert_eq!(
            plugin_scopes(["devices:ingest", "pipes:read", "*", " ", "devices:ingest"]),
            BTreeSet::from(["devices:ingest".to_string()])
        );
    }

    #[test]
    fn a_minted_tokens_debug_hides_its_secret() {
        let token = MintedToken {
            id: Uuid::nil(),
            label: "device-1".into(),
            scopes: vec!["devices:ingest".into()],
            expiration: None,
            secret: "synthetic-secret".into(),
            api_key: "mrsm-key:synthetic".into(),
        };
        let debug = format!("{token:?}");
        assert!(
            debug.contains("device-1") && !debug.contains("synthetic"),
            "{debug}"
        );
        assert_eq!(token.to_json()["secret"], "synthetic-secret");
        assert_eq!(token.to_json()["expiration"], Json::Null);
    }
}
