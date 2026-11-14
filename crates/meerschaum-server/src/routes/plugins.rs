//! `/plugins/*` (`api/routes/_plugins.py`), including the archive upload and download.

use std::path::PathBuf;

use axum::Router;
use axum::extract::{DefaultBodyLimit, FromRequest, Multipart, Path, Request};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use meerschaum_connector::{PluginRecord, pipe_ops};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};
use serde_json::json;

use super::Ctx;
use crate::auth::{Principal, load_user_or_token, manager, scoped_auth_if_private};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, ok_json, tuple, tuple_response};
use crate::state::AppState;

/// The largest plugin archive accepted by `POST /plugins/{name}` (and by the console's upload form,
/// which forwards to it). v4 (Starlette) has no limit; axum's default of 2 MiB would reject real
/// plugin archives, so the limit is raised to a bound that still protects the server's memory.
pub const ARCHIVE_MAX_BYTES: usize = 128 * 1024 * 1024;

/// Register the `plugins` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/plugins", get(get_plugins))
        .route(
            "/plugins/{name}",
            get(get_plugin)
                .delete(delete_plugin)
                .merge(post(register_plugin).layer(DefaultBodyLimit::max(ARCHIVE_MAX_BYTES))),
        )
        .route("/plugins/{name}/attributes", get(get_plugin_attributes))
        .route("/plugins/{name}/version", get(get_plugin_version))
}

fn archive_path(state: &AppState, name: &str) -> PathBuf {
    state
        .config
        .plugins_archives_path
        .join(format!("{name}.tar.gz"))
}

fn record(name: &str) -> PluginRecord {
    PluginRecord {
        name: name.to_string(),
        ..Default::default()
    }
}

async fn register_plugin(ctx: Ctx, Path(name): Path<String>, req: Request) -> ApiResult {
    let state = ctx.state.clone();
    let principal = if ctx
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|auth| auth.contains("mrsm-key:"))
    {
        load_user_or_token(&state, &ctx.headers).await?
    } else {
        manager(&state, &ctx.headers).await?
    };
    let user = match principal {
        Principal::User(user) => user,
        Principal::Token(_) => {
            // RC30: publishing with client credentials or an API key acts as its owner,
            // with the token's scopes capped by that owner's current permissions.
            let access =
                crate::access::require_fresh(&state, &ctx.headers, &["plugins:write"]).await?;
            User::new(
                access
                    .username()
                    .ok_or_else(|| ApiError::new(403, "This token has no owner."))?,
                None,
            )
        }
    };
    // `archive: UploadFile = File(...)`.
    let mut archive: Option<bytes::Bytes> = None;
    if let Ok(mut mp) = Multipart::from_request(req, &state).await {
        while let Ok(Some(field)) = mp.next_field().await {
            if field.name() == Some("archive") {
                archive = match field.bytes().await {
                    Ok(bytes) => Some(bytes),
                    // An archive over the limit is a `413`, not a missing archive.
                    Err(e)
                        if e.status() == StatusCode::PAYLOAD_TOO_LARGE
                            || super::exceeded_length_limit(&e) =>
                    {
                        return Err(super::body_too_large(ARCHIVE_MAX_BYTES));
                    }
                    Err(_) => None,
                };
            }
        }
    }
    let Some(archive) = archive else {
        return Err(ApiError::missing(&["body", "archive"]));
    };
    if !state.config.registration_plugins {
        return Ok(tuple(
            false,
            "The administrator for this server has not allowed plugin registration.\n\n\
             Please contact the system administrator, or if you are running this server, \
             open the configuration file with `edit config api` and search for 'permissions'.  \
             Under the keys `api:permissions:registration`, \
             you can toggle various registration types.",
        ));
    }
    let raw_attrs = ctx.query.str_or("attributes", "{}");
    let mut attributes = Value::from_json_str(&raw_attrs)
        .map_err(|e| ApiError::exception("json.loads(attributes)", e))?;
    if let Value::Str(s) = &attributes
        && s.starts_with('{')
        && let Ok(v) = Value::from_json_str(s)
    {
        attributes = v;
    }
    let conn = state.instance();
    let mut plugin = PluginRecord {
        name: name.clone(),
        version: ctx.query.opt_str("version"),
        user_id: None,
        attributes: match attributes {
            Value::Map(m) => m,
            _ => Map::new(),
        },
    };
    let plugin_user_id = conn
        .get_plugin_user_id(&plugin)
        .await
        .map_err(|e| ApiError::exception("get_plugin_user_id", e))?;
    let curr_user_id = conn
        .get_user_id(&user)
        .await
        .map_err(|e| ApiError::exception("get_user_id", e))?;
    if let Some(owner) = &plugin_user_id
        && Some(owner) != curr_user_id.as_ref()
    {
        return Ok(tuple(
            false,
            format!("User '{}' cannot edit plugin '{name}'.", user.username),
        ));
    }
    plugin.user_id = curr_user_id;
    // The check above is not atomic with the write: `register_plugin_as_owner` refuses a row
    // another user registered after it (v4's `register_plugin` would move it to this user).
    let res = conn.register_plugin_as_owner(&plugin, false).await;
    if !res.success && res.message == pipe_ops::plugin_owner_conflict(&plugin).message {
        return Ok(tuple(
            false,
            format!("User '{}' cannot edit plugin '{name}'.", user.username),
        ));
    }
    if res.success {
        let path = archive_path(&state, &name);
        let tmp = PathBuf::from(format!("{}.tmp", path.display()));
        let write = async {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(&tmp, &archive).await?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o775)).await?;
            }
            tokio::fs::rename(&tmp, &path).await
        };
        write
            .await
            .map_err(|e| ApiError::exception("Failed to save the plugin archive", e))?;
    }
    Ok(tuple_response(&res))
}

async fn get_plugin(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["plugins:read"]).await?;
    check_visible(&ctx, &name).await?;
    let path = archive_path(&ctx.state, &name);
    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            let mut resp = (StatusCode::OK, bytes).into_response();
            // `FileResponse(path, filename=f'{name}.tar.gz')`: the type guessed from the name.
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-tar"),
            );
            if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{name}.tar.gz\""))
            {
                resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
            }
            Ok(resp)
        }
        Err(_) => Ok(tuple(
            false,
            format!("Archive for plugin '{name}' could not be found."),
        )),
    }
}

async fn get_plugin_attributes(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["plugins:read"]).await?;
    check_visible(&ctx, &name).await?;
    let attrs = ctx
        .state
        .instance()
        .get_plugin_attributes(&record(&name))
        .await
        .map_err(|e| ApiError::exception("get_plugin_attributes", e))?;
    Ok(ok_json(&from_core(&Value::Map(attrs))))
}

async fn get_plugin_version(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["plugins:read"]).await?;
    check_visible(&ctx, &name).await?;
    let version = ctx
        .state
        .instance()
        .get_plugin_version(&record(&name))
        .await
        .map_err(|e| ApiError::exception("get_plugin_version", e))?
        .filter(|v| !v.is_empty());
    Ok(ok_json(&json!(version)))
}

/// The caller when they are walled off (every team of theirs is isolated), else `None`: the
/// repository then serves only the plugins their teams were granted.
async fn hidden_from(ctx: &Ctx) -> Result<Option<crate::access::Access>, ApiError> {
    Ok(crate::access::resolve_optional(&ctx.state, &ctx.headers)
        .await?
        .filter(|caller| caller.principal.is_isolated()))
}

/// `404` for a plugin a walled-off caller may not see.
async fn check_visible(ctx: &Ctx, name: &str) -> Result<(), ApiError> {
    match hidden_from(ctx).await? {
        Some(caller) if !meerschaum_core::access::can_use_plugin(&caller.principal, name) => {
            Err(ApiError::not_found())
        }
        _ => Ok(()),
    }
}

async fn get_plugins(ctx: Ctx) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["plugins:read"]).await?;
    let user_id = ctx.query.opt_int("user_id")?.map(Value::Int);
    let search_term = ctx.query.opt_str("search_term");
    let plugins = ctx
        .state
        .instance()
        .get_plugins(user_id.as_ref(), search_term.as_deref())
        .await
        .map_err(|e| ApiError::exception("get_plugins", e))?;
    // A walled-off caller (when the request names one) sees the plugins of their teams.
    let plugins: Vec<String> = match hidden_from(&ctx).await? {
        Some(caller) => plugins
            .into_iter()
            .filter(|name| meerschaum_core::access::can_use_plugin(&caller.principal, name))
            .collect(),
        None => plugins,
    };
    Ok(ok_json(&json!(plugins)))
}

async fn delete_plugin(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let principal = manager(&ctx.state, &ctx.headers).await?;
    let conn = ctx.state.instance();
    let plugin = record(&name);
    let plugin_user_id = conn
        .get_plugin_user_id(&plugin)
        .await
        .map_err(|e| ApiError::exception("get_plugin_user_id", e))?;
    let Some(owner) = plugin_user_id else {
        return Ok(tuple(false, format!("Plugin '{name}' is not registered.")));
    };
    let curr_user_id = match &principal {
        Principal::User(u) => conn.get_user_id(u).await,
        Principal::Token(t) => match t.id {
            Some(id) => conn.get_token_user_id(&id).await,
            None => Ok(None),
        },
    }
    .map_err(|e| ApiError::exception("get_user_id", e))?;
    if Some(&owner) != curr_user_id.as_ref() {
        let Some(username) = principal.username() else {
            // PARITY: `curr_user.username` on a token raises `AttributeError` (a 500).
            return Err(ApiError::internal());
        };
        return Ok(tuple(
            false,
            format!("User '{username}' cannot delete plugin '{name}'."),
        ));
    }
    let path = archive_path(&ctx.state, &name);
    if path.exists()
        && let Err(e) = tokio::fs::remove_file(&path).await
    {
        return Ok(tuple(
            false,
            format!("Failed to remove archive for plugin '{name}':\n{e}"),
        ));
    }
    let res = conn.delete_plugin(&plugin).await;
    if !res.success {
        return Ok(tuple_response(&res));
    }
    Ok(tuple(
        true,
        format!("Successfully deleted plugin '{name}'."),
    ))
}
