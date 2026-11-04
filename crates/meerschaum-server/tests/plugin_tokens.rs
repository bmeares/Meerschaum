//! The rules of the tokens native plugins mint and revoke for their callers
//! (`meerschaum_server::plugin_tokens`, behind the SDK's `host::mint_token` /
//! `host::revoke_token`), over the in-memory instance. All data is synthetic.

mod common;

use std::collections::BTreeSet;

use axum::http::StatusCode;
use meerschaum_server::auth::{Principal, load_token, load_user};
use meerschaum_server::plugin_tokens::{self, MintedToken};
use meerschaum_server::{AppState, app};

/// The custom scopes plugin `devices` declares (and one core scope, which never counts).
fn allowed() -> BTreeSet<String> {
    plugin_tokens::plugin_scopes(["devices:ingest", "devices:admin", "pipes:read"])
}

fn strings(scopes: &[&str]) -> Vec<String> {
    scopes.iter().map(|s| s.to_string()).collect()
}

async fn user(state: &AppState, name: &str) -> Principal {
    load_user(state, name).await.unwrap().unwrap()
}

async fn token(state: &AppState, minted: &MintedToken) -> Option<Principal> {
    load_token(state, &minted.id.to_string()).await.unwrap()
}

async fn mint(
    state: &AppState,
    caller: Option<&Principal>,
    granted: &[&str],
    label: &str,
    scopes: &[&str],
) -> Result<MintedToken, String> {
    plugin_tokens::mint(
        state,
        caller,
        &strings(granted),
        "devices",
        &allowed(),
        label,
        &strings(scopes),
        None,
    )
    .await
}

async fn revoke(state: &AppState, caller: &Principal, id: &str) -> Result<String, String> {
    plugin_tokens::revoke(state, Some(caller), "devices", &allowed(), id)
        .await
        .map(|tuple| tuple.message)
}

/// The status of `GET /pipes` and `POST /tokens/validate` with an API key.
async fn statuses(state: &AppState, api_key: &str) -> (StatusCode, StatusCode) {
    let app = app(state.clone());
    let auth = format!("Bearer {api_key}");
    let pipes = common::send(&app, common::req("GET", "/pipes", Some(&auth), None)).await;
    let valid = common::send(
        &app,
        common::req("POST", "/tokens/validate", Some(&auth), None),
    )
    .await;
    (pipes.status, valid.status)
}

#[tokio::test]
async fn a_plugin_mints_its_own_scopes_for_its_caller_and_rotates_them() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _instance) = common::state_with(common::config(dir.path()));
    let admin = user(&state, "admin").await;

    // Provisioning: an administrator's request mints a device's token.
    let first = mint(
        &state,
        Some(&admin),
        &["*"],
        "device-17",
        &["devices:ingest"],
    )
    .await
    .unwrap();
    assert_eq!(first.label, "device-17");
    assert_eq!(first.scopes, ["devices:ingest"]);
    assert!(first.api_key.starts_with("mrsm-key:"));
    let json = first.to_json();
    assert_eq!(json["id"], first.id.to_string());
    assert_eq!(json["secret"], first.secret);
    let Some(Principal::Token(model)) = token(&state, &first).await else {
        panic!("the minted token is registered");
    };
    // Owned by the calling user, with exactly the scopes asked for.
    let Principal::User(admin_user) = &admin else {
        unreachable!()
    };
    assert_eq!(model.user_id, admin_user.user_id);
    assert_eq!(model.scopes, ["devices:ingest"]);
    // The key authenticates and holds no core scope.
    assert_eq!(
        statuses(&state, &first.api_key).await,
        (StatusCode::FORBIDDEN, StatusCode::OK)
    );

    // Rotation: the device's own request (its token holds only the plugin scope) mints its
    // successor, owned by the same user, then revokes itself.
    let device = token(&state, &first).await.unwrap();
    let second = mint(
        &state,
        Some(&device),
        &["devices:ingest"],
        "device-17",
        &["devices:ingest"],
    )
    .await
    .unwrap();
    let Some(Principal::Token(successor)) = token(&state, &second).await else {
        panic!("the successor is registered");
    };
    assert_eq!(successor.user_id, admin_user.user_id);
    let revoked = revoke(&state, &device, &first.id.to_string())
        .await
        .unwrap();
    assert!(revoked.contains(&first.id.to_string()), "{revoked}");
    // The next request with the old token is refused; the successor works.
    assert!(token(&state, &first).await.is_none());
    assert_eq!(
        statuses(&state, &first.api_key).await.1,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(statuses(&state, &second.api_key).await.1, StatusCode::OK);
    // A revoked token can no longer act at all, not even to mint.
    let error = mint(
        &state,
        Some(&device),
        &["devices:ingest"],
        "device-17",
        &["devices:ingest"],
    )
    .await
    .unwrap_err();
    assert!(error.contains("revoked"), "{error}");
}

#[tokio::test]
async fn a_plugin_never_mints_or_revokes_beyond_its_own_scopes_and_caller() {
    let dir = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(dir.path()));
    instance.seed_user("eve", "pass", None);
    instance.seed_user_scopes("eve", &["devices:ingest", "pipes:read"]);
    instance.seed_user("mallory", "pass", None);
    instance.seed_user_scopes("mallory", &["pipes:read"]);
    let admin = user(&state, "admin").await;
    let eve = user(&state, "eve").await;
    let mallory = user(&state, "mallory").await;

    // `*`, a core scope, another plugin's scope, none at all, and no label: refused.
    for scopes in [
        &["*"][..],
        &["pipes:read"],
        &["devices:ingest", "users:write"],
        &["weather:read"],
        &[],
        &[" "],
    ] {
        let error = mint(&state, Some(&admin), &["*"], "device-1", scopes)
            .await
            .unwrap_err();
        assert!(error.contains("devices"), "{scopes:?}: {error}");
    }
    let error = mint(&state, Some(&admin), &["*"], " ", &["devices:ingest"])
        .await
        .unwrap_err();
    assert!(error.contains("label"), "{error}");
    let past = chrono::Utc::now() - chrono::Duration::hours(1);
    let error = plugin_tokens::mint(
        &state,
        Some(&admin),
        &strings(&["*"]),
        "devices",
        &allowed(),
        "device-1",
        &strings(&["devices:ingest"]),
        Some(past),
    )
    .await
    .unwrap_err();
    assert!(error.contains("future"), "{error}");

    // The caller must hold each scope it grants: on the instance, and in this request.
    let error = mint(
        &state,
        Some(&mallory),
        &["pipes:read"],
        "device-1",
        &["devices:ingest"],
    )
    .await
    .unwrap_err();
    assert!(error.contains("does not hold"), "{error}");
    let error = mint(
        &state,
        Some(&eve),
        &["pipes:read"],
        "device-1",
        &["devices:ingest"],
    )
    .await
    .unwrap_err();
    assert!(error.contains("does not hold"), "{error}");
    let eves = mint(
        &state,
        Some(&eve),
        &["devices:ingest", "pipes:read"],
        "device-2",
        &["devices:ingest"],
    )
    .await
    .unwrap();

    // Anonymous callers and `--no-auth` have no identity to own a token.
    let error = mint(&state, None, &["*"], "device-1", &["devices:ingest"])
        .await
        .unwrap_err();
    assert!(error.contains("anonymous"), "{error}");
    let mut open = common::config(dir.path());
    open.no_auth = true;
    let (open, _) = common::state_with(open);
    let error = mint(&open, Some(&admin), &["*"], "device-1", &["devices:ingest"])
        .await
        .unwrap_err();
    assert!(error.contains("--no-auth"), "{error}");

    // Revoking another user's token, a token with a core scope, a missing id and a malformed id
    // all get the same refusal (nothing tells them apart).
    let admins = mint(
        &state,
        Some(&admin),
        &["*"],
        "device-3",
        &["devices:ingest"],
    )
    .await
    .unwrap();
    let app = app(state.clone());
    let login = common::login(&app, "admin", "pass", None).await;
    let registered = common::send(
        &app,
        common::req(
            "POST",
            "/tokens/register",
            Some(&login),
            Some((
                "application/json",
                br#"{"label": "ops", "scopes": ["devices:ingest", "pipes:read"]}"#.to_vec(),
            )),
        ),
    )
    .await;
    assert_eq!(registered.status, StatusCode::OK, "{}", registered.text());
    let core = registered.json()["id"].as_str().unwrap().to_string();
    let missing = uuid::Uuid::new_v4().to_string();
    let refusal = |id: &str| {
        format!(
            "Plugin 'devices' may not revoke token '{id}': a plugin revokes only its caller's \
             own tokens that carry nothing but the plugin's own scopes."
        )
    };
    for (caller, id) in [
        (&eve, admins.id.to_string()),
        (&admin, eves.id.to_string()),
        (&admin, core.clone()),
        (&admin, missing.clone()),
        (&admin, "not-a-token".to_string()),
    ] {
        assert_eq!(revoke(&state, caller, &id).await.unwrap_err(), refusal(&id));
    }
    // ... and changes nothing.
    assert!(token(&state, &admins).await.is_some());
    assert!(token(&state, &eves).await.is_some());
    // Each owner revokes its own.
    revoke(&state, &eve, &eves.id.to_string()).await.unwrap();
    revoke(&state, &admin, &admins.id.to_string())
        .await
        .unwrap();
    assert!(token(&state, &eves).await.is_none());
}
