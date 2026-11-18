//! Main callback authority is a short-lived launch capability, never a reusable user/admin JWT.
mod common;

use axum::http::{HeaderMap, StatusCode};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::users::{Expiration, Token, User};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_server::auth::{self, Principal};
use meerschaum_server::executor::callbacks::{self, Purpose};
use serde_json::json;
use uuid::Uuid;

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
fn headers(authorization: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", authorization.parse().unwrap());
    headers
}

#[tokio::test]
async fn suspended_jobs_resume_only_the_original_source_and_invalidate_old_bearers() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(json!({"role":"operator"})));
    let app = meerschaum_server::app(state.clone());
    let caller = access(&state, &app, "alice").await;
    let grant = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        None,
        &["jobs:read"],
        Purpose::Jobs,
    )
    .await
    .unwrap();
    let old = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    callbacks::suspend(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    assert!(
        callbacks::renew(&state, grant.id, grant.launch_id)
            .await
            .is_err()
    );
    assert!(
        callbacks::credential(&state, grant.id, grant.launch_id)
            .await
            .is_err()
    );
    assert!(
        callbacks::resume(&state, grant.id, Uuid::new_v4())
            .await
            .is_err()
    );
    let next = callbacks::resume(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    assert_ne!(next.id, grant.id);
    assert_eq!(next.launch_id, grant.launch_id);
    assert_eq!(next.permissions, grant.permissions);
    assert!(
        callbacks::renew(&state, grant.id, grant.launch_id)
            .await
            .is_err()
    );
    assert_eq!(
        common::send(
            &app,
            common::req("GET", "/jobs", Some(&format!("Bearer {}", old.token)), None)
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
    let credential = callbacks::credential(&state, next.id, next.launch_id)
        .await
        .unwrap();
    assert_eq!(
        common::send(
            &app,
            common::req(
                "GET",
                "/jobs",
                Some(&format!("Bearer {}", credential.token)),
                None
            )
        )
        .await
        .status,
        StatusCode::OK
    );
    // Canonical persistence of the new ID lets main remove the old inactive recovery proof.
    callbacks::revoke(&state, grant.id).await.unwrap();
    assert!(
        callbacks::resume(&state, grant.id, grant.launch_id)
            .await
            .is_err()
    );
    callbacks::suspend(&state, next.id, next.launch_id)
        .await
        .unwrap();
    assert!(
        instance
            .edit_user(&User::new("alice", Some("new-pass")))
            .await
            .success
    );
    assert!(
        callbacks::resume(&state, next.id, next.launch_id)
            .await
            .is_err()
    );
}
async fn access(
    state: &meerschaum_server::AppState,
    app: &axum::Router,
    user: &str,
) -> meerschaum_server::access::Access {
    let bearer = common::login(app, user, "pass", None).await;
    meerschaum_server::access::resolve(state, &headers(&bearer))
        .await
        .unwrap()
}

#[tokio::test]
async fn team_boundary_precedes_same_owner_and_admin_and_all_side_routes_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role": "operator", "primary_team": "Harbor"})),
    );
    let mut harbor = Team::new("Harbor");
    harbor.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut harbor).await.success);
    let mut forest = Team::new("Forest");
    forest.members.insert("alice".into(), TeamRole::Manager);
    assert!(instance.register_team(&mut forest).await.success);
    for (name, team) in [("harbor_private", "Harbor"), ("forest_private", "Forest")] {
        let pipe = Pipe::builder("manual:fixture", name)
            .instance("sql:fixture")
            .parameters(map(
                json!({"access":{"owner":"alice", "team":team, "visibility":"private"}}),
            ))
            .build()
            .unwrap();
        assert!(instance.register_pipe(&pipe).await.success);
    }
    let app = meerschaum_server::app(state.clone());
    let alice = access(&state, &app, "alice").await;
    let team_id = Uuid::parse_str(harbor.id.as_deref().unwrap()).unwrap();
    let grant = callbacks::register(
        &state,
        &alice,
        Uuid::new_v4(),
        Some(team_id),
        &["pipes:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let bearer = format!("Bearer {}", credential.token);
    let listing = common::send(&app, common::req("GET", "/pipes/keys", Some(&bearer), None)).await;
    assert_eq!(listing.status, StatusCode::OK, "{}", listing.text());
    assert!(
        listing.text().contains("harbor_private"),
        "{}",
        listing.text()
    );
    assert!(
        !listing.text().contains("forest_private"),
        "{}",
        listing.text()
    );
    for (method, path) in [
        ("POST", "/login"),
        ("GET", "/users"),
        ("GET", "/teams"),
        ("POST", "/tokens/register"),
        ("POST", "/actions/show"),
        ("POST", "/execute"),
        ("GET", "/mcp"),
        ("POST", "/plugins/test"),
        ("GET", "/custom-plugin-route"),
        ("GET", "/%75sers"),
        ("GET", "/_mrsm/native-pages/test"),
    ] {
        let response = common::send(&app, common::req(method, path, Some(&bearer), None)).await;
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{method} {path}: {}",
            response.text()
        );
    }
    let console = axum::Router::new().route(
        "/dash/secret",
        axum::routing::get(|| async { "must never run" }),
    );
    let combined = meerschaum_server::app_with_console(state.clone(), console);
    assert_eq!(
        common::send(
            &combined,
            common::req("GET", "/dash/secret", Some(&bearer), None)
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
    let admin = access(&state, &app, "admin").await;
    let admin_grant = callbacks::register(
        &state,
        &admin,
        Uuid::new_v4(),
        Some(team_id),
        &["pipes:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, admin_grant.id, admin_grant.launch_id)
        .await
        .unwrap();
    let headers = headers(&format!("Bearer {}", credential.token));
    let principal = auth::manager(&state, &headers).await.unwrap();
    assert!(matches!(principal, Principal::Execution(_)));
    assert!(principal.as_user().is_none());
    assert!(!auth::is_admin(&state, Some(&principal)).await);
    assert!(
        auth::grantable_scopes(&state, Some(&principal))
            .await
            .is_err()
    );
    let access = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    assert!(!access.is_admin() && !access.full_admin());
    assert_eq!(access.principal.team_boundary.as_deref(), Some("Harbor"));
}

#[tokio::test]
async fn live_role_password_and_registration_changes_reach_existing_credentials_immediately() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(json!({"role":"operator"})));
    let app = meerschaum_server::app(state.clone());
    let caller = access(&state, &app, "alice").await;
    let grant = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        None,
        &["pipes:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let bearer = format!("Bearer {}", credential.token);
    assert_eq!(
        common::send(&app, common::req("GET", "/pipes/keys", Some(&bearer), None))
            .await
            .status,
        StatusCode::OK
    );
    instance.seed_user_scopes("alice", &["jobs:read"]);
    assert_eq!(
        common::send(&app, common::req("GET", "/pipes/keys", Some(&bearer), None))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
    instance.seed_user_attributes("alice", map(json!({"role":"operator"})));
    let changed = User::new("alice", Some("new-pass"));
    assert!(instance.edit_user(&changed).await.success);
    assert_eq!(
        common::send(&app, common::req("GET", "/pipes/keys", Some(&bearer), None))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
    // A request authenticated before a long prepare cannot adopt the new password binding.
    assert!(
        callbacks::register(
            &state,
            &caller,
            Uuid::new_v4(),
            None,
            &["pipes:read"],
            Purpose::Command
        )
        .await
        .is_err()
    );
    assert!(
        instance
            .delete_user(&User::new("alice", None))
            .await
            .success
    );
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(json!({"role":"operator"})));
    assert_eq!(
        common::send(&app, common::req("GET", "/version", Some(&bearer), None))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn origin_token_narrowing_expiry_and_invalidation_are_preserved() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(json!({"role":"operator"})));
    let mut token = Token::new(
        Some("synthetic-client"),
        Expiration::Never,
        Some(vec!["pipes:read".into(), "jobs:read".into()]),
        366,
        chrono::Utc::now(),
    );
    token.user = Some(User::new("alice", None));
    assert!(instance.register_token(&mut token).await.success);
    let app = meerschaum_server::app(state.clone());
    let api_key = token.get_api_key();
    let caller = meerschaum_server::access::resolve(&state, &headers(&format!("Bearer {api_key}")))
        .await
        .unwrap();
    let grant = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        None,
        &["jobs:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let bearer = format!("Bearer {}", credential.token);
    assert_eq!(
        common::send(&app, common::req("GET", "/pipes/keys", Some(&bearer), None))
            .await
            .status,
        StatusCode::OK
    );
    token.scopes = vec!["jobs:read".into()];
    assert!(instance.edit_token(&token).await.success);
    assert_eq!(
        common::send(&app, common::req("GET", "/pipes/keys", Some(&bearer), None))
            .await
            .status,
        StatusCode::FORBIDDEN
    );
    token.expiration = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
    assert!(instance.edit_token(&token).await.success);
    assert_eq!(
        common::send(&app, common::req("GET", "/version", Some(&bearer), None))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
    token.expiration = None;
    assert!(instance.edit_token(&token).await.success);
    let before_password_change = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        None,
        &["jobs:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let old = callbacks::credential(
        &state,
        before_password_change.id,
        before_password_change.launch_id,
    )
    .await
    .unwrap();
    assert!(
        instance
            .edit_user(&User::new("alice", Some("changed-owner-pass")))
            .await
            .success
    );
    assert!(
        auth::load_token(&state, &token.id.unwrap().to_string())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        common::send(
            &app,
            common::req(
                "GET",
                "/version",
                Some(&format!("Bearer {}", old.token)),
                None
            )
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
    token.is_valid = false;
    assert!(instance.edit_token(&token).await.success);
    assert_eq!(
        common::send(&app, common::req("GET", "/version", Some(&bearer), None))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn jobs_only_purpose_launch_binding_revocation_and_corrupt_storage_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(json!({"role":"operator"})));
    let app = meerschaum_server::app(state.clone());
    let caller = access(&state, &app, "alice").await;
    let grant = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        None,
        &["jobs:read"],
        Purpose::Jobs,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    assert!(format!("{credential:?}").find(&credential.token).is_none());
    let bearer = format!("Bearer {}", credential.token);
    assert_eq!(
        common::send(&app, common::req("GET", "/version", Some(&bearer), None))
            .await
            .status,
        StatusCode::OK
    );
    assert_eq!(
        common::send(&app, common::req("GET", "/pipes/keys", Some(&bearer), None))
            .await
            .status,
        StatusCode::FORBIDDEN
    );
    assert!(
        callbacks::credential(&state, grant.id, Uuid::new_v4())
            .await
            .is_err()
    );
    let token = bearer.strip_prefix("Bearer ").unwrap();
    let mut claims = auth::jwt_decode(token, state.secret(), chrono::Utc::now()).unwrap();
    let mut wrong_audience = claims.clone();
    wrong_audience.insert("aud".into(), json!("another-api"));
    let mut expired = claims.clone();
    expired.insert("iat".into(), json!(chrono::Utc::now().timestamp() - 31));
    expired.insert("exp".into(), json!(chrono::Utc::now().timestamp() - 1));
    let mut overlong = claims.clone();
    overlong.insert("iat".into(), json!(chrono::Utc::now().timestamp() - 60));
    for invalid_claims in [wrong_audience, expired, overlong] {
        let invalid = auth::jwt_encode(
            &map(serde_json::Value::Object(invalid_claims)),
            state.secret(),
        );
        assert_eq!(
            common::send(
                &app,
                common::req("GET", "/version", Some(&format!("Bearer {invalid}")), None)
            )
            .await
            .status,
            StatusCode::UNAUTHORIZED
        );
    }
    claims.insert("mrsm_launch".into(), json!(Uuid::new_v4()));
    let invalid = auth::jwt_encode(&map(serde_json::Value::Object(claims)), state.secret());
    assert_eq!(
        common::send(
            &app,
            common::req("GET", "/version", Some(&format!("Bearer {invalid}")), None)
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
    callbacks::revoke(&state, grant.id).await.unwrap();
    assert!(
        callbacks::renew(&state, grant.id, grant.launch_id)
            .await
            .is_err()
    );
    assert_eq!(
        common::send(&app, common::req("GET", "/version", Some(&bearer), None))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
    let replacement = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        None,
        &["jobs:read"],
        Purpose::Jobs,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, replacement.id, replacement.launch_id)
        .await
        .unwrap();
    std::fs::write(
        root.path()
            .join(format!("executors/callbacks/{}.json", replacement.id)),
        "corrupt",
    )
    .unwrap();
    assert_eq!(
        common::send(
            &app,
            common::req(
                "GET",
                "/version",
                Some(&format!("Bearer {}", credential.token)),
                None
            )
        )
        .await
        .status,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn retiring_a_generation_or_changing_placement_revokes_existing_callback_bearers() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"operator", "primary_team":"Harbor"})),
    );
    let mut team = Team::new("Harbor");
    team.members.insert("alice".into(), TeamRole::Member);
    team.attributes = map(
        json!({"isolated":true,"plugins_write":true,"connectors":["sql:fixture"],"instance":"sql:fixture",
        "execution":{"mode":"api","connector":"api:executor"}}),
    );
    assert!(instance.register_team(&mut team).await.success);
    let id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    let app = meerschaum_server::app(state.clone());
    let caller = access(&state, &app, "alice").await;
    // A selected remote executor cannot mint callbacks until main registers its generation.
    assert!(
        callbacks::register(
            &state,
            &caller,
            Uuid::new_v4(),
            Some(id),
            &["pipes:read"],
            Purpose::Command
        )
        .await
        .is_err()
    );
    callbacks::set_generation(root.path(), id, Some(Uuid::new_v4())).unwrap();
    let grant = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        Some(id),
        &["pipes:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let bearer = format!("Bearer {}", credential.token);
    assert_eq!(
        common::send(&app, common::req("GET", "/pipes/keys", Some(&bearer), None))
            .await
            .status,
        StatusCode::OK
    );
    callbacks::set_generation(root.path(), id, Some(Uuid::new_v4())).unwrap();
    assert_eq!(
        common::send(&app, common::req("GET", "/version", Some(&bearer), None))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
    let next = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        Some(id),
        &["pipes:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, next.id, next.launch_id)
        .await
        .unwrap();
    let bearer = format!("Bearer {}", credential.token);
    team.attributes.shift_remove("execution");
    assert!(instance.edit_team(&team).await.success);
    assert_eq!(
        common::send(&app, common::req("GET", "/version", Some(&bearer), None))
            .await
            .status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_child_grant_keeps_the_parent_scope_and_team_ceiling() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(json!({"role":"operator"})));
    let mut harbor = Team::new("Harbor");
    harbor.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut harbor).await.success);
    let harbor_id = Uuid::parse_str(harbor.id.as_deref().unwrap()).unwrap();
    let mut forest = Team::new("Forest");
    forest.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut forest).await.success);
    let forest_id = Uuid::parse_str(forest.id.as_deref().unwrap()).unwrap();
    let app = meerschaum_server::app(state.clone());
    let narrow = common::login(&app, "alice", "pass", Some("pipes:read")).await;
    let caller = meerschaum_server::access::resolve(&state, &headers(&narrow))
        .await
        .unwrap();
    let grant = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        Some(harbor_id),
        &["pipes:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let current = meerschaum_server::access::resolve(
        &state,
        &headers(&format!("Bearer {}", credential.token)),
    )
    .await
    .unwrap();
    assert!(
        callbacks::register(
            &state,
            &current,
            Uuid::new_v4(),
            Some(harbor_id),
            &["jobs:read"],
            Purpose::Jobs
        )
        .await
        .is_err()
    );
    assert!(
        callbacks::register(
            &state,
            &current,
            Uuid::new_v4(),
            Some(forest_id),
            &["pipes:read"],
            Purpose::Command
        )
        .await
        .is_err()
    );
    let child = callbacks::register(
        &state,
        &current,
        Uuid::new_v4(),
        Some(harbor_id),
        &["pipes:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    assert_eq!(child.permissions, grant.permissions);
    assert_eq!(child.depth, 1);
    // A completed parent does not silently cancel a separately accepted persistent child job.
    callbacks::revoke(&state, grant.id).await.unwrap();
    assert!(
        callbacks::credential(&state, child.id, child.launch_id)
            .await
            .is_ok()
    );
}
