//! Private host-job history follows registered users and teams through name reuse.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};
use meerschaum_jobs::{JobsContext, Launcher};
use meerschaum_server::backends::JobsBackend;
use serde_json::{Value as Json, json};
use tower::ServiceExt;

struct Actions;
#[async_trait::async_trait]
impl meerschaum_server::backends::ActionsBackend for Actions {
    fn list(&self) -> Vec<String> {
        vec!["show".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> meerschaum_core::SuccessTuple {
        panic!("The fixture writes job history through its inert supervisor, never inline actions.")
    }
}

async fn request(
    app: &axum::Router,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<(&str, String)>,
) -> (StatusCode, Json) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(bearer) = bearer {
        builder = builder.header("authorization", bearer);
    }
    let body = match body {
        Some((kind, text)) => {
            builder = builder.header("content-type", kind);
            Body::from(text)
        }
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Json::Null))
}

async fn login(app: &axum::Router, username: &str, password: &str) -> String {
    let (status, response) = request(
        app,
        "POST",
        "/login",
        None,
        Some((
            "application/x-www-form-urlencoded",
            format!("username={username}&password={password}"),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    format!("Bearer {}", response["access_token"].as_str().unwrap())
}

fn attributes(value: Json) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_api_history_rejects_reused_usernames_and_team_names_but_retains_original_owner() {
    let parent = std::env::var_os("MRSM_ROOT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root")
        });
    std::fs::create_dir_all(&parent).unwrap();
    let root = tempfile::Builder::new()
        .prefix("local-history-")
        .tempdir_in(parent)
        .unwrap();
    let instance = Arc::new(
        meerschaum_connector_sql::SqlConnector::from_uri(
            "main",
            &format!("sqlite:///{}", root.path().join("history.db").display()),
        )
        .await
        .unwrap(),
    );
    for (name, kind) in [("alice", None), ("manager", None), ("admin", Some("admin"))] {
        let mut user = User::new(name, Some("synthetic-original-pass"));
        user.user_type = kind.map(str::to_string);
        user.attributes = Some(attributes(
            json!({"role":"operator", "primary_team":"Wetlands"}),
        ));
        assert!(instance.register_user(&mut user).await.success);
    }
    let original_user = User::new("alice", None);
    let original_id = instance.get_user_id(&original_user).await.unwrap().unwrap();
    let mut team = Team::new("Wetlands");
    team.members.insert("alice".into(), TeamRole::Member);
    team.members.insert("manager".into(), TeamRole::Manager);
    assert!(instance.register_team(&mut team).await.success);
    let original_team = team.id.clone().unwrap();
    let context = JobsContext::for_root(root.path())
        .with_launcher(Launcher {
            supervisor: vec!["/usr/bin/true".into()],
            program: vec!["/usr/bin/true".into()],
        })
        .with_launch_environment(vec![
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), root.path().display().to_string()),
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        ]);
    let backend = Arc::new(meerschaum_cli::jobs_backend::LocalJobsBackend::new(
        context.clone(),
    ));
    let mut config = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
    config.actions_non_admin = true;
    config.no_dash = true;
    config.no_webterm = true;
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .actions(Arc::new(Actions))
        .jobs(backend.clone())
        .secret(b"synthetic-local-history-secret-01234567890123456789".to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state.clone());
    let alice = login(&app, "alice", "synthetic-original-pass").await;
    let manager = login(&app, "manager", "synthetic-original-pass").await;
    let admin = login(&app, "admin", "synthetic-original-pass").await;
    let name = "execute-private-local-history";
    let body = json!({"sysargs":["show","version"], "properties":{
        "owner":"admin", "team":"Everyone",
        "_mrsm_owner_binding":{"kind":"user","username":"admin","user_id":999},
        "_mrsm_team_id":"00000000-0000-4000-8000-000000000001",
        "mrsm_execution":{"kind":"temporary_execution"}
    }});
    let (status, created) = request(
        &app,
        "POST",
        &format!("/jobs/{name}"),
        Some(&alice),
        Some(("application/json", body.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(created[0], true, "{created}");
    let internal = backend.get(name).await.unwrap();
    assert_eq!(
        internal["daemon"]["properties"]["_mrsm_owner_binding"]["user_id"],
        serde_json::to_value(&original_id).unwrap()
    );
    assert_eq!(
        internal["daemon"]["properties"]["_mrsm_team_id"],
        original_team
    );
    std::fs::create_dir_all(&context.paths.logs_dir).unwrap();
    std::fs::write(
        context.paths.logs_dir.join(format!("{name}.log")),
        "private-host-history\n",
    )
    .unwrap();
    for caller in [&alice, &manager, &admin] {
        let (status, metadata) =
            request(&app, "GET", &format!("/jobs/{name}"), Some(caller), None).await;
        assert_eq!(status, StatusCode::OK, "{metadata}");
        assert!(!metadata.to_string().contains("_mrsm_owner_binding"));
        assert!(!metadata.to_string().contains("_mrsm_team_id"));
    }
    // Rename directly in the registry: UUID projection remains correct without retagging files.
    assert!(instance.rename_team("Wetlands", "Marsh").await.success);
    team = instance.get_team("Marsh").await.unwrap().unwrap();
    state.access_cache().forget_identities();
    let (_, metadata) = request(&app, "GET", &format!("/jobs/{name}"), Some(&manager), None).await;
    assert_eq!(metadata["daemon"]["properties"]["team"], "Marsh");
    // A password change and departure preserve ownership of already retained history.
    assert!(
        instance
            .edit_user(&User::new("alice", Some("synthetic-changed-pass")))
            .await
            .success
    );
    team.members.shift_remove("alice");
    assert!(instance.edit_team(&team).await.success);
    state.access_cache().forget_identities();
    let alice = login(&app, "alice", "synthetic-changed-pass").await;
    assert_eq!(
        request(&app, "GET", &format!("/jobs/{name}"), Some(&alice), None)
            .await
            .0,
        StatusCode::OK
    );
    // Deletion never changes a captured team UUID into Everyone or a same-name replacement.
    assert!(instance.delete_team(&team.name).await.success);
    backend.retag_team("Wetlands", None).await;
    let mut replacement_team = Team::new("Wetlands");
    replacement_team
        .members
        .insert("manager".into(), TeamRole::Manager);
    replacement_team
        .members
        .insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut replacement_team).await.success);
    assert_ne!(replacement_team.id.as_deref(), Some(original_team.as_str()));
    state.access_cache().forget_identities();
    assert_eq!(
        request(&app, "GET", &format!("/jobs/{name}"), Some(&manager), None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, "GET", &format!("/jobs/{name}"), Some(&alice), None)
            .await
            .0,
        StatusCode::OK
    );
    assert!(instance.delete_user(&original_user).await.success);
    let mut replacement = User::new("alice", Some("synthetic-replacement-pass"));
    replacement.attributes = Some(attributes(
        json!({"role":"operator", "primary_team":"Wetlands"}),
    ));
    assert!(instance.register_user(&mut replacement).await.success);
    assert_ne!(
        instance.get_user_id(&replacement).await.unwrap().unwrap(),
        original_id
    );
    replacement_team
        .members
        .insert("alice".into(), TeamRole::Member);
    assert!(instance.edit_team(&replacement_team).await.success);
    state.access_cache().forget_identities();
    let replacement = login(&app, "alice", "synthetic-replacement-pass").await;
    for (method, suffix) in [
        ("GET", format!("/jobs/{name}")),
        ("GET", format!("/logs/{name}")),
        ("POST", format!("/jobs/{name}/start")),
        ("POST", format!("/jobs/{name}/stop")),
        ("POST", format!("/jobs/{name}/pause")),
        ("DELETE", format!("/jobs/{name}")),
    ] {
        assert_eq!(
            request(&app, method, &suffix, Some(&replacement), None)
                .await
                .0,
            StatusCode::NOT_FOUND,
            "{method} {suffix}"
        );
    }
    let (_, listed) = request(&app, "GET", "/jobs", Some(&replacement), None).await;
    assert!(listed.get(name).is_none());
    let (_, exists) = request(
        &app,
        "GET",
        &format!("/jobs/{name}/exists"),
        Some(&replacement),
        None,
    )
    .await;
    assert_eq!(exists, false);
    // The hidden name must not fall through to a different local operation.
    let current = meerschaum_server::access::resolve(&state, &{
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("authorization", replacement.parse().unwrap());
        headers
    })
    .await
    .unwrap();
    assert!(
        !meerschaum_server::executor::context::scope(Some(Arc::new(current)), backend.delete(name))
            .await
            .success
    );
    assert!(backend.exists(name).await);
    assert_eq!(
        request(&app, "GET", &format!("/jobs/{name}"), Some(&admin), None)
            .await
            .0,
        StatusCode::OK
    );
    assert!(backend.delete(name).await.success);
}
