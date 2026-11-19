//! Exact archive capabilities do not become ordinary Main API principals.
mod common;

use axum::http::{HeaderMap, StatusCode};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::{callbacks, plugin_delivery, plugins};
use serde_json::json;
use uuid::Uuid;

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

struct Fixture {
    _root: tempfile::TempDir,
    state: meerschaum_server::AppState,
    instance: std::sync::Arc<common::MemoryInstance>,
    team: Team,
    team_id: Uuid,
    generation: Uuid,
    grant: callbacks::Grant,
    archive: Vec<u8>,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_scope("jobs:execute", callbacks::Purpose::Jobs).await
    }

    async fn with_scope(required: &str, purpose: callbacks::Purpose) -> Self {
        let root = tempfile::tempdir().unwrap();
        let (state, instance) = common::state_with(common::config(root.path()));
        instance.seed_user("alice", "pass", None);
        instance.seed_user_attributes(
            "alice",
            map(json!({"role":"operator","primary_team":"Harbor"})),
        );
        let mut team = Team::new("Harbor");
        team.attributes = map(json!({"isolated":true,"plugins_write":true,"plugins":["probe"]}));
        team.members.insert("alice".into(), TeamRole::Member);
        let registered = instance.register_team(&mut team).await;
        assert!(registered.success, "{registered}");
        let team_id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
        let generation = Uuid::new_v4();
        callbacks::set_generation(root.path(), team_id, Some(generation)).unwrap();
        let app = meerschaum_server::app(state.clone());
        let bearer = common::login(&app, "alice", "pass", None).await;
        let mut headers = HeaderMap::new();
        headers.insert("authorization", bearer.parse().unwrap());
        let source = meerschaum_server::access::require_fresh(&state, &headers, &[required])
            .await
            .unwrap();
        let archive = b"synthetic-exact-plugin-archive-v1".to_vec();
        let ceiling = meerschaum_server::executor::source_ceiling::Ceiling {
            sources: std::collections::BTreeMap::from([(
                "probe".into(),
                meerschaum_server::executor::source_ceiling::Fingerprint::Archive {
                    source_kind: plugins::SourceKind::PythonModule,
                    archive_sha256: plugins::sha256(&archive),
                },
            )]),
        };
        let grant = callbacks::register_with_sources(
            &state,
            &source,
            Uuid::new_v4(),
            Some(team_id),
            &[required],
            purpose,
            ceiling,
        )
        .await
        .unwrap();
        std::fs::create_dir_all(&state.config.plugins_archives_path).unwrap();
        std::fs::write(
            state.config.plugins_archives_path.join("probe.tar.gz"),
            &archive,
        )
        .unwrap();
        Self {
            _root: root,
            state,
            instance,
            team,
            team_id,
            generation,
            grant,
            archive,
        }
    }

    async fn credential(&self) -> plugin_delivery::Credential {
        plugin_delivery::issue(
            &self.state,
            self.grant.id,
            self.grant.launch_id,
            self.team_id,
            self.generation,
            "probe",
            &plugins::sha256(&self.archive),
        )
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn read_only_extension_source_can_pull_only_under_its_command_purpose_and_exact_ceiling() {
    for (required, purpose, allowed) in [
        ("plugins:read", callbacks::Purpose::Command, true),
        ("plugins:read", callbacks::Purpose::Jobs, false),
        ("pipes:read", callbacks::Purpose::Command, false),
    ] {
        let fixture = Fixture::with_scope(required, purpose).await;
        let result = plugin_delivery::issue(
            &fixture.state,
            fixture.grant.id,
            fixture.grant.launch_id,
            fixture.team_id,
            fixture.generation,
            "probe",
            &plugins::sha256(&fixture.archive),
        )
        .await;
        if !allowed {
            assert!(
                result.is_err(),
                "An unrelated callback purpose/scope minted plugin archive authority"
            );
            continue;
        }
        let credential = result.unwrap();
        let app = meerschaum_server::app(fixture.state.clone());
        let response = common::send(
            &app,
            common::req(
                "GET",
                "/plugins/probe",
                Some(credential.authorization()),
                None,
            ),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert_eq!(response.body.as_ref(), fixture.archive.as_slice());
        assert!(
            plugin_delivery::issue(
                &fixture.state,
                fixture.grant.id,
                fixture.grant.launch_id,
                fixture.team_id,
                fixture.generation,
                "ungranted",
                &plugins::sha256(&fixture.archive)
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn exact_plugin_pull_is_bounded_authenticated_and_never_adopts_a_general_principal() {
    let fixture = Fixture::new().await;
    let credential = fixture.credential().await;
    assert!(!format!("{credential:?}").contains("Mrsm-Plugin"));
    let mut headers = HeaderMap::new();
    headers.insert("authorization", credential.authorization().parse().unwrap());
    assert!(
        meerschaum_server::auth::manager(&fixture.state, &headers)
            .await
            .is_err()
    );
    let app = meerschaum_server::app(fixture.state.clone());
    let response = common::send(
        &app,
        common::req(
            "GET",
            "/plugins/probe",
            Some(credential.authorization()),
            None,
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.body.as_ref(), fixture.archive.as_slice());
    assert_eq!(response.headers["cache-control"], "no-store");
    let token = credential.authorization().split_once(' ').unwrap().1;
    let tampered = format!("Mrsm-Plugin X{}", &token[1..]);
    assert_eq!(
        common::send(
            &app,
            common::req("GET", "/plugins/probe", Some(&tampered), None)
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
    for (method, path) in [
        ("GET", "/plugins"),
        ("GET", "/plugins/other"),
        ("GET", "/plugins/probe/attributes"),
        ("GET", "/plugins/probe/version"),
        ("POST", "/plugins/probe"),
        ("DELETE", "/plugins/probe"),
        ("GET", "/plugins/probe?instance_keys=sql:other"),
        ("GET", "/plugins/%70robe"),
        ("GET", "/jobs"),
        ("GET", "/version"),
        ("GET", "/users"),
        ("POST", "/login"),
        ("POST", "/execute"),
        ("POST", "/actions/show"),
        ("POST", "/mcp"),
    ] {
        let refused = common::send(
            &app,
            common::req(method, path, Some(credential.authorization()), None),
        )
        .await;
        assert_eq!(
            refused.status,
            StatusCode::UNAUTHORIZED,
            "{method} {path}: {}",
            refused.text()
        );
    }
    let mut cookie = common::req(
        "GET",
        "/plugins/probe",
        Some(credential.authorization()),
        None,
    );
    cookie
        .headers_mut()
        .insert("cookie", "mrsm-session-id=ambient-cookie".parse().unwrap());
    assert_eq!(
        common::send(&app, cookie).await.status,
        StatusCode::UNAUTHORIZED
    );
    let wrong = plugin_delivery::issue(
        &fixture.state,
        fixture.grant.id,
        fixture.grant.launch_id,
        fixture.team_id,
        fixture.generation,
        "probe",
        &"f".repeat(64),
    )
    .await;
    assert!(
        wrong.is_err(),
        "A digest outside the captured source ceiling minted archive authority"
    );
    let archive_path = fixture
        .state
        .config
        .plugins_archives_path
        .join("probe.tar.gz");
    std::fs::write(&archive_path, b"changed-after-exact-capability-issue").unwrap();
    assert_eq!(
        common::send(
            &app,
            common::req(
                "GET",
                "/plugins/probe",
                Some(credential.authorization()),
                None
            )
        )
        .await
        .status,
        StatusCode::CONFLICT
    );
    std::fs::write(&archive_path, &fixture.archive).unwrap();
    let ordinary = callbacks::credential(&fixture.state, fixture.grant.id, fixture.grant.launch_id)
        .await
        .unwrap();
    assert_eq!(
        common::send(
            &app,
            common::req(
                "GET",
                "/plugins/probe",
                Some(&format!("Bearer {}", ordinary.token)),
                None
            )
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
    assert!(
        plugin_delivery::issue(
            &fixture.state,
            fixture.grant.id,
            fixture.grant.launch_id,
            Uuid::new_v4(),
            fixture.generation,
            "probe",
            &plugins::sha256(&fixture.archive)
        )
        .await
        .is_err()
    );
    assert!(
        plugin_delivery::issue(
            &fixture.state,
            fixture.grant.id,
            fixture.grant.launch_id,
            fixture.team_id,
            fixture.generation,
            "ungranted",
            &plugins::sha256(&fixture.archive)
        )
        .await
        .is_err()
    );
    let mut cfg = (*fixture.state.config).clone();
    cfg.routes_allowlist = vec!["version".into()];
    let restricted = meerschaum_server::AppState::builder(cfg, fixture.instance.clone())
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    let response = common::send(
        &meerschaum_server::app(restricted),
        common::req(
            "GET",
            "/plugins/probe",
            Some(credential.authorization()),
            None,
        ),
    )
    .await;
    assert_ne!(
        response.status,
        StatusCode::OK,
        "Delivery bypassed the Main route allowlist."
    );
}

#[tokio::test]
async fn every_plugin_pull_rechecks_original_source_current_grants_and_generation() {
    let mut fixture = Fixture::new().await;
    let credential = fixture.credential().await;
    let app = meerschaum_server::app(fixture.state.clone());
    fixture
        .team
        .attributes
        .insert("plugins".into(), Value::List(vec![]));
    assert!(fixture.instance.edit_team(&fixture.team).await.success);
    assert_eq!(
        common::send(
            &app,
            common::req(
                "GET",
                "/plugins/probe",
                Some(credential.authorization()),
                None
            )
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
    fixture
        .team
        .attributes
        .insert("plugins".into(), Value::List(vec!["probe".into()]));
    assert!(fixture.instance.edit_team(&fixture.team).await.success);
    assert_eq!(
        common::send(
            &app,
            common::req(
                "GET",
                "/plugins/probe",
                Some(credential.authorization()),
                None
            )
        )
        .await
        .status,
        StatusCode::OK
    );
    callbacks::set_generation(fixture._root.path(), fixture.team_id, Some(Uuid::new_v4())).unwrap();
    assert_eq!(
        common::send(
            &app,
            common::req(
                "GET",
                "/plugins/probe",
                Some(credential.authorization()),
                None
            )
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );

    let fixture = Fixture::new().await;
    let credential = fixture.credential().await;
    assert!(
        fixture
            .instance
            .edit_user(&meerschaum_core::users::User::new(
                "alice",
                Some("new-pass")
            ))
            .await
            .success
    );
    assert_eq!(
        common::send(
            &meerschaum_server::app(fixture.state.clone()),
            common::req(
                "GET",
                "/plugins/probe",
                Some(credential.authorization()),
                None
            )
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );

    let fixture = Fixture::new().await;
    let credential = fixture.credential().await;
    assert!(
        fixture
            .instance
            .delete_user(&meerschaum_core::users::User::new("alice", None))
            .await
            .success
    );
    fixture.instance.seed_user("alice", "pass", None);
    fixture.instance.seed_user_attributes(
        "alice",
        map(json!({"role":"operator","primary_team":"Harbor"})),
    );
    assert_eq!(
        common::send(
            &meerschaum_server::app(fixture.state.clone()),
            common::req(
                "GET",
                "/plugins/probe",
                Some(credential.authorization()),
                None
            )
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );

    let fixture = Fixture::new().await;
    let credential = fixture.credential().await;
    callbacks::revoke(&fixture.state, fixture.grant.id)
        .await
        .unwrap();
    assert_eq!(
        common::send(
            &meerschaum_server::app(fixture.state.clone()),
            common::req(
                "GET",
                "/plugins/probe",
                Some(credential.authorization()),
                None
            )
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
}
