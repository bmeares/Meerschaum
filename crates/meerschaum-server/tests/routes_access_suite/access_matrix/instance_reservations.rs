//! Untagged registrations on instances reserved for isolated teams.

use super::*;
use meerschaum_server::backends::{PipelinesBackend, PlanError, PlanOptions, TeamEvents};
use std::sync::RwLock;

const RESERVED: &str = "sql:reserved";
const UNRESERVED: &str = "sql:unreserved";
const PRIVATE: (&str, &str) = ("plugin:shared", "private_untagged");
const OWNER_ONLY: (&str, &str) = ("plugin:shared", "owner_only_untagged");

/// A fixture that follows live connector configuration the way the API's resolver does.
struct LiveInstances {
    connectors: RwLock<Arc<Map>>,
    reserved: Arc<MemoryInstance>,
    unreserved: Arc<MemoryInstance>,
}

impl LiveInstances {
    fn change_reservation(&self, teams: Json) {
        *self.connectors.write().unwrap() = Arc::new(map(json!({
            "sql": {"reserved": {"teams": teams}, "unreserved": {}}
        })));
    }

    fn move_reservation(&self, old: &str, new: &str) {
        let mut connectors = self.connectors.write().unwrap();
        meerschaum_core::access::move_reservations(Arc::make_mut(&mut connectors), old, new);
    }
}

impl InstanceResolver for LiveInstances {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        match keys {
            RESERVED => Some(self.reserved.clone()),
            UNRESERVED => Some(self.unreserved.clone()),
            _ => None,
        }
    }

    fn connectors(&self) -> Option<Arc<Map>> {
        Some(self.connectors.read().unwrap().clone())
    }
}

#[async_trait]
impl TeamEvents for LiveInstances {
    async fn renamed(&self, team: &Team, old: &str) -> Result<(), String> {
        self.move_reservation(old, &team.name);
        Ok(())
    }

    async fn deleted(&self, team: &Team) -> Result<(), String> {
        let retired = meerschaum_core::access::retired_reservation(&team.name, team.id.as_deref());
        self.move_reservation(&team.name, &retired);
        Ok(())
    }
}

fn reserved_ref(keys: (&str, &str)) -> String {
    format!("{RESERVED}|{}|{}|None", keys.0, keys.1)
}

struct Plan;

#[async_trait]
impl PipelinesBackend for Plan {
    async fn plan(&self, _: &str, registry: &str, _: &PlanOptions) -> Result<Json, PlanError> {
        assert_eq!(registry, RESERVED);
        let parent = reserved_ref(LEGACY);
        let child = reserved_ref(OWNER_ONLY);
        Ok(json!({
            "plan": {
                "nodes": [{"pipe_ref": parent}, {"pipe_ref": child}],
                "edges": [[parent, child]],
            },
            "text": "legacy → owner_only_untagged",
        }))
    }
}

async fn configured_instance(teams: Json) -> (Fixture, Arc<LiveInstances>) {
    let mut f = fixture().await;
    let instances = Arc::new(LiveInstances {
        connectors: RwLock::default(),
        reserved: Arc::new(MemoryInstance::new("reserved")),
        unreserved: Arc::new(MemoryInstance::new("unreserved")),
    });
    instances.change_reservation(teams);
    register(
        &instances.reserved,
        LEGACY,
        json!({"columns": {"id": "id"}}),
    )
    .await;
    register(&instances.unreserved, LEGACY, json!({})).await;
    register(
        &instances.reserved,
        PRIVATE,
        json!({"access": {"visibility": "private"}}),
    )
    .await;
    register(
        &instances.reserved,
        OWNER_ONLY,
        json!({"access": {"edit": "owner"}}),
    )
    .await;
    register(
        &instances.reserved,
        PUBLIC,
        json!({"access": {"team": "Dry", "visibility": "everyone"}}),
    )
    .await;
    let mut config = (*f.state.config).clone();
    config.allow_multiple_instances = true;
    config.allowed_instance_keys = vec![RESERVED.into(), UNRESERVED.into(), INSTANCE.into()];
    f.state = AppState::builder(config, f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(instances.clone())
        .team_events(instances.clone())
        .actions(Arc::new(Actions))
        .jobs(Arc::new(RecordingJobs::default()))
        .engine(Arc::new(MemoryEngine::default()))
        .pipelines(Arc::new(Plan))
        .build()
        .unwrap();
    f.app = meerschaum_server::app(f.state.clone());
    f.mcp = McpServer::for_state(&f.state).with_backend(Arc::new(Mcp::default()));
    (f, instances)
}

async fn isolated_team(
    f: &Fixture,
    name: &str,
    members: &[(&str, TeamRole)],
    grants: &[&str],
    default_instance: Option<&str>,
) -> Team {
    let mut team = Team::new(name);
    for (username, role) in members {
        if f.instance
            .get_user_id(&User::new(username, None))
            .await
            .unwrap()
            .is_none()
        {
            f.instance.seed_user(username, "pw", None);
            f.instance
                .seed_user_attributes(username, map(json!({"role": "operator"})));
        }
        team.add_member(username, *role);
    }
    team.attributes = map(json!({
        "isolated": true,
        "instance": default_instance,
        "connectors": grants,
        "plugins": ["shared", "ops"],
    }));
    let result = f.instance.register_team(&mut team).await;
    assert!(result.success, "{result}");
    team
}

fn on_reserved(path: &str) -> String {
    let separator = if path.contains('?') { '&' } else { '?' };
    format!("{path}{separator}instance_keys={RESERVED}")
}

#[tokio::test]
async fn reserved_valkey_listings_use_registered_access() {
    use meerschaum_connector_valkey::{MemoryBackend, ValkeyBackend, ValkeyConnector, keys};

    let mut f = fixture().await;
    let instance_keys = "valkey:reserved";
    let backend = Arc::new(MemoryBackend::new());
    let valkey = Arc::new(ValkeyConnector::with_backend(
        "reserved",
        Map::new(),
        backend.clone(),
    ));
    for (metric, parameters) in [
        ("untagged", json!({})),
        ("private", json!({"access": {"visibility": "private"}})),
        ("malformed", json!({"access": "broken JSON"})),
        ("missing", json!({"access": {"visibility": "private"}})),
        (
            "legacy_json",
            json!({"access": "{\"team\":null,\"visibility\":\"team\"}"}),
        ),
    ] {
        let pipe = Pipe::builder("manual:list", metric)
            .instance(instance_keys)
            .parameters(map(parameters))
            .build()
            .unwrap();
        assert!(valkey.register_pipe(&pipe).await.success);
    }
    // Its listing row remains after the registration disappears.
    assert_eq!(
        backend
            .del(&[keys::get_pipe_key("manual:list", "missing", None)])
            .await
            .unwrap(),
        1
    );
    let mut config = (*f.state.config).clone();
    config.allow_multiple_instances = true;
    config.allowed_instance_keys = vec![INSTANCE.into(), instance_keys.into()];
    config.connectors = map(json!({"valkey": {"reserved": {"teams": ["Wet", null]}}}));
    f.state = AppState::builder(config, f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(Arc::new(Instances(vec![(instance_keys.into(), valkey)])))
        .build()
        .unwrap();
    f.app = meerschaum_server::app(f.state.clone());
    f.mcp = McpServer::for_state(&f.state).with_backend(Arc::new(Mcp::default()));
    isolated_team(
        &f,
        "Wet",
        &[("wren", TeamRole::Member)],
        &[],
        Some(instance_keys),
    )
    .await;
    let auth = token(&f, "wren").await;
    let catalog = get(&f, &auth, "/pipes").await;
    assert_eq!(catalog.status, StatusCode::OK, "{}", catalog.text());
    let listing = catalog.json();
    let listed_metrics = listing["manual:list"].as_object().unwrap();
    assert_eq!(listed_metrics.len(), 2, "{listing}");
    assert_eq!(listed_metrics["untagged"]["None"]["parameters"], json!({}));
    assert!(listed_metrics.contains_key("legacy_json"));
    assert_eq!(
        listed(&get(&f, &auth, "/pipes/keys").await),
        names(&[("manual:list", "untagged"), ("manual:list", "legacy_json")])
    );
    let response = call(
        &f,
        "POST",
        &auth,
        "/mcp",
        Some(json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "list_pipes", "arguments": {}},
        })),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let reply = tool_result(response.json()).unwrap().to_string();
    for visible in ["untagged", "legacy_json"] {
        assert!(reply.contains(visible), "{reply}");
    }
    for hidden in ["private", "malformed", "missing"] {
        assert!(!reply.contains(hidden), "{reply}");
        let path = format!("/pipes/manual:list/{hidden}/None/attributes");
        assert_eq!(get(&f, &auth, &path).await.status, StatusCode::NOT_FOUND);
    }
    isolated_team(
        &f,
        "None",
        &[("null-member", TeamRole::Member)],
        &[instance_keys],
        None,
    )
    .await;
    let auth = token(&f, "null-member").await;
    let catalog = get(&f, &auth, &format!("/pipes?instance_keys={instance_keys}")).await;
    assert_eq!(catalog.status, StatusCode::OK, "{}", catalog.text());
    assert_eq!(catalog.json(), json!({}));
}

#[tokio::test]
async fn catalog_keeps_its_default_routing_for_unreserved_team_instances() {
    let (f, instances) = configured_instance(json!(["Wet"])).await;
    let team_pipe = ("manual:team", "only_on_team_instance");
    register(
        &instances.unreserved,
        team_pipe,
        json!({"access": {"team": "Wet"}}),
    )
    .await;
    isolated_team(
        &f,
        "Wet",
        &[("wren", TeamRole::Member)],
        &[UNRESERVED],
        Some(UNRESERVED),
    )
    .await;
    let auth = token(&f, "wren").await;
    let implicit = get(&f, &auth, "/pipes").await;
    let explicit = get(&f, &auth, &format!("/pipes?instance_keys={UNRESERVED}")).await;
    assert_eq!(implicit.status, StatusCode::OK, "{}", implicit.text());
    assert_eq!(explicit.status, StatusCode::OK, "{}", explicit.text());
    assert!(!implicit.text().contains(team_pipe.1));
    assert!(explicit.text().contains(team_pipe.1), "{}", explicit.text());
}

#[tokio::test]
async fn isolated_members_see_and_change_untagged_pipes_on_their_reserved_instance() {
    let (f, instances) = configured_instance(json!(["Wet"])).await;
    isolated_team(
        &f,
        "Wet",
        &[("wren", TeamRole::Member)],
        &[],
        Some(RESERVED),
    )
    .await;
    let member = token(&f, "wren").await;
    assert_eq!(
        listed(&get(&f, &member, &on_reserved("/pipes/keys")).await),
        names(&[LEGACY, OWNER_ONLY, PUBLIC])
    );
    // The same team instance is used when the client omits instance_keys.
    let catalog = get(&f, &member, "/pipes").await;
    assert_eq!(catalog.status, StatusCode::OK, "{}", catalog.text());
    let catalog = catalog.json();
    assert_eq!(
        catalog[LEGACY.0][LEGACY.1]["None"]["parameters"]["columns"]["id"],
        "id"
    );

    let attributes = get(&f, &member, &on_reserved(&url(LEGACY, "/attributes"))).await;
    assert_eq!(attributes.status, StatusCode::OK, "{}", attributes.text());
    assert!(attributes.json()["parameters"].get("access").is_none());
    let stored = captured_registration(instances.reserved.as_ref(), LEGACY).await;
    let edit = call(
        &f,
        "PATCH",
        &member,
        &on_reserved(&url(LEGACY, "/edit?patch=true")),
        Some(json!({"tags": ["ported"]})),
    )
    .await;
    assert!(tuple_of(&edit).0, "{}", edit.text());
    let sync = call(
        &f,
        "POST",
        &member,
        &on_reserved(&url(LEGACY, "/data")),
        Some(json!([{"id": 7, "reading": "ready"}])),
    )
    .await;
    assert!(tuple_of(&sync).0, "{}", sync.text());
    let stored_attributes = get(&f, &member, &on_reserved(&url(LEGACY, "/attributes"))).await;
    assert!(
        stored_attributes.json()["parameters"]
            .get("access")
            .is_none()
    );
    let now = captured_registration(instances.reserved.as_ref(), LEGACY).await;
    assert_eq!(now, stored);
    let rows = get(&f, &member, &on_reserved(&url(LEGACY, "/data"))).await;
    assert!(rows.text().contains("ready"), "{}", rows.text());

    let member = caller("wren", Role::Operator);
    let arguments =
        json!({"connector_keys": LEGACY.0, "metric_key": LEGACY.1, "instance_keys": RESERVED});
    let listing = tool(
        &f,
        &member,
        "list_pipes",
        json!({"instance_keys": RESERVED}),
    )
    .await
    .unwrap();
    assert!(listing.to_string().contains(LEGACY.1), "{listing}");
    let synced = tool(&f, &member, "sync_pipe", arguments).await.unwrap();
    assert!(synced.to_string().contains("synced"), "{synced}");
    let output = tool(
        &f,
        &member,
        "read_sql",
        json!({
            "connector_keys": RESERVED,
            "query": "SELECT * FROM plugin_shared_legacy",
        }),
    )
    .await
    .unwrap();
    assert_eq!(output["rows"], json!([{"x": 1}]));
    let refused = tool(
        &f,
        &member,
        "read_sql",
        json!({
            "connector_keys": RESERVED,
            "query": "SELECT * FROM plugin_shared_private_untagged",
        }),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("cannot see"), "{refused}");
}

#[tokio::test]
async fn managers_can_protect_untagged_pipes_and_give_them_back_to_everyone() {
    let (f, _) = configured_instance(json!(["Wet"])).await;
    isolated_team(
        &f,
        "Wet",
        &[("wren", TeamRole::Member), ("wade", TeamRole::Manager)],
        &[],
        None,
    )
    .await;
    let member = token(&f, "wren").await;
    let manager = token(&f, "wade").await;
    for (auth, expected) in [(&member, StatusCode::NOT_FOUND), (&manager, StatusCode::OK)] {
        let private = get(&f, auth, &on_reserved(&url(PRIVATE, "/attributes"))).await;
        assert_eq!(private.status, expected, "{}", private.text());
    }
    let owner_only = get(&f, &member, &on_reserved(&url(OWNER_ONLY, "/access"))).await;
    assert_eq!(owner_only.status, StatusCode::OK, "{}", owner_only.text());
    assert_eq!(owner_only.json()["may"]["edit"], false);
    let refused = call(
        &f,
        "PATCH",
        &member,
        &on_reserved(&url(OWNER_ONLY, "/edit")),
        Some(json!({"tags": ["x"]})),
    )
    .await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN, "{}", refused.text());
    let access = on_reserved(&url(LEGACY, "/access"));
    let lock = on_reserved(&url(LEGACY, "/lock"));
    let refused = call(&f, "POST", &member, &lock, Some(json!({"locks": ["sync"]}))).await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN, "{}", refused.text());
    let refused = call(&f, "PATCH", &member, &access, Some(json!({"team": "Wet"}))).await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN, "{}", refused.text());

    let locked = call(
        &f,
        "POST",
        &manager,
        &lock,
        Some(json!({"locks": ["sync"]})),
    )
    .await;
    assert_eq!(locked.status, StatusCode::OK, "{}", locked.text());
    assert_eq!(locked.json()["locks"], json!(["sync"]));
    let unlocked = call(
        &f,
        "POST",
        &manager,
        &on_reserved(&url(LEGACY, "/unlock")),
        Some(json!({"locks": ["sync"]})),
    )
    .await;
    assert_eq!(unlocked.status, StatusCode::OK, "{}", unlocked.text());
    assert_eq!(unlocked.json()["locks"], json!([]));

    let assigned = call(
        &f,
        "PATCH",
        &manager,
        &access,
        Some(json!({"team": "Wet", "owner": "wren"})),
    )
    .await;
    assert_eq!(assigned.status, StatusCode::OK, "{}", assigned.text());
    assert_eq!(assigned.json()["access"]["owner"], "wren");
    let everyone = call(&f, "PATCH", &manager, &access, Some(json!({"team": null}))).await;
    assert_eq!(everyone.status, StatusCode::OK, "{}", everyone.text());
    assert!(everyone.json()["access"]["team"].is_null());
}

#[tokio::test]
async fn the_live_reservation_controls_a_shared_instances_audience() {
    let (f, instances) = configured_instance(json!(["Wet"])).await;
    isolated_team(
        &f,
        "Wet",
        &[("wren", TeamRole::Member)],
        &[RESERVED, UNRESERVED],
        None,
    )
    .await;
    isolated_team(&f, "Dry", &[("dove", TeamRole::Member)], &[RESERVED], None).await;
    isolated_team(&f, "Field", &[("finch", TeamRole::Member)], &[], None).await;
    let mut open = Team::new("Open");
    open.add_member("ed", TeamRole::Member);
    open.attributes = map(json!({"connectors": [RESERVED]}));
    assert!(f.instance.register_team(&mut open).await.success);
    let wet = token(&f, "wren").await;
    let dry = token(&f, "dove").await;
    let field = token(&f, "finch").await;
    let open = token(&f, "ed").await;

    let keys = on_reserved("/pipes/keys");
    let granted = listed(&get(&f, &dry, &keys).await);
    assert_eq!(granted, names(&[PUBLIC]));
    assert_eq!(
        listed(&get(&f, &wet, &keys).await),
        names(&[LEGACY, PUBLIC, OWNER_ONLY])
    );
    let unrelated = get(&f, &field, &keys).await;
    assert_eq!(
        unrelated.status,
        StatusCode::FORBIDDEN,
        "{}",
        unrelated.text()
    );
    let unrestricted = get(&f, &wet, "/pipes/keys?instance_keys=sql:unreserved").await;
    assert!(listed(&unrestricted).is_empty());
    let open_side = names(&[LEGACY, PUBLIC, OWNER_ONLY]);
    assert_eq!(listed(&get(&f, &open, &keys).await), open_side);

    instances.change_reservation(json!("Wet, Dry"));
    for auth in [&wet, &dry, &open] {
        assert_eq!(listed(&get(&f, auth, &keys).await), open_side);
    }
    instances.change_reservation(json!(["Dry"]));
    assert!(listed(&get(&f, &wet, &keys).await).is_empty());
    assert_eq!(listed(&get(&f, &dry, &keys).await), open_side);
    assert_eq!(listed(&get(&f, &open, &keys).await), open_side);
}

#[tokio::test]
async fn reservation_rename_and_retirement_take_effect_without_a_restart() {
    let (f, instances) = configured_instance(json!(["Wet"])).await;
    isolated_team(&f, "Wet", &[("wren", TeamRole::Member)], &[], None).await;
    let member = token(&f, "wren").await;
    let admin = token(&f, "admin").await;
    let keys = on_reserved("/pipes/keys");
    assert!(listed(&get(&f, &member, &keys).await).contains("plugin:shared|legacy"));
    let renamed = call(
        &f,
        "POST",
        &admin,
        "/teams/Wet/rename",
        Some(json!({"name": "Harbor"})),
    )
    .await;
    assert_eq!(renamed.status, StatusCode::OK, "{}", renamed.text());
    assert!(listed(&get(&f, &member, &keys).await).contains("plugin:shared|legacy"));
    let deleted = call(&f, "DELETE", &admin, "/teams/Harbor/delete", None).await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.text());
    assert_eq!(get(&f, &member, &keys).await.status, StatusCode::FORBIDDEN);
    assert!(listed(&get(&f, &admin, &keys).await).contains("plugin:shared|legacy"));
    let recreated = call(
        &f,
        "POST",
        &admin,
        "/teams/Harbor/register",
        Some(json!({"members": ["wren"], "attributes": {"isolated": true}})),
    )
    .await;
    assert_eq!(recreated.status, StatusCode::OK, "{}", recreated.text());
    assert_eq!(get(&f, &member, &keys).await.status, StatusCode::FORBIDDEN);

    instances.change_reservation(json!(["Misspelled"]));
    assert_eq!(get(&f, &member, &keys).await.status, StatusCode::FORBIDDEN);
    assert!(listed(&get(&f, &admin, &keys).await).contains("plugin:shared|legacy"));
}

#[tokio::test]
async fn execution_credentials_use_their_bound_teams_reservation() {
    use meerschaum_server::executor::callbacks::{self, Purpose};

    let (f, instances) = configured_instance(json!(["Wet"])).await;
    let posted = ("manual:fixture", "bound_untagged");
    register(&instances.reserved, posted, json!({})).await;
    let member = &[("wren", TeamRole::Member)];
    let wet = isolated_team(&f, "Wet", member, &[], None).await;
    let dry = isolated_team(&f, "Dry", member, &[RESERVED], None).await;
    let member = token(&f, "wren").await;
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("authorization", member.parse().unwrap());
    let current = meerschaum_server::access::resolve(&f.state, &headers)
        .await
        .unwrap();
    for (team, expect) in [(wet, StatusCode::OK), (dry, StatusCode::NOT_FOUND)] {
        let grant = callbacks::register(
            &f.state,
            &current,
            uuid::Uuid::new_v4(),
            Some(uuid::Uuid::parse_str(team.id.as_deref().unwrap()).unwrap()),
            &["pipes:read", "pipes:write"],
            Purpose::Command,
        )
        .await
        .unwrap();
        let credential = callbacks::credential(&f.state, grant.id, grant.launch_id)
            .await
            .unwrap();
        let bearer = format!("Bearer {}", credential.token);
        let read = get(&f, &bearer, &on_reserved(&url(posted, "/attributes"))).await;
        assert_eq!(read.status, expect, "{}: {}", team.name, read.text());
        let edit = call(
            &f,
            "PATCH",
            &bearer,
            &on_reserved(&url(posted, "/edit?patch=true")),
            Some(json!({"tags": ["from-execution"]})),
        )
        .await;
        assert_eq!(edit.status, expect, "{}: {}", team.name, edit.text());
        if expect == StatusCode::OK {
            assert!(tuple_of(&edit).0, "{}", edit.text());
        }
    }
}

#[tokio::test]
async fn reserved_pipeline_plans_and_run_history_keep_untagged_refs() {
    let (f, instances) = configured_instance(json!(["Wet"])).await;
    let registration = Pipe::builder("mrsm", "pipeline")
        .location(Some("imported"))
        .instance(RESERVED)
        .parameters(map(json!({"pipeline": {
            "name": "imported", "id": "fixture-imported", "revision": 1,
            "targets": [reserved_ref(LEGACY)],
        }})))
        .build()
        .unwrap();
    assert!(
        instances
            .reserved
            .register_pipe(&registration)
            .await
            .success
    );
    isolated_team(&f, "Wet", &[("wren", TeamRole::Manager)], &[], None).await;
    let member = token(&f, "wren").await;
    let admin = token(&f, "admin").await;
    let r = get(&f, &member, &on_reserved("/pipelines/imported")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["hidden_members"], 0);
    let graph = call(
        &f,
        "POST",
        &member,
        &on_reserved("/pipelines/imported/plan"),
        None,
    )
    .await;
    assert_eq!(graph.status, StatusCode::OK, "{}", graph.text());
    assert_eq!(
        graph.json()["plan"]["edges"],
        json!([[reserved_ref(LEGACY), reserved_ref(OWNER_ONLY)]])
    );
    assert!(graph.json().get("hidden_nodes").is_none());
    let sync = call(
        &f,
        "POST",
        &member,
        &on_reserved("/pipelines/imported/sync"),
        None,
    )
    .await;
    assert_eq!(sync.status, StatusCode::OK, "{}", sync.text());

    let registration = captured_registration(instances.reserved.as_ref(), LEGACY).await;
    let started = "2026-10-27T00:00:00Z";
    let run = call(&f, "POST", &admin, &on_reserved("/engine/runs"), Some(json!([
        {"run_id": "imported-run", "started": started, "status": "success", "num_nodes": 1, "targets": [reserved_ref(LEGACY)]},
        {"run_id": "imported-run", "started": started, "status": "success",
         "pipe_ref": reserved_ref(LEGACY), "registration_id": registration},
    ]))).await;
    assert_eq!(run.status, StatusCode::OK, "{}", run.text());
    let history = get(&f, &member, &on_reserved("/engine/runs/imported-run")).await;
    assert_eq!(history.status, StatusCode::OK, "{}", history.text());
    assert!(
        history.text().contains(&reserved_ref(LEGACY)),
        "{}",
        history.text()
    );
}
