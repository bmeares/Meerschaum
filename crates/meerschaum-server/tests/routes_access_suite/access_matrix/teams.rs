//! Teams: created by administrators, staffed by their managers; isolated teams see only their
//! own pipes, over HTTP and MCP.

use super::*;

#[tokio::test]
async fn teams_are_created_by_administrators_and_staffed_by_their_managers() {
    let f = fixture().await;
    let (vera, mia, otto, admin) = (
        token(&f, "vera").await,
        token(&f, "mia").await,
        token(&f, "otto").await,
        token(&f, "admin").await,
    );
    let r = get(&f, &vera, "/teams").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let team_names: Vec<String> = r.json()["teams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(team_names, ["Everyone", "Data", "Ops"]);
    // Creating, renaming and deleting teams is the administrators'.
    let r = call(&f, "POST", &mia, "/teams/Lab/register", Some(json!({}))).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &admin,
        "/teams/Lab/register",
        Some(json!({"members": ["vera"], "managers": ["otto"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["team"]["managers"], json!(["otto"]));
    let r = call(&f, "POST", &admin, "/teams/lab/register", Some(json!({}))).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &admin,
        "/teams/Everyone/register",
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
    // A manager adds and removes members of their team, and nothing else.
    let r = get(&f, &otto, &url(TEMPERATURE, "/attributes")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = call(
        &f,
        "PATCH",
        &mia,
        "/teams/Data/edit",
        Some(json!({"add_members": ["otto"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // The new member sees the team's pipes at once (the server forgets what it cached).
    let r = get(&f, &otto, &url(TEMPERATURE, "/attributes")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &mia,
        "/teams/Data/edit",
        Some(json!({"managers": ["mia", "otto"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &mia,
        "/teams/Ops/edit",
        Some(json!({"add_members": ["vera"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &mia,
        "/teams/Data/edit",
        Some(json!({"remove_members": ["otto"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(
        get(&f, &otto, &url(TEMPERATURE, "/attributes"))
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    // Renaming a team renames it in its pipes; deleting one gives them to Everyone.
    let r = call(
        &f,
        "POST",
        &admin,
        "/teams/Ops/rename",
        Some(json!({"name": "Operations"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(stored_access(&f, PRESSURE)["team"], "Operations");
    let r = get(&f, &admin, "/teams/Operations?counts=true").await;
    assert_eq!(r.json()["pipes"], json!(2), "{}", r.text());
    let r = call(&f, "DELETE", &admin, "/teams/Operations/delete", None).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(stored_access(&f, PRESSURE)["team"], Json::Null);
    let r = call(&f, "DELETE", &admin, "/teams/Everyone/delete", None).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
    let r = call(&f, "DELETE", &mia, "/teams/Data/delete", None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

const MARSH: (&str, &str) = ("plugin:wet", "marsh");

/// The fixture plus an isolated team `Wet` (granted `sql:secure`, which is also its default
/// instance, and the plugin `noaa`) with `wren` (an operator) and `wade` (its manager), and one
/// pipe of that team.
async fn isolated_fixture() -> Fixture {
    let f = fixture().await;
    f.instance.seed_user("wren", "pw", None);
    f.instance
        .seed_user_attributes("wren", map(json!({"role": "operator"})));
    f.instance.seed_user("wade", "pw", None);
    f.instance.seed_user_attributes("wade", map(json!({})));
    let mut wet = Team::new("Wet");
    wet.add_member("wren", TeamRole::Member);
    wet.add_member("wade", TeamRole::Manager);
    wet.attributes = map(json!({
        "isolated": true,
        "instance": "sql:secure",
        "connectors": ["sql:secure"],
        "plugins": ["noaa"],
    }));
    let res = f.instance.register_team(&mut wet).await;
    assert!(res.success, "{res}");
    register(&f.instance, MARSH, json!({"access": {"team": "Wet"}})).await;
    f
}

#[tokio::test]
async fn members_of_an_isolated_team_see_only_their_team() {
    let f = isolated_fixture().await;
    let wren = token(&f, "wren").await;
    let ed = token(&f, "ed").await;
    let admin = token(&f, "admin").await;

    // Pipes: their team's only. Everyone's legacy pipe and a pipe another team shares with
    // "everyone" are hidden; other users are unaffected.
    assert_eq!(
        listed(&get(&f, &wren, "/pipes/keys").await),
        BTreeSet::from(["plugin:wet|marsh".to_string()])
    );
    let seen_by_ed = listed(&get(&f, &ed, "/pipes/keys").await);
    assert!(
        seen_by_ed.contains("plugin:shared|legacy") && seen_by_ed.contains("plugin:ops|public")
    );
    assert!(!seen_by_ed.contains("plugin:wet|marsh"));
    for hidden in [LEGACY, PUBLIC, TEMPERATURE] {
        let r = get(&f, &wren, &url(hidden, "/attributes")).await;
        assert_ne!(r.status, StatusCode::OK, "{hidden:?}: {}", r.text());
    }
    assert_eq!(
        get(&f, &wren, &url(MARSH, "/attributes")).await.status,
        StatusCode::OK
    );

    // Connectors: what the team was granted, not the unrestricted ones.
    let connectors = get(&f, &wren, "/connectors").await;
    assert_eq!(connectors.status, StatusCode::OK, "{}", connectors.text());
    assert_eq!(connectors.json()["sql"], json!(["secure"]));

    // The instance: the team's default when none is named; another is refused.
    let r = get(&f, &wren, "/pipes/keys?instance_keys=sql:main").await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        message(&r).contains("not available to your teams"),
        "{}",
        r.text()
    );
    assert_eq!(
        get(&f, &wren, "/pipes/keys?instance_keys=sql:secure")
            .await
            .status,
        StatusCode::OK
    );

    // Registering on a connector the team does not hold is refused.
    let r = call(
        &f,
        "POST",
        &wren,
        &url(("sql:main", "x"), "/register"),
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    // Teams and accounts: their own.
    let teams = get(&f, &wren, "/teams").await.json();
    let names: Vec<&str> = teams["teams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["Wet"]);
    assert_eq!(
        get(&f, &wren, "/teams/Data").await.status,
        StatusCode::NOT_FOUND
    );
    let users = get(&f, &wren, "/users/permissions").await.json();
    let mut usernames: Vec<&str> = users["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["username"].as_str().unwrap())
        .collect();
    usernames.sort_unstable();
    assert_eq!(usernames, ["wade", "wren"]);
    assert_eq!(
        get(&f, &wren, "/users/ed/permissions").await.status,
        StatusCode::NOT_FOUND
    );

    // An operator's actions and terminal act with the server's credentials: withheld.
    let own = get(&f, &wren, "/users/wren/permissions").await.json();
    assert_eq!(own["user"]["role"], json!("operator"));
    let r = call(
        &f,
        "POST",
        &wren,
        "/actions/show",
        Some(json!({"action": ["pipes"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    // A team's settings are its members' and the administrators' to read.
    let as_member = get(&f, &wren, "/teams/Wet").await.json();
    assert_eq!(as_member["team"]["attributes"]["isolated"], json!(true));
    let as_outsider = get(&f, &ed, "/teams/Wet").await.json();
    assert_eq!(as_outsider["team"]["attributes"], json!({}));
    let as_admin = get(&f, &admin, "/teams/Wet").await.json();
    assert_eq!(
        as_admin["team"]["attributes"]["instance"],
        json!("sql:secure")
    );
}

#[tokio::test]
async fn mcp_follows_the_isolated_team() {
    let f = isolated_fixture().await;
    let access = meerschaum_server::access::access_of(
        &f.state,
        Principal::User(User::new("wren", None)),
        &Scopes::List(vec!["*".into()]),
    )
    .await
    .unwrap();
    assert!(access.principal.is_isolated());
    let mut caller = Caller::api(access.scopes(), access.auth.clone());
    caller.plugins_hidden = true;
    caller.team_principal = Some(access.principal.clone());

    // Without an instance, the team's default; pipes of the team only.
    let pipes = tool(&f, &caller, "list_pipes", json!({})).await.unwrap();
    let text = pipes.to_string();
    assert!(text.contains("marsh"), "{text}");
    assert!(
        !text.contains("legacy") && !text.contains("temperature"),
        "{text}"
    );
    // Another instance is refused.
    let refused = tool(
        &f,
        &caller,
        "list_pipes",
        json!({"instance_keys": "sql:main"}),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("not available to your teams"), "{refused}");
    // An unrestricted connector is not theirs to query.
    let refused = tool(
        &f,
        &caller,
        "read_sql",
        json!({"connector_keys": "sql:main", "query": "SELECT 1"}),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("not available to your teams"), "{refused}");
    // Plugin entries are hidden from a walled-off caller.
    assert!(caller.may_use(meerschaum_server::mcp::registry::Source::Builtin));
    assert!(!caller.may_use(meerschaum_server::mcp::registry::Source::Host));
    assert!(!caller.may_use(meerschaum_server::mcp::registry::Source::Native));
}
