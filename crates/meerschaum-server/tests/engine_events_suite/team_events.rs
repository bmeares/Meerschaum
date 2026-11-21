use crate::common;
use std::sync::{Arc, Mutex};

use crate::common::{SECRET, req, send};
use async_trait::async_trait;
use axum::http::StatusCode;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::Value;
use meerschaum_core::access::Team;
use meerschaum_server::AppState;
use meerschaum_server::backends::TeamEvents;

/// What the events saw: `(event, team name, old name, the team's attributes)`.
type Seen = Vec<(&'static str, String, String, String)>;

#[derive(Default)]
struct Events(Mutex<Seen>);

#[async_trait]
impl TeamEvents for Events {
    async fn renamed(&self, team: &Team, old: &str) -> Result<(), String> {
        self.0.lock().unwrap().push((
            "renamed",
            team.name.clone(),
            old.into(),
            format!("{:?}", team.attributes),
        ));
        Err("Grafana unavailable".into())
    }

    async fn deleted(&self, team: &Team) -> Result<(), String> {
        self.0.lock().unwrap().push((
            "deleted",
            team.name.clone(),
            String::new(),
            format!("{:?}", team.attributes),
        ));
        Err("Grafana unavailable".into())
    }
}

fn with_org(name: &str, org: i64) -> Team {
    let mut team = Team::new(name);
    let mut record = meerschaum_core::Map::new();
    record.insert("org_id".into(), Value::Int(org));
    team.attributes.insert("grafana".into(), Value::Map(record));
    team
}

#[tokio::test]
async fn rename_notifies_once_and_keeps_the_team_when_grafana_fails() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("team-events"));
    instance.seed_user("admin", "pass", Some("admin"));
    assert!(
        instance
            .register_team(&mut with_org("Data", 7))
            .await
            .success
    );
    let events = Arc::new(Events::default());
    let state = AppState::builder(common::config(dir.path()), instance.clone())
        .secret(SECRET.to_vec())
        .team_events(events.clone())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let admin = common::login(&app, "admin", "pass", None).await;
    let rename = || Some(("application/json", br#"{"name":"Analytics"}"#.to_vec()));
    let rejected = send(&app, req("POST", "/teams/Data/rename", None, rename())).await;
    assert!(!rejected.status.is_success());
    assert!(events.0.lock().unwrap().is_empty());

    let result = send(
        &app,
        req("POST", "/teams/Data/rename", Some(&admin), rename()),
    )
    .await;
    assert_eq!(result.status, StatusCode::OK, "{}", result.text());
    assert_eq!(result.json()["notes"][0], "Grafana unavailable");
    {
        let seen = events.0.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let (event, name, old, attributes) = &seen[0];
        assert_eq!(
            (*event, name.as_str(), old.as_str()),
            ("renamed", "Analytics", "Data")
        );
        // The renamed team's record carries its saved organization id.
        assert!(attributes.contains("org_id"), "{attributes}");
    }
    assert!(instance.get_team("Analytics").await.unwrap().is_some());
    assert!(instance.get_team("Data").await.unwrap().is_none());
}

#[tokio::test]
async fn deletion_notifies_once_with_the_deleted_record() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("team-events-delete"));
    instance.seed_user("admin", "pass", Some("admin"));
    assert!(
        instance
            .register_team(&mut with_org("Wetlands", 9))
            .await
            .success
    );
    let events = Arc::new(Events::default());
    let state = AppState::builder(common::config(dir.path()), instance.clone())
        .secret(SECRET.to_vec())
        .team_events(events.clone())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let admin = common::login(&app, "admin", "pass", None).await;
    let rejected = send(&app, req("DELETE", "/teams/Wetlands/delete", None, None)).await;
    assert!(!rejected.status.is_success());
    assert!(events.0.lock().unwrap().is_empty());

    let result = send(
        &app,
        req("DELETE", "/teams/Wetlands/delete", Some(&admin), None),
    )
    .await;
    assert_eq!(result.status, StatusCode::OK, "{}", result.text());
    // The team is deleted even though Grafana failed; the failure is a note.
    assert_eq!(result.json()["notes"][0], "Grafana unavailable");
    assert!(instance.get_team("Wetlands").await.unwrap().is_none());
    let seen = events.0.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let (event, name, _, attributes) = &seen[0];
    assert_eq!((*event, name.as_str()), ("deleted", "Wetlands"));
    assert!(
        attributes.contains("org_id") && attributes.contains('9'),
        "{attributes}"
    );
}

/// A connector still reserved for a team name that no team has (left by a deleted team, or
/// written by hand) is not handed to a new team of that name, nor to a team renamed to it, unless
/// an administrator sends `adopt`.
#[tokio::test]
async fn a_reserved_connector_is_not_adopted_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("team-adopt"));
    instance.seed_user("admin", "pass", Some("admin"));
    assert!(
        instance
            .register_team(&mut Team::new("Forest"))
            .await
            .success
    );
    let mut config = common::config(dir.path());
    config.connectors = Value::from_json_str(
        r#"{"sql": {"wetlands": {"flavor": "timescaledb", "database": "wetlands",
                                 "teams": ["Wetlands"]}}}"#,
    )
    .unwrap()
    .to_map();
    let state = AppState::builder(config, instance.clone())
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let admin = common::login(&app, "admin", "pass", None).await;
    let json = |text: &str| Some(("application/json", text.as_bytes().to_vec()));

    let refused = send(
        &app,
        req("POST", "/teams/wetlands/register", Some(&admin), json("{}")),
    )
    .await;
    assert_eq!(refused.status, StatusCode::CONFLICT, "{}", refused.text());
    assert!(
        refused.text().contains("sql:wetlands"),
        "{}",
        refused.text()
    );
    assert!(instance.get_team("wetlands").await.unwrap().is_none());

    let refused = send(
        &app,
        req(
            "POST",
            "/teams/Forest/rename",
            Some(&admin),
            json(r#"{"name": "Wetlands"}"#),
        ),
    )
    .await;
    assert_eq!(refused.status, StatusCode::CONFLICT, "{}", refused.text());
    assert!(instance.get_team("Forest").await.unwrap().is_some());

    // An administrator who means it.
    let adopted = send(
        &app,
        req(
            "POST",
            "/teams/Wetlands/register",
            Some(&admin),
            json(r#"{"adopt": true}"#),
        ),
    )
    .await;
    assert_eq!(adopted.status, StatusCode::OK, "{}", adopted.text());
    // A name nothing is reserved for is unaffected.
    let plain = send(
        &app,
        req("POST", "/teams/Prairie/register", Some(&admin), json("{}")),
    )
    .await;
    assert_eq!(plain.status, StatusCode::OK, "{}", plain.text());
}
