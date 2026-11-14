mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::http::StatusCode;
use common::{SECRET, req, send};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::Team;
use meerschaum_server::AppState;
use meerschaum_server::backends::TeamEvents;

#[derive(Default)]
struct Events(Mutex<Vec<(String, String)>>);

#[async_trait]
impl TeamEvents for Events {
    async fn renamed(&self, old: &str, new: &str) -> Result<(), String> {
        self.0.lock().unwrap().push((old.into(), new.into()));
        Err("Grafana unavailable".into())
    }
}

#[tokio::test]
async fn rename_notifies_once_and_keeps_the_team_when_grafana_fails() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("team-events"));
    instance.seed_user("admin", "pass", Some("admin"));
    assert!(instance.register_team(&mut Team::new("Data")).await.success);
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
    assert_eq!(
        events.0.lock().unwrap().as_slice(),
        &[("Data".into(), "Analytics".into())]
    );
    assert!(instance.get_team("Analytics").await.unwrap().is_some());
    assert!(instance.get_team("Data").await.unwrap().is_none());
}
