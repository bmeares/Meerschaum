//! Actual router checks for a fresh console permission projection. These do not claim
//! kernel, terminal, plugin, graph or database-backend execution readiness.
mod common;

use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{
    Map, SuccessTuple, Value,
    access::{Team, TeamRole},
    users::{Expiration, Token, User},
};
use meerschaum_server::{
    AppState,
    access::{self, Access},
    backends::{ActionsBackend, ApiExecution},
    engine_stream::BrowserAuth,
    executor::{callbacks, plugins},
};
use serde_json::{Value as Json, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;
use uuid::Uuid;

const SESSION: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";
const COOKIE: &str = "mrsm-session-id=67e55044-10b1-426f-9247-bb680e5fe0c8";

struct Actions {
    capability: ApiExecution,
    executions: AtomicUsize,
}

#[async_trait]
impl ActionsBackend for Actions {
    fn api_execution(&self) -> ApiExecution {
        self.capability
    }
    fn list(&self) -> Vec<String> {
        Vec::new()
    }
    async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
        self.executions.fetch_add(1, Ordering::SeqCst);
        panic!("A permission projection must not execute an action")
    }
    async fn extension_catalog(&self) -> Result<Option<plugins::Validated>, String> {
        panic!("A permission projection must not start source discovery")
    }
    async fn team_execution_probe(
        &self,
        _: &Team,
    ) -> Option<meerschaum_server::team_execution::HostFacts> {
        panic!("A permission projection must not start a confinement probe")
    }
}

/// A verified console provider over the actual login credential. The bounded barrier
/// holds its final lookup, so revocations occur after initial request capture.
struct Cookies {
    bearer: String,
    live: AtomicBool,
    calls: AtomicUsize,
    hold_at: AtomicUsize,
    entered: Notify,
    release: Notify,
}

impl Cookies {
    fn hold_final_lookup(&self) {
        self.hold_at
            .store(self.calls.load(Ordering::SeqCst) + 2, Ordering::SeqCst);
    }
}

#[async_trait]
impl BrowserAuth for Cookies {
    async fn authorize(
        &self,
        _: &AppState,
        headers: &HeaderMap,
    ) -> Option<Result<(HeaderMap, String), StatusCode>> {
        if headers.get("cookie").and_then(|value| value.to_str().ok()) != Some(COOKIE) {
            return None;
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.hold_at.load(Ordering::SeqCst) == call {
            self.entered.notify_one();
            if tokio::time::timeout(Duration::from_secs(5), self.release.notified())
                .await
                .is_err()
            {
                return Some(Err(StatusCode::SERVICE_UNAVAILABLE));
            }
        }
        if !self.live.load(Ordering::SeqCst) {
            return Some(Err(StatusCode::UNAUTHORIZED));
        }
        let mut headers = HeaderMap::new();
        headers.insert("authorization", self.bearer.parse().unwrap());
        Some(Ok((headers, SESSION.into())))
    }
    async fn session_active(&self, _: &AppState, session: &str) -> bool {
        session == SESSION && self.live.load(Ordering::SeqCst)
    }
}

struct Fixture {
    state: AppState,
    app: Router,
    instance: Arc<common::MemoryInstance>,
    actions: Arc<Actions>,
    cookies: Arc<Cookies>,
    team: Team,
}

fn map(value: Json) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

async fn fixture(root: &std::path::Path, capability: ApiExecution) -> Fixture {
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"operator","primary_team":"ApiTeam",
            "api_execution":{"tracked":{"terminals":true}}})),
    );
    let mut team = Team::new("ApiTeam");
    team.attributes = map(
        json!({"isolated":true,"plugins_write":false,"plugins":["probe"],
        "api_execution":{"tracked":{"terminals":true}}}),
    );
    team.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut team).await.success);
    let actions = Arc::new(Actions {
        capability,
        executions: AtomicUsize::new(0),
    });
    let mut config = common::config(root);
    config.actions_non_admin = true;
    let state = AppState::builder(config, instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(actions.clone())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state.clone());
    let bearer = common::login(&app, "alice", "pass", None).await;
    let cookies = Arc::new(Cookies {
        bearer,
        live: AtomicBool::new(true),
        calls: AtomicUsize::new(0),
        hold_at: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Notify::new(),
    });
    assert!(state.engine_streams.set_browser_auth(cookies.clone()));
    Fixture {
        state,
        app,
        instance,
        actions,
        cookies,
        team,
    }
}

fn request(bearer: Option<&str>, cookie: bool) -> Request<Body> {
    let mut request = common::req("GET", "/execute/capabilities", bearer, None);
    if cookie {
        request
            .headers_mut()
            .insert("cookie", COOKIE.parse().unwrap());
    }
    request
}

async fn caller(fixture: &Fixture, bearer: &str) -> Access {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    access::require_fresh(&fixture.state, &headers, &[])
        .await
        .unwrap()
}

#[tokio::test]
async fn only_the_native_backend_preserves_held_actions_and_terminal_availability_stays_separate() {
    for (capability, actions_allowed) in [
        (ApiExecution::Unavailable, false),
        (ApiExecution::Tracked { terminals: false }, true),
    ] {
        let root = tempfile::tempdir().unwrap();
        let fixture = fixture(root.path(), capability).await;
        let response = common::send(&fixture.app, request(None, true)).await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        let body = response.json();
        assert_eq!(body["schema"], 1);
        assert_eq!(body["username"], "alice");
        assert_eq!(
            body["source"]["team_id"],
            fixture.team.id.as_deref().unwrap()
        );
        assert_eq!(body["permissions"]["actions_execute"], actions_allowed);
        assert_eq!(body["permissions"]["jobs_execute"], true);
        assert_eq!(body["permissions"]["jobs_write"], true);
        assert_eq!(body["permissions"]["webterm_use"], false);
        assert_eq!(body["capabilities"]["tracked_api"], actions_allowed);
        assert_eq!(body["capabilities"]["api_terminals"], false);
        assert_eq!(response.headers["cache-control"], "no-store");
        assert_eq!(response.headers["vary"], "Authorization, Cookie");
        assert!(!response.text().contains(&fixture.cookies.bearer));
        assert!(!response.text().contains(SESSION));
        assert_eq!(fixture.actions.executions.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn a_cookie_cannot_widen_a_narrow_bearer_token_or_execution_parent() {
    let root = tempfile::tempdir().unwrap();
    let fixture = fixture(root.path(), ApiExecution::Tracked { terminals: false }).await;
    let narrow = common::login(&fixture.app, "alice", "pass", Some("pipes:read")).await;
    let response = common::send(&fixture.app, request(Some(&narrow), true)).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let body = response.json();
    assert_eq!(body["permissions"]["actions_execute"], false);
    assert_eq!(body["permissions"]["jobs_execute"], false);
    assert_eq!(body["permissions"]["jobs_write"], false);
    assert_eq!(body["permissions"]["webterm_use"], false);

    let bearer_only =
        common::send(&fixture.app, request(Some(&fixture.cookies.bearer), false)).await;
    assert_eq!(bearer_only.status, StatusCode::UNAUTHORIZED);
    let invalid = common::send(&fixture.app, request(Some("Bearer invalid"), true)).await;
    assert_eq!(invalid.status, StatusCode::UNAUTHORIZED);

    let mut token = Token::new(
        Some("console-narrow-token"),
        Expiration::Never,
        Some(vec!["pipes:read".into()]),
        366,
        chrono::Utc::now(),
    );
    token.user = Some(User::new("alice", None));
    assert!(fixture.instance.register_token(&mut token).await.success);
    let token_bearer = format!("Bearer {}", token.get_api_key());
    let response = common::send(&fixture.app, request(Some(&token_bearer), true)).await;
    assert!(!response.status.is_success(), "{}", response.text());

    let source = caller(&fixture, &fixture.cookies.bearer).await;
    let grant = callbacks::register(
        &fixture.state,
        &source,
        Uuid::new_v4(),
        Some(Uuid::parse_str(fixture.team.id.as_deref().unwrap()).unwrap()),
        &["jobs:read"],
        callbacks::Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&fixture.state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let parent = format!("Bearer {}", credential.token);
    let response = common::send(&fixture.app, request(Some(&parent), true)).await;
    assert!(!response.status.is_success(), "{}", response.text());
    assert_eq!(fixture.actions.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_only_role_and_session_revocation_during_the_final_lookup_refuse_publication() {
    for change in ["source", "role", "membership", "session"] {
        let root = tempfile::tempdir().unwrap();
        let fixture = fixture(root.path(), ApiExecution::Tracked { terminals: false }).await;
        fixture.cookies.hold_final_lookup();
        let app = fixture.app.clone();
        let waiting = tokio::spawn(async move { common::send(&app, request(None, true)).await });
        tokio::time::timeout(Duration::from_secs(5), fixture.cookies.entered.notified())
            .await
            .unwrap();
        match change {
            "source" => {
                let mut changed = fixture.team.clone();
                changed
                    .attributes
                    .insert("plugins".into(), Value::List(vec![]));
                assert!(fixture.instance.edit_team(&changed).await.success);
                assert!(
                    caller(&fixture, &fixture.cookies.bearer)
                        .await
                        .has("actions:execute"),
                    "This case removes source grants while leaving action permission held"
                );
            }
            "role" => fixture.instance.seed_user_scopes("alice", &["pipes:read"]),
            "membership" => {
                let mut changed = fixture.team.clone();
                changed.members.shift_remove("alice");
                assert!(fixture.instance.edit_team(&changed).await.success);
            }
            "session" => fixture.cookies.live.store(false, Ordering::SeqCst),
            _ => unreachable!(),
        }
        fixture.cookies.release.notify_one();
        let response = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                response.status,
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            ),
            "{change}: {} {}",
            response.status,
            response.text()
        );
        assert!(response.json().get("permissions").is_none());
        assert!(fixture.instance.edit_team(&fixture.team).await.success);
        fixture.instance.seed_user_attributes(
            "alice",
            map(json!({"role":"operator","primary_team":"ApiTeam"})),
        );
        fixture.cookies.live.store(true, Ordering::SeqCst);
        let reused = common::send(&fixture.app, request(None, true)).await;
        assert_eq!(reused.status, StatusCode::OK, "{change}: {}", reused.text());
        assert_eq!(reused.json()["permissions"]["actions_execute"], true);
        assert_eq!(fixture.actions.executions.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn a_role_gain_during_the_final_lookup_cannot_expand_the_original_scope_ceiling() {
    let root = tempfile::tempdir().unwrap();
    let fixture = fixture(root.path(), ApiExecution::Tracked { terminals: false }).await;
    fixture.instance.seed_user_scopes("alice", &["pipes:read"]);
    fixture.cookies.hold_final_lookup();
    let app = fixture.app.clone();
    let waiting = tokio::spawn(async move { common::send(&app, request(None, true)).await });
    tokio::time::timeout(Duration::from_secs(5), fixture.cookies.entered.notified())
        .await
        .unwrap();
    fixture.instance.seed_user_attributes(
        "alice",
        map(json!({"role":"operator","primary_team":"ApiTeam"})),
    );
    fixture.cookies.release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.json()["permissions"]["actions_execute"], false);
    assert_eq!(response.json()["permissions"]["jobs_execute"], false);
    assert_eq!(response.json()["permissions"]["webterm_use"], false);
    let fresh = common::send(&fixture.app, request(None, true)).await;
    assert_eq!(fresh.status, StatusCode::OK, "{}", fresh.text());
    assert_eq!(fresh.json()["permissions"]["actions_execute"], true);
    assert_eq!(fixture.actions.executions.load(Ordering::SeqCst), 0);
}
