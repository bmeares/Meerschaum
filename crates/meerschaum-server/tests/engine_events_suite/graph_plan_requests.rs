//! Real router/header freshness across an awaited canonical instruction lookup.
//! The controlled provider supplies trusted metadata; it never executes a job.
use crate::common;
use async_trait::async_trait;
use axum::{
    Router,
    http::{HeaderMap, StatusCode},
};
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_api::client::{
    BatchOutcome, BatchReply, BatchRequest, GRAPH_BATCH_PATH, GraphBinding,
};
use meerschaum_core::{
    Map, Pipe, SuccessTuple, Value,
    access::{Team, TeamRole},
    success::Bound,
};
use meerschaum_server::{
    AppState,
    access::Access,
    auth::{self, Principal},
    backends::ActionsBackend,
    executor::{
        callbacks::{self, Purpose},
        pipe_operation::{
            NativeInstructions, NodeWindow, PipeOperation, ReadAccess, Snapshot, SnapshotMember,
            WindowAxis, WindowCoverage, WindowRange,
        },
        source_ceiling::Ceiling,
    },
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use uuid::Uuid;

struct SourceProbe(AtomicUsize);
#[async_trait]
impl meerschaum_server::backends::SourceFetchBackend for SourceProbe {
    async fn native_instance(
        &self,
        _: &str,
    ) -> Result<Arc<dyn InstanceConnector>, meerschaum_server::backends::SourceFetchError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(meerschaum_server::backends::SourceFetchError::Unsupported)
    }
}

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
struct Actions {
    state: Mutex<Option<AppState>>,
    operation: Mutex<Option<PipeOperation>>,
    hold: AtomicBool,
    batch_reply: AtomicBool,
    wrong_issuer: AtomicBool,
    hold_batch: AtomicBool,
    entered: Notify,
    release: Notify,
}
#[async_trait]
impl ActionsBackend for Actions {
    fn list(&self) -> Vec<String> {
        vec!["sync".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
        panic!("Instruction lookup cannot execute an action")
    }
    async fn graph_batch(
        &self,
        caller: &Access,
        request: &BatchRequest,
    ) -> Result<Option<BatchReply>, String> {
        if !self.batch_reply.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let Some(Principal::Execution(identity)) = &caller.auth else {
            return Err("Expected originating execution.".into());
        };
        let origin = identity
            .grant
            .graph_origin
            .as_ref()
            .ok_or("Missing graph origin.")?;
        let consumer = GraphBinding {
            grant: identity.grant.id,
            launch: identity.grant.launch_id,
            revision: origin.context_revision,
        };
        let mut issuer = GraphBinding {
            grant: origin.grant,
            launch: origin.launch,
            revision: origin.context_revision,
        };
        if self.wrong_issuer.load(Ordering::SeqCst) {
            issuer.launch = Uuid::new_v4();
        }
        if self.hold_batch.load(Ordering::SeqCst) {
            self.entered.notify_one();
            tokio::time::timeout(Duration::from_secs(8), self.release.notified())
                .await
                .map_err(|_| "The native batch fixture was not released.")?;
        }
        Ok(Some(BatchReply {
            version: 1,
            consumer,
            issuer,
            reference: request.reference().into(),
            outcome: BatchOutcome::Unknown,
        }))
    }
    async fn pipe_operation_read(&self, caller: &Access) -> Result<Option<ReadAccess>, String> {
        if self.hold.load(Ordering::SeqCst) {
            self.entered.notify_one();
            tokio::time::timeout(Duration::from_secs(6), self.release.notified())
                .await
                .map_err(|_| "The bounded fixture lookup was not released.")?;
        }
        let operation = self.operation.lock().unwrap().clone();
        let Some(operation) = operation else {
            return Ok(None);
        };
        let Some(Principal::Execution(parent)) = &caller.auth else {
            return Err("Expected execution".into());
        };
        let state = self.state.lock().unwrap().clone().unwrap();
        let fresh = callbacks::load_identity(&state, parent.grant.id, parent.grant.launch_id)
            .await
            .map_err(|error| error.detail_text())?;
        let binding = fresh
            .grant
            .graph_origin
            .clone()
            .ok_or("Missing graph origin")?;
        // Deliberately mirrors the old wider-provider behavior: the route must
        // preserve the original data-request JWT/ACL ceiling independently.
        Ok(Some(ReadAccess {
            caller: fresh.caller(),
            operation: operation.into(),
            binding: binding.clone(),
            issuer: binding,
            pending_nodes: Default::default(),
        }))
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    state: AppState,
    app: Router,
    instance: Arc<common::MemoryInstance>,
    actions: Arc<Actions>,
    grant: callbacks::Grant,
    team: Team,
    pipe: Pipe,
    source_probe: Arc<SourceProbe>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // The provider's test-only state link is otherwise a strong Arc cycle.
        self.actions.state.lock().unwrap().take();
    }
}
async fn source(state: &AppState, app: &Router) -> Access {
    let bearer = common::login(app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    meerschaum_server::access::require_fresh(state, &headers, &[])
        .await
        .unwrap()
}
async fn fixture(graph: bool, hold: bool) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"operator","primary_team":"Harbor"})),
    );
    let mut team = Team::new("Harbor");
    // The controlled in-memory instance has no native connector configuration.
    // This accepted graph's immutable Team boundary must grant its exact label.
    team.attributes
        .insert("connectors".into(), Value::List(vec!["sql:fixture".into()]));
    team.members.insert("alice".into(), TeamRole::Manager);
    assert!(instance.register_team(&mut team).await.success);
    let actions = Arc::new(Actions {
        state: Mutex::new(None),
        operation: Mutex::new(None),
        hold: AtomicBool::new(hold),
        batch_reply: AtomicBool::new(false),
        wrong_issuer: AtomicBool::new(false),
        hold_batch: AtomicBool::new(false),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let source_probe = Arc::new(SourceProbe(AtomicUsize::new(0)));
    let state = AppState::builder(common::config(root.path()), instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(actions.clone())
        .source_fetch(source_probe.clone())
        .build()
        .unwrap();
    *actions.state.lock().unwrap() = Some(state.clone());
    let app = meerschaum_server::app(state.clone());
    let caller = source(&state, &app).await;
    let team_id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    let grant = if graph {
        let pending = callbacks::register_graph_with_sources(
            &state,
            &caller,
            Uuid::new_v4(),
            Some(team_id),
            &["pipes:write"],
            Ceiling::default(),
        )
        .await
        .unwrap();
        callbacks::bind_graph_origin(&state, pending.id, pending.launch_id, 1, None)
            .await
            .unwrap()
    } else {
        callbacks::register(
            &state,
            &caller,
            Uuid::new_v4(),
            Some(team_id),
            &["pipes:write"],
            Purpose::Command,
        )
        .await
        .unwrap()
    };
    let pipe = Pipe::builder("sql:fixture", "protected")
        .instance("sql:fixture")
        .parameters(map(
            json!({"columns":{"datetime":"id"},"dtypes":{"id":"int64"},
            "access":{"owner":"bob","team":"Harbor","visibility":"private","edit":"owner"}}),
        ))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let registration = instance
        .get_pipe_id(&pipe)
        .await
        .unwrap()
        .unwrap()
        .to_value();
    if graph {
        let members = BTreeMap::from([(
            pipe.pipe_ref(),
            SnapshotMember {
                registration: registration.clone(),
                parameters: pipe.raw_parameters().clone(),
                writable: true,
                node_window: None,
            },
        )]);
        let mut snapshot = Snapshot::new(members).unwrap();
        let instruction = NativeInstructions::from_main(json!({"nodes":[{"pipe_ref":pipe.pipe_ref(),"action":"root","bounds":{"begin":5,"end":10},"reason":"Main fixture","definition_hash":"fixture"}],"edges":[],"flat":true,"warnings":[]}),snapshot.members()).unwrap();
        let window = NodeWindow::from_main_plan(
            &pipe,
            registration.clone(),
            WindowAxis::Integer {
                column: "id".into(),
                explicit_unit: None,
            },
            vec![WindowRange {
                begin: Some(Bound::Int(5)),
                end: Some(Bound::Int(10)),
            }],
            WindowCoverage::AxisRanges,
            false,
        )
        .unwrap();
        snapshot
            .install_issued(BTreeMap::from([(pipe.pipe_ref(), window)]), instruction)
            .unwrap();
        *actions.operation.lock().unwrap() = Some(PipeOperation {
            row_params: None,
            pipe_ref: pipe.pipe_ref(),
            registration,
            verify: false,
            begin: Some("5".into()),
            end: Some("10".into()),
            parents: None,
            children: None,
            metadata: Some(snapshot),
        });
    }
    Fixture {
        _root: root,
        state,
        app,
        instance,
        actions,
        grant,
        team,
        pipe,
        source_probe,
    }
}
async fn bearer(fixture: &Fixture) -> String {
    format!(
        "Bearer {}",
        callbacks::credential(&fixture.state, fixture.grant.id, fixture.grant.launch_id)
            .await
            .unwrap()
            .token
    )
}
fn request(bearer: &str) -> axum::http::Request<axum::body::Body> {
    common::req(
        "POST",
        meerschaum_connector_api::client::GRAPH_PLAN_PATH,
        Some(bearer),
        Some((
            "application/json",
            serde_json::to_vec(&json!({"phase":"initial"})).unwrap(),
        )),
    )
}

#[tokio::test]
async fn expired_original_data_jwt_cannot_adopt_a_renewed_grant_or_emit_even_no_graph() {
    let fixture = fixture(false, true).await;
    let token = bearer(&fixture).await;
    let mut claims = auth::jwt_decode(
        token.strip_prefix("Bearer ").unwrap(),
        fixture.state.secret(),
        chrono::Utc::now(),
    )
    .unwrap();
    let expires = chrono::Utc::now().timestamp() + 2;
    claims.insert("exp".into(), json!(expires));
    let token = format!(
        "Bearer {}",
        auth::jwt_encode(
            &map(serde_json::Value::Object(claims)),
            fixture.state.secret()
        )
    );
    let app = fixture.app.clone();
    let req = request(&token);
    let pending = tokio::spawn(async move { common::send(&app, req).await });
    tokio::time::timeout(Duration::from_secs(3), fixture.actions.entered.notified())
        .await
        .unwrap();
    callbacks::renew(&fixture.state, fixture.grant.id, fixture.grant.launch_id)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        while chrono::Utc::now().timestamp() <= expires {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    fixture.actions.release.notify_one();
    let response = pending.await.unwrap();
    assert_eq!(
        response.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        response.text()
    );
    assert!(response.json().get("instruction").is_none());
}

#[tokio::test]
async fn a_manager_role_gained_during_lookup_cannot_expand_the_original_object_ceiling() {
    let mut fixture = fixture(true, true).await;
    fixture
        .team
        .members
        .insert("alice".into(), TeamRole::Member);
    assert!(fixture.instance.edit_team(&fixture.team).await.success);
    let token = bearer(&fixture).await;
    let initial = {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", token.parse().unwrap());
        meerschaum_server::access::require_fresh(&fixture.state, &headers, &[])
            .await
            .unwrap()
    };
    assert!(meerschaum_server::access::guard::may_use_connector(
        &fixture.state,
        &initial,
        "sql:fixture"
    ));
    assert!(!initial.can_see_pipe(&fixture.pipe));
    let app = fixture.app.clone();
    let req = request(&token);
    let pending = tokio::spawn(async move { common::send(&app, req).await });
    tokio::time::timeout(Duration::from_secs(3), fixture.actions.entered.notified())
        .await
        .unwrap();
    fixture
        .team
        .members
        .insert("alice".into(), TeamRole::Manager);
    assert!(fixture.instance.edit_team(&fixture.team).await.success);
    let gained =
        callbacks::load_identity(&fixture.state, fixture.grant.id, fixture.grant.launch_id)
            .await
            .unwrap()
            .caller();
    assert!(gained.can_see_pipe(&fixture.pipe) && gained.can_edit(fixture.pipe.raw_parameters()));
    fixture.actions.release.notify_one();
    let response = pending.await.unwrap();
    assert_eq!(
        response.status,
        StatusCode::FORBIDDEN,
        "{}",
        response.text()
    );
    assert!(response.json().get("instruction").is_none());
}

#[tokio::test]
async fn a_wider_provider_cannot_restore_write_to_a_narrow_data_request_jwt() {
    let fixture = fixture(true, false).await;
    let token = bearer(&fixture).await;
    let mut claims = auth::jwt_decode(
        token.strip_prefix("Bearer ").unwrap(),
        fixture.state.secret(),
        chrono::Utc::now(),
    )
    .unwrap();
    let mut write_only = claims.clone();
    claims.insert("scopes".into(), json!(["pipes:read"]));
    let narrow = format!(
        "Bearer {}",
        auth::jwt_encode(
            &map(serde_json::Value::Object(claims)),
            fixture.state.secret()
        )
    );
    let response = common::send(&fixture.app, request(&narrow)).await;
    assert_eq!(
        response.status,
        StatusCode::FORBIDDEN,
        "{}",
        response.text()
    );
    assert!(response.json().get("instruction").is_none());
    assert!(
        response.text().contains("pipes:write"),
        "{}",
        response.text()
    );
    // Instructions for an accepted write graph require its original write scope,
    // but no unrelated read/action/job scope is added by internal tracking.
    write_only.insert("scopes".into(), json!(["pipes:write"]));
    let write_only = format!(
        "Bearer {}",
        auth::jwt_encode(
            &map(serde_json::Value::Object(write_only)),
            fixture.state.secret()
        )
    );
    let mut headers = HeaderMap::new();
    headers.insert("authorization", write_only.parse().unwrap());
    let limited = meerschaum_server::access::require_fresh(&fixture.state, &headers, &[])
        .await
        .unwrap();
    assert!(limited.has("pipes:write") && !limited.has("pipes:read"));
    assert!(limited.can_see_pipe(&fixture.pipe) && limited.can_edit(fixture.pipe.raw_parameters()));
    assert!(meerschaum_server::access::guard::may_use_connector(
        &fixture.state,
        &limited,
        "sql:fixture"
    ));
    let write_only = common::send(&fixture.app, request(&write_only)).await;
    assert_eq!(write_only.status, StatusCode::OK, "{}", write_only.text());
    assert_eq!(
        write_only.json()["instruction"]["instruction"]["kind"],
        "initial"
    );
    let full = common::send(&fixture.app, request(&token)).await;
    assert_eq!(full.status, StatusCode::OK, "{}", full.text());
    assert_eq!(full.json()["instruction"]["instruction"]["kind"], "initial");
}

#[tokio::test]
async fn a_missing_provider_cannot_declassify_a_captured_graph_as_local_execution() {
    let graph = fixture(true, false).await;
    graph.actions.operation.lock().unwrap().take();
    let response = common::send(&graph.app, request(&bearer(&graph).await)).await;
    assert_eq!(
        response.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        response.text()
    );
    assert!(response.json().get("instruction").is_none());
    let ordinary = fixture(false, false).await;
    let response = common::send(&ordinary.app, request(&bearer(&ordinary).await)).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert!(response.json()["instruction"].is_null());
}

#[tokio::test]
async fn broad_read_scopes_cannot_skip_a_finite_graph_source_window() {
    use meerschaum_connector::{DataQuery, DateBound};
    use meerschaum_connector_api::source_fetch::Request;
    let fixture = fixture(true, false).await;
    let token = bearer(&fixture).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", token.parse().unwrap());
    let caller = meerschaum_server::access::require_fresh(&fixture.state, &headers, &[])
        .await
        .unwrap();
    assert!(caller.has("pipes:read") && caller.has("sql:read") && caller.has("pipes:write"));
    for (begin, end) in [(None, None), (Some(0), Some(20)), (Some(5), Some(11))] {
        let request = Request::for_pipe(
            "sql:fixture",
            &fixture.pipe,
            &DataQuery {
                begin: begin.map(DateBound::Int),
                end: end.map(DateBound::Int),
                ..Default::default()
            },
        )
        .unwrap();
        let response = common::send(
            &fixture.app,
            common::req(
                "POST",
                "/pipes/sql:fixture/protected/None/fetch",
                Some(&token),
                Some(("application/json", serde_json::to_vec(&request).unwrap())),
            ),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{}",
            response.text()
        );
    }
    assert_eq!(
        fixture.source_probe.0.load(Ordering::SeqCst),
        0,
        "An outside-window source request reached the native backend"
    );
    let request = Request::for_pipe(
        "sql:fixture",
        &fixture.pipe,
        &DataQuery {
            begin: Some(DateBound::Int(5)),
            end: Some(DateBound::Int(10)),
            ..Default::default()
        },
    )
    .unwrap();
    let response = common::send(
        &fixture.app,
        common::req(
            "POST",
            "/pipes/sql:fixture/protected/None/fetch",
            Some(&token),
            Some(("application/json", serde_json::to_vec(&request).unwrap())),
        ),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        response.text()
    );
    assert_eq!(
        fixture.source_probe.0.load(Ordering::SeqCst),
        1,
        "The exact issued query did not reach the deliberately unavailable native fixture"
    );
}

#[tokio::test]
async fn graph_data_docs_and_csv_reads_keep_bounds_while_first_party_reads_remain_ordinary() {
    let fixture = fixture(true, false).await;
    let docs = [-5, 6, 15]
        .into_iter()
        .map(|value| {
            meerschaum_connector::pipe_ops::Doc::from_iter([(
                "id".into(),
                meerschaum_kernel::Value::Int(value),
            )])
        })
        .collect::<Vec<_>>();
    let frame = meerschaum_kernel::docs::frame_from_docs(&docs).unwrap();
    assert!(
        fixture
            .instance
            .sync_pipe(&fixture.pipe, frame, &Default::default())
            .await
            .success
    );
    let token = bearer(&fixture).await;
    for suffix in ["data", "docs", "csv"] {
        let base = format!("/pipes/sql:fixture/protected/None/{suffix}");
        let denied =
            common::send(&fixture.app, common::req("GET", &base, Some(&token), None)).await;
        assert_eq!(
            denied.status,
            StatusCode::FORBIDDEN,
            "{suffix}: {}",
            denied.text()
        );
        let allowed = common::send(
            &fixture.app,
            common::req("GET", &format!("{base}?begin=5&end=10"), Some(&token), None),
        )
        .await;
        assert_eq!(
            allowed.status,
            StatusCode::OK,
            "{suffix}: {}",
            allowed.text()
        );
        assert!(
            allowed.text().contains('6')
                && !allowed.text().contains("15")
                && !allowed.text().contains("-5"),
            "{}",
            allowed.text()
        );
    }
    let original = common::login(&fixture.app, "alice", "pass", None).await;
    let ordinary = common::send(
        &fixture.app,
        common::req(
            "GET",
            "/pipes/sql:fixture/protected/None/data",
            Some(&original),
            None,
        ),
    )
    .await;
    assert_eq!(ordinary.status, StatusCode::OK, "{}", ordinary.text());
    assert!(
        ordinary.text().contains("-5") && ordinary.text().contains("15"),
        "{}",
        ordinary.text()
    );
}

fn batch_request(token: &str, pipe: &Pipe) -> axum::http::Request<axum::body::Body> {
    let issue = BatchRequest::Issue {
        reference: pipe.pipe_ref(),
        predecessor: None,
        request_sha256: "ab".repeat(32),
    };
    common::req(
        "POST",
        GRAPH_BATCH_PATH,
        Some(token),
        Some(("application/json", serde_json::to_vec(&issue).unwrap())),
    )
}

#[tokio::test]
async fn batch_delivery_requires_originating_execution_and_canonical_provenance() {
    let ordinary = fixture(false, false).await;
    let user = common::login(&ordinary.app, "alice", "pass", None).await;
    let response = common::send(&ordinary.app, batch_request(&user, &ordinary.pipe)).await;
    assert_eq!(
        response.status,
        StatusCode::FORBIDDEN,
        "{}",
        response.text()
    );
    let response = common::send(
        &ordinary.app,
        batch_request(&bearer(&ordinary).await, &ordinary.pipe),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.json().get("batch"), Some(&serde_json::Value::Null));

    let graph = fixture(true, false).await;
    let token = bearer(&graph).await;
    let response = common::send(&graph.app, batch_request(&token, &graph.pipe)).await;
    assert_eq!(
        response.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        response.text()
    );
    graph.actions.batch_reply.store(true, Ordering::SeqCst);
    graph.actions.wrong_issuer.store(true, Ordering::SeqCst);
    let response = common::send(&graph.app, batch_request(&token, &graph.pipe)).await;
    assert_eq!(
        response.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        response.text()
    );
    assert!(response.json().get("batch").is_none());
    graph.actions.wrong_issuer.store(false, Ordering::SeqCst);
    let response = common::send(&graph.app, batch_request(&token, &graph.pipe)).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.json()["batch"]["outcome"]["kind"], "unknown");
}

#[tokio::test]
async fn revoked_callback_cannot_receive_a_pending_batch_lookup_reply() {
    let graph = fixture(true, false).await;
    graph.actions.batch_reply.store(true, Ordering::SeqCst);
    graph.actions.hold_batch.store(true, Ordering::SeqCst);
    let req = batch_request(&bearer(&graph).await, &graph.pipe);
    let app = graph.app.clone();
    let pending = tokio::spawn(async move { common::send(&app, req).await });
    tokio::time::timeout(Duration::from_secs(4), graph.actions.entered.notified())
        .await
        .unwrap();
    callbacks::revoke(&graph.state, graph.grant.id)
        .await
        .unwrap();
    graph.actions.release.notify_one();
    let response = pending.await.unwrap();
    assert!(
        matches!(
            response.status,
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
        ),
        "{}",
        response.text()
    );
    assert!(response.json().get("batch").is_none());
}
