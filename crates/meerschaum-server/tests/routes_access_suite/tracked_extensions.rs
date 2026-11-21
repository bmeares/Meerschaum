//! Real in-process router interception. These spies prove routing/authority only;
//! the CLI's actual native Host fixtures independently prove worker/kernel confinement.
use crate::common;
use async_trait::async_trait;
use axum::http::{HeaderMap, StatusCode};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{
    Map, SuccessTuple, Value,
    access::{EVERYONE, Team, TeamRole},
};
use meerschaum_server::{
    AppState,
    backends::{ActionsBackend, ApiExecution},
    engine_stream::BrowserAuth,
    executor::{
        context, plugin_handlers as handlers,
        plugin_host::{HostIdentity, Placement},
        plugins,
    },
    mcp::{
        Caller, McpServer, NativeMcp,
        registry::{Source, Tool},
    },
    native::{
        NativeAdmission, NativeRoute, NativeRouteHandler, NativeRouteRequest, NativeRouteResponse,
    },
};
use serde_json::{Value as Json, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::Notify;
use uuid::Uuid;
fn core(value: Json) -> Value {
    serde_json::from_value(value).unwrap()
}
fn map(value: Json) -> Map {
    core(value).to_map()
}
fn catalog(team_id: Option<Uuid>) -> plugins::Validated {
    let host = HostIdentity {
        team_id,
        generation: Uuid::new_v4(),
        files: BTreeMap::from([("probe.py".into(), "a".repeat(64))]),
        view_sha256: "b".repeat(64),
    };
    let mut catalog = plugins::Catalog {
        schema: plugins::SCHEMA,
        release: env!("CARGO_PKG_VERSION").into(),
        team_id: team_id.unwrap_or_else(Uuid::nil),
        generation: host.generation,
        placement: Placement::Host(host.clone()),
        plugins: BTreeMap::from([(
            "probe".into(),
            plugins::Plugin {
                archive_sha256: String::new(),
                kind: plugins::SourceKind::PythonModule,
                metadata: None,
            },
        )]),
    };
    let grants = BTreeSet::from(["probe".into()]);
    let digest = plugins::Validated::decode_host(
        &serde_json::to_vec(&catalog).unwrap(),
        &host,
        &grants,
        &Default::default(),
    )
    .unwrap()
    .source_digest()
    .unwrap();
    let source = host.plugin_digests().unwrap().remove("probe").unwrap();
    let api: Vec<_> = ["/tracked/matched", "/tracked/fallback"]
        .into_iter()
        .map(|path| {
            let selector = handlers::Selector::Api {
                method: "GET".into(),
                path: path.into(),
            };
            handlers::Handler {
                id: handlers::handler_id(&digest, "probe", &source, &selector).unwrap(),
                selector,
                scopes: BTreeSet::new(),
                dash: None,
            }
        })
        .collect();
    let selector = handlers::Selector::McpTool {
        name: "scoped_tool".into(),
    };
    let tool = handlers::Handler {
        id: handlers::handler_id(&digest, "probe", &source, &selector).unwrap(),
        selector,
        scopes: BTreeSet::new(),
        dash: None,
    };
    catalog.plugins.get_mut("probe").unwrap().metadata=Some(plugins::Metadata {
        api_routes:vec![plugins::Route{name:"matched".into(),method:"GET".into(),path:"/tracked/matched".into(),scopes:vec![]},plugins::Route{name:"fallback".into(),method:"GET".into(),path:"/tracked/fallback".into(),scopes:vec![]}],
        mcp_tools:vec![serde_json::from_value(json!({"name":"scoped_tool","description":"Scoped tool","annotations":{"readOnlyHint":true},"input_schema":{"type":"object","properties":{}},"scopes":[]})).unwrap()],
        registry:Some(handlers::Registry {
            source_catalog_sha256:digest,plugin:"probe".into(),archive_sha256:String::new(),files_sha256:Some(source),
            api:handlers::Discovery::Complete{entries:api},mcp:handlers::Discovery::Complete{entries:vec![tool]},
            pages:handlers::Discovery::Complete{entries:vec![]},dash_callbacks:handlers::Discovery::Complete{entries:vec![]},
        }),..Default::default()
    });
    plugins::Validated::decode_host(
        &serde_json::to_vec(&catalog).unwrap(),
        &host,
        &grants,
        &Default::default(),
    )
    .unwrap()
}
#[derive(Default)]
struct Counts {
    legacy_admit: AtomicUsize,
    legacy_api: AtomicUsize,
    legacy_mcp: AtomicUsize,
    typed: AtomicUsize,
    metadata: AtomicUsize,
}
struct Gate {
    armed: AtomicBool,
    entered: Notify,
    release: Notify,
}
impl Gate {
    fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
}
struct Typed {
    counts: Arc<Counts>,
    catalogs: BTreeMap<String, plugins::Validated>,
    gate: Arc<Gate>,
    invocation_gate: Arc<Gate>,
    missing: AtomicBool,
}
#[async_trait]
impl ActionsBackend for Typed {
    fn api_execution(&self) -> ApiExecution {
        ApiExecution::Tracked { terminals: false }
    }
    fn list(&self) -> Vec<String> {
        vec![]
    }
    async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
        panic!("A typed extension must not become generic Main action execution")
    }
    async fn extension_catalog(&self) -> Result<Option<plugins::Validated>, String> {
        self.counts.metadata.fetch_add(1, Ordering::SeqCst);
        if self.gate.armed.swap(false, Ordering::SeqCst) {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
        }
        if self.missing.load(Ordering::SeqCst) {
            return Err("The exact source metadata is unavailable".into());
        }
        let caller = context::current().ok_or("Missing accepted caller")?;
        Ok(self.catalogs.get(&caller.primary_team).cloned())
    }
    async fn extension_invoke(
        &self,
        invocation: handlers::Invocation,
    ) -> Result<handlers::Reply, String> {
        self.counts.typed.fetch_add(1, Ordering::SeqCst);
        if self.invocation_gate.armed.swap(false, Ordering::SeqCst) {
            self.invocation_gate.entered.notify_one();
            self.invocation_gate.release.notified().await;
        }
        let caller = context::current().ok_or("Missing caller")?;
        assert!(caller.has("plugins:read"));
        assert!(!caller.full_admin());
        assert!(!caller.has("actions:execute") && !caller.has("jobs:execute"));
        match invocation.request {
            handlers::Request::Api {
                method, headers, ..
            } => {
                assert_eq!(method, "GET");
                assert!(!headers.contains_key("authorization") && !headers.contains_key("cookie"));
                Ok(handlers::Reply::Api{status:200,body:json!({"typed":true,"user":caller.username(),"console":context::extension_console_session()}).to_string()})
            }
            handlers::Request::Mcp { .. } => Ok(handlers::Reply::Json {
                value: json!({"content":[{"type":"text","text":"typed-scoped-result"}],"isError":false}),
            }),
            _ => Err("Unexpected request".into()),
        }
    }
}
struct Legacy(Arc<Counts>);
#[async_trait]
impl NativeRouteHandler for Legacy {
    async fn admit(&self, _: &NativeRoute) -> Result<NativeAdmission, NativeRouteResponse> {
        self.0.legacy_admit.fetch_add(1, Ordering::SeqCst);
        Ok(NativeAdmission::default())
    }
    async fn handle(
        &self,
        _: &NativeRoute,
        _: NativeRouteRequest,
    ) -> Result<NativeRouteResponse, String> {
        self.0.legacy_api.fetch_add(1, Ordering::SeqCst);
        Ok(NativeRouteResponse {
            status: 200,
            headers: vec![],
            body: b"legacy-main".to_vec(),
        })
    }
}
#[async_trait]
impl NativeMcp for Legacy {
    fn tools(&self) -> Vec<Tool> {
        Tool::from_value(&core(json!({"name":"legacy_tool","description":"Main-trusted legacy","read_only":true,"input_schema":{"type":"object"}})),Source::Native).into_iter().collect()
    }
    fn resources(&self) -> Vec<meerschaum_server::mcp::registry::Resource> {
        Vec::new()
    }
    fn prompts(&self) -> Vec<meerschaum_server::mcp::registry::Prompt> {
        Vec::new()
    }
    async fn read_resource(
        &self,
        _: &str,
        _: &str,
        _: &Map,
        _: &Caller,
    ) -> Result<meerschaum_server::mcp::NativeResource, String> {
        Err("No legacy resource".into())
    }
    async fn get_prompt(
        &self,
        _: &str,
        _: &Map,
        _: &Caller,
    ) -> Result<(Option<String>, Vec<Value>), String> {
        Err("No legacy prompt".into())
    }
    async fn call_tool(&self, _: &str, _: &Map, _: &Caller) -> Result<Value, String> {
        self.0.legacy_mcp.fetch_add(1, Ordering::SeqCst);
        Ok(Value::Str("legacy-main".into()))
    }
}
struct Cookies {
    bearer: String,
    active: AtomicBool,
}
#[async_trait]
impl BrowserAuth for Cookies {
    async fn authorize(
        &self,
        _: &AppState,
        headers: &HeaderMap,
    ) -> Option<Result<(HeaderMap, String), StatusCode>> {
        let cookie = headers.get("cookie")?.to_str().ok()?;
        if cookie != "mrsm-session-id=owned-cookie" || !self.active.load(Ordering::SeqCst) {
            return Some(Err(StatusCode::UNAUTHORIZED));
        }
        let mut headers = HeaderMap::new();
        headers.insert("authorization", self.bearer.parse().unwrap());
        Some(Ok((headers, "owned-cookie".into())))
    }
}
struct Fixture {
    state: AppState,
    instance: Arc<common::MemoryInstance>,
    app: axum::Router,
    typed: Arc<Typed>,
    counts: Arc<Counts>,
}
async fn fixture(root: &std::path::Path, no_auth: bool) -> Fixture {
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    for (name, primary) in [("alice", EVERYONE), ("host_user", "HostScope")] {
        instance.seed_user(name, "pass", None);
        instance.seed_user_attributes(
            name,
            map(json!({"role":"custom","scopes":["plugins:read"],"primary_team":primary})),
        );
    }
    let mut team = Team::new("HostScope");
    team.attributes = map(json!({"isolated":true,"plugins":["probe"],"execution":{"mode":"host"}}));
    team.members.insert("host_user".into(), TeamRole::Member);
    let registered = instance.register_team(&mut team).await;
    assert!(registered.success, "{registered}");
    let counts = Arc::new(Counts::default());
    let gate = Arc::new(Gate::new());
    let typed = Arc::new(Typed {
        counts: counts.clone(),
        catalogs: BTreeMap::from([
            (EVERYONE.into(), catalog(None)),
            (
                "HostScope".into(),
                catalog(Some(Uuid::parse_str(team.id.as_deref().unwrap()).unwrap())),
            ),
        ]),
        gate,
        invocation_gate: Arc::new(Gate::new()),
        missing: AtomicBool::new(false),
    });
    let mut config = common::config(root);
    config.no_auth = no_auth;
    let state = AppState::builder(config, instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(typed.clone())
        .build()
        .unwrap();
    let legacy = Arc::new(Legacy(counts.clone()));
    let mcp = Arc::new(McpServer::for_state(&state).with_native(Some(legacy.clone())));
    let (native, notices) = meerschaum_server::native::native_router(
        &state,
        vec![NativeRoute {
            plugin: "legacy".into(),
            name: "matched".into(),
            method: "GET".into(),
            path: "/tracked/matched".into(),
            scopes: vec![],
            public: false,
        }],
        legacy,
    );
    assert!(notices.is_empty(), "{notices:?}");
    let app = meerschaum_server::app_with_mcp(state.clone(), mcp).merge(native);
    Fixture {
        state,
        instance,
        app,
        typed,
        counts,
    }
}
async fn rpc(
    app: &axum::Router,
    auth: Option<&str>,
    cookie: Option<&str>,
    method: &str,
    params: Json,
) -> common::Resp {
    let mut req = common::req(
        "POST",
        "/mcp",
        auth,
        Some((
            "application/json",
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
                .unwrap(),
        )),
    );
    if let Some(cookie) = cookie {
        req.headers_mut().insert("cookie", cookie.parse().unwrap());
    }
    common::send(app, req).await
}
#[tokio::test]
async fn tracked_host_isolated_native_match_fallback_head_and_mcp_never_call_main_plugins() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path(), false).await;
    let bearer = common::login(&f.app, "host_user", "pass", Some("plugins:read")).await;
    for path in ["/tracked/matched", "/tracked/fallback"] {
        let response = common::send(&f.app, common::req("GET", path, Some(&bearer), None)).await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert_eq!(response.json()["typed"], true);
        let head = common::send(&f.app, common::req("HEAD", path, Some(&bearer), None)).await;
        assert_eq!(head.status, StatusCode::OK, "{}", head.text());
        assert!(head.body.is_empty());
        let wrong = common::send(&f.app, common::req("POST", path, Some(&bearer), None)).await;
        assert!(
            !wrong.status.is_success(),
            "An undeclared method was normalized into GET"
        );
    }
    let listed = rpc(&f.app, Some(&bearer), None, "tools/list", json!({})).await;
    assert_eq!(listed.status, StatusCode::OK, "{}", listed.text());
    let names: Vec<_> = listed.json()["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.iter().any(|name| name == "scoped_tool"), "{names:?}");
    assert!(!names.iter().any(|name| name == "legacy_tool"), "{names:?}");
    let called = rpc(
        &f.app,
        Some(&bearer),
        None,
        "tools/call",
        json!({"name":"scoped_tool","arguments":{}}),
    )
    .await;
    assert_eq!(called.status, StatusCode::OK, "{}", called.text());
    assert!(
        called.text().contains("typed-scoped-result"),
        "{}",
        called.text()
    );
    assert_eq!(f.counts.legacy_admit.load(Ordering::SeqCst), 0);
    assert_eq!(f.counts.legacy_api.load(Ordering::SeqCst), 0);
    assert_eq!(f.counts.legacy_mcp.load(Ordering::SeqCst), 0);
    assert!(f.counts.typed.load(Ordering::SeqCst) >= 5);
}
#[tokio::test]
async fn non_isolated_cookie_and_custom_scope_callers_keep_host_plugin_routes() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path(), false).await;
    f.instance.seed_user_attributes(
        "alice",
        map(json!({"role":"custom","scopes":["sra"],"primary_team":EVERYONE})),
    );
    let bearer = common::login(&f.app, "alice", "pass", Some("sra")).await;
    let cookies = Arc::new(Cookies {
        bearer: bearer.clone(),
        active: AtomicBool::new(true),
    });
    assert!(f.state.engine_streams.set_browser_auth(cookies.clone()));

    // Non-isolated session-cookie caller with custom-only scope ["sra"] (no `plugins:read`):
    // 1. Host native route succeeds (200) instead of 403 Missing required scope 'plugins:read'.
    let mut native_req = common::req("GET", "/tracked/matched", Some(&bearer), None);
    native_req
        .headers_mut()
        .insert("cookie", "mrsm-session-id=owned-cookie".parse().unwrap());
    let response = common::send(&f.app, native_req).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.text(), "legacy-main");

    // 2. Host plugin page/API fallback path with session cookie only (no Bearer, no `plugins:read`)
    //    is NOT intercepted with 403 by the confined extension adapter (reaches host fallback -> 404
    //    when no Python host is mounted, never 403 Missing required scope 'plugins:read').
    for path in ["/app/dashboard", "/sra/web/status"] {
        let mut req = common::req("GET", path, None, None);
        req.headers_mut()
            .insert("cookie", "mrsm-session-id=owned-cookie".parse().unwrap());
        let resp = common::send(&f.app, req).await;
        assert_ne!(
            resp.status,
            StatusCode::FORBIDDEN,
            "host plugin path {path} must not demand plugins:read: {}",
            resp.text()
        );
        assert_eq!(resp.status, StatusCode::NOT_FOUND, "{}", resp.text());
    }

    // 3. Stale/expired session cookie on a public plugin path does not fail with 401 from
    //    api_fallback before reaching the host plugin handler.
    cookies.active.store(false, Ordering::SeqCst);
    let mut stale_req = common::req("GET", "/app/dashboard", None, None);
    stale_req
        .headers_mut()
        .insert("cookie", "mrsm-session-id=owned-cookie".parse().unwrap());
    let stale_resp = common::send(&f.app, stale_req).await;
    assert_eq!(
        stale_resp.status,
        StatusCode::NOT_FOUND,
        "expired cookie must fall through to host handler: {}",
        stale_resp.text()
    );

    // 4. Policy endpoint classifies non-isolated Everyone callers as unconfined.
    let policy = common::send(
        &f.app,
        common::req("GET", "/plugins/extensions/policy", Some(&bearer), None),
    )
    .await;
    assert_eq!(policy.status, StatusCode::OK, "{}", policy.text());
    assert_eq!(policy.json(), json!({"confined": false}));
    assert_eq!(f.counts.typed.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn verified_cookie_keeps_scopes_and_fake_bearer_or_revoked_metadata_never_falls_back() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path(), false).await;
    let bearer = common::login(&f.app, "host_user", "pass", Some("plugins:read")).await;
    let cookies = Arc::new(Cookies {
        bearer: bearer.clone(),
        active: AtomicBool::new(true),
    });
    assert!(f.state.engine_streams.set_browser_auth(cookies.clone()));
    for path in ["/tracked/matched", "/tracked/fallback"] {
        let mut req = common::req("GET", path, None, None);
        req.headers_mut()
            .insert("cookie", "mrsm-session-id=owned-cookie".parse().unwrap());
        let response = common::send(&f.app, req).await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert_eq!(response.json()["console"], true);
        let mut req = common::req(
            "GET",
            path,
            Some("Bearer invalid-explicit-credential"),
            None,
        );
        req.headers_mut()
            .insert("cookie", "mrsm-session-id=owned-cookie".parse().unwrap());
        let response = common::send(&f.app, req).await;
        assert_eq!(
            response.status,
            StatusCode::UNAUTHORIZED,
            "{}",
            response.text()
        );
    }
    let mcp = rpc(
        &f.app,
        None,
        Some("mrsm-session-id=owned-cookie"),
        "tools/call",
        json!({"name":"scoped_tool","arguments":{}}),
    )
    .await;
    assert_eq!(mcp.status, StatusCode::OK, "{}", mcp.text());
    assert!(mcp.text().contains("typed-scoped-result"), "{}", mcp.text());
    let bad = rpc(
        &f.app,
        Some("Bearer invalid-explicit-credential"),
        Some("mrsm-session-id=owned-cookie"),
        "tools/list",
        json!({}),
    )
    .await;
    assert_eq!(bad.status, StatusCode::UNAUTHORIZED, "{}", bad.text());
    f.typed.invocation_gate.armed.store(true, Ordering::SeqCst);
    let app = f.app.clone();
    let mut pending = common::req("GET", "/tracked/matched", None, None);
    pending
        .headers_mut()
        .insert("cookie", "mrsm-session-id=owned-cookie".parse().unwrap());
    let waiting = tokio::spawn(async move { common::send(&app, pending).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        f.typed.invocation_gate.entered.notified(),
    )
    .await
    .unwrap();
    cookies.active.store(false, Ordering::SeqCst);
    f.typed.invocation_gate.release.notify_one();
    let withheld = tokio::time::timeout(std::time::Duration::from_secs(3), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        withheld.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        withheld.text()
    );
    assert!(
        withheld.json().get("typed").is_none(),
        "{}",
        withheld.text()
    );
    let mut req = common::req("GET", "/tracked/matched", None, None);
    req.headers_mut()
        .insert("cookie", "mrsm-session-id=owned-cookie".parse().unwrap());
    assert_eq!(
        common::send(&f.app, req).await.status,
        StatusCode::UNAUTHORIZED
    );
    f.typed.missing.store(true, Ordering::SeqCst);
    let missing = common::send(
        &f.app,
        common::req("GET", "/tracked/matched", Some(&bearer), None),
    )
    .await;
    assert_eq!(
        missing.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        missing.text()
    );
    assert_eq!(f.counts.legacy_admit.load(Ordering::SeqCst), 0);
    assert_eq!(f.counts.legacy_api.load(Ordering::SeqCst), 0);
    assert_eq!(f.counts.legacy_mcp.load(Ordering::SeqCst), 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_scope_loss_during_catalog_or_handler_wait_never_releases_stale_output() {
    for during_invocation in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let f = fixture(root.path(), false).await;
        let bearer = common::login(&f.app, "host_user", "pass", Some("plugins:read")).await;
        let gate = if during_invocation {
            f.typed.invocation_gate.clone()
        } else {
            f.typed.gate.clone()
        };
        gate.armed.store(true, Ordering::SeqCst);
        let app = f.app.clone();
        let request = common::req("GET", "/tracked/matched", Some(&bearer), None);
        let task = tokio::spawn(async move { common::send(&app, request).await });
        tokio::time::timeout(std::time::Duration::from_secs(3), gate.entered.notified())
            .await
            .unwrap();
        f.instance.seed_user_attributes(
            "host_user",
            map(json!({"role":"custom","scopes":["logs:read"],"primary_team":"HostScope"})),
        );
        gate.release.notify_one();
        let response = tokio::time::timeout(std::time::Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                response.status,
                StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED
            ),
            "{}",
            response.text()
        );
        assert_eq!(
            f.counts.typed.load(Ordering::SeqCst),
            usize::from(during_invocation)
        );
        assert_eq!(f.counts.legacy_admit.load(Ordering::SeqCst), 0);
        assert_eq!(f.counts.legacy_api.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn missing_original_plugin_scope_does_not_discover_or_fall_back_to_main() {
    let root = tempfile::tempdir().unwrap();
    let f = fixture(root.path(), false).await;
    f.instance.seed_user_attributes(
        "host_user",
        map(json!({"role":"custom","scopes":["logs:read"],"primary_team":"HostScope"})),
    );
    let bearer = common::login(&f.app, "host_user", "pass", Some("logs:read")).await;
    for path in ["/tracked/matched", "/tracked/fallback"] {
        let response = common::send(&f.app, common::req("GET", path, Some(&bearer), None)).await;
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{}",
            response.text()
        );
    }
    let listed = rpc(&f.app, Some(&bearer), None, "tools/list", json!({})).await;
    assert_eq!(listed.status, StatusCode::OK, "{}", listed.text());
    assert!(!listed.text().contains("scoped_tool"), "{}", listed.text());
    assert!(!listed.text().contains("legacy_tool"), "{}", listed.text());
    assert_eq!(f.counts.typed.load(Ordering::SeqCst), 0);
    assert_eq!(f.counts.legacy_admit.load(Ordering::SeqCst), 0);
    assert_eq!(f.counts.legacy_api.load(Ordering::SeqCst), 0);
    assert_eq!(f.counts.legacy_mcp.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn full_administrator_and_no_auth_keep_deliberate_trusted_legacy_handlers() {
    for no_auth in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let f = fixture(root.path(), no_auth).await;
        let bearer = if no_auth {
            None
        } else {
            Some(common::login(&f.app, "admin", "pass", None).await)
        };
        let response = common::send(
            &f.app,
            common::req("GET", "/tracked/matched", bearer.as_deref(), None),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert_eq!(response.text(), "legacy-main");
        let listed = rpc(&f.app, bearer.as_deref(), None, "tools/list", json!({})).await;
        assert_eq!(listed.status, StatusCode::OK, "{}", listed.text());
        assert!(listed.text().contains("legacy_tool"), "{}", listed.text());
        assert!(!listed.text().contains("scoped_tool"));
        let called = rpc(
            &f.app,
            bearer.as_deref(),
            None,
            "tools/call",
            json!({"name":"legacy_tool","arguments":{}}),
        )
        .await;
        assert_eq!(called.status, StatusCode::OK, "{}", called.text());
        assert!(called.text().contains("legacy-main"), "{}", called.text());
        assert_eq!(f.counts.legacy_mcp.load(Ordering::SeqCst), 1);
        assert_eq!(f.counts.typed.load(Ordering::SeqCst), 0);
        assert_eq!(f.counts.legacy_admit.load(Ordering::SeqCst), 1);
        assert_eq!(f.counts.legacy_api.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_page_policy_needs_no_plugin_scope_or_discovery_and_preserves_trusted_legacy() {
    for no_auth in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let f = fixture(root.path(), no_auth).await;
        f.typed.missing.store(true, Ordering::SeqCst);
        if no_auth {
            let response = common::send(
                &f.app,
                common::req("GET", "/plugins/extensions/policy", None, None),
            )
            .await;
            assert_eq!(response.status, StatusCode::OK, "{}", response.text());
            assert_eq!(response.json(), json!({"confined":false}));
        } else {
            for (user, primary, expected_confined) in
                [("alice", EVERYONE, false), ("host_user", "HostScope", true)]
            {
                // Isolated team members remain confined when plugin rights are absent;
                // non-isolated Everyone callers keep host plugin routing.
                f.instance.seed_user_attributes(
                    user,
                    map(json!({
                        "role":"custom","scopes":["logs:read"],"primary_team":primary
                    })),
                );
                let bearer = common::login(&f.app, user, "pass", Some("logs:read")).await;
                let response = common::send(
                    &f.app,
                    common::req("GET", "/plugins/extensions/policy", Some(&bearer), None),
                )
                .await;
                assert_eq!(response.status, StatusCode::OK, "{}", response.text());
                assert_eq!(response.json(), json!({"confined": expected_confined}));
                assert_eq!(response.headers.get("cache-control").unwrap(), "no-store");
            }
            let administrator = common::login(&f.app, "admin", "pass", None).await;
            let response = common::send(
                &f.app,
                common::req(
                    "GET",
                    "/plugins/extensions/policy",
                    Some(&administrator),
                    None,
                ),
            )
            .await;
            assert_eq!(response.status, StatusCode::OK, "{}", response.text());
            assert_eq!(response.json(), json!({"confined":false}));
            let invalid = common::send(
                &f.app,
                common::req(
                    "GET",
                    "/plugins/extensions/policy",
                    Some("Bearer invalid"),
                    None,
                ),
            )
            .await;
            assert_eq!(
                invalid.status,
                StatusCode::UNAUTHORIZED,
                "{}",
                invalid.text()
            );
        }
        assert_eq!(f.counts.metadata.load(Ordering::SeqCst), 0);
        assert_eq!(f.counts.typed.load(Ordering::SeqCst), 0);
        assert_eq!(f.counts.legacy_admit.load(Ordering::SeqCst), 0);
        assert_eq!(f.counts.legacy_api.load(Ordering::SeqCst), 0);
        assert_eq!(f.counts.legacy_mcp.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_scopes_do_not_publish_source_revoked_during_catalog_or_invocation() {
    for mcp in [false, true] {
        for during_invocation in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let f = fixture(root.path(), false).await;
            let bearer = common::login(&f.app, "host_user", "pass", Some("plugins:read")).await;
            let mut original_headers = HeaderMap::new();
            original_headers.insert("authorization", bearer.parse().unwrap());
            let gate = if during_invocation {
                f.typed.invocation_gate.clone()
            } else {
                f.typed.gate.clone()
            };
            gate.armed.store(true, Ordering::SeqCst);
            let app = f.app.clone();
            let task = tokio::spawn(async move {
                if mcp {
                    rpc(
                        &app,
                        Some(&bearer),
                        None,
                        "tools/call",
                        json!({"name":"scoped_tool","arguments":{}}),
                    )
                    .await
                } else {
                    common::send(
                        &app,
                        common::req("GET", "/tracked/matched", Some(&bearer), None),
                    )
                    .await
                }
            });
            tokio::time::timeout(std::time::Duration::from_secs(3), gate.entered.notified())
                .await
                .unwrap();
            let mut team = f.instance.get_team("HostScope").await.unwrap().unwrap();
            team.attributes
                .insert("plugins".into(), Value::List(vec![]));
            assert!(f.instance.edit_team(&team).await.success);
            let current = meerschaum_server::access::require_fresh(
                &f.state,
                &original_headers,
                &["plugins:read"],
            )
            .await
            .unwrap();
            assert!(
                current.has("plugins:read"),
                "Only source grants were revoked"
            );
            gate.release.notify_one();
            let response = tokio::time::timeout(std::time::Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap();
            // MCP handler failures are JSON-RPC error results; HTTP/API refusals keep403.
            assert!(
                response.status == StatusCode::FORBIDDEN
                    || (mcp
                        && response.status == StatusCode::OK
                        && (response.json().get("error").is_some()
                            || response.json()["result"]["isError"] == true)),
                "{}",
                response.text()
            );
            assert!(
                !response.text().contains("typed-scoped-result"),
                "{}",
                response.text()
            );
            assert!(
                response.json().get("typed").is_none(),
                "{}",
                response.text()
            );
            assert_eq!(
                f.counts.typed.load(Ordering::SeqCst),
                usize::from(during_invocation)
            );
            assert_eq!(f.counts.legacy_admit.load(Ordering::SeqCst), 0);
            assert_eq!(f.counts.legacy_api.load(Ordering::SeqCst), 0);
            assert_eq!(f.counts.legacy_mcp.load(Ordering::SeqCst), 0);
        }
    }
}
