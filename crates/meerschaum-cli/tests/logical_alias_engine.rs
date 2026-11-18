//! Real Main SQLite, configured self-API services, logical engine/data writes and lock waits.
//! No SDK, plugin interpreter, external service or Docker daemon is used by this fixture.

use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::{Connector, DataQuery, InstanceConnector, SyncOptions};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_sql::{SqlConnector, driver::SqlDriver, uri::ConnectionSpec};
use meerschaum_core::{
    Map, Pipe, Value,
    access::{Team, TeamRole},
    users::User,
};
use meerschaum_server::{AppState, ServerConfig, backends::InstanceResolver};
use serde_json::{Value as Json, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn map(value: Json) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}
fn rights(scopes: &[&str], team: Option<&str>) -> Map {
    meerschaum_core::access::attributes_with_permissions(&map(json!({"primary_team":team})), scopes)
}
fn pipe(metric: &str) -> Pipe {
    Pipe::builder("manual:posted", metric)
        .instance("sql:main")
        .build()
        .unwrap()
}
fn reference(alias: &str, metric: &str) -> String {
    format!("api:{alias}|manual:posted|{metric}|None")
}

struct Resolver(Vec<Arc<APIConnector>>);
impl InstanceResolver for Resolver {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        self.0
            .iter()
            .find(|connector| connector.connector_keys() == keys)
            .map(|connector| connector.clone() as Arc<dyn InstanceConnector>)
    }
}

struct Served {
    _root: tempfile::TempDir,
    main: Arc<SqlConnector>,
    spec: ConnectionSpec,
    state: AppState,
    origin: String,
    token: String,
    client: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Served {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Served {
    /// Hyper 1.12's EXPECT channel waits for the application to poll Body before sending
    /// 100 Continue. body_as finishes engine_access (including service password proof)
    /// before body_json polls Body, giving this fixture an actual handler barrier.
    async fn state_after_initial_authorization(
        &self,
        hash: &str,
    ) -> (tokio::net::TcpStream, Vec<u8>) {
        let body = serde_json::to_vec(
            &json!([{"pipe_ref":reference("writer","owned"),"definition_hash":hash}]),
        )
        .unwrap();
        let address = self.origin.strip_prefix("http://").unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let head = format!(
            "PUT /engine/state?instance_keys=api%3Awriter HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
            self.token,
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        let interim = tokio::time::timeout(Duration::from_secs(15), async {
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                assert!(
                    head.len() < 8192,
                    "owned interim HTTP headers exceeded the bound"
                );
                head.push(stream.read_u8().await.unwrap());
            }
            head
        })
        .await
        .expect("the actual engine handler did not reach its authorized body read");
        assert!(
            interim.starts_with(b"HTTP/1.1 100 Continue\r\n"),
            "expected the authorized raw-body barrier, got {}",
            String::from_utf8_lossy(&interim)
        );
        (stream, body)
    }

    async fn new() -> Self {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_test_writer()
            .try_init();
        let root = tempfile::tempdir().unwrap();
        let uri = format!("sqlite:///{}", root.path().join("main.db").display());
        let spec = ConnectionSpec::from_uri(&uri, Some("main"), None).unwrap();
        let main = Arc::new(SqlConnector::connect("main", spec.clone()).await.unwrap());
        for (name, scopes, team) in [
            ("alice", vec!["pipes:read", "pipes:write"], Some("Data")),
            ("svc_writer", vec!["pipes:read", "pipes:write"], None),
            ("svc_reader", vec!["pipes:read"], None),
        ] {
            let mut user = User::new(name, Some("synthetic-pass"));
            user.attributes = Some(rights(&scopes, team));
            let result = main.register_user(&mut user).await;
            assert!(result.success, "{}", result.message);
        }
        let mut team = Team::new("Data");
        team.add_member("alice", TeamRole::Member);
        team.attributes = map(
            json!({"isolated":true,"instance":"api:writer","connectors":["api:writer","api:reader","api:external"]}),
        );
        assert!(main.register_team(&mut team).await.success);
        for (metric, owner) in [("owned", "svc_writer"), ("read_only", "svc_reader")] {
            let mut pipe = pipe(metric);
            pipe.set_parameters(map(json!({"columns":{"id":"id"},"access":{"owner":owner,"team":"Data","visibility":"team"}})));
            assert!(main.register_pipe(&pipe).await.success);
            let frame = meerschaum_kernel::docs::frame_from_docs(&[[(
                "id".into(),
                meerschaum_kernel::value::Value::Int(1),
            )]
            .into_iter()
            .collect()])
            .unwrap();
            let result = main.sync_pipe(&pipe, frame, &SyncOptions::default()).await;
            assert!(result.success, "{}", result.message);
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let origin = format!("http://127.0.0.1:{port}");
        let attributes = |username: &str, port: u16| {
            map(
                json!({"host":"127.0.0.1","port":port,"instance_keys":"sql:main","username":username,"password":"synthetic-pass","teams":["Data"]}),
            )
        };
        let writer = attributes("svc_writer", port);
        let reader = attributes("svc_reader", port);
        let external = attributes("svc_writer", port.saturating_add(1));
        let mut cfg = ServerConfig::for_root("sql:main", root.path());
        cfg.no_dash = true;
        cfg.no_webterm = true;
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec![
            "sql:main".into(),
            "api:writer".into(),
            "api:reader".into(),
            "api:external".into(),
        ];
        cfg.execution_callback_url = Some(origin.clone());
        cfg.connectors = map(
            json!({"sql":{"main":{}},"api":{"writer":writer,"reader":reader,"external":external}}),
        );
        let resolver = Resolver(vec![
            Arc::new(APIConnector::new(Some("writer"), writer).unwrap()),
            Arc::new(APIConnector::new(Some("reader"), reader).unwrap()),
            Arc::new(APIConnector::new(Some("external"), external).unwrap()),
        ]);
        let state = AppState::builder(cfg, main.clone())
            .secret(b"synthetic-logical-engine-fixture-key-0123456789".to_vec())
            .resolver(Arc::new(resolver))
            .engine(Arc::new(
                meerschaum_server::engine_store::InstanceEngine::new(main.clone(), "sql:main"),
            ))
            .native_self_api_engines()
            .build()
            .unwrap();
        let reader = main
            .get_user_attributes(&User::new("svc_reader", None))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            meerschaum_core::access::user_permissions(
                None,
                Some(&reader),
                meerschaum_core::access::Role::Viewer
            ),
            ["pipes:read".to_string()].into_iter().collect()
        );
        let principal = meerschaum_server::auth::load_from_subject(&state, "svc_reader")
            .await
            .unwrap()
            .unwrap();
        let effective = meerschaum_server::access::access_of_fresh(
            &state,
            principal,
            &meerschaum_server::auth::Scopes::List(vec!["*".into()]),
        )
        .await
        .unwrap();
        assert!(effective.has("pipes:read") && !effective.has("pipes:write"));
        let app = meerschaum_server::app(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();
        let login: Json = client
            .post(format!("{origin}/login"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body("username=alice&password=synthetic-pass")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let token = login["access_token"].as_str().unwrap().to_string();
        Self {
            _root: root,
            main,
            spec,
            state,
            origin,
            token,
            client,
            task,
        }
    }
    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        alias: &str,
        body: Option<Json>,
    ) -> (u16, Json) {
        let mut request = self
            .client
            .request(
                method,
                format!(
                    "{}{path}{}instance_keys=api%3A{alias}",
                    self.origin,
                    if path.contains('?') { "&" } else { "?" }
                ),
            )
            .bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let body = response.text().await.unwrap();
        (
            status,
            serde_json::from_str(&body).unwrap_or(Json::String(body)),
        )
    }
    async fn state(&self, alias: &str, metric: &str, hash: &str) -> (u16, Json) {
        self.request(
            reqwest::Method::PUT,
            "/engine/state",
            alias,
            Some(json!([{"pipe_ref":reference(alias,metric),"definition_hash":hash}])),
        )
        .await
    }
    async fn user_attributes(&self, username: &str) -> Map {
        self.main
            .get_user_attributes(&User::new(username, None))
            .await
            .unwrap()
            .unwrap()
    }
}

async fn bounded_http_response(stream: tokio::net::TcpStream) -> (u16, String) {
    let mut bytes = Vec::new();
    stream
        .take(64 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await
        .unwrap();
    assert!(
        bytes.len() <= 64 * 1024,
        "owned response exceeded its byte bound"
    );
    let head_end = bytes
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .expect("missing owned response headers");
    let head = std::str::from_utf8(&bytes[..head_end]).unwrap();
    let status = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let body = &bytes[head_end + 4..];
    let body = if head
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        let mut decoded = Vec::new();
        let mut remaining = body;
        loop {
            let end = remaining
                .windows(2)
                .position(|part| part == b"\r\n")
                .expect("invalid owned chunk size");
            let length = usize::from_str_radix(
                std::str::from_utf8(&remaining[..end])
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap(),
                16,
            )
            .unwrap();
            remaining = &remaining[end + 2..];
            if length == 0 {
                break;
            }
            assert!(
                length <= remaining.len().saturating_sub(2),
                "invalid owned chunk payload"
            );
            decoded.extend_from_slice(&remaining[..length]);
            assert_eq!(&remaining[length..length + 2], b"\r\n");
            remaining = &remaining[length + 2..];
        }
        decoded
    } else {
        body.to_vec()
    };
    (status, String::from_utf8(body).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_self_api_keeps_reader_denial_and_writes_owned_engine_and_real_data_without_main_grant()
 {
    let f = Served::new().await;
    let response = f
        .client
        .get(format!(
            "{}/engine/state?instance_keys=sql%3Amain",
            f.origin
        ))
        .bearer_auth(&f.token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 403);
    let (status, result) = f.state("reader", "read_only", "refused").await;
    assert_eq!(status, 403, "{result}");
    let (status, result) = f
        .request(
            reqwest::Method::GET,
            "/pipes/manual:posted/read_only/None/data",
            "reader",
            None,
        )
        .await;
    assert_eq!(status, 200, "{result}");
    let (status, result) = f.state("writer", "owned", "first").await;
    assert_eq!(status, 200, "{result}");
    let req = json!({"pipe_ref":reference("writer","owned"),"holder":"one","ttl_ms":10_000});
    let (status, result) = f
        .request(
            reqwest::Method::POST,
            "/engine/leases",
            "writer",
            Some(req.clone()),
        )
        .await;
    assert_eq!(status, 200, "{result}");
    let mut competing = req.clone();
    competing["holder"] = json!("two");
    assert_eq!(
        f.request(
            reqwest::Method::POST,
            "/engine/leases",
            "writer",
            Some(competing.clone())
        )
        .await
        .0,
        409
    );
    assert_eq!(
        f.request(
            reqwest::Method::DELETE,
            "/engine/leases",
            "writer",
            Some(req)
        )
        .await
        .0,
        200
    );
    assert_eq!(
        f.request(
            reqwest::Method::POST,
            "/engine/leases",
            "writer",
            Some(competing)
        )
        .await
        .0,
        200
    );
    let (status, result) = f
        .request(
            reqwest::Method::POST,
            "/pipes/manual:posted/owned/None/data",
            "writer",
            Some(json!([{"id":2}])),
        )
        .await;
    assert_eq!(status, 200, "{result}");
    assert_eq!(result[0], true, "{result}");
    assert_eq!(
        f.main
            .get_pipe_rowcount(&pipe("owned"), &DataQuery::default())
            .await
            .unwrap(),
        Some(2)
    );
    let (status, result) = f
        .request(
            reqwest::Method::POST,
            "/pipes/manual:posted/read_only/None/data",
            "reader",
            Some(json!([{"id":2}])),
        )
        .await;
    assert_eq!(status, 403, "{result}");
    assert_eq!(
        f.main
            .get_pipe_rowcount(&pipe("read_only"), &DataQuery::default())
            .await
            .unwrap(),
        Some(1)
    );
    let (status, result) = f.request(reqwest::Method::PUT,"/engine/state","writer",Some(json!([{"pipe_ref":"sql:main|manual:posted|owned|None","definition_hash":"forged"}]))).await;
    assert!(status >= 400, "{result}");
    let (status, result) = f.state("external", "owned", "external-refused").await;
    assert_eq!(status, 501, "{result}");
    let reader = f.user_attributes("svc_reader").await;
    assert_eq!(
        meerschaum_core::access::user_permissions(
            None,
            Some(&reader),
            meerschaum_core::access::Role::Viewer
        ),
        ["pipes:read".to_string()].into_iter().collect()
    );
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "authorization",
        format!("Bearer {}", f.token).parse().unwrap(),
    );
    let original = meerschaum_server::access::require_fresh(&f.state, &headers, &["pipes:write"])
        .await
        .unwrap();
    assert!(!meerschaum_server::access::guard::may_use_connector(
        &f.state, &original, "sql:main"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logical_native_writer_rechecks_both_registered_actors_after_real_sqlite_lock_and_rolls_back()
 {
    for actor in ["alice", "svc_writer"] {
        let f = Served::new().await;
        let (status, result) = f.state("writer", "owned", "before").await;
        assert_eq!(status, 200, "{result}");
        let before = f.user_attributes(actor).await;
        let reduced = rights(&["pipes:read"], (actor == "alice").then_some("Data"));
        let (mut stream, body) = f.state_after_initial_authorization("must-not-commit").await;
        let holder = SqlDriver::connect(&f.spec).await.unwrap();
        let mut held = holder.begin_write().await.unwrap();
        stream.write_all(&body).await.unwrap();
        let mut pending = tokio::spawn(bounded_http_response(stream));
        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut pending)
                .await
                .is_err(),
            "owned native writer did not wait"
        );
        let text = Value::Map(reduced).to_json_string().replace('\'', "''");
        held.exec(&format!(
            "UPDATE {} SET attributes='{text}' WHERE username='{actor}'",
            f.main.instance_table_name("mrsm_users")
        ))
        .await
        .unwrap();
        held.commit().await.unwrap();
        let (status, body) = tokio::time::timeout(Duration::from_secs(15), pending)
            .await
            .unwrap()
            .unwrap();
        assert!(
            status >= 400
                || serde_json::from_str::<Json>(&body)
                    .unwrap_or(Json::Null)
                    .get("success")
                    == Some(&json!(false)),
            "{actor}: {status} {body}"
        );
        assert!(
            body.contains("pipes:write"),
            "{actor}: a native authorization refusal must identify the removed scope: {status} {body}"
        );
        let engine_pipe = f
            .main
            .engine_state_pipe("api:writer", "engine_state")
            .unwrap();
        let docs = f
            .main
            .get_pipe_docs(&engine_pipe, &DataQuery::default())
            .await
            .unwrap();
        assert!(docs.iter().all(|doc| doc.get("definition_hash")
            != Some(&meerschaum_kernel::value::Value::Str(
                "must-not-commit".into()
            ))));
        let mut restored = User::new(actor, None);
        restored.attributes = Some(before);
        assert!(f.main.edit_user(&restored).await.success);
        let login: Json = f
            .client
            .post(format!("{}/login", f.origin))
            .header("content-type", "application/x-www-form-urlencoded")
            .body("username=alice&password=synthetic-pass")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let response = f
            .client
            .put(format!(
                "{}/engine/state?instance_keys=api%3Awriter",
                f.origin
            ))
            .bearer_auth(login["access_token"].as_str().unwrap())
            .json(&json!([{"pipe_ref":reference("writer","owned"),"definition_hash":"restored"}]))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            200,
            "{actor}: {}",
            response.text().await.unwrap()
        );
        holder.close().await;
    }
}
