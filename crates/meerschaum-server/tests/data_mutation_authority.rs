//! Data posts authorize implicit registration and recheck authority after preparation waits.
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::http::StatusCode;
use indexmap::IndexMap;
use meerschaum_connector::{
    Connector, ConnectorError, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId,
    PipesKeys, PipesKeysQuery, SyncOptions,
};
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_server::backends::InstanceResolver;
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};
use tokio::sync::Notify;

fn map(value: Json) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

fn pipe(connector: &str, metric: &str) -> Pipe {
    Pipe::builder(connector, metric)
        .instance("sql:data")
        .build()
        .unwrap()
}

struct PausedData {
    inner: common::MemoryInstance,
    pause: AtomicBool,
    entered: Notify,
    release: Notify,
    writes: AtomicUsize,
    metadata_writes: AtomicUsize,
}

impl PausedData {
    fn new() -> Self {
        Self {
            inner: common::MemoryInstance::new("data"),
            pause: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            writes: AtomicUsize::new(0),
            metadata_writes: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl Connector for PausedData {
    fn connector_type(&self) -> &str {
        "sql"
    }
    fn label(&self) -> &str {
        "data"
    }
    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }
}

#[async_trait]
impl InstanceConnector for PausedData {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.inner.register_pipe(pipe).await
    }
    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        self.inner.get_pipe_attributes(pipe).await
    }
    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        self.inner.get_pipe_id(pipe).await
    }
    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        self.inner.fetch_pipes_keys(query).await
    }
    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        if self.pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            tokio::time::timeout(Duration::from_secs(10), self.release.notified())
                .await
                .map_err(|_| ConnectorError::Backend("Test preparation wait expired.".into()))?;
        }
        self.inner.pipe_exists(pipe).await
    }
    async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        if pipe.connector_keys() != "mrsm" {
            self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.edit_pipe(pipe, patch).await
    }
    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.inner.drop_pipe(pipe).await
    }
    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        self.inner.get_pipe_data(pipe, query).await
    }
    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        self.inner.get_sync_time(pipe, params, newest).await
    }
    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        self.inner.get_pipe_columns_types(pipe).await
    }
    async fn sync_pipe(&self, pipe: &Pipe, data: Frame, opts: &SyncOptions) -> SuccessTuple {
        if pipe.connector_keys() != "mrsm" {
            self.writes.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.sync_pipe(pipe, data, opts).await
    }
    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        self.inner.get_pipe_rowcount(pipe, query).await
    }
}

struct Instances(Arc<PausedData>);
impl InstanceResolver for Instances {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        (keys == "sql:data").then(|| self.0.clone() as Arc<dyn InstanceConnector>)
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    app: Router,
    auth: Arc<common::MemoryInstance>,
    data: Arc<PausedData>,
}

async fn fixture(tweak: impl FnOnce(&mut ServerConfig)) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let auth = Arc::new(common::MemoryInstance::new("fixture"));
    auth.seed_user("admin", "pass", Some("admin"));
    auth.seed_user("alice", "pass", None);
    auth.seed_user_attributes(
        "alice",
        map(json!({
            "scopes":["pipes:read", "pipes:write", "pipes:register"],
            "role":"custom", "primary_team":"Data"
        })),
    );
    let mut team = Team::new("Data");
    team.members.insert("alice".into(), TeamRole::Member);
    assert!(auth.register_team(&mut team).await.success);
    let data = Arc::new(PausedData::new());
    let mut config = common::config(root.path());
    config.allow_multiple_instances = true;
    config.allowed_instance_keys = vec!["sql:*".into()];
    config.connectors = map(json!({"sql": {
        "fixture": {"flavor":"sqlite"},
        "data": {"flavor":"sqlite"},
        "reserved": {"flavor":"sqlite", "teams":["Other"]}
    }}));
    tweak(&mut config);
    let state = AppState::builder(config, auth.clone())
        .resolver(Arc::new(Instances(data.clone())))
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    Fixture {
        _root: root,
        app: meerschaum_server::app(state),
        auth,
        data,
    }
}

async fn post(app: &Router, auth: Option<&str>, connector: &str, metric: &str) -> common::Resp {
    common::send(
        app,
        common::req(
            "POST",
            &format!("/pipes/{connector}/{metric}/None/data?instance_keys=sql:data"),
            auth,
            Some((
                "application/json",
                br#"[{"id":1,"value":{"nested":1}}]"#.to_vec(),
            )),
        ),
    )
    .await
}

#[tokio::test]
async fn data_post_requires_registration_scope_policy_and_source_before_creating_anything() {
    let f = fixture(|_| {}).await;
    f.auth.seed_user_attributes(
        "alice",
        map(json!({
            "scopes":["pipes:read", "pipes:write"], "role":"custom", "primary_team":"Data"
        })),
    );
    let auth = common::login(&f.app, "alice", "pass", None).await;
    let response = post(&f.app, Some(&auth), "manual:posted", "new").await;
    assert_eq!(
        response.status,
        StatusCode::FORBIDDEN,
        "{}",
        response.text()
    );
    assert!(
        response.text().contains("pipes:register"),
        "{}",
        response.text()
    );
    assert!(
        f.data
            .inner
            .get_pipe_id(&pipe("manual:posted", "new"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.data.writes.load(Ordering::SeqCst), 0);

    let f = fixture(|c| c.registration_pipes = false).await;
    let auth = common::login(&f.app, "admin", "pass", None).await;
    let response = post(&f.app, Some(&auth), "manual:posted", "disabled").await;
    assert_eq!(
        response.status,
        StatusCode::FORBIDDEN,
        "{}",
        response.text()
    );
    assert!(
        f.data
            .inner
            .get_pipe_id(&pipe("manual:posted", "disabled"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.data.writes.load(Ordering::SeqCst), 0);

    let f = fixture(|_| {}).await;
    let auth = common::login(&f.app, "alice", "pass", None).await;
    let response = post(&f.app, Some(&auth), "sql:reserved", "new").await;
    assert_eq!(
        response.status,
        StatusCode::FORBIDDEN,
        "{}",
        response.text()
    );
    assert!(
        f.data
            .inner
            .get_pipe_id(&pipe("sql:reserved", "new"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.data.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn authorized_data_registration_stamps_owner_and_team_and_existing_writes_still_work() {
    let f = fixture(|_| {}).await;
    let auth = common::login(&f.app, "alice", "pass", None).await;
    for _ in 0..2 {
        let response = post(&f.app, Some(&auth), "manual:posted", "owned").await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert_eq!(response.json()[0], true, "{}", response.text());
    }
    let attributes = f
        .data
        .inner
        .get_pipe_attributes(&pipe("manual:posted", "owned"))
        .await
        .unwrap();
    let parameters = serde_json::to_value(&attributes).unwrap();
    assert_eq!(parameters["parameters"]["access"]["owner"], "alice");
    assert_eq!(parameters["parameters"]["access"]["team"], "Data");
    assert_eq!(f.data.writes.load(Ordering::SeqCst), 2);

    let f = fixture(|c| c.no_auth = true).await;
    let response = post(&f.app, None, "manual:posted", "host").await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.json()[0], true, "{}", response.text());
    let attributes = f
        .data
        .inner
        .get_pipe_attributes(&pipe("manual:posted", "host"))
        .await
        .unwrap();
    assert!(attributes["parameters"].get("access").is_none());
}

#[tokio::test]
async fn prepared_sync_rechecks_original_permissions_canonical_acl_and_locks_before_writes() {
    for change in ["permissions", "acl", "locks"] {
        let f = fixture(|_| {}).await;
        let mut p = pipe("manual:posted", "pending");
        p.set_parameters(map(json!({"columns":{"id":"id"}, "access":{
            "owner":"alice", "team":"Data", "visibility":"private"
        }})));
        assert!(f.data.inner.register_pipe(&p).await.success);
        let auth = common::login(&f.app, "alice", "pass", None).await;
        f.data.pause.store(true, Ordering::SeqCst);
        let app = f.app.clone();
        let pending =
            tokio::spawn(async move { post(&app, Some(&auth), "manual:posted", "pending").await });
        tokio::time::timeout(Duration::from_secs(10), f.data.entered.notified())
            .await
            .expect("data preparation must reach the held metadata read");
        match change {
            "permissions" => f.auth.seed_user_attributes(
                "alice",
                map(json!({
                    "scopes":["pipes:read"], "role":"custom", "primary_team":"Data"
                })),
            ),
            "acl" => {
                p.set_parameters(map(json!({"columns":{"id":"id"}, "access":{
                    "owner":"other", "team":"Other", "visibility":"private"
                }})));
                assert!(f.data.inner.edit_pipe(&p, false).await.success);
            }
            "locks" => {
                p.update_parameters(map(json!({"locks":["sync"]})));
                assert!(f.data.inner.edit_pipe(&p, false).await.success);
            }
            _ => unreachable!(),
        }
        f.data.release.notify_one();
        let response = tokio::time::timeout(Duration::from_secs(10), pending)
            .await
            .unwrap()
            .unwrap();
        let expected = match change {
            "acl" => StatusCode::NOT_FOUND,
            "locks" => StatusCode::CONFLICT,
            _ => StatusCode::FORBIDDEN,
        };
        assert_eq!(response.status, expected, "{change}: {}", response.text());
        assert_eq!(
            f.data.writes.load(Ordering::SeqCst),
            0,
            "{change}: no data mutation"
        );
        assert_eq!(
            f.data.metadata_writes.load(Ordering::SeqCst),
            0,
            "{change}: no dtype metadata mutation"
        );
    }
}

struct SqlData(Arc<meerschaum_connector_sql::SqlConnector>);
impl InstanceResolver for SqlData {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        (keys == "sql:data").then(|| self.0.clone() as Arc<dyn InstanceConnector>)
    }
}

#[tokio::test]
async fn real_sqlite_data_writer_reconstructs_original_caller_after_database_lock_wait() {
    use meerschaum_connector_sql::SqlConnector;
    use meerschaum_connector_sql::driver::SqlDriver;
    use meerschaum_connector_sql::uri::ConnectionSpec;

    let root = tempfile::tempdir().unwrap();
    let auth = Arc::new(common::MemoryInstance::new("fixture"));
    auth.seed_user("alice", "pass", None);
    let rights = |permissions: &[&str]| {
        meerschaum_core::access::attributes_with_permissions(
            &map(json!({"primary_team":"Data"})),
            permissions,
        )
    };
    auth.seed_user_attributes(
        "alice",
        rights(&["pipes:read", "pipes:write", "pipes:register"]),
    );
    let mut team = Team::new("Data");
    team.members.insert("alice".into(), TeamRole::Member);
    assert!(auth.register_team(&mut team).await.success);
    let mut spec = ConnectionSpec::from_uri(
        &format!("sqlite:///{}", root.path().join("owned-data.db").display()),
        Some("data"),
        None,
    )
    .unwrap();
    spec.pool_size = 1;
    spec.max_overflow = 0;
    let data = Arc::new(SqlConnector::connect("data", spec.clone()).await.unwrap());
    let mut p = pipe("manual:posted", "locked");
    p.set_parameters(map(json!({"columns":{"id":"id"}, "access":{
        "owner":"alice", "team":"Data", "visibility":"private"
    }})));
    assert!(data.register_pipe(&p).await.success);
    let seed = meerschaum_kernel::docs::frame_from_docs(&[[(
        "id".into(),
        meerschaum_kernel::value::Value::Int(0),
    )]
    .into_iter()
    .collect()])
    .unwrap();
    assert!(
        data.sync_pipe(&p, seed, &SyncOptions::default())
            .await
            .success
    );
    let mut config = common::config(root.path());
    config.allow_multiple_instances = true;
    config.allowed_instance_keys = vec!["sql:*".into()];
    config.connectors = map(json!({"sql":{"data":{"flavor":"sqlite"}}}));
    let state = AppState::builder(config, auth.clone())
        .resolver(Arc::new(SqlData(data.clone())))
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let token = common::login(&app, "alice", "pass", None).await;
    let request = |token: &str, id: i64| {
        common::req(
            "POST",
            "/pipes/manual:posted/locked/None/data?instance_keys=sql:data",
            Some(token),
            Some((
                "application/json",
                json!([{"id":id}]).to_string().into_bytes(),
            )),
        )
    };
    // Warm the route and native table state before holding an independent writer lock.
    let response = common::send(&app, request(&token, 1)).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.json()[0], true, "{}", response.text());
    let before = data
        .get_pipe_rowcount(&p, &DataQuery::default())
        .await
        .unwrap();
    let holder = SqlDriver::connect(&spec).await.unwrap();
    let held = holder.begin_write().await.unwrap();
    let pending_app = app.clone();
    let pending_request = request(&token, 2);
    let mut pending =
        tokio::spawn(async move { common::send(&pending_app, pending_request).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    auth.seed_user_attributes("alice", rights(&["pipes:read"]));
    held.rollback().await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.status.is_client_error() || response.json()[0] == false,
        "{}",
        response.text()
    );
    assert!(
        response.text().contains("pipes:write"),
        "{}",
        response.text()
    );
    assert_eq!(
        data.get_pipe_rowcount(&p, &DataQuery::default())
            .await
            .unwrap(),
        before
    );
    // Restoring permission allows a fresh request and proves both pools remain reusable.
    auth.seed_user_attributes(
        "alice",
        rights(&["pipes:read", "pipes:write", "pipes:register"]),
    );
    // The fixture edits the backing store directly, bypassing the permission route's
    // cache invalidation. A fresh login reconstructs the restored user before reuse.
    let fresh_token = common::login(&app, "alice", "pass", None).await;
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        common::send(&app, request(&fresh_token, 2)),
    )
    .await
    .unwrap();
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.json()[0], true, "{}", response.text());
    holder.close().await;
    data.driver().close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_sqlite_metadata_and_destructive_writes_recheck_after_database_lock_wait() {
    use meerschaum_connector_sql::SqlConnector;
    use meerschaum_connector_sql::driver::SqlDriver;
    use meerschaum_connector_sql::uri::ConnectionSpec;

    for operation in ["register", "edit", "delete", "drop", "clear"] {
        let root = tempfile::tempdir().unwrap();
        let auth = Arc::new(common::MemoryInstance::new("fixture"));
        auth.seed_user("alice", "pass", None);
        let rights = |permissions: &[&str]| {
            meerschaum_core::access::attributes_with_permissions(
                &map(json!({"primary_team":"Data"})),
                permissions,
            )
        };
        let allowed = [
            "pipes:read",
            "pipes:write",
            "pipes:register",
            "pipes:delete",
            "pipes:drop",
        ];
        auth.seed_user_attributes("alice", rights(&allowed));
        let mut team = Team::new("Data");
        team.members.insert("alice".into(), TeamRole::Member);
        assert!(auth.register_team(&mut team).await.success);
        let mut spec = ConnectionSpec::from_uri(
            &format!(
                "sqlite:///{}",
                root.path().join("owned-metadata.db").display()
            ),
            Some("data"),
            None,
        )
        .unwrap();
        spec.pool_size = 1;
        spec.max_overflow = 0;
        let data = Arc::new(SqlConnector::connect("data", spec.clone()).await.unwrap());
        // Prepare the instance's own metadata before taking the independent write lock.
        let mut seeded = pipe("manual:posted", "seeded");
        seeded.set_parameters(map(json!({"columns":{"id":"id"},"access":{
            "owner":"alice","team":"Data","visibility":"private"
        }})));
        assert!(data.register_pipe(&seeded).await.success);
        let seed = meerschaum_kernel::docs::frame_from_docs(&[[(
            "id".into(),
            meerschaum_kernel::value::Value::Int(1),
        )]
        .into_iter()
        .collect()])
        .unwrap();
        assert!(
            data.sync_pipe(&seeded, seed, &SyncOptions::default())
                .await
                .success
        );
        let target = if operation == "register" {
            "new"
        } else {
            "seeded"
        };
        let p = pipe("manual:posted", target);
        let before = data.get_pipe_attributes(&p).await.unwrap();
        let mut config = common::config(root.path());
        config.allow_multiple_instances = true;
        config.allowed_instance_keys = vec!["sql:*".into()];
        config.connectors = map(json!({"sql":{"data":{"flavor":"sqlite"}}}));
        let state = AppState::builder(config, auth.clone())
            .resolver(Arc::new(SqlData(data.clone())))
            .secret(common::SECRET.to_vec())
            .build()
            .unwrap();
        let app = meerschaum_server::app(state);
        let token = common::login(&app, "alice", "pass", None).await;
        let request = |token: &str| {
            let method = match operation {
                "register" => "POST",
                "edit" => "PATCH",
                _ => "DELETE",
            };
            let body = match operation {
                "register" => Some(json!({"columns":{"id":"id"}})),
                "edit" => Some(json!({"tags":["allowed-after-regrant"]})),
                _ => None,
            }
            .map(|body| ("application/json", body.to_string().into_bytes()));
            common::req(
                method,
                &format!(
                    "/pipes/manual:posted/{target}/None/{operation}?instance_keys=sql:data&patch=true"
                ),
                Some(token),
                body,
            )
        };
        let holder = SqlDriver::connect(&spec).await.unwrap();
        let held = holder.begin_write().await.unwrap();
        let pending_app = app.clone();
        let pending_request = request(&token);
        let mut pending =
            tokio::spawn(async move { common::send(&pending_app, pending_request).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err(),
            "{operation} did not await the owned database lock"
        );
        auth.seed_user_attributes("alice", rights(&["pipes:read"]));
        held.rollback().await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(10), pending)
            .await
            .unwrap()
            .unwrap();
        assert!(
            response.status.is_client_error() || response.json()[0] == false,
            "{operation}: {}",
            response.text()
        );
        assert_eq!(
            data.get_pipe_attributes(&p).await.unwrap(),
            before,
            "{operation}"
        );
        assert_eq!(
            data.get_pipe_rowcount(&seeded, &DataQuery::default())
                .await
                .unwrap(),
            Some(1),
            "{operation}"
        );
        auth.seed_user_attributes("alice", rights(&allowed));
        let fresh = common::login(&app, "alice", "pass", None).await;
        let response =
            tokio::time::timeout(Duration::from_secs(10), common::send(&app, request(&fresh)))
                .await
                .unwrap();
        assert_eq!(
            response.status,
            StatusCode::OK,
            "{operation}: {}",
            response.text()
        );
        assert_eq!(response.json()[0], true, "{operation}: {}", response.text());
        holder.close().await;
        data.driver().close().await;
    }
}
