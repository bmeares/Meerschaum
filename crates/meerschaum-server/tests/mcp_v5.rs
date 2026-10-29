//! The MCP tools v5 adds (Q12): saved Pipelines (`list_pipelines`, `plan_pipeline`,
//! `sync_pipeline`), runs (`list_runs`, `get_run`), jobs (`list_jobs`, `get_job_logs`) and
//! connectors (`list_connectors`); `sync_pipe` with `parents` / `children`; the pipe tools'
//! refusal of the internal `mrsm` keys (B2); scopes and route groups like the HTTP routes.

mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::fakes::{FakePlans, RecordingJobs};
use common::{MemoryInstance, SECRET, config};
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_api::models::RunRecord;
use meerschaum_core::users::User;
use meerschaum_core::{Pipe, SuccessTuple, Value};
use meerschaum_server::auth::{Principal, Scopes};
use meerschaum_server::backends::{EngineBackend, MemoryEngine};
use meerschaum_server::mcp::registry::tools;
use meerschaum_server::mcp::{Caller, McpBackend, McpServer, SqlReadError, v5};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};

/// A graph sync: `(metric key, parents, children)`.
type GraphSync = (String, Option<Value>, Option<Value>);

/// Records the graph syncs it is asked for.
#[derive(Default)]
struct GraphBackend {
    graph: Mutex<Vec<GraphSync>>,
}

#[async_trait]
impl McpBackend for GraphBackend {
    async fn sync_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::ok("synced alone")
    }
    async fn sync_pipe_graph(
        &self,
        pipe: &Pipe,
        _: Option<String>,
        _: Option<String>,
        parents: Option<Value>,
        children: Option<Value>,
    ) -> SuccessTuple {
        self.graph
            .lock()
            .unwrap()
            .push((pipe.metric_key().to_string(), parents, children));
        SuccessTuple::ok("synced with the graph")
    }
    async fn verify_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn read_sql(&self, _: &str, _: &str, _: usize) -> Result<Vec<Value>, SqlReadError> {
        Err(SqlReadError::Unsupported)
    }
}

struct Fixture {
    state: AppState,
    server: McpServer,
    backend: Arc<GraphBackend>,
    jobs: Arc<RecordingJobs>,
    engine: Arc<MemoryEngine>,
    instance: Arc<MemoryInstance>,
    _root: tempfile::TempDir,
}

fn fixture(cfg: impl FnOnce(&mut ServerConfig)) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let mut c = config(root.path());
    c.connectors = serde_json::from_value::<Value>(json!({
        "sql": {"main": {"uri": "postgresql://u:secret@h/db"}, "default": {}},
        "valkey": {"cache": {}},
    }))
    .unwrap()
    .to_map();
    cfg(&mut c);
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("ron", "pass", None);
    let jobs = Arc::new(RecordingJobs::default());
    let engine = Arc::new(MemoryEngine::default());
    let state = AppState::builder(c, instance.clone())
        .secret(SECRET.to_vec())
        .jobs(jobs.clone())
        .engine(engine.clone())
        .pipelines(Arc::new(FakePlans))
        .build()
        .unwrap();
    let backend = Arc::new(GraphBackend::default());
    let server = McpServer::for_state(&state).with_backend(backend.clone());
    Fixture {
        state,
        server,
        backend,
        jobs,
        engine,
        instance,
        _root: root,
    }
}

fn admin() -> Caller {
    Caller::api(
        Scopes::List(vec!["*".into()]),
        Some(Principal::User(User::new("admin", None))),
    )
}

fn user(scopes: &[&str]) -> Caller {
    Caller::api(
        Scopes::List(scopes.iter().map(|s| s.to_string()).collect()),
        Some(Principal::User(User::new("ron", None))),
    )
}

async fn rpc(f: &Fixture, caller: &Caller, method: &str, params: Json) -> Json {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let reply = f
        .server
        .handle_payload(&f.state, serde_json::from_value(message).unwrap(), caller)
        .await
        .expect("a reply");
    serde_json::to_value(reply).unwrap()
}

/// Call a tool: `Ok(structured result)` or `Err(error text)`.
async fn call(f: &Fixture, caller: &Caller, name: &str, arguments: Json) -> Result<Json, String> {
    let reply = rpc(
        f,
        caller,
        "tools/call",
        json!({"name": name, "arguments": arguments}),
    )
    .await;
    let result = &reply["result"];
    let text = result["content"][0]["text"]
        .as_str()
        .unwrap_or("")
        .to_string();
    if result["isError"] == json!(true) {
        return Err(text);
    }
    Ok(serde_json::from_str(&text).unwrap_or(Json::String(text)))
}

async fn listed(f: &Fixture, caller: &Caller) -> Vec<String> {
    rpc(f, caller, "tools/list", json!({})).await["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect()
}

async fn register_pipeline(f: &Fixture, name: &str, targets: Json) {
    let mut pipe = Pipe::builder("mrsm", "pipeline")
        .location(Some(name))
        .instance("sql:fixture")
        .build()
        .unwrap();
    pipe.set_parameters(
        serde_json::from_value::<Value>(json!({"pipeline": {
            "name": name, "id": format!("id-{name}"), "revision": 1, "targets": targets,
            "description": "every morning",
        }}))
        .unwrap()
        .to_map(),
    );
    assert!(f.instance.register_pipe(&pipe).await.success);
}

#[tokio::test]
async fn new_tools_are_listed_by_scope_and_route_group() {
    let f = fixture(|_| {});
    let all = listed(&f, &admin()).await;
    for name in v5::NEW_TOOLS {
        assert!(all.contains(&name.to_string()), "{name}: {all:?}");
    }
    let reader = listed(&f, &user(&["pipes:read"])).await;
    for name in ["list_pipelines", "plan_pipeline", "list_runs", "get_run"] {
        assert!(reader.contains(&name.to_string()), "{name}");
    }
    for name in [
        "sync_pipeline",
        "list_jobs",
        "get_job_logs",
        "list_connectors",
    ] {
        assert!(!reader.contains(&name.to_string()), "{name}");
    }
    let operator = listed(&f, &user(&["pipes:write", "jobs:execute"])).await;
    assert!(operator.contains(&"sync_pipeline".to_string()));
    // A route group the server does not serve hides (and refuses) its tools over HTTP.
    let f = fixture(|c| c.routes_disabled = vec!["pipelines".into(), "engine".into()]);
    let all = listed(&f, &admin()).await;
    for name in [
        "list_pipelines",
        "plan_pipeline",
        "sync_pipeline",
        "list_runs",
        "get_run",
    ] {
        assert!(!all.contains(&name.to_string()), "{name}");
    }
    assert!(all.contains(&"list_jobs".to_string()));
    let refused = call(&f, &admin(), "list_pipelines", json!({})).await;
    assert!(refused.unwrap_err().contains("not available"));
    // Over stdio (local trust) they are all there.
    let stdio = listed(&f, &Caller::stdio()).await;
    assert!(stdio.contains(&"list_pipelines".to_string()));
    // The served registry: v4's tools with typed bounds, then v5's.
    let sync = tools().iter().find(|t| t.name == "sync_pipe").unwrap();
    let props = serde_json::to_value(&sync.input_schema).unwrap()["properties"].clone();
    assert_eq!(props["begin"]["type"], json!(["string", "integer"]));
    assert!(props["parents"].is_object() && props["children"].is_object());
}

#[tokio::test]
async fn pipeline_tools_list_plan_and_sync() {
    let f = fixture(|_| {});
    register_pipeline(&f, "daily", json!(["sql:fixture|sql:b|raw|None"])).await;
    let selection = json!({"pipeline_definitions": [{"name": "daily"}]}).to_string();
    let record: RunRecord = serde_json::from_value(json!({
        "run_id": "r1", "started": "2026-01-01T00:00:00Z", "finished": "2026-01-01T00:01:00Z",
        "status": "success", "selection": selection,
    }))
    .unwrap();
    assert!(f.engine.record_runs(vec![record]).await.success);

    let listing = call(&f, &admin(), "list_pipelines", json!({}))
        .await
        .unwrap();
    assert_eq!(listing["count"], 1);
    let daily = &listing["pipelines"][0];
    assert_eq!(daily["name"], "daily");
    assert_eq!(daily["description"], "every morning");
    assert_eq!(daily["members"], json!(["sql:fixture|sql:b|raw|None"]));
    assert_eq!(daily["last_run"]["run_id"], "r1");

    let plan = call(
        &f,
        &user(&["pipes:read"]),
        "plan_pipeline",
        json!({"name": "daily", "begin": 5}),
    )
    .await
    .unwrap();
    assert_eq!(plan["plan"]["nodes"][0]["action"], "sync");
    assert!(
        plan["text"].as_str().unwrap().contains("Some(\"5\")"),
        "{plan}"
    );
    let missing = call(&f, &admin(), "plan_pipeline", json!({"name": "nope"})).await;
    assert!(missing.unwrap_err().contains("not registered"));

    let started = call(&f, &admin(), "sync_pipeline", json!({"name": "daily"}))
        .await
        .unwrap();
    assert_eq!(started["success"], true, "{started}");
    let run_id = started["run_id"].as_str().unwrap();
    assert!(run_id.contains("-api-"), "{run_id}");
    assert!(started["job"].as_str().unwrap().starts_with("execute-"));
    assert!(
        started["stream"]
            .as_str()
            .unwrap()
            .starts_with("/engine/runs/")
    );
    let created = f.jobs.created.lock().unwrap().clone();
    assert_eq!(created.len(), 1);
    assert_eq!(
        &created[0].1[..5],
        ["sync", "pipelines", "daily", "-i", "sql:fixture"]
    );
    // A caller without `jobs:execute` cannot.
    let refused = call(
        &f,
        &user(&["pipes:write"]),
        "sync_pipeline",
        json!({"name": "daily"}),
    )
    .await;
    assert!(refused.unwrap_err().contains("Missing required scope"));
}

#[tokio::test]
async fn run_tools_filter_and_show_one_run() {
    let f = fixture(|_| {});
    let succeeded = Pipe::builder("sql:x", "succeeded")
        .instance("sql:fixture")
        .build()
        .unwrap();
    let failed = Pipe::builder("sql:x", "m")
        .instance("sql:fixture")
        .build()
        .unwrap();
    // These synthetic completed runs carry snapshots captured before their work.
    let mut identities = Vec::new();
    for pipe in [&succeeded, &failed] {
        assert!(f.instance.register_pipe(pipe).await.success);
        let attributes = f.instance.get_pipe_attributes(pipe).await.unwrap();
        identities.push(
            meerschaum_connector::sync_status::registration_from_attributes(&attributes).unwrap(),
        );
    }
    let records: Vec<RunRecord> = serde_json::from_value(json!([
        {"run_id": "a", "started": "2026-01-01T00:00:00Z", "finished": "2026-01-01T00:01:00Z",
         "status": "success", "num_nodes": 1, "targets": [succeeded.pipe_ref()]},
        {"run_id": "a", "pipe_ref": succeeded.pipe_ref(), "started": "2026-01-01T00:00:00Z",
         "status": "success", "registration_id": identities[0]},
        {"run_id": "b", "started": "2026-01-02T00:00:00Z", "finished": "2026-01-02T00:01:00Z",
         "status": "failed", "num_nodes": 1, "targets": [failed.pipe_ref()]},
        {"run_id": "b", "pipe_ref": "sql:fixture|sql:x|m|None", "started": "2026-01-02T00:00:00Z",
         "status": "failed", "message": "boom", "registration_id": identities[1]},
    ]))
    .unwrap();
    assert!(f.engine.record_runs(records).await.success);
    let reader = user(&["pipes:read"]);
    let runs = call(&f, &reader, "list_runs", json!({})).await.unwrap();
    assert_eq!(runs["count"], 2);
    assert_eq!(runs["runs"][0]["run_id"], "b");
    let failed = call(&f, &reader, "list_runs", json!({"status": "failed"}))
        .await
        .unwrap();
    assert_eq!(failed["count"], 1);
    let by_pipe = call(
        &f,
        &reader,
        "list_runs",
        json!({"pipe": "sql:fixture|sql:x|m|None"}),
    )
    .await
    .unwrap();
    assert_eq!(by_pipe["runs"][0]["run_id"], "b");
    let limited = call(&f, &reader, "list_runs", json!({"limit": 1}))
        .await
        .unwrap();
    assert_eq!(limited["count"], 1);
    let bad = call(&f, &reader, "list_runs", json!({"status": "maybe"})).await;
    assert!(bad.is_err());
    let one = call(&f, &reader, "get_run", json!({"run_id": "b"}))
        .await
        .unwrap();
    assert_eq!(one["run"]["status"], "failed");
    assert_eq!(one["nodes"][0]["message"], "boom");
    let missing = call(&f, &reader, "get_run", json!({"run_id": "zzz"})).await;
    assert!(missing.unwrap_err().contains("not found"));
}

#[tokio::test]
async fn job_tools_follow_the_jobs_routes_visibility() {
    let f = fixture(|_| {});
    // A temporary execution of `admin`, which `ron` may not see.
    let started = call(&f, &admin(), "sync_pipeline", json!({"name": "daily"})).await;
    assert!(started.is_err(), "no Pipeline yet");
    register_pipeline(&f, "daily", json!([])).await;
    let started = call(&f, &admin(), "sync_pipeline", json!({"name": "daily"}))
        .await
        .unwrap();
    let job = started["job"].as_str().unwrap().to_string();
    let jobs = call(&f, &admin(), "list_jobs", json!({})).await.unwrap();
    assert_eq!(jobs["count"], 1);
    assert_eq!(jobs["jobs"][0]["name"], job.as_str());
    assert_eq!(jobs["jobs"][0]["result"]["success"], true);
    let ron = user(&["jobs:read", "logs:read"]);
    let theirs = call(&f, &ron, "list_jobs", json!({})).await.unwrap();
    assert_eq!(theirs["count"], 0);
    let hidden = call(&f, &ron, "get_job_logs", json!({"name": job})).await;
    assert!(hidden.unwrap_err().contains("does not exist"));
    let logs = call(
        &f,
        &admin(),
        "get_job_logs",
        json!({"name": job, "lines": 2}),
    )
    .await
    .unwrap();
    assert_eq!(logs["logs"], "two\nthree");
    assert_eq!(logs["truncated"], true);
    let stopped = call(&f, &admin(), "list_jobs", json!({"status": "stopped"}))
        .await
        .unwrap();
    assert_eq!(stopped["count"], 0);
}

#[tokio::test]
async fn list_connectors_names_keys_without_credentials() {
    let f = fixture(|_| {});
    let out = call(
        &f,
        &user(&["connectors:read"]),
        "list_connectors",
        json!({}),
    )
    .await
    .unwrap();
    let keys: Vec<&str> = out["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["keys"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["sql:main", "valkey:cache"]);
    assert!(!out.to_string().contains("secret"));
    let sql = call(&f, &admin(), "list_connectors", json!({"type": "sql"}))
        .await
        .unwrap();
    assert_eq!(sql["count"], 1);
}

#[tokio::test]
async fn sync_pipe_takes_parents_and_children() {
    let f = fixture(|_| {});
    let pipe = Pipe::builder("sql:x", "child")
        .instance("sql:fixture")
        .build()
        .unwrap();
    assert!(f.instance.register_pipe(&pipe).await.success);
    let alone = call(
        &f,
        &admin(),
        "sync_pipe",
        json!({"connector_keys": "sql:x", "metric_key": "child"}),
    )
    .await
    .unwrap();
    assert_eq!(alone["message"], "synced alone");
    let graph = call(
        &f,
        &admin(),
        "sync_pipe",
        json!({"connector_keys": "sql:x", "metric_key": "child", "parents": true, "children": 2}),
    )
    .await
    .unwrap();
    assert_eq!(graph["message"], "synced with the graph");
    assert_eq!(
        f.backend.graph.lock().unwrap().clone(),
        vec![(
            "child".to_string(),
            Some(Value::Bool(true)),
            Some(Value::Int(2))
        )]
    );
    let bad = call(
        &f,
        &admin(),
        "sync_pipe",
        json!({"connector_keys": "sql:x", "metric_key": "child", "parents": -1}),
    )
    .await;
    assert!(bad.unwrap_err().contains("Invalid arguments"));
}

#[tokio::test]
async fn pipe_tools_refuse_the_internal_mrsm_keys() {
    let f = fixture(|_| {});
    register_pipeline(&f, "daily", json!([])).await;
    let keys = json!({"connector_keys": "mrsm", "metric_key": "pipeline", "location_key": "daily"});
    for tool in [
        "get_pipe_attributes",
        "get_pipe_data",
        "delete_pipe",
        "drop_pipe",
        "clear_pipe",
    ] {
        let refused = call(&f, &admin(), tool, keys.clone()).await;
        let text = refused.unwrap_err();
        assert!(text.contains("`mrsm`"), "{tool}: {text}");
    }
    let mut edit = keys.clone();
    edit["parameters"] = json!({"pipeline": {}});
    assert!(call(&f, &admin(), "edit_pipe", edit).await.is_err());
    let register = call(
        &f,
        &admin(),
        "register_pipe",
        json!({"connector_keys": "mrsm", "metric_key": "x"}),
    )
    .await;
    assert!(register.unwrap_err().contains("`mrsm`"));
    // Even asked for explicitly, the listing has none of them.
    let listing = call(
        &f,
        &admin(),
        "list_pipes",
        json!({"connector_keys": ["mrsm"]}),
    )
    .await
    .unwrap();
    assert_eq!(listing["total"], 0, "{listing}");
    // The registration itself is intact.
    let pipe = Pipe::builder("mrsm", "pipeline")
        .location(Some("daily"))
        .instance("sql:fixture")
        .build()
        .unwrap();
    assert!(f.instance.get_pipe_id(&pipe).await.unwrap().is_some());
}

#[tokio::test]
async fn read_sql_refuses_internal_tables_however_they_are_spelled() {
    let f = fixture(|_| {});
    let reader = user(&["sql:read"]);
    for query in [
        "SELECT * FROM \"mrsm_users\"",
        "SELECT * FROM \"MRSM_TOKENS\"",
        "SELECT * FROM `mrsm_users`",
        "SELECT * FROM [mrsm_users]",
        "SELECT * FROM \"mrsm_engine_runs\"",
        "SELECT query_to_xml('select * from mrsm_users', true, true, '')",
        "SELECT query_to_xml('select * from ' || 'mrsm' || '_users', true, true, '')",
        "SELECT * FROM U&\"\\006Drsm_users\"",
        // A comment between the function and its arguments; a function that runs its text.
        "SELECT query_to_xml/**/(E'select * from mrsm\\x5fusers', true, true, '')",
        "SELECT * FROM ts_stat(E'select to_tsvector(password_hash) from mrsm\\x5fusers')",
    ] {
        let refused = call(
            &f,
            &reader,
            "read_sql",
            json!({"connector_keys": "sql:fixture", "query": query}),
        )
        .await;
        let text = refused.unwrap_err();
        assert!(text.contains("Refusing"), "{query}: {text}");
    }
    // v4's own refusal keeps its message; other queries reach the connector.
    let refused = call(
        &f,
        &reader,
        "read_sql",
        json!({"connector_keys": "sql:fixture", "query": "SELECT * FROM mrsm_users"}),
    )
    .await
    .unwrap_err();
    assert!(
        refused.ends_with("Refusing to query protected table 'mrsm_users'."),
        "{refused}"
    );
    let read = call(
        &f,
        &reader,
        "read_sql",
        json!({"connector_keys": "sql:fixture", "query": "SELECT 'users' AS name, query FROM t"}),
    )
    .await;
    assert!(
        read.unwrap_err().contains("does not support SQL reads"),
        "the guard refused a query that names no internal table"
    );
}

#[tokio::test]
async fn the_primer_and_reference_mention_pipelines_runs_and_jobs() {
    let f = fixture(|_| {});
    let init = rpc(&f, &admin(), "initialize", json!({})).await;
    let instructions = init["result"]["instructions"].as_str().unwrap();
    for word in ["Pipelines", "list_runs", "get_job_logs", "sync_pipeline"] {
        assert!(instructions.contains(word), "{word}");
    }
    let docs = rpc(
        &f,
        &admin(),
        "resources/read",
        json!({"uri": "mrsm://docs"}),
    )
    .await;
    let text = docs["result"]["contents"][0]["text"].as_str().unwrap();
    assert!(text.contains("| `list_pipelines` |"), "{text}");
    assert!(text.contains("**Pipelines**"));
}
